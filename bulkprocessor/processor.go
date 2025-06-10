package bulkprocessor

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// BulkProcessor represents a bulk processor for PostgreSQL
type BulkProcessor struct {
	config            Config
	processId         string // Unique ID for this processor instance
	pgClient          *PostgreSQLClient
	s3Client          *S3Client
	fileManager       *FileManager
	structType        reflect.Type
	fields            []FieldInfo
	importerWg        sync.WaitGroup
	mutex             sync.RWMutex
	ctx               context.Context
	cancel            context.CancelFunc
	isStarted         bool
	flushMutex       sync.RWMutex
	isShutdown        bool
	importErrorChan   chan error
	fileQueue         chan string    // Queue of file IDs to be imported
	batchQueue        chan string    // Queue of batch directories to be imported
	pendingBatchFiles map[string]int // Map of batch directory to count of pending files
	pendingBatchMutex sync.RWMutex

	recordsQueue      chan []string // Channel to write records, support one processor used by multiple goroutines
	recordsNum        int32 // Number of records in the recordsQueue
	// this hashmap used to store the column value, return the values to the upper layer
	// when import failed.
	// init: new processor, initialize the map.
	// write: for every insert record, write the specific column value into the map if 
	// the value does not existed in the map.
	// read: when import failed, scan the map to get all the column values.
	// clear: finish the import, clear the map.
	feedbackKeys      map[string]bool
	feedbackKeysMutex sync.RWMutex
	ImportErrorCallback ImportErrorHandler // Callback function to handle import errors
	feedFieldIndex    int
	lastFlushTime     time.Time
}

// New creates a new BulkProcessor instance
func New(config Config) (*BulkProcessor, error) {
	// Validate config
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// Generate a unique process ID
	processId := uuid.New().String()

	// Create PostgreSQL client first, as we need it to get S3 config
	pgClient, err := NewPostgreSQLClient(config.PostgreSQL)
	if err != nil {
		return nil, err
	}

	// Get S3 config from database
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get S3 config from database
	s3Config, err := pgClient.GetLoadConfigFromDB(ctx, &config)
	if err != nil {
		pgClient.Close()
		return nil, errors.Wrap(err, "failed to get S3 configuration from database")
	}

	// Create S3 client with the appropriate config
	s3Client, err := NewS3Client(*s3Config)
	if err != nil {
		// Make sure to close the PostgreSQL client if S3 client creation fails
		pgClient.Close()
		return nil, err
	}

	// Create file manager
	filePrefix := fmt.Sprintf("relyt_bulk_%s", strings.ReplaceAll(config.PostgreSQL.Table, ".", "_"))
	fileManager, err := NewFileManager(s3Client, filePrefix, config.BatchSize, processId, config.BatchImportSize)
	if err != nil {
		// Make sure to close the clients if file manager creation fails
		pgClient.Close()
		return nil, err
	}

	// Initialize checkpoint for this process
	pgTable := fmt.Sprintf("%s.%s", config.PostgreSQL.Schema, config.PostgreSQL.Table)
	if err := pgClient.InitializeCheckpoint(ctx, processId, pgTable); err != nil {
		pgClient.Close()
		return nil, errors.Wrap(err, "failed to initialize checkpoint")
	}

	ctx, cancel = context.WithCancel(context.Background())

	return &BulkProcessor{
		config:            config,
		processId:         processId,
		pgClient:          pgClient,
		s3Client:          s3Client,
		fileManager:       fileManager,
		ctx:               ctx,
		cancel:            cancel,
		importErrorChan:   make(chan error, 100),   // Buffer for import errors
		fileQueue:         make(chan string, 1000), // Buffer for file queue
		batchQueue:        make(chan string, 1000),  // Buffer for batch queue
		pendingBatchFiles: make(map[string]int),    // Tracks files pending in each batch
		feedbackKeys:      make(map[string]bool),   // Tracks error keys for failed imports
		recordsQueue:      make(chan []string, 10000),
		feedFieldIndex:    -1,
		lastFlushTime:     time.Now(),
	}, nil
}

// GetProcessId returns the unique processor ID
func (p *BulkProcessor) GetProcessId() string {
	return p.processId
}

// Start starts the importer thread
func (p *BulkProcessor) Start() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.isStarted {
		return errors.New("bulk processor already started")
	}

	p.isStarted = true

	p.importerWg.Add(1)
	go p.InsertThread()

	p.importerWg.Add(1)
	go p.ImporterThread()

	p.importerWg.Add(1)
	go p.AutoFlushThread()

	p.importerWg.Add(1)
	go p.GCThread()

	return nil
}

// Shutdown shuts down the processor
func (p *BulkProcessor) Shutdown() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.isShutdown {
		return nil
	}

	p.isShutdown = true
	p.cancel()

	// Wait for importer to finish
	p.importerWg.Wait()

	// Create a list of all files that need to be cleaned up from S3
	allFiles := append(
		p.fileManager.GetFilesByState(FileStateFrozen),
		append(
			p.fileManager.GetFilesByState(FileStateImporting),
			p.fileManager.GetFilesByState(FileStateImported)...,
		)...,
	)

	// Cleanup files from local filesystem
	for _, file := range allFiles {
		if err := file.CleanupFile(); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to cleanup local file: %v\n", err)
		}
	}

	// Cleanup S3 files
	// First gather all S3 keys that need to be deleted
	var s3Keys []string
	var normalFilepaths []string
	for _, file := range allFiles {
		if file.S3Key != "" && file.State != FileStateError {
			s3Keys = append(s3Keys, file.S3Key)
			normalFilepaths = append(normalFilepaths, file.S3Key)
		}
	}

	// Delete files from S3 if there are any
	if len(s3Keys) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := p.s3Client.DeleteObjects(ctx, s3Keys); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to delete S3 objects during shutdown: %v\n", err)
		}
		cancel()
	}

	// Cleanup checkpoint records
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := p.pgClient.DeleteDeltaCheckpointByProcessIdAndFilepaths(ctx, p.processId, normalFilepaths); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to delete checkpoint records during shutdown: %v\n", err)
	}
	cancel()

	// Close clients
	p.pgClient.Close()

	return nil
}

// checkErrorCount checks if the current error can be ignored based on max error limit
// returns true if the error should be ignored, false if the error should be returned
func (p *BulkProcessor) checkErrorCount(err error, errorRecordsCount *int, recordIndex int, errorContext string) bool {
	if p.config.MaxErrorRecords <= 0 {
		return false
	}

	// Check if we've reached the max error limit
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	currentErrorCount, getErr := p.pgClient.GetCheckpointErrorRecords(ctx, p.processId)
	cancel()

	if getErr != nil {
		fmt.Fprintf(os.Stderr, "Failed to get current error count: %v\n", getErr)
		return false
	}

	if currentErrorCount+*errorRecordsCount+1 <= p.config.MaxErrorRecords {
		// This error is within our acceptable limit
		*errorRecordsCount++
		fmt.Fprintf(os.Stderr, "Ignoring error %s record %d: %v (total errors: %d/%d)\n",
			errorContext, recordIndex, err, currentErrorCount+*errorRecordsCount, p.config.MaxErrorRecords)
		return true
	}

	return false
}

// checkImportError checks if there's an error in the importErrorChan without blocking
// returns nil if no error is available
func (p *BulkProcessor) checkImportError() error {
	select {
	case err := <-p.importErrorChan:
		return err
	default:
		return nil
	}
}

// Insert inserts data into the processor
func (p *BulkProcessor) Insert(data interface{}) error {
	p.mutex.RLock()
	if p.isShutdown {
		p.mutex.RUnlock()
		return ErrProcessorClosed
	}
	p.mutex.RUnlock()

	// Start processor if not started
	if !p.isStarted {
		if err := p.Start(); err != nil {
			return err
		}
	}

	// Check if data is a slice
	val := reflect.ValueOf(data)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Slice {
		return ErrInvalidInput
	}

	if val.Len() == 0 {
		return ErrEmptyInput
	}

	// Get the struct type if not already set
	if p.structType == nil {
		elemType := val.Type().Elem()
		if elemType.Kind() == reflect.Ptr {
			elemType = elemType.Elem()
		}

		if elemType.Kind() != reflect.Struct {
			return errors.New("input must be a slice of structs")
		}

		fields, err := GetStructFields(elemType)
		if err != nil {
			return err
		}

		if len(fields) == 0 {
			return errors.New("no valid fields found in struct")
		}

		p.structType = elemType
		p.fields = fields
	}

	// To track error records in this batch
	errorRecordsCount := 0

	// Process each record
	for i := 0; i < val.Len(); i++ {
		elemVal := val.Index(i)
		if elemVal.Kind() == reflect.Ptr {
			if elemVal.IsNil() {
				continue
			}
			elemVal = elemVal.Elem()
		}

		// Get field values
		values, err := GetFieldValues(elemVal.Interface(), p.fields)
		if err != nil {
			if p.checkErrorCount(err, &errorRecordsCount, i, "in") {
				continue // Skip this record and continue
			}

			// Either too many errors or couldn't get error count
			return err
		}

		if p.feedFieldIndex < 0 {
			p.feedFieldIndex = GetColumnIndex(p.fields, p.config.FeedbackColumn)
		}

		p.recordsQueue <- values
		atomic.AddInt32(&p.recordsNum, 1)
	}

	return nil
}

// consume records from the recordsQueue and process them
func (p *BulkProcessor) InsertThread() error {
	defer p.importerWg.Done()

	for {
		select {
		case <-p.ctx.Done():
			log.Println("Insert thread context canceled, exiting...")
			return nil
		case values := <-p.recordsQueue:
			if p.feedFieldIndex >= 0 {
				index := p.feedFieldIndex
				p.feedbackKeysMutex.Lock()
				// Check if feedback column value already exists in the map
				if _, exists := p.feedbackKeys[values[index]]; !exists {
					// Add the feedback column value to the map
					p.feedbackKeys[values[index]] = true
				} else {
					// If it already exists, we can skip adding it again
					log.Printf("Feedback key already exists: %s", values[index])
				}
				p.feedbackKeysMutex.Unlock()
			}

			p.fileManager.fileOperationsMutex.Lock()

			currentFile := p.fileManager.GetCurrentFile()
			if currentFile == nil || currentFile.State != FileStateOpen {
				log.Printf("InsertThread Current file is nil or not open, creating a new file")
				// Create a new file
				columnNames := GetColumnNames(p.fields)
				var err error
				currentFile, err = p.fileManager.CreateFile(columnNames)
				if err != nil {
					feedbackKeysArray := p.getFeedbackValues()
					if p.config.ImportErrorCallback != nil {
						p.config.ImportErrorCallback(p.config.FeedbackColumn, feedbackKeysArray, err, p.config.CallbackResource)
					} else {
						feedbackKeysString := fmt.Sprintf("failed %s is [%s].", p.config.FeedbackColumn, strings.Join(feedbackKeysArray, ","))
						log.Printf("Batch import failed with no callback: %s", feedbackKeysString)
					}
					p.fileManager.fileOperationsMutex.Unlock()
					continue
				}
				p.fileManager.SetCurrentFile(currentFile)
				log.Printf("InsertThread Current file created: %s", currentFile.S3Key)

				// Update checkpoint with new file
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				pgTable := fmt.Sprintf("%s.%s", p.config.PostgreSQL.Schema, p.config.PostgreSQL.Table)

				if err := p.pgClient.InsertDeltaCheckpoint(ctx, p.processId, pgTable, currentFile.S3Key); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to insert delta checkpoint with new file: %v\n", err)
				}
			}
			//log.Printf("InsertThread Current file state: %v, num records: %d", currentFile.State, currentFile.NumRecords)
			// Write record to file
			err := currentFile.WriteRecord(values)
			if err != nil {
				feedbackKeysArray := p.getFeedbackValues()
				if p.config.ImportErrorCallback != nil {
					p.config.ImportErrorCallback(p.config.FeedbackColumn, feedbackKeysArray, err, p.config.CallbackResource)
				} else {
					feedbackKeysString := fmt.Sprintf("failed %s is [%s].", p.config.FeedbackColumn, strings.Join(feedbackKeysArray, ","))
					log.Printf("Batch import failed with no callback: %s", feedbackKeysString)
				}
				p.fileManager.fileOperationsMutex.Unlock()
				continue
			}

			// Check if file is full
			if currentFile.IsFull(p.config.BatchSize) {
				// Flush and close the file
				if err := currentFile.Flush(); err != nil {
					feedbackKeysArray := p.getFeedbackValues()
					if p.config.ImportErrorCallback != nil {
						p.config.ImportErrorCallback(p.config.FeedbackColumn, feedbackKeysArray, err, p.config.CallbackResource)
					} else {
						feedbackKeysString := fmt.Sprintf("failed %s is [%s].", p.config.FeedbackColumn, strings.Join(feedbackKeysArray, ","))
						log.Printf("Batch import failed with no callback: %s", feedbackKeysString)
					}
					p.fileManager.fileOperationsMutex.Unlock()
					continue
				}

				// Close the file to finalize the S3 upload
				if err := currentFile.Close(); err != nil {
					feedbackKeysArray := p.getFeedbackValues()
					if p.config.ImportErrorCallback != nil {
						p.config.ImportErrorCallback(p.config.FeedbackColumn, feedbackKeysArray, err, p.config.CallbackResource)
					} else {
						feedbackKeysString := fmt.Sprintf("failed %s is [%s].", p.config.FeedbackColumn, strings.Join(feedbackKeysArray, ","))
						log.Printf("Batch import failed with no callback: %s", feedbackKeysString)
					}
					p.fileManager.fileOperationsMutex.Unlock()
					continue
				}

				p.fileManager.fileOperationsMutex.Unlock()

				// Set S3 URL
				currentFile.S3URL = p.s3Client.GetS3URL(currentFile.S3Key)

				// Update file state to frozen
				p.fileManager.UpdateFileState(currentFile.ID, FileStateFrozen)
				
				if err := p.AddFileToPendingBatchFiles(currentFile); err != nil {
					feedbackKeysArray := p.getFeedbackValues()
					if p.config.ImportErrorCallback != nil {
						p.config.ImportErrorCallback(p.config.FeedbackColumn, feedbackKeysArray, err, p.config.CallbackResource)
					} else {
						feedbackKeysString := fmt.Sprintf("failed %s is [%s].", p.config.FeedbackColumn, strings.Join(feedbackKeysArray, ","))
						log.Printf("Batch import failed with no callback: %s", feedbackKeysString)
					}
					p.fileManager.fileOperationsMutex.Unlock()
					continue
				}
			} else {
				p.fileManager.fileOperationsMutex.Unlock()
			}
			atomic.AddInt32(&p.recordsNum, -1)
		}
	}
}

// Flush flushes all pending data and waits for import to complete
func (p *BulkProcessor) Flush() error {
	p.mutex.Lock()
	if p.isShutdown {
		p.mutex.Unlock()
		return ErrProcessorClosed
	}
	p.mutex.Unlock()

	p.flushMutex.Lock()
	defer p.flushMutex.Unlock()

	// check recordsNum
	for {
		if atomic.LoadInt32(&p.recordsNum) == 0 {
			break
		}
		log.Printf("Flush wait for recordsNum to be 0, current recordsNum: %d", atomic.LoadInt32(&p.recordsNum))
		time.Sleep(time.Duration(1) * time.Second)
	}

	p.fileManager.fileOperationsMutex.Lock()
	// Flush current file if it has records
	currentFile := p.fileManager.GetCurrentFile()
	if currentFile != nil && currentFile.State == FileStateOpen && currentFile.NumRecords > 0 {
		log.Printf("Flushing current file: %s", currentFile.S3Key)
		if err := currentFile.Flush(); err != nil {
			p.fileManager.fileOperationsMutex.Unlock()
			return err
		}

		// Close the file to finalize the S3 upload
		if err := currentFile.Close(); err != nil {
			p.fileManager.fileOperationsMutex.Unlock()
			return err
		}

		// Set S3 URL
		currentFile.S3URL = p.s3Client.GetS3URL(currentFile.S3Key)

		// Update file state to frozen
		p.fileManager.UpdateFileState(currentFile.ID, FileStateFrozen)

		if err := p.AddFileToPendingBatchFiles(currentFile); err != nil {
			p.fileManager.fileOperationsMutex.Unlock()
			return err
		}
	}
	p.fileManager.fileOperationsMutex.Unlock()

	batchDir := ""
	sendDirectory := false

	// flush all pendingBatchFiles
	p.pendingBatchMutex.Lock()
	for dir, count := range p.pendingBatchFiles {
		if count > 0 {
			sendDirectory = true
			log.Printf("Queuing batch directory for import: %s with %d files", dir, count)
			batchDir = dir
			p.batchQueue <- dir
		}
		delete(p.pendingBatchFiles, dir)
	}
	p.lastFlushTime = time.Now()
	p.pendingBatchMutex.Unlock()

	// if no directory send to import, return ASAP
	if sendDirectory == false {
		log.Printf("flush without any directory")
		return nil
	}

	// Wait for all files to be imported or an error to occur
	for {
		// Check for context cancellation
		select {
		case <-p.ctx.Done():
			// Context canceled, might be a shutdown
			// Only report it as an error if we're not in shutdown
			p.mutex.RLock()
			isShutdown := p.isShutdown
			p.mutex.RUnlock()

			if !isShutdown {
				return errors.New("import process canceled")
			}
			// If in shutdown mode, continue to check files
		default:
			// Not canceled
		}

		// Check if all files are imported under the given directory
		frozenFiles := p.fileManager.GetFilesByStateAndDirectory(FileStateFrozen, batchDir)
		importingFiles := p.fileManager.GetFilesByStateAndDirectory(FileStateImporting, batchDir)
		errorFiles := p.fileManager.GetFilesByStateAndDirectory(FileStateError, batchDir)

		// If we have error files but no error from channel, check if context was canceled
		if len(errorFiles) > 0 {
			// check import error 
			// sleep at least 100ms because there are two updates to checkpoint 
			// between setError and send importErrorChan
			sleepTime := p.config.FlushSleepTime
			if sleepTime <= 1000 {
				sleepTime = 1000 
			}
			time.Sleep(time.Duration(sleepTime) * time.Millisecond)
			if err := p.checkImportError(); err != nil {
				return errors.Wrap(err, "import failed with error: ")
			} else {
				return errors.New("import failed, found error files but no error reported")
			}
		}

		// If no files are pending, we're done
		if len(frozenFiles) == 0 && len(importingFiles) == 0 {
			return nil
		}

		// Short sleep to avoid excessive CPU usage
		time.Sleep(time.Duration(p.config.FlushSleepTime) * time.Millisecond)
	}
}

// importerThread processes files from the queue
func (p *BulkProcessor) ImporterThread() {
	defer p.importerWg.Done()

	for {
		select {
		case <-p.ctx.Done():
			log.Println("Importer thread context canceled, exiting...")
			return

		case batchDir := <-p.batchQueue:
			log.Printf("Geting batch directory for import: %s", batchDir)
			// Process a whole batch directory
			if batchDir == "" {
				continue
			}

			// Get all files in this batch directory
			files := p.fileManager.GetFilesByBatchDirectory(batchDir)
			if len(files) == 0 {
				continue
			}

			feedbackKeysArray := p.getFeedbackValues()

			// Update state of all files to importing
			for _, file := range files {
				// Skip if not in frozen state
				if file.State != FileStateFrozen {
					continue
				}
				// Update state to importing
				p.fileManager.UpdateFileState(file.ID, FileStateImporting)
			}

			// Import the entire batch directory
			isFailed := false
			maxLoopNum := p.config.ImportTimeout / p.config.ImportErrorSleepTime
			for i := 0; i < maxLoopNum; i++ {
				err := p.importBatchDirectory(batchDir)
				if err != nil {
					var filepaths []string
					for _, file := range files {
						filepaths = append(filepaths, file.S3Key)
					}

					// Update checkpoint with all error file
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					if err2 := p.pgClient.UpdateDeltaCheckpointStatus(ctx, p.processId, filepaths, CheckpointStatusFailed, -1, err.Error()); err2 != nil {
						fmt.Fprintf(os.Stderr, "Failed to update checkpoint for error file: %v\n", err2)
					}
					cancel()

					// all the errors without parsing error are not expected, we will retry, parsing error just call 
					// the callback and continue to next batch
					if !strings.Contains(err.Error(), "Bad literal") && !strings.Contains(err.Error(), "Dimensions") {
						time.Sleep(time.Duration(p.config.ImportErrorSleepTime) * time.Second)
						continue
					}

					// Mark all files as error
					for _, file := range files {
						file.SetError(fmt.Sprintf("failed to load batch: %v", err))
					}

					feedbackKeysString := fmt.Sprintf("failed %s is [%s].", p.config.FeedbackColumn, strings.Join(feedbackKeysArray, ","))

					// Send error to import error channel and ensure it's received
					p.importErrorChan <- errors.Wrap(err, feedbackKeysString)

					if p.config.ImportErrorCallback != nil {
						log.Printf("Batch import failed before callback: %s", feedbackKeysString)
						p.config.ImportErrorCallback(p.config.FeedbackColumn, feedbackKeysArray, err, p.config.CallbackResource)
					} else {
						log.Printf("Batch import failed with no callback: %s", feedbackKeysString)
					}

					// continue to next batch, it is more reasonable to continue processing if upper-level
					// does not exit in the callback.
					isFailed = true
					break
				} else {
					isFailed = false
					break
				}
			}

			//超时未成功导入，还需要调用失败的回调。
			if isFailed {
				continue
			}

			// Update all files to imported state
			var filepaths []string
			for _, file := range files {
				// Update state to imported and record import time
				file.ImportedAt = time.Now()
				log.Printf("File imported: %s", file.S3Key)

				p.fileManager.UpdateFileState(file.ID, FileStateImported)

				// Cleanup file
				if err := file.CleanupFile(); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to cleanup file: %v\n", err)
				}
				filepaths = append(filepaths, file.S3Key)
			}

			// Update checkpoint with all imported file
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := p.pgClient.UpdateDeltaCheckpointStatus(ctx, p.processId, filepaths, CheckpointStatusCompleted, 0, ""); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to update checkpoint for imported file: %v\n", err)
			}
			cancel()
		}
	}
}

// importBatchDirectory imports all files in a batch directory with a single operation
func (p *BulkProcessor) importBatchDirectory(batchDir string) error {
	ctx, cancel := context.WithTimeout(p.ctx, 60*time.Minute) // Longer timeout for batch imports
	defer cancel()

	// Get S3 directory path
	datePath := time.Now().Format("2006-01-02")
	dirPath := filepath.Join(datePath, p.processId, batchDir)

	// Get S3 directory URL
	dirURL := p.s3Client.GetS3DirURL(dirPath)

	// Generate a unique table name for the external table
	externalTableName := fmt.Sprintf("ext_%s_%s",
		strings.ReplaceAll(p.config.PostgreSQL.Table, ".", "_"),
		batchDir)

	// Get column names from fields
	columnNames := GetColumnNames(p.fields)

	// Get fresh S3 config from database for the import
	s3Config, err := p.pgClient.GetLoadConfigFromDB(ctx, &p.config)
	if err != nil {
		return errors.Wrap(err, "failed to get loader configuration from database for batch import")
	}
	// Create external table with column names (types will be taken from target table)
	// Note: Using directory URL instead of single file URL
    // Drop external table, becuase we have a retry mechanism in the import process
	err = p.pgClient.DropExternalTable(ctx, externalTableName)
	if err != nil {
		return err
	}

	err = p.pgClient.CreateExternalTable(ctx, dirURL, externalTableName, columnNames, *s3Config)
	if err != nil {
		return err
	}

	// Import data from external table
	err = p.pgClient.ImportFromExternalTable(ctx, externalTableName, columnNames, p.config.UpdateOnConflict)
	if err != nil {
		return err
	}

	// Drop external table
	err = p.pgClient.DropExternalTable(ctx, externalTableName)
	if err != nil {
		return err
	}

	return nil
}

func (p *BulkProcessor) AutoFlushThread() {
	defer p.importerWg.Done()

	for {
		select {
		case <-p.ctx.Done():
			log.Println("AutoFlush thread context canceled, exiting...")
			return
		default:
			// Not canceled
		}
		fileTimeoutDuration := time.Duration(p.config.FileWriteTimeout) * time.Second
		log.Printf("AutoFlush thread checking autoflush, last flush time: %v, timeout: %v", p.lastFlushTime, fileTimeoutDuration)
		if time.Since(p.lastFlushTime) >= fileTimeoutDuration {
			log.Printf("AutoFlush thread doing flush")
			p.Flush()
		} else {
			timeSleep := fileTimeoutDuration - time.Since(p.lastFlushTime)
			log.Printf("AutoFlush thread waiting for autoflush, sleep %v seconds", timeSleep)
			time.Sleep(time.Duration(timeSleep))
		}
	}
}

func (p *BulkProcessor) GCThread() {
	defer p.importerWg.Done()

	for {
		select {
		case <-p.ctx.Done():
			log.Println("GC thread context canceled, exiting...")
			return
		default:
			// Not canceled
		}
		
		importedFilepaths := p.fileManager.RecycleFiles()
		if len(importedFilepaths) > 0 {
			log.Printf("GC thread recycling files: %v", importedFilepaths)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := p.pgClient.DeleteDeltaCheckpointByProcessIdAndFilepaths(ctx, p.processId, importedFilepaths); err != nil {
				log.Printf("Failed to delete delta checkpoint for recycled file: %v\n", err)
			}
			cancel()
		}
		time.Sleep(time.Duration(p.config.GCInterval) * time.Second);
	}
}

func (p *BulkProcessor) getFeedbackValues() []string {
	// scan the feedbackKeys map to get all the column values
	// concate the column values with a comma and send it to the import error channel
	var feedbackKeysArray []string
	p.feedbackKeysMutex.Lock()
	for key := range p.feedbackKeys {
		feedbackKeysArray = append(feedbackKeysArray, key)
	}

	// clear the feedbackKeys map
	p.feedbackKeys = make(map[string]bool)
	p.feedbackKeysMutex.Unlock()
	
	return feedbackKeysArray
}

// add file to pendingBatchFiles
func (p *BulkProcessor) AddFileToPendingBatchFiles(file *File) error {
	// Skip if not in frozen state
	if file.State != FileStateFrozen {
		return errors.New("file is not in frozen state")
	}

	batchDir := file.BatchDir

	// Add to pending batch files
	p.pendingBatchMutex.Lock()
	p.pendingBatchFiles[batchDir]++
	pendingCount := p.pendingBatchFiles[batchDir]

	log.Printf("File %s added to pending batch directory %s, current count: %d", file.S3Key, batchDir, pendingCount)

	// If we have collected all files for this batch, queue the batch for import
	if pendingCount >= p.config.BatchImportSize {
		delete(p.pendingBatchFiles, batchDir) // Clear the counter
		log.Printf("Queuing batch directory for import: %s with %d files", batchDir, pendingCount)
		// Queue the batch for import
		p.batchQueue <- batchDir
		p.lastFlushTime = time.Now()
	}

	p.pendingBatchMutex.Unlock()
	return nil
}
