package bulkprocessor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
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
	isFlushing        bool
	isShutdown        bool
	importErrorChan   chan error
	fileQueue         chan string    // Queue of file IDs to be imported
	batchQueue        chan string    // Queue of batch directories to be imported
	pendingBatchFiles map[string]int // Map of batch directory to count of pending files
	pendingBatchMutex sync.RWMutex
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
	s3Config, err := pgClient.GetS3ConfigFromDB(ctx)
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
		batchQueue:        make(chan string, 100),  // Buffer for batch queue
		pendingBatchFiles: make(map[string]int),    // Tracks files pending in each batch
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
	go p.importerThread()

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

	// Update checkpoint status to completed
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.pgClient.UpdateCheckpointStatus(ctx, p.processId, CheckpointStatusCompleted, ""); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to update checkpoint status on shutdown: %v\n", err)
	}

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
	for _, file := range allFiles {
		if file.S3Key != "" {
			s3Keys = append(s3Keys, file.S3Key)
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
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	if err := p.pgClient.DeleteCheckpoint(ctx, p.processId); err != nil {
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
	// Check for import errors first
	if err := p.checkImportError(); err != nil {
		return errors.Wrap(err, "previous import operation failed")
	}

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

	// Prepare current file
	currentFile := p.fileManager.GetCurrentFile()
	if currentFile == nil || currentFile.State != FileStateOpen {
		p.mutex.Lock()
		defer p.mutex.Unlock()

		currentFile = p.fileManager.GetCurrentFile()
		if currentFile == nil || currentFile.State != FileStateOpen {
			// Create a new file
			columnNames := GetColumnNames(p.fields)
			var err error
			currentFile, err = p.fileManager.CreateFile(columnNames)
			if err != nil {
				return err
			}
			p.fileManager.SetCurrentFile(currentFile)

			// Update checkpoint with new file
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := p.pgClient.UpdateCheckpointFile(ctx, p.processId, currentFile.ToCheckpointInfo()); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to update checkpoint with new file: %v\n", err)
			}
		}
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

		// Write record to file
		err = currentFile.WriteRecord(values)
		if err != nil {
			if p.checkErrorCount(err, &errorRecordsCount, i, "writing") {
				continue // Skip this record and continue
			}

			// Either too many errors or couldn't get error count
			return err
		}

		// Check if file is full
		if currentFile.IsFull(p.config.BatchSize) {
			p.mutex.Lock()
			defer p.mutex.Unlock()

			currentFile = p.fileManager.GetCurrentFile()
			if !currentFile.IsFull(p.config.BatchSize) {
				// already been proccessed by other thread
				continue
			}

			// Flush and close the file
			if err := currentFile.Flush(); err != nil {
				return err
			}

			// Close the file to finalize the S3 upload
			if err := currentFile.Close(); err != nil {
				return err
			}

			// Set S3 URL
			currentFile.S3URL = p.s3Client.GetS3URL(currentFile.S3Key)

			// Update file state to frozen
			p.fileManager.UpdateFileState(currentFile.ID, FileStateFrozen)

			// Update checkpoint with frozen file
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			fileInfo := currentFile.ToCheckpointInfo()
			if err := p.pgClient.UpdateCheckpointFile(ctx, p.processId, fileInfo); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to update checkpoint with frozen file: %v\n", err)
			}
			cancel()

			// Queue file for import
			p.fileQueue <- currentFile.ID

			// Create a new file
			columnNames := GetColumnNames(p.fields)
			currentFile, err = p.fileManager.CreateFile(columnNames)
			if err != nil {
				return err
			}
			p.fileManager.SetCurrentFile(currentFile)

			// Update checkpoint with new file
			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			if err := p.pgClient.UpdateCheckpointFile(ctx, p.processId, currentFile.ToCheckpointInfo()); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to update checkpoint with new file: %v\n", err)
			}
			cancel()
		}
	}

	// If we had error records in this batch, update the checkpoint
	if errorRecordsCount > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := p.pgClient.UpdateCheckpointErrorRecords(ctx, p.processId, errorRecordsCount); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to update checkpoint error records count: %v\n", err)
		}
		cancel()
	}

	// Update last insert time
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.pgClient.UpdateCheckpointLastInsert(ctx, p.processId); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to update checkpoint last insert time: %v\n", err)
	}

	return nil
}

// Flush flushes all pending data and waits for import to complete
func (p *BulkProcessor) Flush() error {
	// Check for import errors first
	if err := p.checkImportError(); err != nil {
		return errors.Wrap(err, "previous import operation failed")
	}

	p.mutex.Lock()
	if p.isShutdown {
		p.mutex.Unlock()
		return ErrProcessorClosed
	}

	if p.isFlushing {
		p.mutex.Unlock()
		return errors.New("already flushing")
	}

	p.isFlushing = true
	p.mutex.Unlock()

	defer func() {
		p.mutex.Lock()
		p.isFlushing = false
		p.mutex.Unlock()
	}()

	// Flush current file if it has records
	currentFile := p.fileManager.GetCurrentFile()
	if currentFile != nil && currentFile.State == FileStateOpen && currentFile.NumRecords > 0 {
		if err := currentFile.Flush(); err != nil {
			return err
		}

		// Close the file to finalize the S3 upload
		if err := currentFile.Close(); err != nil {
			return err
		}

		// Set S3 URL
		currentFile.S3URL = p.s3Client.GetS3URL(currentFile.S3Key)

		// Update file state to frozen
		p.fileManager.UpdateFileState(currentFile.ID, FileStateFrozen)

		// Queue file for import
		p.fileQueue <- currentFile.ID
	}

	// Wait for all files to be imported or an error to occur
	for {
		// Check for import errors
		if err := p.checkImportError(); err != nil {
			return err
		}

		// flush all pendingBatchFiles
		p.pendingBatchMutex.Lock()
		for batchDir, count := range p.pendingBatchFiles {
			if count > 0 {
				p.batchQueue <- batchDir
			}
			delete(p.pendingBatchFiles, batchDir)
		}
		p.pendingBatchMutex.Unlock()

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

		// Check if all files are imported
		frozenFiles := p.fileManager.GetFilesByState(FileStateFrozen)
		importingFiles := p.fileManager.GetFilesByState(FileStateImporting)
		errorFiles := p.fileManager.GetFilesByState(FileStateError)

		// If we have error files but no error from channel, check if context was canceled
		if len(errorFiles) > 0 {
			return errors.New("import failed, found error files but no error reported")
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
func (p *BulkProcessor) importerThread() {
	defer p.importerWg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return

		case batchDir := <-p.batchQueue:
			// Process a whole batch directory
			if batchDir == "" {
				continue
			}

			// Get all files in this batch directory
			files := p.fileManager.GetFilesByBatchDirectory(batchDir)
			if len(files) == 0 {
				continue
			}

			// Update state of all files to importing
			for _, file := range files {
				// Skip if not in frozen state
				if file.State != FileStateFrozen {
					continue
				}

				// Update state to importing
				p.fileManager.UpdateFileState(file.ID, FileStateImporting)

				// Update checkpoint with importing file
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := p.pgClient.UpdateCheckpointFile(ctx, p.processId, file.ToCheckpointInfo()); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to update checkpoint for importing file: %v\n", err)
				}
				cancel()
			}

			// Import the entire batch directory
			err := p.importBatchDirectory(batchDir)
			if err != nil {
				// Mark all files as error
				for _, file := range files {
					file.SetError(fmt.Sprintf("failed to load batch: %v", err))

					// Update checkpoint with error file
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					if err2 := p.pgClient.UpdateCheckpointFile(ctx, p.processId, file.ToCheckpointInfo()); err2 != nil {
						fmt.Fprintf(os.Stderr, "Failed to update checkpoint for error file: %v\n", err2)
					}
					cancel()
				}

				// Update checkpoint status to failed
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				errMsg := fmt.Sprintf("Batch import failed: %v", err)
				if err3 := p.pgClient.UpdateCheckpointStatus(ctx, p.processId, CheckpointStatusFailed, errMsg); err3 != nil {
					fmt.Fprintf(os.Stderr, "Failed to update checkpoint status to failed: %v\n", err3)
				}
				cancel()

				// Send error to import error channel and ensure it's received
				p.importErrorChan <- err

				// Cancel context to signal other operations to stop
				p.cancel()

				// Exit the importer thread after an error
				return
			}

			// Update all files to imported state
			for _, file := range files {
				// Update state to imported and record import time
				file.ImportedAt = time.Now()
				p.fileManager.UpdateFileState(file.ID, FileStateImported)

				// Update checkpoint with imported file
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := p.pgClient.UpdateCheckpointFile(ctx, p.processId, file.ToCheckpointInfo()); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to update checkpoint for imported file: %v\n", err)
				}
				cancel()

				// Cleanup file
				if err := file.CleanupFile(); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to cleanup file: %v\n", err)
				}
			}

		case fileID := <-p.fileQueue:
			file := p.fileManager.GetFile(fileID)
			if file == nil {
				continue
			}

			// Skip if not in frozen state
			if file.State != FileStateFrozen {
				continue
			}

			batchDir := file.BatchDir

			// Add to pending batch files
			p.pendingBatchMutex.Lock()
			p.pendingBatchFiles[batchDir]++
			pendingCount := p.pendingBatchFiles[batchDir]

			// If we have collected all files for this batch, queue the batch for import
			if pendingCount >= p.config.BatchImportSize {
				delete(p.pendingBatchFiles, batchDir) // Clear the counter

				// Queue the batch for import
				p.batchQueue <- batchDir
			}
			p.pendingBatchMutex.Unlock()
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
		strings.ReplaceAll(uuid.New().String(), "-", ""))

	// Get column names from fields
	columnNames := GetColumnNames(p.fields)

	// Get fresh S3 config from database for the import
	s3Config, err := p.pgClient.GetS3ConfigFromDB(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get S3 configuration from database for batch import")
	}
	// Create external table with column names (types will be taken from target table)
	// Note: Using directory URL instead of single file URL
	err = p.pgClient.CreateExternalTable(ctx, dirURL, externalTableName, columnNames, *s3Config)
	if err != nil {
		return errors.Wrap(err, "failed to create external table for batch import")
	}

	// Import data from external table
	err = p.pgClient.ImportFromExternalTable(ctx, externalTableName, columnNames, p.config.UpdateOnConflict)
	if err != nil {
		return errors.Wrap(err, "failed to load data from external table for batch import")
	}

	// Drop external table
	err = p.pgClient.DropExternalTable(ctx, externalTableName)
	if err != nil {
		return errors.Wrap(err, "failed to drop external table for batch import")
	}

	return nil
}
