package bulkprocessor

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type RecordTag int
type BufferStatus int

const (
	OperationInsert RecordTag = iota
	OperationDelete
)

const (
	BufferStatusActive            BufferStatus = iota // buffer is active, records can be added
	BufferStatusFrozen                                // buffer is full, no more records can be added, will be flushed to local csv file
	BufferStatusFlushed                               // buffer is written to local csv file, but not import to database
	BufferStatusImported                              // buffer is imported to database
	BufferStatusFlushError                            // buffer flushed to local csv file error
	BufferStatusImportError                           // buffer imported to database error
	BufferStatusCheckpointPending                     // shard committed; main completion metadata pending
)

const (
	BufferPrefixNormal = "buffer"     // main table batch prefix
	BufferPrefixAux    = "aux_buffer" // aux table batch prefix
)

// Record 表示一条记录
type Record struct {
	Tag        RecordTag
	FileID     string
	RoutingID  string
	GroupID    string
	PKValues   []string
	Version    string
	Values     []string
	Offset     int64  // Kafka offset for this record
	InstanceID string // owning instance for sharded tables, "" otherwise
}

// Buffer 表示一个缓冲区
type Buffer struct {
	ID              string
	Records         []*Record
	MaxRecords      int
	S3FilePath      string
	LocalFilePath   string
	CreatedAt       time.Time
	BufferMutex     sync.RWMutex
	status          BufferStatus
	MaxOffset       int64 // Maximum offset in this buffer
	MaxVersionMap   map[RecordIndex]string
	FeedbackKeys    map[string]bool // feedback column values in this buffer (V2 path)
	InstanceID      string
	IsAux           bool
	CheckpointReady <-chan error // completion of the initial main checkpoint insert
}

type BufferManager struct {
	buffers               map[string]*Buffer
	filePrefix            string
	processId             string
	mutex                 sync.RWMutex
	bufferOperationsMutex sync.Mutex
	currentBuffers        map[string]*Buffer // current buffer per key, see bufferKey
}

// bufferKey identifies a current buffer: "" main, "aux" aux table,
// instanceID for sharded tables. Aux and sharded modes never combine.
func bufferKey(instanceID string, isAux bool) string {
	if isAux {
		return "aux"
	}
	return instanceID
}

// NewBufferManager 创建新的缓冲区管理器
func NewBufferManager(filePrefix string, processId string) *BufferManager {
	// Create a unique batch directory identifier
	return &BufferManager{
		buffers:        make(map[string]*Buffer),
		filePrefix:     filePrefix,
		processId:      processId,
		currentBuffers: make(map[string]*Buffer),
	}
}

func (bm *BufferManager) GetLocalCSVDir() string {
	datePath := time.Now().Format("2006-01-02")
	fullPath := filepath.Join(datePath, bm.filePrefix, bm.processId)
	return fullPath
}

// NewBuffer 创建新的缓冲区
func (bm *BufferManager) NewBuffer(localFilePrefix string, maxRecords int, isAux bool, instanceID string) *Buffer {
	id := uuid.New().String()[:8]
	timestamp := time.Now().Format("150405.000")

	buffer_id := fmt.Sprintf("%s_%s_%s", BufferPrefixNormal, timestamp, id)
	if isAux {
		buffer_id = fmt.Sprintf("%s_%s_%s", BufferPrefixAux, timestamp, id)
	} else if instanceID != "" {
		// instance id in the name is for debuggability only, never parsed back
		buffer_id = fmt.Sprintf("%s_%s_%s_%s", BufferPrefixNormal, instanceID, timestamp, id)
	}

	fileDir := bm.GetLocalCSVDir()
	localFilePath := filepath.Join(localFilePrefix, fileDir, buffer_id)
	s3FilePath := filepath.Join(fileDir, buffer_id)

	buffer := &Buffer{
		ID:            id,
		Records:       make([]*Record, 0),
		MaxRecords:    maxRecords,
		LocalFilePath: localFilePath,
		S3FilePath:    s3FilePath,
		CreatedAt:     time.Now(),
		status:        BufferStatusActive,
		MaxVersionMap: make(map[RecordIndex]string),
		FeedbackKeys:  make(map[string]bool),
		InstanceID:    instanceID,
		IsAux:         isAux,
	}
	bm.mutex.Lock()
	defer bm.mutex.Unlock()
	bm.buffers[buffer.ID] = buffer

	return buffer
}

func (bm *BufferManager) RecycleBuffers() []string {
	allBuffers := bm.GetBufferByStatus(BufferStatusImported, BufferStatusImportError)

	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	var filePaths []string
	for _, buffer := range allBuffers {
		if buffer.LocalFilePath != "" {
			CleanupLocalFile(buffer.LocalFilePath)
			filePaths = append(filePaths, buffer.LocalFilePath)
		}
		delete(bm.buffers, buffer.ID)
	}
	return filePaths
}

func (bm *BufferManager) RecycleLocalDir(localFilePrefix string, intervalDays int) []string {
	return bm.RecycleLocalDirExcept(localFilePrefix, intervalDays, nil)
}

// RecycleLocalDirExcept bounds stale-disk growth while retaining files which
// still have recoverable checkpoint metadata. Unknown old files (for example
// remnants predating checkpoint insertion) follow the historical age policy.
func (bm *BufferManager) RecycleLocalDirExcept(localFilePrefix string, intervalDays int, protected map[string]struct{}) []string {
	cutoffDate := time.Now().AddDate(0, 0, -intervalDays)

	var deletedPaths []string

	err := filepath.Walk(localFilePrefix, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if path == localFilePrefix || !info.IsDir() {
			return nil
		}

		dirName := filepath.Base(path)
		if len(dirName) != 10 || dirName[4] != '-' || dirName[7] != '-' {
			return nil
		}

		dirDate, err := time.Parse("2006-01-02", dirName)
		if err != nil {
			return nil
		}

		if !dirDate.Before(cutoffDate) {
			return filepath.SkipDir
		}
		if protected == nil {
			log.Printf("Deleting old local directory: %s (date: %s)", path, dirName)
			if err := os.RemoveAll(path); err != nil {
				log.Printf("Failed to delete directory %s: %v", path, err)
			} else {
				deletedPaths = append(deletedPaths, path)
			}
			return filepath.SkipDir
		}

		containsProtected := false
		_ = filepath.Walk(path, func(candidate string, candidateInfo os.FileInfo, walkErr error) error {
			if walkErr != nil || candidateInfo.IsDir() {
				return nil
			}
			if _, keep := protected[candidate]; keep {
				containsProtected = true
				return nil
			}
			if err := os.Remove(candidate); err != nil {
				log.Printf("Failed to delete old local file %s: %v", candidate, err)
			} else {
				deletedPaths = append(deletedPaths, candidate)
			}
			return nil
		})
		if !containsProtected {
			log.Printf("Deleting old local directory: %s (date: %s)", path, dirName)
			if err := os.RemoveAll(path); err != nil {
				log.Printf("Failed to delete directory %s: %v", path, err)
			}
		}
		return filepath.SkipDir
	})

	if err != nil {
		log.Printf("Error walking local directory %s: %v", localFilePrefix, err)
	}

	return deletedPaths
}

func (bm *BufferManager) GetCurrentBuffer(key string) *Buffer {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	return bm.currentBuffers[key]
}

func (bm *BufferManager) SetCurrentBuffer(newBuffer *Buffer, key string) *Buffer {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	if newBuffer == nil {
		delete(bm.currentBuffers, key)
	} else {
		bm.currentBuffers[key] = newBuffer
	}
	return newBuffer
}

// GetCurrentBufferKeys returns the keys of all current buffers.
func (bm *BufferManager) GetCurrentBufferKeys() []string {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	keys := make([]string, 0, len(bm.currentBuffers))
	for key := range bm.currentBuffers {
		keys = append(keys, key)
	}
	return keys
}

func (bm *BufferManager) SetBufferStatus(bufferID string, status BufferStatus) {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()
	buffer, exists := bm.buffers[bufferID]
	if !exists {
		return
	}
	// terminal statuses are final: a racing non-terminal write must not
	// resurrect a buffer that was already imported or failed
	if buffer.status == BufferStatusImported || buffer.status == BufferStatusImportError {
		return
	}
	buffer.status = status
}

func (bm *BufferManager) GetBufferByStatus(status ...BufferStatus) []*Buffer {
	needStatus := make(map[BufferStatus]bool)
	for _, s := range status {
		needStatus[s] = true
	}

	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	var buffers []*Buffer
	for _, buffer := range bm.buffers {
		if needStatus[buffer.status] {
			buffers = append(buffers, buffer)
		}
	}
	return buffers
}

func (bm *BufferManager) GetAllBuffers() []*Buffer {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	var buffers []*Buffer
	for _, buffer := range bm.buffers {
		buffers = append(buffers, buffer)
	}
	return buffers
}

func (bm *BufferManager) GetBufferByID(bufferID string) *Buffer {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	if buffer, exists := bm.buffers[bufferID]; exists {
		return buffer
	}
	return nil
}

func (bm *BufferManager) GetBufferStatus(bufferID string) (BufferStatus, bool) {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	buffer, exists := bm.buffers[bufferID]
	if !exists {
		return 0, false
	}
	return buffer.status, true
}

func (bm *BufferManager) IsActive(bufferID string) bool {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	buffer, exists := bm.buffers[bufferID]
	if !exists {
		return false
	}
	return buffer.status == BufferStatusActive
}

func (bm *BufferManager) IsFrozen(bufferID string) bool {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	buffer, exists := bm.buffers[bufferID]
	if !exists {
		return false
	}
	return buffer.status == BufferStatusFrozen
}

func (bm *BufferManager) IsFlushed(bufferID string) bool {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	buffer, exists := bm.buffers[bufferID]
	if !exists {
		return false
	}
	return buffer.status == BufferStatusFlushed
}

func (bm *BufferManager) IsError(bufferID string) bool {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	buffer, exists := bm.buffers[bufferID]
	if !exists {
		return false
	}
	return buffer.status == BufferStatusImportError || buffer.status == BufferStatusFlushError
}

func (b *Buffer) AddRecord(record *Record) {
	b.BufferMutex.Lock()
	defer b.BufferMutex.Unlock()

	b.Records = append(b.Records, record)

	// Update max offset if this record has a higher offset
	if record.Offset > b.MaxOffset {
		b.MaxOffset = record.Offset
	}
}

func (b *Buffer) IsFull() bool {
	b.BufferMutex.RLock()
	defer b.BufferMutex.RUnlock()
	return len(b.Records) >= b.MaxRecords
}

func (b *Buffer) GetRecordCount() int {
	b.BufferMutex.RLock()
	defer b.BufferMutex.RUnlock()
	return len(b.Records)
}

func (b *Buffer) GetMaxOffset() int64 {
	b.BufferMutex.RLock()
	defer b.BufferMutex.RUnlock()
	return b.MaxOffset
}

// GetFeedbackKeys returns the buffer's feedback keys as a slice.
func (b *Buffer) GetFeedbackKeys() []string {
	keys := make([]string, 0, len(b.FeedbackKeys))
	for key := range b.FeedbackKeys {
		keys = append(keys, key)
	}
	return keys
}

type RecordIndex struct {
	fileID    string
	routingID string
}

type GroupIndex struct {
	groupID   string
	routingID string
}

type PrimaryKey struct {
	PKValues []string
}

func (pk *PrimaryKey) toString() string {
	return strings.Join(pk.PKValues, "-")
}

func (b *Buffer) DeduplicateRecords(havePK, haveVersion bool, deleteBeforeInsert bool) ([]RecordIndex, []GroupIndex, map[RecordIndex]string) {
	b.BufferMutex.Lock()
	defer b.BufferMutex.Unlock()

	if len(b.Records) == 0 {
		return []RecordIndex{}, []GroupIndex{}, make(map[RecordIndex]string)
	}

	// records the delete records
	deleteMap := make(map[RecordIndex]bool)
	deleteGroupMap := make(map[GroupIndex]bool)
	//hash<routingID-fileID, version>
	versionMap := make(map[RecordIndex]string)
	primarySet := make(map[string]struct{})
	// records the records to keep
	keepMap := make(map[int]bool)

	// iterate the records from the end to the start
	for i := len(b.Records) - 1; i >= 0; i-- {
		record := b.Records[i]
		key := RecordIndex{
			fileID:    record.FileID,
			routingID: record.RoutingID,
		}
		if record.Tag == OperationDelete {
			if record.GroupID != "" {
				deleteGroupMap[GroupIndex{
					groupID:   record.GroupID,
					routingID: record.RoutingID,
				}] = true
			} else {
				deleteMap[key] = true
			}
			keepMap[i] = false
		} else if record.Tag == OperationInsert {
			// if the insert record has the same fileID and routingID as the delete record, mark it as delete
			if deleteMap[key] {
				keepMap[i] = false
			} else {
				if !havePK && !haveVersion {
					keepMap[i] = true
				} else if havePK && haveVersion {
					pk := PrimaryKey{
						PKValues: record.PKValues,
					}
					// save the latest version for the insert record
					if version, exists := versionMap[key]; exists {
						if record.Version == version {
							// if the insert record has the same version then check the primary key
							// if the primary key is already in the set, mark it as delete
							if _, exists := primarySet[pk.toString()]; exists {
								keepMap[i] = false
							} else {
								primarySet[pk.toString()] = struct{}{}
								keepMap[i] = true
							}
						} else if record.Version < version {
							keepMap[i] = false
						} else {
							//InsertThreadV2 have filter the records which have the smaller version
							Error("HavePK and haveVersion, but the record versions in the buffer are in descending order")
							keepMap[i] = false
						}
					} else {
						if _, exists := primarySet[pk.toString()]; exists {
							keepMap[i] = false
						} else {
							versionMap[key] = record.Version
							primarySet[pk.toString()] = struct{}{}
							keepMap[i] = true
						}
					}
				} else if havePK && !haveVersion {
					pk := PrimaryKey{
						PKValues: record.PKValues,
					}

					if _, exists := primarySet[pk.toString()]; exists {
						keepMap[i] = false
					} else {
						primarySet[pk.toString()] = struct{}{}
						keepMap[i] = true
					}
				} else { // !havePK && haveVersion
					if version, exists := versionMap[key]; exists {
						if record.Version == version {
							keepMap[i] = true
						} else if record.Version < version {
							keepMap[i] = false
						} else {
							Error("!HavePK and haveVersion, but the record versions in the buffer are in descending order")
							keepMap[i] = false
						}
					} else {
						versionMap[key] = record.Version
						keepMap[i] = true
					}
				}
			}
		} else {
			keepMap[i] = true
		}
	}

	// rebuild the records array, only keep the records marked as true
	var newRecords []*Record
	for i, record := range b.Records {
		if keepMap[i] {
			newRecords = append(newRecords, record)
		}
	}

	b.Records = newRecords
	// collect all the deleted records
	var deletedIndices []RecordIndex
	var deletedGroupIndices []GroupIndex
	for key := range deleteMap {
		deletedIndices = append(deletedIndices, key)
	}
	for key := range deleteGroupMap {
		deletedGroupIndices = append(deletedGroupIndices, key)
	}

	if deleteBeforeInsert && haveVersion {
		return deletedIndices, deletedGroupIndices, versionMap
	}

	return deletedIndices, deletedGroupIndices, nil
}

func CleanupLocalFile(filePath string) error {
	if filePath == "" {
		return nil
	}
	Debug("Removing temporary directory %s", filePath)
	return os.RemoveAll(filePath)
}

func GetLocalFileFullPath(localFilePath string) string {
	return fmt.Sprintf("%s/local.csv", localFilePath)
}

// BufferWriteToFile write the records to the local file
func (b *Buffer) BufferWriteToFile(headers []string, s3Client *S3Client, tuplesPrePartition int, copyFromS3 bool) error {
	b.BufferMutex.Lock()
	defer b.BufferMutex.Unlock()

	if len(b.Records) == 0 {
		return nil
	}

	fullPath := GetLocalFileFullPath(b.LocalFilePath)

	// ensure the directory exists
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return fmt.Errorf("Write buffer to local file failed: %w", err)
	}

	// Write to local file
	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("Write buffer to local file failed: %w", err)
	}
	defer file.Close()

	// create the csv writer for local file
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Filter records (exclude delete operations)
	var validRecords []*Record
	for _, record := range b.Records {
		if record.Tag != OperationDelete {
			validRecords = append(validRecords, record)
		}
	}

	// Write all valid records to local file
	for _, record := range validRecords {
		if err := writer.Write(record.Values); err != nil {
			return fmt.Errorf("Write buffer to local file failed: %w", err)
		}
	}

	Debug("Buffer %s wrote %d records to local file: %s", b.ID, len(validRecords), fullPath)

	// Write to S3 in multiple partitions using streaming:
	// 1. Lower memory usage: data flows through pipe without buffering entire file
	// 2. No file splitting needed: can write to multiple S3 partitions without creating local files
	// 3. Parallel processing: data generation and S3 upload happen simultaneously
	// 4. Reduced disk I/O: avoids writing large files to local disk
	if s3Client != nil && (tuplesPrePartition > 0 || copyFromS3) && len(validRecords) > 0 {
		totalRecords := len(validRecords)
		numPartitions := (totalRecords + tuplesPrePartition - 1) / tuplesPrePartition // Ceiling division

		for i := range numPartitions {
			start := i * tuplesPrePartition
			end := min(start+tuplesPrePartition, totalRecords)

			if start >= totalRecords {
				break
			}

			// Create S3 path for this partition
			s3PartitionPath := fmt.Sprintf("%s/part_%d.csv", b.S3FilePath, i+1)

			// Create streaming writer for S3
			s3Writer, err := s3Client.NewStreamingWriter(s3PartitionPath)
			if err != nil {
				return fmt.Errorf("Upload buffer to s3 failed: create S3 streaming writer for partition %d: %w", i+1, err)
			}

			// Create CSV writer for S3
			csvWriter := csv.NewWriter(s3Writer)

			// Write records directly to S3
			for j := start; j < end; j++ {
				if err := csvWriter.Write(validRecords[j].Values); err != nil {
					s3Writer.Close()
					return fmt.Errorf("Upload buffer to s3 failed: failed to write record to S3 partition %d: %w", i+1, err)
				}
			}

			// Flush and close S3 writer
			csvWriter.Flush()
			if err := csvWriter.Error(); err != nil {
				s3Writer.Close()
				return fmt.Errorf("Upload buffer to s3 failed: CSV writer error for S3 partition %d: %w", i+1, err)
			}

			if err := s3Writer.Close(); err != nil {
				return fmt.Errorf("Upload buffer to s3 failed: failed to close S3 writer for partition %d: %w", i+1, err)
			}
		}
	}

	return nil
}

// BufferTask represents the buffer task
type BufferTask struct {
	TaskId              string // same as buffer id
	LocalFile           string // same as buffer local file path
	S3File              string // same as buffer s3 file path
	RecordCount         int
	CreatedAt           time.Time
	DeletedRecords      []RecordIndex
	DeletedGroupRecords []GroupIndex
	FileVersionMap      map[RecordIndex]string
	MaxOffset           int64  // Maximum offset in this buffer task
	ImportStrategy      int    // Import strategy
	InstanceID          string // owning instance for sharded tables, "" otherwise
	IsAux               bool
	FeedbackKeys        []string // feedback column values of this buffer's records
	CheckpointReady     <-chan error
}

// NewBufferTask create a new buffer task
func NewBufferTask(buffer *Buffer, deletedRecords []RecordIndex, deletedGroupRecords []GroupIndex, fileVersionMap map[RecordIndex]string, importStrategy int) *BufferTask {
	bufferTask := BufferTask{
		TaskId:              buffer.ID,
		LocalFile:           buffer.LocalFilePath,
		S3File:              buffer.S3FilePath,
		DeletedRecords:      deletedRecords,
		DeletedGroupRecords: deletedGroupRecords,
		FileVersionMap:      fileVersionMap,
		RecordCount:         buffer.GetRecordCount(),
		CreatedAt:           time.Now(),
		MaxOffset:           buffer.GetMaxOffset(),
		ImportStrategy:      importStrategy,
		InstanceID:          buffer.InstanceID,
		IsAux:               buffer.IsAux,
		FeedbackKeys:        buffer.GetFeedbackKeys(),
		CheckpointReady:     buffer.CheckpointReady,
	}
	buffer.Records = nil
	buffer.MaxVersionMap = nil
	// Keep the small key set on the frozen buffer as well as the task. If
	// shutdown wins the enqueue race, the caller still has the keys needed to
	// report/requeue the failed records.
	return &bufferTask
}

func (bm *BufferManager) GetCurrentBufferInfo(key string) *Buffer {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	return bm.currentBuffers[key]
}
