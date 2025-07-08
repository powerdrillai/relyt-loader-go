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
	BufferStatusActive      BufferStatus = iota // buffer is active, records can be added
	BufferStatusFrozen                          // buffer is full, no more records can be added, will be flushed to local csv file
	BufferStatusFlushed                         // buffer is written to local csv file, but not import to database
	BufferStatusImported                        // buffer is imported to database
	BufferStatusFlushError                      // buffer flushed to local csv file error
	BufferStatusImportError                     // buffer imported to database error
)

const (
	BufferPrefixNormal = "buffer"     // main table batch prefix
	BufferPrefixAux    = "aux_buffer" // aux table batch prefix
)

// Record 表示一条记录
type Record struct {
	Tag       RecordTag
	FileID    string
	RoutingID string
	PKValues  []string
	Version   string
	Values    []string
	Offset    int64 // Kafka offset for this record
}

// Buffer 表示一个缓冲区
type Buffer struct {
	ID            string
	Records       []*Record
	MaxRecords    int
	S3FilePath    string
	LocalFilePath string
	CreatedAt     time.Time
	BufferMutex   sync.RWMutex
	status        BufferStatus
	MaxOffset     int64 // Maximum offset in this buffer
}

type BufferManager struct {
	buffers               map[string]*Buffer
	filePrefix            string
	processId             string
	mutex                 sync.RWMutex
	bufferOperationsMutex sync.Mutex
	buffer                *Buffer
	auxBuffer             *Buffer
}

// NewBufferManager 创建新的缓冲区管理器
func NewBufferManager(filePrefix string, processId string) *BufferManager {
	// Create a unique batch directory identifier
	return &BufferManager{
		buffers:    make(map[string]*Buffer),
		filePrefix: filePrefix,
		processId:  processId,
		buffer:     nil,
		auxBuffer:  nil,
	}
}

func (bm *BufferManager) GetLocalCSVDir() string {
	datePath := time.Now().Format("2006-01-02")
	fullPath := filepath.Join(datePath, bm.processId, bm.filePrefix)
	return fullPath
}

// NewBuffer 创建新的缓冲区
func (bm *BufferManager) NewBuffer(localFilePrefix string, maxRecords int, isAux bool) *Buffer {
	id := uuid.New().String()[:8]
	timestamp := time.Now().Format("150405.000")

	buffer_id := fmt.Sprintf("%s_%s_%s", BufferPrefixNormal, timestamp, id)
	if isAux {
		buffer_id = fmt.Sprintf("%s_%s_%s", BufferPrefixAux, timestamp, id)
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

func (bm *BufferManager) RecycleLocalDir(localFilePrefix string, interval_days int) []string {
	cutoffDate := time.Now().AddDate(0, 0, -interval_days)

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

		if dirDate.Before(cutoffDate) {
			log.Printf("Deleting old local directory: %s (date: %s)", path, dirName)
			if err := os.RemoveAll(path); err != nil {
				log.Printf("Failed to delete directory %s: %v", path, err)
				return nil
			}
			deletedPaths = append(deletedPaths, path)
		}

		return nil
	})

	if err != nil {
		log.Printf("Error walking local directory %s: %v", localFilePrefix, err)
	}

	return deletedPaths
}

func (bm *BufferManager) GetCurrentBuffer(isAux bool) *Buffer {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	if isAux {
		return bm.auxBuffer
	}
	return bm.buffer
}

func (bm *BufferManager) SetCurrentBuffer(newBuffer *Buffer, isAux bool) *Buffer {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	if isAux {
		bm.auxBuffer = newBuffer
	} else {
		bm.buffer = newBuffer
	}
	return newBuffer
}

func (bm *BufferManager) SetBufferStatus(bufferID string, status BufferStatus) {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()
	buffer, exists := bm.buffers[bufferID]
	if !exists {
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

type RecordIndex struct {
	fileID    string
	routingID string
}

type PrimaryKey struct {
	PKValues []string
}

func (pk *PrimaryKey) toString() string {
	return strings.Join(pk.PKValues, "-")
}

func (b *Buffer) DeduplicateRecords(havePK, haveVersion bool, asyncDelete bool) ([]RecordIndex, map[RecordIndex]string) {
	b.BufferMutex.Lock()
	defer b.BufferMutex.Unlock()

	if len(b.Records) == 0 {
		return []RecordIndex{}, make(map[RecordIndex]string)
	}

	// records the delete records
	deleteMap := make(map[RecordIndex]bool)
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
			deleteMap[key] = true
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
							log.Printf("NOTICE: The record versions in the buffer are in descending order")
						}
					} else {
						versionMap[key] = record.Version
						primarySet[pk.toString()] = struct{}{}
						keepMap[i] = true
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
							log.Printf("NOTICE: The record versions in the buffer are in descending order")
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
	for key := range deleteMap {
		deletedIndices = append(deletedIndices, key)
	}

	if asyncDelete {
		return deletedIndices, versionMap
	}

	return deletedIndices, nil
}

func CleanupLocalFile(filePath string) error {
	if filePath == "" {
		return nil
	}
	log.Printf("Removing temporary directory %s", filePath)
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
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Write to local file
	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
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
			return fmt.Errorf("failed to write record to local file: %w", err)
		}
	}

	log.Printf("Buffer %s wrote %d records to local file: %s", b.ID, len(validRecords), fullPath)

	// Write to S3 in multiple partitions using streaming:
	// 1. Lower memory usage: data flows through pipe without buffering entire file
	// 2. No file splitting needed: can write to multiple S3 partitions without creating local files
	// 3. Parallel processing: data generation and S3 upload happen simultaneously
	// 4. Reduced disk I/O: avoids writing large files to local disk
	if s3Client != nil && (tuplesPrePartition > 0 || copyFromS3) && len(validRecords) > 0 {
		totalRecords := len(validRecords)
		numPartitions := (totalRecords + tuplesPrePartition - 1) / tuplesPrePartition // Ceiling division

		for i := 0; i < numPartitions; i++ {
			start := i * tuplesPrePartition
			end := start + tuplesPrePartition
			if end > totalRecords {
				end = totalRecords
			}

			if start >= totalRecords {
				break
			}

			// Create S3 path for this partition
			s3PartitionPath := fmt.Sprintf("%s/part_%d.csv", b.S3FilePath, i+1)

			// Create streaming writer for S3
			s3Writer, err := s3Client.NewStreamingWriter(s3PartitionPath)
			if err != nil {
				return fmt.Errorf("failed to create S3 streaming writer for partition %d: %w", i+1, err)
			}

			// Create CSV writer for S3
			csvWriter := csv.NewWriter(s3Writer)

			// Write records directly to S3
			for j := start; j < end; j++ {
				if err := csvWriter.Write(validRecords[j].Values); err != nil {
					s3Writer.Close()
					return fmt.Errorf("failed to write record to S3 partition %d: %w", i+1, err)
				}
			}

			// Flush and close S3 writer
			csvWriter.Flush()
			if err := csvWriter.Error(); err != nil {
				s3Writer.Close()
				return fmt.Errorf("CSV writer error for S3 partition %d: %w", i+1, err)
			}

			if err := s3Writer.Close(); err != nil {
				return fmt.Errorf("failed to close S3 writer for partition %d: %w", i+1, err)
			}
		}
	}

	return nil
}

// BufferTask represents the buffer task
type BufferTask struct {
	TaskId         string // same as buffer id
	LocalFile      string // same as buffer local file path
	S3File         string // same as buffer s3 file path
	RecordCount    int
	CreatedAt      time.Time
	DeletedRecords []RecordIndex
	FileVersionMap map[RecordIndex]string
	MaxOffset      int64 // Maximum offset in this buffer task
}

// NewBufferTask create a new buffer task
func NewBufferTask(buffer *Buffer, deletedRecords []RecordIndex, fileVersionMap map[RecordIndex]string) *BufferTask {
	bufferTask := BufferTask{
		TaskId:         buffer.ID,
		LocalFile:      buffer.LocalFilePath,
		S3File:         buffer.S3FilePath,
		DeletedRecords: deletedRecords,
		FileVersionMap: fileVersionMap,
		RecordCount:    buffer.GetRecordCount(),
		CreatedAt:      time.Now(),
		MaxOffset:      buffer.GetMaxOffset(),
	}
	buffer.Records = nil
	return &bufferTask
}

func (bm *BufferManager) GetCurrentBufferInfo(isAux bool) *Buffer {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	if isAux {
		return bm.auxBuffer
	}
	return bm.buffer
}
