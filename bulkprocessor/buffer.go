package bulkprocessor

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
	Values    []string
	Timestamp time.Time
	Offset    int64 // Kafka offset for this record
}

// Buffer 表示一个缓冲区
type Buffer struct {
	ID            string
	Records       []*Record
	MaxRecords    int
	LocalFilePath string
	CreatedAt     time.Time
	FirstWriteAt  time.Time
	LastWriteAt   time.Time
	BufferMutex   sync.RWMutex
	status        BufferStatus
	LocalFile     string
	MaxOffset     int64 // Maximum offset in this buffer
}

type BufferManager struct {
	buffers               map[string]*Buffer
	currentBuffer         *Buffer
	currentAuxBuffer      *Buffer
	mutex                 sync.RWMutex
	bufferOperationsMutex sync.Mutex
}

// NewBufferManager 创建新的缓冲区管理器
func NewBufferManager() *BufferManager {
	return &BufferManager{
		buffers: make(map[string]*Buffer),
	}
}

// NewBuffer 创建新的缓冲区
func (bm *BufferManager) NewBuffer(localFilePrefix string, maxRecords int, isAux bool) *Buffer {
	id := uuid.New().String()[:8]
	// 创建临时文件
	filename := fmt.Sprintf("%s_%s.csv", BufferPrefixNormal, id)
	if isAux {
		filename = fmt.Sprintf("%s_%s.csv", BufferPrefixAux, id)
	}

	// 添加日期路径
	relytName := "relyt_data"
	datePath := time.Now().Format("2006-01-02")
	fullPath := filepath.Join(localFilePrefix, relytName, datePath, filename)
	buffer := &Buffer{
		ID:            id,
		Records:       make([]*Record, 0),
		MaxRecords:    maxRecords,
		LocalFilePath: fullPath,
		CreatedAt:     time.Now(),
		FirstWriteAt:  time.Time{},
		LastWriteAt:   time.Now(),
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
		if buffer.LocalFile != "" {
			CleanupLocalFile(buffer.LocalFile)
			filePaths = append(filePaths, buffer.LocalFile)
		}
		delete(bm.buffers, buffer.ID)
	}
	return filePaths
}

func (bm *BufferManager) GetCurrentBuffer(isAux bool) *Buffer {
	bm.mutex.RLock()
	defer bm.mutex.RUnlock()
	if isAux {
		return bm.currentAuxBuffer
	}
	return bm.currentBuffer
}

func (bm *BufferManager) SetCurrentBuffer(newBuffer *Buffer, isAux bool) *Buffer {
	bm.mutex.Lock()
	defer bm.mutex.Unlock()

	// 设置新的当前缓冲区
	if isAux {
		bm.currentAuxBuffer = newBuffer
	} else {
		bm.currentBuffer = newBuffer
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

	if b.FirstWriteAt.IsZero() {
		b.FirstWriteAt = time.Now()
	}

	b.Records = append(b.Records, record)
	b.LastWriteAt = time.Now()

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

func (b *Buffer) DeduplicateRecords() []RecordIndex {
	b.BufferMutex.Lock()
	defer b.BufferMutex.Unlock()

	if len(b.Records) == 0 {
		return []RecordIndex{}
	}

	// records the delete records
	deleteMap := make(map[RecordIndex]bool)
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
				keepMap[i] = true
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
	return deletedIndices
}

func CleanupLocalFile(filePath string) error {
	if filePath == "" {
		return nil
	}
	log.Printf("Removing temporary file %s", filePath)
	return os.Remove(filePath)
}

// WriteToLocalFile write the records to the local file
func (b *Buffer) WriteToLocalFile(headers []string) error {
	b.BufferMutex.Lock()
	defer b.BufferMutex.Unlock()

	if len(b.Records) == 0 {
		return nil
	}

	fullPath := b.LocalFilePath

	// ensure the directory exists
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer file.Close()

	// create the csv writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// write all the records (without headers)
	for _, record := range b.Records {
		if record.Tag == OperationDelete {
			continue
		}
		if err := writer.Write(record.Values); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	b.LocalFile = fullPath
	log.Printf("Buffer %s wrote %d records to local file: %s", b.ID, len(b.Records), fullPath)
	return nil
}

// BufferTask represents the buffer task
type BufferTask struct {
	TaskId         string
	LocalFile      string
	RecordCount    int
	CreatedAt      time.Time
	DeletedRecords []RecordIndex
	MaxOffset      int64 // Maximum offset in this buffer task
}

// NewBufferTask create a new buffer task
func NewBufferTask(buffer *Buffer, deletedRecords []RecordIndex) *BufferTask {
	bufferTask := BufferTask{
		TaskId:         buffer.ID,
		LocalFile:      buffer.LocalFile,
		DeletedRecords: deletedRecords,
		RecordCount:    buffer.GetRecordCount(),
		CreatedAt:      time.Now(),
		MaxOffset:      buffer.GetMaxOffset(),
	}
	buffer.Records = nil
	return &bufferTask
}
