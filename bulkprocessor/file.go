package bulkprocessor

import (
	"encoding/csv"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// FileState represents the state of a file
type FileState int

const (
	// FileStateOpen represents an open file that is still being written to
	FileStateOpen FileState = iota
	// FileStateFrozen represents a file that is ready to be imported
	FileStateFrozen
	// FileStateImporting represents a file that is being imported
	FileStateImporting
	// FileStateImported represents a file that has been imported
	FileStateImported
	// FileStateError represents a file that encountered an error during import
	FileStateError
)

// File represents a file being processed
type File struct {
	ID          string    // Unique ID for the file
	S3Key       string    // S3 key for the file
	S3URL       string    // S3 URL for the file
	BatchDir    string    // Batch directory this file belongs to
	NumRecords  int       // Number of records in the file
	CreatedAt   time.Time // Time the file was created
	FrozenAt    time.Time // Time the file was frozen
	ImportedAt  time.Time // Time the file was imported
	State       FileState // Current state of the file
	ErrorReason string    // Reason for error, if any
	csvWriter   *csv.Writer
	s3Writer    io.WriteCloser // S3 streaming writer
	mutex       sync.Mutex
	headers     []string // CSV headers
}

// FileManager manages files being processed
type FileManager struct {
	files           map[string]*File // Map of file ID to file
	mutex           sync.RWMutex
	s3Client        *S3Client // S3 client for streaming writes
	filePrefix      string    // Prefix for file names
	maxRecords      int       // Maximum number of records per file
	currentFile     *File     // Current file being written to
	processId       string    // Unique process ID for distinguishing task files
	currentBatchDir string    // Current batch directory (changes every batchImportSize files)
	batchCounter    int       // Counter for files in the current batch
	batchImportSize int       // Number of files per batch (from config)
}

// NewFileManager creates a new file manager
func NewFileManager(s3Client *S3Client, filePrefix string, maxRecords int, processId string, batchImportSize int) (*FileManager, error) {
	// Create a unique batch directory identifier
	batchDir := fmt.Sprintf("batch_%s", uuid.New().String()[:8])

	return &FileManager{
		files:           make(map[string]*File),
		s3Client:        s3Client,
		filePrefix:      filePrefix,
		maxRecords:      maxRecords,
		processId:       processId,
		currentBatchDir: batchDir,
		batchCounter:    0,
		batchImportSize: batchImportSize,
	}, nil
}

// CreateFile creates a new file
func (m *FileManager) CreateFile(headers []string) (*File, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Check if we need to create a new batch directory
	if m.batchCounter >= m.batchImportSize {
		// Create a new batch directory for the next set of files
		m.currentBatchDir = fmt.Sprintf("batch_%s", uuid.New().String()[:8])
		m.batchCounter = 0
	}

	fileID := uuid.New().String()
	fileName := fmt.Sprintf("%s_%s.csv", m.filePrefix, fileID)

	// Include process ID and batch directory in S3 key path
	datePath := time.Now().Format("2006-01-02")

	s3Key := filepath.Join(datePath, m.processId, m.currentBatchDir, fileName)

	// Create S3 streaming writer
	s3Writer, err := m.s3Client.NewStreamingWriter(s3Key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create S3 streaming writer")
	}

	// Create CSV writer that writes to the S3 writer
	csvWriter := csv.NewWriter(s3Writer)

	f := &File{
		ID:         fileID,
		S3Key:      s3Key,
		BatchDir:   m.currentBatchDir,
		NumRecords: 0,
		CreatedAt:  time.Now(),
		State:      FileStateOpen,
		csvWriter:  csvWriter,
		s3Writer:   s3Writer,
		headers:    headers,
	}

	m.files[fileID] = f
	m.batchCounter++

	return f, nil
}

// GetCurrentFile returns the current file being written to
func (m *FileManager) GetCurrentFile() *File {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.currentFile
}

// SetCurrentFile sets the current file being written to
func (m *FileManager) SetCurrentFile(file *File) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.currentFile = file
}

// GetFile returns a file by ID
func (m *FileManager) GetFile(id string) *File {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	// Return nil if file not found
	if file, ok := m.files[id]; ok {
		return file
	}

	return nil
}

// GetFilesByState returns files by state
func (m *FileManager) GetFilesByState(state FileState) []*File {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	var files []*File
	for _, file := range m.files {
		if file.State == state {
			files = append(files, file)
		}
	}

	return files
}

// UpdateFileState updates the state of a file
func (m *FileManager) UpdateFileState(fileID string, state FileState) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if file, ok := m.files[fileID]; ok {
		file.mutex.Lock()
		defer file.mutex.Unlock()

		file.State = state

		switch state {
		case FileStateFrozen:
			file.FrozenAt = time.Now()
		case FileStateImported:
			file.ImportedAt = time.Now()
		}
	}
}

// WriteRecord writes a record to a file
func (f *File) WriteRecord(record []string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.State != FileStateOpen {
		return errors.New("file is not open for writing")
	}

	err := f.csvWriter.Write(record)
	if err != nil {
		return errors.Wrap(err, "failed to write record")
	}

	f.NumRecords++
	return nil
}

// Flush flushes the file to S3
func (f *File) Flush() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	f.csvWriter.Flush()
	return f.csvWriter.Error()
}

// Close closes the file and finalizes the S3 upload
func (f *File) Close() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	// Flush any buffered data
	f.csvWriter.Flush()

	// Check for CSV writer errors
	if err := f.csvWriter.Error(); err != nil {
		return errors.Wrap(err, "CSV writer error before closing")
	}

	// Close the S3 writer to finalize the upload
	if f.s3Writer != nil {
		err := f.s3Writer.Close()
		f.s3Writer = nil
		return err
	}

	return nil
}

// IsFull returns true if the file has reached the maximum number of records
func (f *File) IsFull(maxRecords int) bool {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.NumRecords >= maxRecords
}

// SetError sets the error state and reason
func (f *File) SetError(reason string) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.State = FileStateError
	f.ErrorReason = reason
}

// CleanupFile cleans up resources
func (f *File) CleanupFile() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	// Close the s3Writer if it's still open
	if f.s3Writer != nil {
		err := f.s3Writer.Close()
		f.s3Writer = nil
		if err != nil {
			return errors.Wrap(err, "failed to close S3 writer")
		}
	}

	return nil
}

// GetProcessId returns the process ID
func (m *FileManager) GetProcessId() string {
	return m.processId
}

// ToCheckpointInfo converts a File to FileCheckpointInfo for checkpoint storage
func (f *File) ToCheckpointInfo() FileCheckpointInfo {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	var status string
	switch f.State {
	case FileStateOpen:
		status = "CREATED"
	case FileStateFrozen:
		status = "FROZEN"
	case FileStateImporting:
		status = "IMPORTING"
	case FileStateImported:
		status = "IMPORTED"
	case FileStateError:
		status = "ERROR"
	}

	metadata := make(map[string]string)
	metadata["batch_dir"] = f.BatchDir

	return FileCheckpointInfo{
		S3Key:       f.S3Key,
		NumRecords:  f.NumRecords,
		CreatedAt:   f.CreatedAt,
		ImportedAt:  f.ImportedAt,
		Status:      status,
		ErrorReason: f.ErrorReason,
		Metadata:    metadata,
	}
}

// GetBatchDirectoryPath returns the S3 path for the current batch directory
func (m *FileManager) GetBatchDirectoryPath() string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	datePath := time.Now().Format("2006-01-02")
	return filepath.Join(datePath, m.processId, m.currentBatchDir)
}

// GetFileBatchDirectory returns the batch directory a file belongs to
func (m *FileManager) GetFileBatchDirectory(file *File) string {
	if file == nil {
		return ""
	}

	// Extract batch directory from S3 key
	// Expected format: datePath/processId/batchDir/filename
	parts := strings.Split(file.S3Key, "/")
	if len(parts) >= 3 {
		// If batch import is used, the batch directory is the second-to-last part before the filename
		return parts[len(parts)-2]
	}
	return ""
}

// GetFilesByBatchDirectory returns all files in a specific batch directory
func (m *FileManager) GetFilesByBatchDirectory(batchDir string) []*File {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	var files []*File

	// Iterate through all files and collect those in the specified batch directory
	for _, file := range m.files {
		// Only include files from the specified batch directory that are part of a batch import
		if file.BatchDir == batchDir {
			files = append(files, file)
		}
	}

	return files
}

// IsInSameBatch checks if two files are in the same batch
func (m *FileManager) IsInSameBatch(file1 *File, file2 *File) bool {
	if file1 == nil || file2 == nil {
		return false
	}

	return m.GetFileBatchDirectory(file1) == m.GetFileBatchDirectory(file2)
}
