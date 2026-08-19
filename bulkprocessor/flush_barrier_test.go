package bulkprocessor

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// flushbarrierProcessor builds only the in-memory pieces Flush needs. Keeping the
// processor detached from Start makes these concurrency tests independent of
// PostgreSQL, S3, and background worker timing.
func flushbarrierProcessor(ctx context.Context) *BulkProcessor {
	return &BulkProcessor{
		config: Config{
			PostgreSQL:       PostgreSQLConfig{Table: "items", Schema: "public"},
			EnableDualBuffer: true,
			FlushSleepTime:   1,
			BufferMaxRecords: 100,
		},
		ctx:               ctx,
		isStarted:         true,
		routingColIndex:   -1,
		versionColIndex:   -1,
		feedFieldIndex:    -1,
		recordQueueV2:     make(chan *Record),
		bufferTaskQueue:   make(chan *BufferTask, 1),
		batchQueue:        make(chan string, 1),
		pendingBatchFiles: make(map[string]int),
		bufferManager:     NewBufferManager("flushbarrier", "process"),
		fileManager:       NewFileManager(nil, "flushbarrier", 100, "process", 1),
		routingHashSet:    make(map[string]struct{}),
	}
}

// Cancellation must participate in the recordsNum wait. The mutex observation
// synchronizes the test with Flush after it has entered that wait, avoiding a
// sleep-based guess about whether the goroutine started.
func TestFlushBarrierFlushRecordWaitIsCancellationAware(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldProcs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := flushbarrierProcessor(ctx)
	p.recordsNum.Store(1)

	flushDone := make(chan error, 1)
	go func() { flushDone <- p.Flush() }()

	lockDeadline := time.NewTimer(2 * time.Second)
	defer lockDeadline.Stop()
	for {
		if !p.flushMutex.TryLock() {
			break // Flush owns it and cannot finish while recordsNum is still one.
		}
		p.flushMutex.Unlock()
		select {
		case <-lockDeadline.C:
			t.Fatal("Flush did not enter its records wait within the bounded timeout")
		default:
			runtime.Gosched()
		}
	}

	cancel()
	cancelDeadline := time.NewTimer(750 * time.Millisecond)
	timedOut := false
	var flushErr error
	select {
	case flushErr = <-flushDone:
		if !cancelDeadline.Stop() {
			<-cancelDeadline.C
		}
	case <-cancelDeadline.C:
		timedOut = true
		// Release current code from its uncancellable one-second polling sleep.
		p.recordsNum.Store(0)
		select {
		case flushErr = <-flushDone:
		case <-time.After(2 * time.Second):
			t.Fatal("Flush remained stuck after test cleanup released recordsNum")
		}
	}

	if timedOut {
		t.Error("Flush did not react to cancellation while waiting for queued records")
	}
	if flushErr == nil {
		t.Error("Flush returned nil after its context was canceled")
	}
}

// A local path whose parent is a regular file makes BufferWriteToFile fail in
// os.MkdirAll without relying on permissions, external services, or timing.
func TestSaveBufferUsesAtomicallyPublishedRuntimeConfig(t *testing.T) {
	p := flushbarrierProcessor(context.Background())
	p.fields = []FieldInfo{{JSONName: "value"}}
	runtimeConfig := p.config
	runtimeConfig.ImportStrategy = CopyFromLocal
	runtimeConfig.DeleteBeforeInsert = true
	runtimeConfig.TuplesPrePartition = 17
	p.runtimeConfig.Store(&runtimeConfig)

	buffer := &Buffer{
		ID: "runtime-config", Records: []*Record{{Tag: OperationInsert, Values: []string{"value"}}},
		LocalFilePath: filepath.Join(t.TempDir(), "buffer.csv"), MaxRecords: 10,
		MaxVersionMap: make(map[RecordIndex]string), FeedbackKeys: make(map[string]bool),
	}
	p.bufferManager.buffers[buffer.ID] = buffer
	if err := p.SaveBufferToFileAndGenerateTask(buffer, false); err != nil {
		t.Fatalf("SaveBufferToFileAndGenerateTask: %v", err)
	}
	select {
	case task := <-p.bufferTaskQueue:
		if task.ImportStrategy != CopyFromLocal {
			t.Fatalf("task import strategy = %d, want refreshed CopyFromLocal", task.ImportStrategy)
		}
	case <-time.After(time.Second):
		t.Fatal("buffer task was not queued")
	}
}

func TestRecycleLocalDirExceptPreservesRecoverableArtifacts(t *testing.T) {
	root := t.TempDir()
	oldDate := time.Now().AddDate(0, 0, -3).Format("2006-01-02")
	protectedFile := filepath.Join(root, oldDate, "table", "old-process", "pending.csv")
	staleFile := filepath.Join(root, oldDate, "table", "other-process", "stale.csv")
	for _, path := range []string{protectedFile, staleFile} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("data"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	manager := NewBufferManager("table", "current-process")
	manager.RecycleLocalDirExcept(root, 2, map[string]struct{}{protectedFile: {}})
	if _, err := os.Stat(protectedFile); err != nil {
		t.Fatalf("recoverable artifact was deleted: %v", err)
	}
	if _, err := os.Stat(staleFile); !os.IsNotExist(err) {
		t.Fatalf("untracked stale artifact still exists: %v", err)
	}
}

func TestNewBufferTaskPreservesFeedbackKeysForEnqueueFailure(t *testing.T) {
	buffer := &Buffer{
		ID: "feedback", FeedbackKeys: map[string]bool{"key-1": true, "key-2": true},
		MaxVersionMap: make(map[RecordIndex]string),
	}
	task := NewBufferTask(buffer, nil, nil, nil, InsertOnConflict)
	if len(task.FeedbackKeys) != 2 {
		t.Fatalf("task feedback keys = %v, want both keys", task.FeedbackKeys)
	}
	if got := buffer.GetFeedbackKeys(); len(got) != 2 {
		t.Fatalf("buffer feedback keys after task creation = %v, want both keys for enqueue error callback", got)
	}
}

func TestFlushBarrierDoesNotWaitForCompletionMetadata(t *testing.T) {
	p := flushbarrierProcessor(context.Background())
	buffer := &Buffer{ID: "checkpoint-pending", status: BufferStatusFrozen}
	p.bufferManager.buffers[buffer.ID] = buffer

	done := make(chan error, 1)
	go func() { done <- p.Flush() }()

	deadline := time.Now().Add(time.Second)
	for {
		if !p.flushMutex.TryLock() {
			break
		}
		p.flushMutex.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("Flush did not start")
		}
		runtime.Gosched()
	}
	p.bufferManager.SetBufferStatus(buffer.ID, BufferStatusCheckpointPending)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Flush returned after committed data with pending metadata: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Flush remained blocked on completion metadata after data was durable")
	}
}

func TestFlushBarrierFlushReturnsSaveBufferFailure(t *testing.T) {
	p := flushbarrierProcessor(context.Background())

	blocker := filepath.Join(t.TempDir(), "regular-file")
	if err := os.WriteFile(blocker, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("create deterministic path blocker: %v", err)
	}

	buffer := &Buffer{
		ID:            "save-failure",
		Records:       []*Record{{Tag: OperationInsert, Values: []string{"value"}}},
		MaxRecords:    100,
		LocalFilePath: filepath.Join(blocker, "buffer"),
		S3FilePath:    "unused",
		status:        BufferStatusActive,
		MaxVersionMap: make(map[RecordIndex]string),
		FeedbackKeys:  make(map[string]bool),
	}
	p.bufferManager.buffers[buffer.ID] = buffer
	p.bufferManager.SetCurrentBuffer(buffer, bufferKey("", false))

	callbackErr := make(chan error, 1)
	p.config.ImportErrorCallback = func(_ string, _ []string, err error, _ any) {
		callbackErr <- err
	}

	flushErr := p.Flush()
	select {
	case err := <-callbackErr:
		if err == nil {
			t.Fatal("SaveBuffer failure callback received a nil error")
		}
	default:
		t.Fatal("deterministic SaveBuffer failure was not reached")
	}
	if flushErr == nil {
		t.Error("Flush returned nil after SaveBufferToFileAndGenerateTask failed")
	}
}
