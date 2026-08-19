package bulkprocessor

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"runtime"
	"testing"
	"time"
)

type processorlifecycleRow struct {
	ID      string `relyt:"id"`
	Payload string `relyt:"payload"`
}

// processorlifecycleProcessor is deliberately started without background consumers.
// That makes a full admission queue a deterministic stand-in for a consumer
// which has already stopped during Shutdown. All fields used by Shutdown are
// real, empty managers, so the lifecycle method itself is exercised rather
// than emulated by setting isShutdown in the test.
func processorlifecycleProcessor(t *testing.T) *BulkProcessor {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	fields, err := GetStructFields(reflect.TypeFor[processorlifecycleRow]())
	if err != nil {
		t.Fatalf("get test record fields: %v", err)
	}

	return &BulkProcessor{
		config: Config{
			PostgreSQL:       PostgreSQLConfig{Schema: "public", Table: "processorlifecycle_items"},
			EnableDualBuffer: true,
		},
		processId:         "processorlifecycle",
		pgClient:          &PostgreSQLClient{},
		fileManager:       NewFileManager(nil, "processorlifecycle", 10, "processorlifecycle", 1),
		bufferManager:     NewBufferManager("processorlifecycle", "processorlifecycle"),
		ctx:               ctx,
		cancel:            cancel,
		isStarted:         true,
		structType:        reflect.TypeFor[processorlifecycleRow](),
		fields:            fields,
		feedFieldIndex:    -1,
		routingColIndex:   -1,
		versionColIndex:   -1,
		routingHashSet:    make(map[string]struct{}),
		feedbackKeys:      make(map[string]bool),
		pendingBatchFiles: make(map[string]int),
		recordsQueue:      make(chan []string, 1),
		recordQueueV2:     make(chan *Record, 1),
		bufferTaskQueue:   make(chan *BufferTask, 1),
		routingQueue:      make(chan bool, 1),
		routingQueueV2:    make(chan bool, 1),
		insertV2Done:      make(chan struct{}),
	}
}

func processorlifecycleShutdownBounded(t *testing.T, p *BulkProcessor) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- p.Shutdown() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Shutdown returned an error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown deadlocked")
	}
}

// processorlifecycleWaitForAdmissionWaiter observes the blocked state rather than
// relying on a scheduling sleep. The queue is already full, and seeing the
// public method's frame blocked in either an unconditional channel send or a
// shutdown-aware select proves that its initial closed check has completed and
// it is at the admission point.
func processorlifecycleWaitForAdmissionWaiter(t *testing.T, method string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	stackSize := 64 << 10
	const maxStackSize = 32 << 20
	for time.Now().Before(deadline) {
		buf := make([]byte, stackSize)
		n := runtime.Stack(buf, true)
		if n == len(buf) {
			if stackSize == maxStackSize {
				t.Fatalf("all-goroutine stack dump exceeded %d bytes while waiting for %s", maxStackSize, method)
			}
			stackSize *= 2
			if stackSize > maxStackSize {
				stackSize = maxStackSize
			}
			continue
		}
		for _, stack := range bytes.Split(buf[:n], []byte("\n\n")) {
			blockedAdmission := bytes.Contains(stack, []byte("[chan send]")) ||
				bytes.Contains(stack, []byte("[select"))
			if blockedAdmission &&
				bytes.Contains(stack, []byte(".(*BulkProcessor)."+method+"(")) {
				return
			}
		}
		runtime.Gosched()
	}
	t.Fatalf("%s did not reach its blocked queue admission", method)
}

func processorlifecycleWaitForShutdownStart(t *testing.T, p *BulkProcessor) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p.mutex.RLock()
		closing := p.isShutdown
		p.mutex.RUnlock()
		if closing {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("Shutdown did not publish the closing state")
}

func TestProcessorLifecycleStartRejectedOnceShutdownStarts(t *testing.T) {
	p := processorlifecycleProcessor(t)
	p.config.PostgreSQL.Table = "" // A buggy Start remains side-effect free after winning the lock.
	p.isStarted = false

	// Keep Shutdown in progress after it publishes isShutdown. This exercises
	// Start concurrently with closing, not merely as a post-Shutdown restart.
	p.importerWg.Add(1)
	workerReleased := false
	shutdownJoined := false
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- p.Shutdown() }()
	t.Cleanup(func() {
		if !workerReleased {
			p.importerWg.Done()
			workerReleased = true
		}
		if !shutdownJoined {
			select {
			case <-shutdownDone:
				shutdownJoined = true
			case <-time.After(2 * time.Second):
				t.Error("Shutdown goroutine did not terminate during cleanup")
			}
		}
	})
	processorlifecycleWaitForShutdownStart(t, p)

	startErr := p.Start()
	p.importerWg.Done()
	workerReleased = true
	select {
	case err := <-shutdownDone:
		shutdownJoined = true
		if err != nil {
			t.Errorf("Shutdown returned an error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not finish after its test worker exited")
	}

	if !errors.Is(startErr, ErrProcessorClosed) {
		t.Errorf("Start admitted after Shutdown began: got %v, want ErrProcessorClosed", startErr)
	}
	p.mutex.RLock()
	started := p.isStarted
	p.mutex.RUnlock()
	if started {
		t.Error("Start marked a closing processor as started")
	}
}

// Every case fills the relevant queue, waits until the API is blocked in its
// send, and then runs the real Shutdown. A lifecycle-aware admission send must
// wake and return ErrProcessorClosed. The old unconditional send stays blocked;
// once the test removes the original filler it returns nil and leaves newly
// accepted, permanently unconsumed work in the stopped processor.
func TestProcessorLifecycleBlockedAdmissionsWakeAndFailOnShutdown(t *testing.T) {
	tests := []struct {
		name   string
		method string
		v2     bool
		call   func(*BulkProcessor) error
	}{
		{
			name:   "InsertV2",
			method: "InsertV2",
			v2:     true,
			call: func(p *BulkProcessor) error {
				return p.InsertV2("file-new", "tenant", []processorlifecycleRow{{ID: "new", Payload: "v2"}})
			},
		},
		{
			name:   "Insert",
			method: "Insert",
			call: func(p *BulkProcessor) error {
				return p.Insert([]processorlifecycleRow{{ID: "new", Payload: "legacy"}})
			},
		},
		{
			name:   "DeleteV2",
			method: "DeleteV2",
			v2:     true,
			call: func(p *BulkProcessor) error {
				return p.DeleteV2("file-new", "tenant")
			},
		},
		{
			name:   "DeleteAsyncGroupV2",
			method: "DeleteAsyncGroupV2",
			v2:     true,
			call: func(p *BulkProcessor) error {
				return p.DeleteAsyncGroupV2("group-new", "tenant")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := processorlifecycleProcessor(t)
			if tc.v2 {
				p.recordQueueV2 <- &Record{FileID: "filler"}
			} else {
				p.recordsQueue <- []string{"filler"}
			}

			result := make(chan error, 1)
			callDone := make(chan struct{})
			go func() {
				defer close(callDone)
				result <- tc.call(p)
			}()
			t.Cleanup(func() {
				// Also make helper failures bounded: cancellation releases a fixed
				// select waiter, while removing one item releases the old send.
				p.cancel()
				if tc.v2 {
					select {
					case <-p.recordQueueV2:
					default:
					}
				} else {
					select {
					case <-p.recordsQueue:
					default:
					}
				}
				select {
				case <-callDone:
				case <-time.After(2 * time.Second):
					t.Errorf("%s caller did not terminate during cleanup", tc.name)
				}
			})
			processorlifecycleWaitForAdmissionWaiter(t, tc.method)
			processorlifecycleShutdownBounded(t, p)

			var (
				err                           error
				remainedBlocked               bool
				removedFillerBeforeCompletion bool
			)
			select {
			case err = <-result:
			case <-time.After(500 * time.Millisecond):
				remainedBlocked = true
				// Unstick buggy code so this test never leaks its caller goroutine.
				// Validate that the item making the queue full really is the original
				// filler; a replacement here is already an orphaning failure.
				if tc.v2 {
					select {
					case record := <-p.recordQueueV2:
						removedFillerBeforeCompletion = true
						if record == nil || record.FileID != "filler" {
							t.Errorf("queue filler was replaced before %s completed: %#v", tc.name, record)
						}
					default:
						t.Error("full admission queue unexpectedly had no item to release")
					}
				} else {
					select {
					case values := <-p.recordsQueue:
						removedFillerBeforeCompletion = true
						if len(values) != 1 || values[0] != "filler" {
							t.Errorf("queue filler was replaced before %s completed: %#v", tc.name, values)
						}
					default:
						t.Error("full admission queue unexpectedly had no item to release")
					}
				}
				select {
				case err = <-result:
				case <-time.After(2 * time.Second):
					t.Fatal("admission remained blocked even after queue capacity was restored")
				}
			}

			// Inspect the queue after every completion, not just after the known
			// blocking failure. If the filler was removed to release buggy code,
			// the queue must now be empty. Otherwise its sole item must still be
			// the original filler. This also catches code which returns a closed
			// error but nevertheless enqueues the caller's work.
			acceptedAfterClosing := false
			entryFound := false
			entryIsFiller := false
			if tc.v2 {
				select {
				case record := <-p.recordQueueV2:
					entryFound = true
					entryIsFiller = record != nil && record.FileID == "filler"
				default:
				}
			} else {
				select {
				case values := <-p.recordsQueue:
					entryFound = true
					entryIsFiller = len(values) == 1 && values[0] == "filler"
				default:
				}
			}
			if removedFillerBeforeCompletion {
				acceptedAfterClosing = entryFound
			} else {
				if !entryFound {
					t.Errorf("%s removed the original accepted filler during Shutdown", tc.name)
				} else if !entryIsFiller {
					acceptedAfterClosing = true
				}
			}

			if remainedBlocked {
				t.Errorf("%s stayed blocked after Shutdown instead of waking with an error", tc.name)
			}
			if acceptedAfterClosing {
				t.Errorf("%s enqueued orphaned work after Shutdown completed", tc.name)
			}
			if got := p.recordsNum.Load(); got != 0 {
				t.Errorf("%s left recordsNum = %d after rejected admission, want 0", tc.name, got)
			}
			if !errors.Is(err, ErrProcessorClosed) {
				t.Errorf("%s returned %v after Shutdown, want ErrProcessorClosed", tc.name, err)
			}
		})
	}
}
