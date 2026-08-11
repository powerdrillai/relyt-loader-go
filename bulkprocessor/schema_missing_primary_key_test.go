package bulkprocessor

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

type missingprimarykeyRecordWithoutPK struct {
	RoutingID string `relyt:"routing_id"`
	Payload   string `relyt:"payload"`
}

func missingprimarykeyProcessor(ctx context.Context) *BulkProcessor {
	return &BulkProcessor{
		config: Config{
			PostgreSQL:       PostgreSQLConfig{Table: "items"},
			EnableDualBuffer: true,
		},
		ctx:             ctx,
		isStarted:       true, // Keep the test independent of DB/background startup.
		routingColIndex: -1,
		versionColIndex: -1,
		feedFieldIndex:  -1,
		pkColumns:       []string{"id"},
		recordQueueV2:   make(chan *Record, 1),
		routingQueueV2:  make(chan bool, 1),
		insertV2Done:    make(chan struct{}),
		routingHashSet:  make(map[string]struct{}),
		bufferTaskQueue: make(chan *BufferTask, 1),
	}
}

func assertMissingPrimaryKeyActionablePKError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Errorf("InsertV2 accepted a record type with no relyt tag for primary-key column id")
		return
	}
	message := strings.ToLower(err.Error())
	if !strings.Contains(message, "primary") || !strings.Contains(message, "id") {
		t.Errorf("InsertV2 error is not actionable for missing primary-key column id: %v", err)
	}
}

// A missing tagged primary-key field must be rejected while the record schema is
// inspected. In particular, validation must happen before instance registration;
// the nil router makes an attempted registration observable without a database.
func TestMissingPrimaryKeyMissingTaggedPKRejectedBeforeRegistrationOrEnqueue(t *testing.T) {
	p := missingprimarykeyProcessor(context.Background())
	p.isSharded = true
	p.instanceRouter = nil

	var (
		err       error
		recovered any
	)
	func() {
		defer func() { recovered = recover() }()
		err = p.InsertV2("file-1", "tenant-1", []missingprimarykeyRecordWithoutPK{{
			RoutingID: "tenant-1",
			Payload:   "value",
		}})
	}()

	if recovered != nil {
		t.Errorf("InsertV2 reached instance registration before validating the primary-key tag: %v", recovered)
	}
	assertMissingPrimaryKeyActionablePKError(t, err)
	if got := len(p.recordQueueV2); got != 0 {
		t.Errorf("schema validation failure enqueued %d record(s), want 0", got)
	}
}

// This exercises the original failure end to end without letting a background
// goroutine panic terminate the test process: InsertV2 accepts the malformed
// schema, stores -1 in pkColumnsIndex, and InsertThreadV2 later indexes Values
// with -1. Correct code returns an actionable synchronous error and queues
// nothing, so the bounded worker exits normally after cancellation.
func TestMissingPrimaryKeyMissingTaggedPKRejectedWithoutAsyncPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := missingprimarykeyProcessor(ctx)

	err := p.InsertV2("file-1", "tenant-1", []missingprimarykeyRecordWithoutPK{{
		RoutingID: "tenant-1",
		Payload:   "value",
	}})
	queuedAfterInsert := len(p.recordQueueV2)

	workerResult := make(chan any, 1)
	p.bufferThreadWg.Add(1)
	go func() {
		defer func() { workerResult <- recover() }()
		p.InsertThreadV2()
	}()

	// With the desired synchronous rejection there is no work for the worker;
	// cancel it. With current code, leave it running so it deterministically
	// consumes the accepted record and exposes the asynchronous bounds panic.
	if err != nil {
		cancel()
	}

	var workerPanic any
	select {
	case workerPanic = <-workerResult:
	case <-time.After(2 * time.Second):
		cancel()
		select {
		case workerPanic = <-workerResult:
		case <-time.After(2 * time.Second):
			t.Fatal("InsertThreadV2 did not terminate within the bounded timeout")
		}
	}

	assertMissingPrimaryKeyActionablePKError(t, err)
	if queuedAfterInsert != 0 {
		t.Errorf("InsertV2 enqueued %d malformed record(s), want 0", queuedAfterInsert)
	}
	if workerPanic != nil {
		t.Errorf("InsertThreadV2 panicked after InsertV2 accepted the missing-PK schema: %s", fmt.Sprint(workerPanic))
	}
}
