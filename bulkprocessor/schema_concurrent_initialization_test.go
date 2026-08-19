package bulkprocessor

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const concurrentschemaWaitTimeout = 10 * time.Second

// These types deliberately have different relyt schemas while retaining the
// same number of Go fields. Reusing FieldInfo from one type with the other is
// therefore silent data corruption rather than an incidental reflect panic.
type concurrentschemaSchemaA struct {
	ID   string `relyt:"id"`
	Name string `relyt:"name"`
}

type concurrentschemaSchemaB struct {
	Key   string `relyt:"key"`
	Count int    `relyt:"count"`
}

type concurrentschemaRoutedRecord struct {
	ID        string `relyt:"id"`
	RoutingID string `relyt:"routing_id"`
}

type concurrentschemaMissingRoutingRecord struct {
	ID string `relyt:"id"`
}

// concurrentschemaStartedProcessor keeps InsertV2 unit tests independent of database
// and background-worker behavior. A buffered queue also lets the tests inspect
// exactly which calls were accepted.
func concurrentschemaStartedProcessor(queueSize int) *BulkProcessor {
	return &BulkProcessor{
		config: Config{
			PostgreSQL:       PostgreSQLConfig{Table: "concurrentschema_items"},
			EnableDualBuffer: true,
		},
		isStarted:       true,
		ctx:             context.Background(),
		recordQueueV2:   make(chan *Record, queueSize),
		feedFieldIndex:  -1,
		routingColIndex: -1,
		versionColIndex: -1,
	}
}

// concurrentschemaColdProcessor supplies the otherwise-New-initialized pieces which
// Start's workers access. It uses a live context so the test follows the normal
// pre-Start lifecycle. The lazy pgx pool makes worker activity harmless and
// never needs a live server.
func concurrentschemaColdProcessor(t *testing.T, queueSize int) *BulkProcessor {
	t.Helper()

	pool, err := pgxpool.New(context.Background(),
		"postgres://localhost:1/concurrentschema?connect_timeout=1")
	if err != nil {
		t.Fatalf("create lazy test pool: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &BulkProcessor{
		config: Config{
			PostgreSQL: PostgreSQLConfig{
				Table:  "concurrentschema_items",
				Schema: "public",
			},
			EnableDualBuffer: true,
			BufferMaxRecords: queueSize + 1,
			LocalFilePrefix:  t.TempDir(),
			FileWriteTimeout: 1,
			BGWorkerInterval: 1,
		},
		processId:         "concurrentschema",
		pgClient:          &PostgreSQLClient{pool: pool},
		fileManager:       NewFileManager(nil, "concurrentschema", queueSize+1, "concurrentschema", 1),
		bufferManager:     NewBufferManager("concurrentschema", "concurrentschema"),
		ctx:               ctx,
		cancel:            cancel,
		recordQueueV2:     make(chan *Record, queueSize),
		bufferTaskQueue:   make(chan *BufferTask, 1),
		insertV2Done:      make(chan struct{}),
		feedFieldIndex:    -1,
		routingColIndex:   -1,
		versionColIndex:   -1,
		feedbackKeys:      make(map[string]bool),
		routingHashSet:    make(map[string]struct{}),
		pendingBatchFiles: make(map[string]int),
		lastFlushTime:     time.Now(),
	}

	t.Cleanup(func() {
		cancel()
		done := make(chan struct{})
		go func() {
			p.importerWg.Wait()
			p.bufferThreadWg.Wait()
			p.workerWg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(concurrentschemaWaitTimeout):
			t.Errorf("timed out waiting for cold processor workers to stop")
		}
		pool.Close()
	})
	return p
}

func concurrentschemaWait(t *testing.T, wg *sync.WaitGroup, what string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(concurrentschemaWaitTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// There is no dedicated record-type mismatch sentinel in the current API. To
// be unambiguous, a compatibility error must name both the already-selected
// concrete type and the concrete type supplied by the rejected call.
func concurrentschemaIsPreciseTypeMismatch(err error, selected, supplied reflect.Type) bool {
	if err == nil || selected == nil || supplied == nil || selected == supplied {
		return false
	}
	message := err.Error()
	return strings.Contains(message, selected.String()) && strings.Contains(message, supplied.String())
}

func concurrentschemaAssertMetadataSnapshot(t *testing.T, p *BulkProcessor, wantType reflect.Type) {
	t.Helper()
	if p.structType != wantType {
		t.Errorf("published structType = %v, want %v", p.structType, wantType)
	}
	wantFields, err := GetStructFields(wantType)
	if err != nil {
		t.Fatalf("derive expected metadata for %v: %v", wantType, err)
	}
	if len(p.fields) != len(wantFields) {
		t.Errorf("published field count = %d, want %d", len(p.fields), len(wantFields))
		return
	}
	for i := range wantFields {
		got, want := p.fields[i], wantFields[i]
		if got.Name != want.Name || got.JSONName != want.JSONName || got.Type != want.Type || got.Index != want.Index {
			t.Errorf("published field[%d] = {Name:%q JSONName:%q Type:%v Index:%d}, want {Name:%q JSONName:%q Type:%v Index:%d}",
				i, got.Name, got.JSONName, got.Type, got.Index, want.Name, want.JSONName, want.Type, want.Index)
		}
	}
}

func TestConcurrentSchemaConcurrentColdInsertV2StartsOnce(t *testing.T) {
	const callers = 64
	p := concurrentschemaColdProcessor(t, callers)
	// Also cancel on an early fatal path; the explicit call below cancels as
	// soon as all admissions finish in the normal path.
	defer p.cancel()

	start := make(chan struct{})
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			errs <- p.InsertV2(fmt.Sprintf("file-%d", i), "tenant",
				[]concurrentschemaSchemaA{{ID: fmt.Sprint(i), Name: "valid"}})
		}(i)
	}
	close(start)
	concurrentschemaWait(t, &wg, "concurrent cold InsertV2 calls")
	p.cancel()
	close(errs)

	var failures []error
	for err := range errs {
		if err != nil {
			failures = append(failures, err)
		}
	}
	if len(failures) != 0 {
		t.Fatalf("%d/%d concurrent valid cold inserts were rejected (Start must be single-flight): first error: %v",
			len(failures), callers, failures[0])
	}
	concurrentschemaAssertMetadataSnapshot(t, p, reflect.TypeFor[concurrentschemaSchemaA]())
}

func TestConcurrentSchemaConcurrentSameTypeInitializationAcceptsEveryCall(t *testing.T) {
	const callers = 32
	p := concurrentschemaStartedProcessor(callers)

	start := make(chan struct{})
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			errs <- p.InsertV2(fmt.Sprintf("same-%d", i), "tenant",
				[]concurrentschemaSchemaA{{ID: fmt.Sprint(i), Name: "same schema"}})
		}(i)
	}
	close(start)
	concurrentschemaWait(t, &wg, "concurrent same-type InsertV2 calls")
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("concurrent insert of the selected type was rejected: %v", err)
		}
	}
	concurrentschemaAssertMetadataSnapshot(t, p, reflect.TypeFor[concurrentschemaSchemaA]())
	if got, want := len(p.recordQueueV2), callers; got != want {
		t.Errorf("accepted records = %d, want %d", got, want)
	}
}

func TestConcurrentSchemaConcurrentConflictingInitializationHasOneWinner(t *testing.T) {
	const (
		rounds  = 6
		callers = 32
	)
	type result struct {
		typ        reflect.Type
		fileID     string
		wantValues []string
		err        error
	}

	for round := 0; round < rounds; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			p := concurrentschemaStartedProcessor(callers)
			start := make(chan struct{})
			results := make(chan result, callers)
			var wg sync.WaitGroup
			wg.Add(callers)

			for i := 0; i < callers; i++ {
				i := i
				if i%2 == 0 {
					go func() {
						defer wg.Done()
						<-start
						fileID := fmt.Sprintf("a-%d-%d", round, i)
						id, name := fmt.Sprintf("id-%d", i), fmt.Sprintf("name-%d", i)
						results <- result{
							typ:        reflect.TypeFor[concurrentschemaSchemaA](),
							fileID:     fileID,
							wantValues: []string{id, name},
							err: p.InsertV2(fileID, "tenant",
								[]concurrentschemaSchemaA{{ID: id, Name: name}}),
						}
					}()
					continue
				}
				go func() {
					defer wg.Done()
					<-start
					fileID := fmt.Sprintf("b-%d-%d", round, i)
					key, count := fmt.Sprintf("key-%d", i), i
					results <- result{
						typ:        reflect.TypeFor[concurrentschemaSchemaB](),
						fileID:     fileID,
						wantValues: []string{key, fmt.Sprint(count)},
						err: p.InsertV2(fileID, "tenant",
							[]concurrentschemaSchemaB{{Key: key, Count: count}}),
					}
				}()
			}

			close(start)
			concurrentschemaWait(t, &wg, "concurrent conflicting InsertV2 calls")
			close(results)

			all := make([]result, 0, callers)
			for result := range results {
				all = append(all, result)
			}

			// The published type identifies the winner. Every call of that type
			// must succeed, every call of the other type must fail precisely, and
			// the rest of the metadata must be the same winner's snapshot.
			winner := p.structType
			typeA, typeB := reflect.TypeFor[concurrentschemaSchemaA](), reflect.TypeFor[concurrentschemaSchemaB]()
			if winner != typeA && winner != typeB {
				t.Fatalf("published winner type = %v, want %v or %v", winner, typeA, typeB)
			}

			accepted := make(map[string][]string, callers/2)
			winnerRejected, loserAccepted, imprecise := 0, 0, 0
			var firstWinnerErr, firstImprecise error
			for _, result := range all {
				if result.typ == winner {
					if result.err != nil {
						winnerRejected++
						if firstWinnerErr == nil {
							firstWinnerErr = result.err
						}
						continue
					}
					accepted[result.fileID] = result.wantValues
					continue
				}
				if result.err == nil {
					loserAccepted++
					continue
				}
				if !concurrentschemaIsPreciseTypeMismatch(result.err, winner, result.typ) {
					imprecise++
					if firstImprecise == nil {
						firstImprecise = result.err
					}
				}
			}
			if winnerRejected != 0 {
				t.Errorf("%d calls of winning type %v were rejected; first error: %v", winnerRejected, winner, firstWinnerErr)
			}
			if loserAccepted != 0 {
				t.Errorf("%d calls of the conflicting losing type were accepted", loserAccepted)
			}
			if imprecise != 0 {
				t.Errorf("%d losing calls returned errors that did not name selected %v and supplied concrete type; first: %v",
					imprecise, winner, firstImprecise)
			}
			if got, want := len(accepted), callers/2; got != want {
				t.Errorf("accepted %d calls of common winning type, want %d", got, want)
			}
			concurrentschemaAssertMetadataSnapshot(t, p, winner)

			if got, want := len(p.recordQueueV2), len(accepted); got != want {
				t.Errorf("queued records = %d, accepted calls = %d", got, want)
			}
			queued := len(p.recordQueueV2)
			seen := make(map[string]bool, queued)
			unexpected, duplicates, corrupt := 0, 0, 0
			var firstUnexpected string
			for i := 0; i < queued; i++ {
				record := <-p.recordQueueV2
				want, ok := accepted[record.FileID]
				if !ok {
					unexpected++
					if firstUnexpected == "" {
						firstUnexpected = record.FileID
					}
					continue
				}
				if seen[record.FileID] {
					duplicates++
				}
				seen[record.FileID] = true
				if !reflect.DeepEqual(record.Values, want) {
					corrupt++
				}
			}
			missing := 0
			for fileID := range accepted {
				if !seen[fileID] {
					missing++
				}
			}
			if unexpected != 0 || duplicates != 0 || corrupt != 0 || missing != 0 {
				t.Errorf("queued snapshot mismatch: unexpected=%d (first %q) duplicates=%d corrupt=%d missing=%d",
					unexpected, firstUnexpected, duplicates, corrupt, missing)
			}
		})
	}
}

func TestConcurrentSchemaIncompatibleLaterTypeIsRejectedBeforeEnqueue(t *testing.T) {
	p := concurrentschemaStartedProcessor(2)
	selected := reflect.TypeFor[concurrentschemaSchemaA]()
	supplied := reflect.TypeFor[concurrentschemaSchemaB]()
	if err := p.InsertV2("first", "tenant",
		[]concurrentschemaSchemaA{{ID: "1", Name: "selected"}}); err != nil {
		t.Fatalf("initialize schema: %v", err)
	}

	err := p.InsertV2("second", "tenant",
		[]concurrentschemaSchemaB{{Key: "not-an-id", Count: 42}})
	if !concurrentschemaIsPreciseTypeMismatch(err, selected, supplied) {
		t.Fatalf("incompatible later type error must name selected %v and supplied %v, got %v",
			selected, supplied, err)
	}
	if got := len(p.recordQueueV2); got != 1 {
		t.Fatalf("incompatible call changed queue length to %d, want 1", got)
	}
	concurrentschemaAssertMetadataSnapshot(t, p, selected)
	record := <-p.recordQueueV2
	if want := []string{"1", "selected"}; !reflect.DeepEqual(record.Values, want) {
		t.Fatalf("selected schema queued values = %v, want %v", record.Values, want)
	}
}

func TestConcurrentSchemaFailedValidationDoesNotPoisonMetadata(t *testing.T) {
	router := &InstanceRouter{
		clients:      make(map[string]*instanceEntry),
		routingCache: map[string]string{"tenant": "instance-1"},
		inFlight:     make(map[string]chan struct{}),
	}
	p := concurrentschemaStartedProcessor(1)
	p.isSharded = true
	p.instanceRouter = router

	err := p.InsertV2("bad", "tenant",
		[]concurrentschemaMissingRoutingRecord{{ID: "bad"}})
	if !errors.Is(err, ErrRoutingColumnRequired) {
		t.Fatalf("schema without routing_id: want ErrRoutingColumnRequired, got %v", err)
	}
	if p.structType != nil || len(p.fields) != 0 {
		t.Errorf("failed validation published metadata: structType=%v fields=%v", p.structType, p.fields)
	}

	if err := p.InsertV2("good", "tenant",
		[]concurrentschemaRoutedRecord{{ID: "good", RoutingID: "tenant"}}); err != nil {
		t.Fatalf("valid type after failed first call was poisoned: %v", err)
	}
	concurrentschemaAssertMetadataSnapshot(t, p, reflect.TypeFor[concurrentschemaRoutedRecord]())
	if got := len(p.recordQueueV2); got != 1 {
		t.Errorf("accepted records after recovery = %d, want 1", got)
	}
}
