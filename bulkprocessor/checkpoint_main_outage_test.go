package bulkprocessor

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// checkpointoutageRow is deliberately small: the fake healthy shard below only has
// to acknowledge an INSERT, while the real routing and import paths still run.
func TestDeltaCheckpointInsertIsIdempotentAfterAmbiguousCommit(t *testing.T) {
	sql := strings.ToUpper(insertDeltaCheckpointSQL)
	if !strings.Contains(sql, "ON CONFLICT (PROCESS_ID, FILEPATH) DO NOTHING") {
		t.Fatalf("checkpoint retry is not idempotent: %s", insertDeltaCheckpointSQL)
	}
}

func TestTerminalCheckpointFailureIsAtomicWithInitialInsert(t *testing.T) {
	sql := strings.ToUpper(failDeltaCheckpointSQL)
	if !strings.Contains(sql, "INSERT INTO") ||
		!strings.Contains(sql, "ON CONFLICT (PROCESS_ID, FILEPATH) DO UPDATE") ||
		!strings.Contains(sql, "STATUS = EXCLUDED.STATUS") {
		t.Fatalf("terminal checkpoint write is not an atomic failure upsert: %s", failDeltaCheckpointSQL)
	}
}

type checkpointoutageRow struct {
	Value     string `relyt:"value"`
	RoutingID string `relyt:"routing_id"`
}

// checkpointoutagePG is a bounded PostgreSQL-protocol equivalence server. It accepts
// authentication, BEGIN, the primary-key lookup, one INSERT, and COMMIT. This
// lets the tests establish that a shard transaction really reached COMMIT
// without depending on, or modifying, a shared database.
type checkpointoutagePG struct {
	listener net.Listener
	commits  chan struct{}
	errs     chan error
	wg       sync.WaitGroup
}

func checkpointoutageStartHealthyShard(t *testing.T) (*PostgreSQLClient, *checkpointoutagePG) {
	t.Helper()

	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen for bounded shard server: %v", err)
	}
	server := &checkpointoutagePG{
		listener: listener,
		commits:  make(chan struct{}, 4),
		errs:     make(chan error, 4),
	}
	server.wg.Add(1)
	go func() {
		defer server.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					server.errs <- err
				}
				return
			}
			server.wg.Add(1)
			go func() {
				defer server.wg.Done()
				if err := server.serve(conn); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
					server.errs <- err
				}
			}()
		}
	}()

	cfg, err := pgxpool.ParseConfig(fmt.Sprintf(
		"postgres://test@%s/test?sslmode=disable", listener.Addr().String()))
	if err != nil {
		listener.Close()
		t.Fatalf("parse bounded shard config: %v", err)
	}
	// With simple protocol, parameterized SDK statements arrive as one bounded
	// Query frame and the equivalence server need not emulate statement caches.
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	cfg.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		listener.Close()
		t.Fatalf("create bounded shard pool: %v", err)
	}
	client := &PostgreSQLClient{
		pool: pool,
		config: PostgreSQLConfig{
			Schema: "public",
			Table:  "checkpointoutage_items",
		},
	}

	t.Cleanup(func() {
		pool.Close()
		listener.Close()
		server.wg.Wait()
		select {
		case err := <-server.errs:
			t.Errorf("bounded shard protocol server failed: %v", err)
		default:
		}
	})
	return client, server
}

func (s *checkpointoutagePG) serve(conn net.Conn) error {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// StartupMessage has no leading message type.
	startupLength, err := checkpointoutageReadInt32(reader)
	if err != nil {
		return err
	}
	if startupLength < 8 || startupLength > 1<<20 {
		return fmt.Errorf("invalid startup length %d", startupLength)
	}
	if _, err := io.CopyN(io.Discard, reader, int64(startupLength-4)); err != nil {
		return err
	}
	if err := checkpointoutageWriteMessage(conn, 'R', checkpointoutageInt32(0)); err != nil { // AuthenticationOk
		return err
	}
	if err := checkpointoutageWriteMessage(conn, 'S', []byte("server_version\x0015.0\x00")); err != nil {
		return err
	}
	if err := checkpointoutageWriteMessage(conn, 'S', []byte("client_encoding\x00UTF8\x00")); err != nil {
		return err
	}
	if err := checkpointoutageWriteMessage(conn, 'S', []byte("standard_conforming_strings\x00on\x00")); err != nil {
		return err
	}
	if err := checkpointoutageWriteMessage(conn, 'K', append(checkpointoutageInt32(8), checkpointoutageInt32(9)...)); err != nil {
		return err
	}
	if err := checkpointoutageWriteMessage(conn, 'Z', []byte{'I'}); err != nil {
		return err
	}

	for {
		messageType, err := reader.ReadByte()
		if err != nil {
			return err
		}
		length, err := checkpointoutageReadInt32(reader)
		if err != nil {
			return err
		}
		if length < 4 || length > 1<<20 {
			return fmt.Errorf("invalid frontend message length %d", length)
		}
		body := make([]byte, length-4)
		if _, err := io.ReadFull(reader, body); err != nil {
			return err
		}
		if messageType == 'X' {
			return nil
		}
		if messageType != 'Q' {
			return fmt.Errorf("unexpected frontend message %q", messageType)
		}

		query := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(string(body), "\x00")))
		switch {
		case strings.HasPrefix(query, "begin"):
			if err := checkpointoutageWriteCommand(conn, "BEGIN", 'T'); err != nil {
				return err
			}
		case strings.Contains(query, "from pg_index"):
			// GetTablePrimaryKeys expects one text column and accepts zero rows.
			if err := checkpointoutageWriteMessage(conn, 'T', checkpointoutageOneTextColumn("attname")); err != nil {
				return err
			}
			if err := checkpointoutageWriteCommand(conn, "SELECT 0", 'T'); err != nil {
				return err
			}
		case strings.HasPrefix(query, "insert into"):
			if err := checkpointoutageWriteCommand(conn, "INSERT 0 1", 'T'); err != nil {
				return err
			}
		case strings.HasPrefix(query, "commit"):
			if err := checkpointoutageWriteCommand(conn, "COMMIT", 'I'); err != nil {
				return err
			}
			select {
			case s.commits <- struct{}{}:
			default:
			}
		case strings.HasPrefix(query, "rollback"):
			if err := checkpointoutageWriteCommand(conn, "ROLLBACK", 'I'); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected query %q", query)
		}
	}
}

func checkpointoutageReadInt32(r io.Reader) (int32, error) {
	var value int32
	err := binary.Read(r, binary.BigEndian, &value)
	return value, err
}

func checkpointoutageInt32(value int32) []byte {
	result := make([]byte, 4)
	binary.BigEndian.PutUint32(result, uint32(value))
	return result
}

func checkpointoutageWriteMessage(w io.Writer, typ byte, body []byte) error {
	message := make([]byte, 5+len(body))
	message[0] = typ
	binary.BigEndian.PutUint32(message[1:5], uint32(len(body)+4))
	copy(message[5:], body)
	_, err := w.Write(message)
	return err
}

func checkpointoutageWriteCommand(w io.Writer, command string, transactionStatus byte) error {
	if err := checkpointoutageWriteMessage(w, 'C', append([]byte(command), 0)); err != nil {
		return err
	}
	return checkpointoutageWriteMessage(w, 'Z', []byte{transactionStatus})
}

func checkpointoutageOneTextColumn(name string) []byte {
	body := make([]byte, 2)
	binary.BigEndian.PutUint16(body, 1)
	body = append(body, []byte(name)...)
	body = append(body, 0)
	body = append(body, checkpointoutageInt32(0)...)  // table oid
	body = append(body, 0, 0)                         // attribute number
	body = append(body, checkpointoutageInt32(25)...) // TEXT oid
	body = append(body, 0xff, 0xff)                   // type size -1
	body = append(body, 0xff, 0xff, 0xff, 0xff)       // type modifier -1
	body = append(body, 0, 0)                         // text format
	return body
}

// checkpointoutageUnavailableMain returns a pool whose connection seam either waits
// for release or fails immediately. No socket and no shared server is touched.
func checkpointoutageUnavailableMain(t *testing.T, release <-chan struct{}) (*PostgreSQLClient, <-chan struct{}) {
	t.Helper()

	cfg, err := pgxpool.ParseConfig("postgres://test@localhost:1/test?sslmode=disable")
	if err != nil {
		t.Fatalf("parse unavailable-main config: %v", err)
	}
	attempted := make(chan struct{})
	var once sync.Once
	cfg.BeforeConnect = func(ctx context.Context, _ *pgx.ConnConfig) error {
		once.Do(func() { close(attempted) })
		if release == nil {
			return errors.New("checkpointoutage: control plane unavailable")
		}
		select {
		case <-release:
			return errors.New("checkpointoutage: control plane unavailable")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("create unavailable-main pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return &PostgreSQLClient{pool: pool}, attempted
}

func checkpointoutageProcessor(main, shard *PostgreSQLClient) *BulkProcessor {
	router := &InstanceRouter{
		main: main,
		clients: map[string]*instanceEntry{
			"shard-a": {client: shard, connstr: "cached", status: instanceStatusActive},
		},
		routingCache: map[string]string{"tenant-a": "shard-a"},
		inFlight:     make(map[string]chan struct{}),
	}
	return &BulkProcessor{
		config: Config{
			PostgreSQL:           PostgreSQLConfig{Schema: "public", Table: "checkpointoutage_items"},
			EnableDualBuffer:     true,
			ImportStrategy:       InsertOnConflict,
			InsertIntoBatchSize:  1,
			ImportTimeout:        1,
			ImportErrorSleepTime: 1,
			TaskTimeout:          5,
			RetrySleepMaxTime:    1,
		},
		processId:       "checkpointoutage-process",
		pgClient:        main,
		instanceRouter:  router,
		isSharded:       true,
		isStarted:       true,
		ctx:             context.Background(),
		bufferManager:   NewBufferManager("checkpointoutage", "checkpointoutage-process"),
		recordQueueV2:   make(chan *Record, 2),
		fields:          []FieldInfo{{JSONName: "value"}, {JSONName: "routing_id"}},
		routingColIndex: 1,
		versionColIndex: -1,
		feedFieldIndex:  -1,
	}
}

func checkpointoutageCommittedTask(t *testing.T, p *BulkProcessor) (*BufferTask, string) {
	t.Helper()
	artifact := filepath.Join(t.TempDir(), "recoverable-buffer")
	if err := os.MkdirAll(artifact, 0o755); err != nil {
		t.Fatalf("create local artifact: %v", err)
	}
	if err := os.WriteFile(GetLocalFileFullPath(artifact), []byte("committed,tenant-a\n"), 0o600); err != nil {
		t.Fatalf("write local artifact: %v", err)
	}
	buffer := &Buffer{
		ID:            "checkpointoutage-task",
		Records:       []*Record{{Tag: OperationInsert, Values: []string{"committed", "tenant-a"}}},
		LocalFilePath: artifact,
		S3FilePath:    "unused",
		status:        BufferStatusFlushed,
		MaxVersionMap: make(map[RecordIndex]string),
		FeedbackKeys:  make(map[string]bool),
		InstanceID:    "shard-a",
	}
	p.bufferManager.buffers[buffer.ID] = buffer
	return NewBufferTask(buffer, nil, nil, nil, InsertOnConflict), artifact
}

func TestCheckpointOutageCachedHealthyShardCompletionDoesNotWaitForMain(t *testing.T) {
	shard, shardServer := checkpointoutageStartHealthyShard(t)
	releaseMain := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseMain) }) }
	defer release()
	main, mainAttempted := checkpointoutageUnavailableMain(t, releaseMain)
	p := checkpointoutageProcessor(main, shard)

	// Admission itself resolves wholly from the permanent tenant and client
	// caches. An unavailable main must not be consulted for known healthy work.
	if err := p.InsertV2("file-a", "tenant-a", []checkpointoutageRow{{Value: "queued", RoutingID: "tenant-a"}}); err != nil {
		t.Fatalf("cached tenant was rejected while main was unavailable: %v", err)
	}
	queued := <-p.recordQueueV2
	p.recordsNum.Add(-1)
	if queued.InstanceID != "shard-a" {
		t.Fatalf("cached tenant routed to %q, want shard-a", queued.InstanceID)
	}
	select {
	case <-mainAttempted:
		t.Fatal("cached admission unexpectedly consulted the unavailable main")
	default:
	}

	task, _ := checkpointoutageCommittedTask(t, p)
	done := make(chan struct{})
	go func() {
		p.processBufferTask(task, -1, nil)
		close(done)
	}()

	select {
	case <-shardServer.commits:
	case <-time.After(2 * time.Second):
		release()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("worker did not exit after the bounded commit wait expired")
		}
		t.Fatal("healthy shard did not commit within the bounded timeout")
	}

	// Once the independent shard has committed, an unavailable control plane
	// must not hold its serial shard worker hostage. Current code performs a
	// synchronous five-second checkpoint UPDATE here.
	select {
	case <-done:
		// desired: completion bookkeeping is deferred/recoverable
	case <-mainAttempted:
		select {
		case <-done:
		case <-time.After(1500 * time.Millisecond):
			t.Error("successful cached-shard work remained blocked on unavailable main checkpoint metadata")
			release()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("worker did not exit after releasing the deterministic outage seam")
			}
		}
	case <-time.After(2 * time.Second):
		t.Error("successful cached-shard work did not complete promptly")
		release()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("worker did not exit after releasing the deterministic outage seam")
		}
	}
}

func TestCheckpointOutageFailedCheckpointCompletionRetainsRecoveryArtifact(t *testing.T) {
	shard, shardServer := checkpointoutageStartHealthyShard(t)
	main, mainAttempted := checkpointoutageUnavailableMain(t, nil)
	p := checkpointoutageProcessor(main, shard)
	task, artifact := checkpointoutageCommittedTask(t, p)

	p.processBufferTask(task, -1, nil)

	select {
	case <-shardServer.commits:
	case <-time.After(2 * time.Second):
		t.Fatal("test did not establish a successful shard COMMIT within the bounded timeout")
	}
	select {
	case <-mainAttempted:
	case <-time.After(2 * time.Second):
		t.Fatal("test did not exercise failed main checkpoint completion")
	}

	// External recovery contract: the delta checkpoint names this local CSV as
	// the restart/reconciliation input. Until the main records COMPLETED, a
	// maintenance pass must neither nominate that filepath for checkpoint
	// deletion nor remove local.csv. Otherwise a restart can observe only an
	// old RUNNING checkpoint with no artifact from which to reconcile the shard
	// COMMIT. Current code violates both observable parts of this contract.
	recycled := p.bufferManager.RecycleBuffers()
	for _, path := range recycled {
		if path == artifact {
			t.Error("maintenance declared the recovery filepath recyclable before checkpoint completion was durable")
		}
	}
	if _, err := os.Stat(GetLocalFileFullPath(artifact)); err != nil {
		t.Errorf("maintenance lost the checkpoint's local recovery artifact after a successful shard commit: %v", err)
	}
}
