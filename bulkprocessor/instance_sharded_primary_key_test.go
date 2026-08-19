package bulkprocessor

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

type shardedprimarykeyRow struct {
	ID        string `relyt:"id"`
	RoutingID string `relyt:"routing_id"`
	Payload   string `relyt:"payload"`
}

// TestShardedPrimaryKeyShardedSchemaRequiresRoutingIDInPrimaryKey exercises New because
// primary-key safety is a table-schema/startup invariant. New has already
// discovered both sharded mode and the table primary key, so an unsafe schema
// must be rejected there rather than being allowed to produce a processor
// which can accept writes.
//
// This uses the repository's loopback Relyt integration environment and only
// creates uniquely named test-owned tables. It does not truncate shared data
// or contact any non-loopback service.
func TestShardedPrimaryKeyShardedSchemaRequiresRoutingIDInPrimaryKey(t *testing.T) {
	eqSkipIfUnreachable(t)

	// Keep every object private to this invocation, including when multiple
	// package test processes use the same local integration cluster.
	stem := fmt.Sprintf("shardedprimarykey_%d", time.Now().UnixNano())
	safeTable := stem + "_safe"
	unsafeTable := stem + "_unsafe"

	db := eqOpenDB(t, eqDBMain)
	t.Cleanup(func() { _ = db.Close() })

	var mainInstanceID string
	if err := db.QueryRow("SELECT relyt.instance_id()").Scan(&mainInstanceID); err != nil {
		t.Fatalf("read local main instance id: %v", err)
	}

	for _, table := range []string{safeTable, unsafeTable} {
		primaryKey := "id"
		if table == safeTable {
			primaryKey = "routing_id, id"
		}
		eqExec(t, db, eqDBMain, "create "+table, fmt.Sprintf(`
CREATE TABLE public.%[1]s (
    id text NOT NULL,
    routing_id text NOT NULL,
    payload text NOT NULL,
    PRIMARY KEY (%[2]s)
);
CREATE TABLE relyt_sys.%[1]s_relyt_instance_routing (
    routing_id text PRIMARY KEY,
    instance_id text NOT NULL
);
INSERT INTO relyt_sys.%[1]s_relyt_instance_routing (routing_id, instance_id)
VALUES ('-1', '%[3]s');`, table, primaryKey, mainInstanceID))
	}

	// Cleanup is restricted by unique names/pg_table values to objects and
	// checkpoint rows created by this test; no shared application rows are
	// truncated or broadly deleted.
	t.Cleanup(func() {
		for _, table := range []string{safeTable, unsafeTable} {
			if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS relyt_sys.%s_relyt_instance_routing", table)); err != nil {
				t.Errorf("drop test routing table for %s: %v", table, err)
			}
			if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS public.%s", table)); err != nil {
				t.Errorf("drop test data table %s: %v", table, err)
			}
			if _, err := db.Exec("DELETE FROM relyt_sys.relyt_loader_checkpoint WHERE pg_table = $1", "public."+table); err != nil {
				t.Errorf("remove test checkpoint for %s: %v", table, err)
			}
		}
	})

	newProcessor := func(table string) (*BulkProcessor, error) {
		return New(Config{
			PostgreSQL: PostgreSQLConfig{
				Host:        eqHost,
				Port:        eqPort,
				Username:    eqUser,
				Password:    eqPassword,
				Database:    eqDBMain,
				Schema:      "public",
				Table:       table,
				MaxPoolSize: 2,
			},
			EnableDualBuffer: true,
			UpdateOnConflict: true, // exercise production DO UPDATE conflict mode
			LocalFilePrefix:  t.TempDir(),
		})
	}

	// Positive control: a composite conflict key containing routing_id is a
	// valid sharded schema, and its processor must accept exactly one record.
	safe, err := newProcessor(safeTable)
	if err != nil {
		t.Fatalf("safe sharded primary key (routing_id, id) was rejected: %v", err)
	}
	if safe.config.ImportStrategy != InsertOnConflict || !safe.config.UpdateOnConflict {
		t.Fatalf("safe control is not in production upsert mode: strategy=%d update_on_conflict=%v",
			safe.config.ImportStrategy, safe.config.UpdateOnConflict)
	}
	safe.isStarted = true // admission-only assertion: do not start import workers
	t.Cleanup(func() {
		if err := safe.Shutdown(); err != nil {
			t.Errorf("shutdown safe processor: %v", err)
		}
	})
	if err := safe.InsertV2("file-safe", "tenant-safe", []shardedprimarykeyRow{{
		ID: "id-1", RoutingID: "tenant-safe", Payload: "accepted",
	}}); err != nil {
		t.Fatalf("safe sharded insert was rejected: %v", err)
	}
	if got := len(safe.recordQueueV2); got != 1 {
		t.Fatalf("safe sharded insert enqueued %d records, want 1", got)
	}

	// Negative control: with PK (id), ON CONFLICT (id) DO UPDATE would treat
	// routing_id as an update column and could move a row between tenants while
	// leaving it on the instance selected for the incoming tenant. Startup must
	// reject this schema before returning a write-capable processor.
	unsafe, err := newProcessor(unsafeTable)
	if unsafe != nil {
		unsafe.isStarted = true
		t.Cleanup(func() {
			if shutdownErr := unsafe.Shutdown(); shutdownErr != nil {
				t.Errorf("shutdown unexpectedly accepted unsafe processor: %v", shutdownErr)
			}
		})
	}
	if err == nil {
		t.Errorf("New accepted unsafe instance-sharded schema: primary key (id) does not include routing_id")
		return
	}
	message := strings.ToLower(err.Error())
	if !strings.Contains(message, "primary key") || !strings.Contains(message, "routing_id") {
		t.Errorf("unsafe-schema error must identify primary-key/routing_id safety, got %v", err)
	}
}
