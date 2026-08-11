package bulkprocessor

import (
	"database/sql"
	"fmt"
	"strconv"
	"testing"
)

// This test follows the rollout procedure in instance_routing.rfc with two
// generations of processors. It deliberately uses its own table so removing
// the routing object cannot change the mode seen by any other equivalence
// test. All control-plane changes stay in control-plane database; data placement uses only
// the existing equivalence instances.
const (
	rolloutMigrationTable = "instance_rollout_migration"

	rolloutLegacyTenant   = "310000001"
	rolloutUpgradedTenant = "310000002"
	rolloutGapTenant      = "310000003"
	rolloutShardTenant    = "310000004"
)

var rolloutMigrationDBs = []struct {
	db   string
	inst string
}{
	{eqDBMain, eqInstMain},
	{eqDBShardA, eqInstA},
	{eqDBShardB, eqInstB},
}

func rolloutMigrationRoutingTable() string {
	return fmt.Sprintf("relyt_sys.%s_relyt_instance_routing", rolloutMigrationTable)
}

// prepareRolloutMigrationEnv starts from the normal equivalence bootstrap,
// provisions this test's data table, and then removes only this test's routing
// table. The removal is essential: BootstrapEquivalenceEnv normally leaves a
// sharded table's routing object present, while the first processor in the RFC
// rollout must detect non-sharded mode at startup.
func prepareRolloutMigrationEnv(t *testing.T) {
	t.Helper()
	BootstrapEquivalenceEnv(t)

	for _, d := range rolloutMigrationDBs {
		bootstrapInstanceRuntime(t, d.db, d.inst, rolloutMigrationTable)
		db := eqOpenDB(t, d.db)
		eqExec(t, db, d.db, "truncate "+rolloutMigrationTable,
			fmt.Sprintf("TRUNCATE TABLE public.%s", rolloutMigrationTable))
		db.Close()
	}

	mainDB := eqOpenDB(t, eqDBMain)
	eqExec(t, mainDB, eqDBMain, "bootstrap rollout routing table",
		fmt.Sprintf(eqInstanceRoutingTableDDLTemplate, rolloutMigrationTable, eqInstMain))
	eqExec(t, mainDB, eqDBMain, "clear rollout tenant mappings",
		fmt.Sprintf("DELETE FROM %s WHERE routing_id <> '-1'", rolloutMigrationRoutingTable()))
	eqExec(t, mainDB, eqDBMain, "reset rollout sentinel",
		fmt.Sprintf("UPDATE %s SET instance_id = '%s' WHERE routing_id = '-1'",
			rolloutMigrationRoutingTable(), eqInstMain))
	mainDB.Close()

	// Register restoration before dropping the object. Processor cleanups are
	// registered later and therefore run first (testing cleanups are LIFO).
	t.Cleanup(func() { restoreRolloutMigrationEnv(t) })

	mainDB = eqOpenDB(t, eqDBMain)
	defer mainDB.Close()
	eqExec(t, mainDB, eqDBMain, "remove only rollout routing table",
		fmt.Sprintf("DROP TABLE IF EXISTS %s", rolloutMigrationRoutingTable()))
}

// restoreRolloutMigrationEnv leaves the dedicated test table bootstrapped in
// the same sentinel-only state used by the equivalence harness. It is best
// effort so an earlier assertion remains the primary failure.
func restoreRolloutMigrationEnv(t *testing.T) {
	t.Helper()
	for _, d := range rolloutMigrationDBs {
		db, err := sql.Open("postgres", eqConnStr(d.db))
		if err != nil {
			t.Logf("cleanup: open %s: %v", d.db, err)
			continue
		}
		if _, err := db.Exec(fmt.Sprintf("TRUNCATE TABLE public.%s", rolloutMigrationTable)); err != nil {
			t.Logf("cleanup: truncate %s on %s: %v", rolloutMigrationTable, d.db, err)
		}
		db.Close()
	}

	mainDB, err := sql.Open("postgres", eqConnStr(eqDBMain))
	if err != nil {
		t.Logf("cleanup: open %s for routing restore: %v", eqDBMain, err)
		return
	}
	defer mainDB.Close()
	if _, err := mainDB.Exec(fmt.Sprintf(eqInstanceRoutingTableDDLTemplate,
		rolloutMigrationTable, eqInstMain)); err != nil {
		t.Logf("cleanup: restore rollout routing table: %v", err)
		return
	}
	if _, err := mainDB.Exec(fmt.Sprintf("DELETE FROM %s WHERE routing_id <> '-1'",
		rolloutMigrationRoutingTable())); err != nil {
		t.Logf("cleanup: clear rollout mappings: %v", err)
	}
	if _, err := mainDB.Exec(fmt.Sprintf("UPDATE %s SET instance_id = $1 WHERE routing_id = '-1'",
		rolloutMigrationRoutingTable()), eqInstMain); err != nil {
		t.Logf("cleanup: reset rollout sentinel: %v", err)
	}
	// The table is dedicated to this serial integration test. Exact pg_table
	// cleanup also recovers metadata left by an interrupted earlier run.
	pgTable := "public." + rolloutMigrationTable
	if _, err := mainDB.Exec(`DELETE FROM relyt_sys.relyt_loader_delta_checkpoint WHERE pg_table = $1`, pgTable); err != nil {
		t.Logf("cleanup: clear rollout delta checkpoints: %v", err)
	}
	if _, err := mainDB.Exec(`DELETE FROM relyt_sys.relyt_loader_checkpoint WHERE pg_table = $1`, pgTable); err != nil {
		t.Logf("cleanup: clear rollout checkpoints: %v", err)
	}
}

func createRolloutMigrationRouting(t *testing.T, mainDB *sql.DB) {
	t.Helper()
	eqExec(t, mainDB, eqDBMain, "create rollout routing table and main sentinel",
		fmt.Sprintf(eqInstanceRoutingTableDDLTemplate, rolloutMigrationTable, eqInstMain))
}

// backfillRolloutMigration applies the RFC's idempotent INSERT ... SELECT
// DISTINCT statement and reports how many new tenant mappings appeared.
func backfillRolloutMigration(t *testing.T, mainDB *sql.DB) int {
	t.Helper()
	before := eqScalarInt(t, mainDB,
		fmt.Sprintf("SELECT count(*) FROM %s WHERE routing_id <> '-1'", rolloutMigrationRoutingTable()))
	stmt := fmt.Sprintf(`
INSERT INTO %s (routing_id, instance_id)
SELECT DISTINCT routing_id, $1 FROM public.%s
WHERE routing_id <> '-1'
ON CONFLICT (routing_id) DO NOTHING`, rolloutMigrationRoutingTable(), rolloutMigrationTable)
	if _, err := mainDB.Exec(stmt, eqInstMain); err != nil {
		t.Fatalf("rollout backfill failed: %v", err)
	}
	after := eqScalarInt(t, mainDB,
		fmt.Sprintf("SELECT count(*) FROM %s WHERE routing_id <> '-1'", rolloutMigrationRoutingTable()))
	return after - before
}

func insertOneRolloutRecord(t *testing.T, p *BulkProcessor, tenant string, fileID int64, id string) {
	t.Helper()
	if err := p.InsertV2(strconv.FormatInt(fileID, 10), tenant,
		eqMakeRecords(tenant, fileID, id)); err != nil {
		t.Fatalf("InsertV2 %s/%d failed: %v", tenant, fileID, err)
	}
	if err := p.Flush(); err != nil {
		t.Fatalf("Flush %s/%d failed: %v", tenant, fileID, err)
	}
}

func assertRolloutPlacement(t *testing.T, dbByInst map[string]*sql.DB, tenant string, wantInst string, wantRows int) {
	t.Helper()
	for inst, db := range dbByInst {
		got := eqCountRoutingID(t, db, rolloutMigrationTable, tenant)
		want := 0
		if inst == wantInst {
			want = wantRows
		}
		if got != want {
			t.Fatalf("tenant %s: got %d rows on instance %s, want %d (owner %s)",
				tenant, got, inst, want, wantInst)
		}
	}
}

func cleanupRolloutMigrationCheckpoints(t *testing.T, processIDs []string) {
	t.Helper()
	db, err := sql.Open("postgres", eqConnStr(eqDBMain))
	if err != nil {
		t.Logf("cleanup: open main for rollout checkpoints: %v", err)
		return
	}
	defer db.Close()
	for _, processID := range processIDs {
		if _, err := db.Exec(`DELETE FROM relyt_sys.relyt_loader_delta_checkpoint WHERE process_id = $1`, processID); err != nil {
			t.Logf("cleanup: delete rollout delta checkpoints for %s: %v", processID, err)
		}
		if _, err := db.Exec(`DELETE FROM relyt_sys.relyt_loader_checkpoint WHERE process_id = $1`, processID); err != nil {
			t.Logf("cleanup: delete rollout checkpoint for %s: %v", processID, err)
		}
	}
}

func searchRolloutTenant(t *testing.T, p *BulkProcessor, tenant string) *SearchResult {
	t.Helper()
	result, err := p.SearchV2(&SearchOptions{
		Table:     rolloutMigrationTable,
		Columns:   []string{"id", "routing_id", "fileid"},
		Condition: "routing_id = '" + tenant + "'",
		OrderBy:   "id ASC",
		RoutingID: tenant,
	})
	if err != nil {
		t.Fatalf("routed search for %s failed: %v", tenant, err)
	}
	return result
}

func TestInstanceRoutingRolloutMigrationStateMachine(t *testing.T) {
	eqSkipIfUnreachable(t)
	prepareRolloutMigrationEnv(t)

	// Register this before processor shutdown cleanups so LIFO cleanup closes
	// every processor before removing only their exact control-plane rows.
	processIDs := []string{}
	t.Cleanup(func() { cleanupRolloutMigrationCheckpoints(t, processIDs) })

	mainDB := eqOpenDB(t, eqDBMain)
	defer mainDB.Close()
	shardADB := eqOpenDB(t, eqDBShardA)
	defer shardADB.Close()
	shardBDB := eqOpenDB(t, eqDBShardB)
	defer shardBDB.Close()
	dbByInst := map[string]*sql.DB{
		eqInstMain: mainDB,
		eqInstA:    shardADB,
		eqInstB:    shardBDB,
	}

	// Step 1: this processor starts before the routing object exists and must
	// keep its startup mode for its whole lifetime.
	straggler := newEqProcessor(t, rolloutMigrationTable)
	processIDs = append(processIDs, straggler.processId)
	stragglerStopped := false
	t.Cleanup(func() {
		if !stragglerStopped {
			_ = straggler.Shutdown()
		}
	})
	if straggler.isSharded {
		t.Fatalf("processor created before routing table must be non-sharded")
	}
	if err := straggler.Start(); err != nil {
		t.Fatalf("start pre-routing processor: %v", err)
	}
	insertOneRolloutRecord(t, straggler, rolloutLegacyTenant, 410001, "legacy-before-routing")
	assertRolloutPlacement(t, dbByInst, rolloutLegacyTenant, eqInstMain, 1)

	// Step 2: enable sharding with the sentinel still on main, then backfill
	// every tenant that existed before the routing object.
	createRolloutMigrationRouting(t, mainDB)
	if straggler.isSharded {
		t.Fatalf("pre-routing processor changed mode without restart")
	}
	if added := backfillRolloutMigration(t, mainDB); added != 1 {
		t.Fatalf("first backfill added %d mappings, want 1", added)
	}
	if inst, found := eqInstanceForRoutingID(t, mainDB, rolloutMigrationTable, rolloutLegacyTenant); !found || inst != eqInstMain {
		t.Fatalf("legacy tenant mapping after first backfill = (%q,%v), want (%q,true)",
			inst, found, eqInstMain)
	}

	// Step 3: rolling window. An upgraded processor registers on main while
	// the pre-routing straggler also continues to write there. A tenant first
	// seen only by the straggler is the rollout's documented temporary gap.
	upgraded := newEqProcessor(t, rolloutMigrationTable)
	processIDs = append(processIDs, upgraded.processId)
	t.Cleanup(func() { _ = upgraded.Shutdown() })
	if !upgraded.isSharded {
		t.Fatalf("processor created after routing table must be sharded")
	}
	if err := upgraded.Start(); err != nil {
		t.Fatalf("start upgraded processor: %v", err)
	}
	insertOneRolloutRecord(t, upgraded, rolloutUpgradedTenant, 410002, "upgraded-window")
	insertOneRolloutRecord(t, straggler, rolloutLegacyTenant, 410003, "legacy-window-growth")
	insertOneRolloutRecord(t, straggler, rolloutGapTenant, 410004, "straggler-unmapped-gap")

	if inst, found := eqInstanceForRoutingID(t, mainDB, rolloutMigrationTable, rolloutUpgradedTenant); !found || inst != eqInstMain {
		t.Fatalf("upgraded-window tenant mapping = (%q,%v), want (%q,true)", inst, found, eqInstMain)
	}
	if _, found := eqInstanceForRoutingID(t, mainDB, rolloutMigrationTable, rolloutGapTenant); found {
		t.Fatalf("straggler-only tenant unexpectedly has a mapping before second backfill")
	}
	assertRolloutPlacement(t, dbByInst, rolloutLegacyTenant, eqInstMain, 2)
	assertRolloutPlacement(t, dbByInst, rolloutUpgradedTenant, eqInstMain, 1)
	assertRolloutPlacement(t, dbByInst, rolloutGapTenant, eqInstMain, 1)
	if got := len(searchRolloutTenant(t, upgraded, rolloutGapTenant).Rows); got != 0 {
		t.Fatalf("unmapped rollout gap returned %d routed rows, want 0", got)
	}

	// Step 4: stop the last process that predates enablement. The second
	// backfill closes exactly the expected gap; repeating it changes nothing.
	if err := straggler.Shutdown(); err != nil {
		t.Fatalf("stop pre-routing straggler: %v", err)
	}
	stragglerStopped = true
	if added := backfillRolloutMigration(t, mainDB); added != 1 {
		t.Fatalf("second backfill added %d mappings, want exactly the straggler gap", added)
	}
	if added := backfillRolloutMigration(t, mainDB); added != 0 {
		t.Fatalf("idempotent backfill repeat added %d mappings, want 0", added)
	}
	if inst, found := eqInstanceForRoutingID(t, mainDB, rolloutMigrationTable, rolloutGapTenant); !found || inst != eqInstMain {
		t.Fatalf("gap tenant mapping after second backfill = (%q,%v), want (%q,true)",
			inst, found, eqInstMain)
	}
	// Reuse the same upgraded processor that observed the pre-backfill miss.
	// A miss must not be cached, so the second backfill becomes visible without
	// restarting an already-upgraded process.
	if got := len(searchRolloutTenant(t, upgraded, rolloutGapTenant).Rows); got != 1 {
		t.Fatalf("running upgraded processor cached pre-backfill miss: got %d rows, want 1", got)
	}

	// A fresh post-backfill processor detects sharded mode and can now see the
	// formerly unmapped main-resident row. This read also establishes its
	// permanent positive routing cache before the sentinel flip.
	fresh := newEqProcessor(t, rolloutMigrationTable)
	processIDs = append(processIDs, fresh.processId)
	t.Cleanup(func() { _ = fresh.Shutdown() })
	if !fresh.isSharded {
		t.Fatalf("fresh post-backfill processor must be sharded")
	}
	if err := fresh.Start(); err != nil {
		t.Fatalf("start fresh sharded processor: %v", err)
	}
	if got := len(searchRolloutTenant(t, fresh, rolloutGapTenant).Rows); got != 1 {
		t.Fatalf("backfilled gap tenant returned %d rows, want 1", got)
	}

	// Step 5: after the point-of-no-return flip, an already mapped tenant is
	// sticky on main while a brand-new tenant registers and lands on shard A.
	eqFlipSentinelT(t, mainDB, rolloutMigrationTable, eqInstA)
	insertOneRolloutRecord(t, fresh, rolloutGapTenant, 410005, "sticky-after-flip")
	insertOneRolloutRecord(t, fresh, rolloutShardTenant, 410006, "new-after-flip")

	if inst, found := eqInstanceForRoutingID(t, mainDB, rolloutMigrationTable, rolloutGapTenant); !found || inst != eqInstMain {
		t.Fatalf("sticky tenant mapping after flip = (%q,%v), want (%q,true)", inst, found, eqInstMain)
	}
	if inst, found := eqInstanceForRoutingID(t, mainDB, rolloutMigrationTable, rolloutShardTenant); !found || inst != eqInstA {
		t.Fatalf("new tenant mapping after flip = (%q,%v), want (%q,true)", inst, found, eqInstA)
	}
	assertRolloutPlacement(t, dbByInst, rolloutGapTenant, eqInstMain, 2)
	assertRolloutPlacement(t, dbByInst, rolloutShardTenant, eqInstA, 1)
}
