package bulkprocessor

import (
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// Phase C: default-instance flip lifecycle. Exercises what happens to routing,
// registration, and physical placement when the sentinel ('-1') default is
// changed while a long-lived processor keeps writing. F1/F2 are equivalence
// cases (run against the non-sharded baseline too); F3-F6 are candidate-only
// invariant checks. See instance_routing.rfc, sections "Default instance id"
// and "Invariants".

const (
	eqFlipIDBase   = 100000 // record id space, disjoint from Phase A/B
	eqFlipFileBase = 110000 // fileid space, disjoint from Phase A/B
	eqFlipGroupID  = 1000
)

// Flip-suite routing ids use the 200000000+idx block (tenant 30 =
// "200000030"), disjoint from the CRUD (1000000xx) and smoke (9000000xx)
// blocks.
func eqFlipTenant(idx int) string  { return fmt.Sprintf("%d", 200000000+idx) }
func eqFlipFileID(idx int) int64   { return int64(eqFlipFileBase + idx) }
func eqFlipStartID(idx int) int    { return eqFlipIDBase + idx*100 }
func eqFlipFileStr(idx int) string { return fmt.Sprintf("%d", eqFlipFileID(idx)) }

func eqFlipTenantNames(lo, hi int) []string {
	names := make([]string, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		names = append(names, eqFlipTenant(i))
	}
	return names
}

// eqFlipSentinel changes the default instance the way the ops runbook does:
// DELETE + INSERT the sentinel row in one transaction (atomic under MVCC, so
// concurrent registrations never see a gap).
func eqFlipSentinel(db *sql.DB, table, instanceID string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	del := fmt.Sprintf("DELETE FROM relyt_sys.%s_relyt_instance_routing WHERE routing_id = '-1'", table)
	if _, err := tx.Exec(del); err != nil {
		tx.Rollback()
		return err
	}
	ins := fmt.Sprintf("INSERT INTO relyt_sys.%s_relyt_instance_routing (routing_id, instance_id) VALUES ('-1', $1)", table)
	if _, err := tx.Exec(ins, instanceID); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func eqFlipSentinelT(t *testing.T, db *sql.DB, table, instanceID string) {
	t.Helper()
	if err := eqFlipSentinel(db, table, instanceID); err != nil {
		t.Fatalf("flip sentinel to %s failed: %v", instanceID, err)
	}
}

// eqDeleteSentinelT removes the sentinel with no replacement, creating a real
// default-instance gap (a misoperation, unlike the transactional flip).
func eqDeleteSentinelT(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	del := fmt.Sprintf("DELETE FROM relyt_sys.%s_relyt_instance_routing WHERE routing_id = '-1'", table)
	if _, err := db.Exec(del); err != nil {
		t.Fatalf("delete sentinel failed: %v", err)
	}
}

// eqFetchSigWhere returns row signatures for rows matching where.
func eqFetchSigWhere(t *testing.T, db *sql.DB, table, where string) []string {
	t.Helper()
	query := fmt.Sprintf(`SELECT concat_ws('|', id, routing_id, chunk_id, chunk_type, user_id,
		creator, sharer, fileid, group_id, ctime, mtime, y, ym, ymd, ext, fsize,
		parent_id, ftype, version, index_update_time, ext_group, vector::text)
		FROM public.%s WHERE %s`, table, where)
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("scoped ground truth query failed on %s: %v", table, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scoped ground truth scan failed on %s: %v", table, err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("scoped ground truth iteration failed on %s: %v", table, err)
	}
	return out
}

// eqAssertScopedGroundTruth compares baseline (plain, on main) against the
// union of the sharded table across all 3 DBs, restricted to flip tenants
// lo..hi (a text BETWEEN works: all flip ids are equal-length numeric
// strings). Used at the finale because only F1/F2 tenants were written to
// the baseline; F3-F6 tenants exist only on the candidate side.
func eqAssertScopedGroundTruth(t *testing.T, env *eqEnv, lo, hi int) {
	t.Helper()
	where := fmt.Sprintf("routing_id BETWEEN '%s' AND '%s'", eqFlipTenant(lo), eqFlipTenant(hi))
	base := eqFetchSigWhere(t, env.mainDB, eqTablePlain, where)
	var cand []string
	for _, db := range env.allDBs() {
		cand = append(cand, eqFetchSigWhere(t, db, eqTableSharded, where)...)
	}
	sort.Strings(base)
	sort.Strings(cand)
	if len(base) != len(cand) {
		t.Fatalf("scoped ground truth row count diverged: baseline=%d candidate=%d", len(base), len(cand))
	}
	for i := range base {
		if base[i] != cand[i] {
			t.Fatalf("scoped ground truth row %d diverged:\n  baseline:  %s\n  candidate: %s", i, base[i], cand[i])
		}
	}
}

// eqAssertTenantInvariants verifies, for candidate-only tenants: exactly one
// mapping row, wantRows rows all on the mapped instance and none elsewhere, and
// no duplicate PKs across the 3-DB union.
func eqAssertTenantInvariants(t *testing.T, env *eqEnv, tenants []string, wantRows int) {
	t.Helper()
	dbByInst := map[string]*sql.DB{eqInstMain: env.mainDB, eqInstA: env.shardA, eqInstB: env.shardB}
	for _, tenant := range tenants {
		mapped, found := eqInstanceForRoutingID(t, env.mainDB, eqTableSharded, tenant)
		if !found {
			t.Fatalf("%s: has no mapping row", tenant)
		}
		for inst, db := range dbByInst {
			n := eqCountRoutingID(t, db, eqTableSharded, tenant)
			if inst == mapped {
				if n != wantRows {
					t.Fatalf("%s: expected %d rows on mapped instance %s, got %d", tenant, wantRows, inst, n)
				}
			} else if n != 0 {
				t.Fatalf("%s: %d rows on non-mapped instance %s (mapped=%s)", tenant, n, inst, mapped)
			}
		}
	}

	tenantSet := make(map[string]bool, len(tenants))
	for _, tn := range tenants {
		tenantSet[tn] = true
	}
	pkSeen := make(map[string]bool)
	for inst, db := range dbByInst {
		rows, err := db.Query(fmt.Sprintf("SELECT routing_id, fileid::text, id FROM public.%s", eqTableSharded))
		if err != nil {
			t.Fatalf("dup PK scan failed on %s: %v", inst, err)
		}
		for rows.Next() {
			var rid, fid, id string
			if err := rows.Scan(&rid, &fid, &id); err != nil {
				rows.Close()
				t.Fatalf("dup PK scan failed on %s: %v", inst, err)
			}
			if !tenantSet[rid] {
				continue
			}
			pk := rid + "|" + fid + "|" + id
			if pkSeen[pk] {
				rows.Close()
				t.Fatalf("duplicate PK across the 3-DB union: %s", pk)
			}
			pkSeen[pk] = true
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatalf("dup PK iteration failed on %s: %v", inst, err)
		}
		rows.Close()
	}
}

func TestEquivalenceFlip(t *testing.T) {
	eqSkipIfUnreachable(t)
	BootstrapEquivalenceEnv(t)
	eqCleanData(t)

	env := &eqEnv{
		mainDB: eqOpenDB(t, eqDBMain),
		shardA: eqOpenDB(t, eqDBShardA),
		shardB: eqOpenDB(t, eqDBShardB),
	}
	defer env.mainDB.Close()
	defer env.shardA.Close()
	defer env.shardB.Close()

	// restore the default sentinel no matter how any case exits
	t.Cleanup(func() {
		if err := eqFlipSentinel(env.mainDB, eqTableSharded, eqInstMain); err != nil {
			t.Logf("cleanup: failed to restore sentinel: %v", err)
		}
	})

	env.plain = newEqProcessor(t, eqTablePlain)
	defer env.plain.Shutdown()
	env.sharded = newEqProcessor(t, eqTableSharded) // long-lived across every flip
	defer env.sharded.Shutdown()
	if env.plain.isSharded || !env.sharded.isSharded {
		t.Fatalf("processor sharding modes wrong: plain=%v sharded=%v", env.plain.isSharded, env.sharded.isSharded)
	}

	rowsFor := func(idx int, version int64) []EquivalenceRecord {
		return eqRows(eqFlipTenant(idx), eqFlipFileID(idx), eqFlipGroupID, version, eqFlipStartID(idx), 10)
	}

	groupA := eqFlipTenantNames(30, 34) // registered before flip -> eqInstMain
	groupB := eqFlipTenantNames(35, 39) // registered after flip  -> eqInstA

	if !t.Run("F1_flip_mid_load", func(t *testing.T) {
		// load group A while default is eqInstMain
		for idx := 30; idx <= 34; idx++ {
			env.insertBoth(t, eqFlipFileStr(idx), eqFlipTenant(idx), rowsFor(idx, 1))
		}
		// transactional flip to eqInstA
		eqFlipSentinelT(t, env.mainDB, eqTableSharded, eqInstA)
		// load group B after the flip
		for idx := 35; idx <= 39; idx++ {
			env.insertBoth(t, eqFlipFileStr(idx), eqFlipTenant(idx), rowsFor(idx, 1))
		}
		env.flushBoth(t)

		// placement + mapping ground truth
		for _, tn := range groupA {
			if inst, found := eqInstanceForRoutingID(t, env.mainDB, eqTableSharded, tn); !found || inst != eqInstMain {
				t.Fatalf("%s: expected mapping to %s, got (%q,%v)", tn, eqInstMain, inst, found)
			}
			if n := eqCountRoutingID(t, env.mainDB, eqTableSharded, tn); n != 10 {
				t.Fatalf("%s: expected 10 rows on control-plane database, got %d", tn, n)
			}
			if n := eqCountRoutingID(t, env.shardA, eqTableSharded, tn) + eqCountRoutingID(t, env.shardB, eqTableSharded, tn); n != 0 {
				t.Fatalf("%s: expected 0 rows off control-plane database, got %d", tn, n)
			}
		}
		for _, tn := range groupB {
			if inst, found := eqInstanceForRoutingID(t, env.mainDB, eqTableSharded, tn); !found || inst != eqInstA {
				t.Fatalf("%s: expected mapping to %s, got (%q,%v)", tn, eqInstA, inst, found)
			}
			if n := eqCountRoutingID(t, env.shardA, eqTableSharded, tn); n != 10 {
				t.Fatalf("%s: expected 10 rows on shard A database, got %d", tn, n)
			}
			if n := eqCountRoutingID(t, env.mainDB, eqTableSharded, tn) + eqCountRoutingID(t, env.shardB, eqTableSharded, tn); n != 0 {
				t.Fatalf("%s: expected 0 rows off shard A database, got %d", tn, n)
			}
		}

		// representative read/update/delete slice (baseline gets identical calls)
		env.assertSearchEqual(t, "F1 routed read g30", SearchOptions{
			Columns: []string{"id", "routing_id"}, Condition: "routing_id = '200000030'", OrderBy: "id ASC",
		}, "200000030", true, 10)
		// one routed read per boundary tenant (pre-flip 30, post-flip 35)
		env.assertSearchEqual(t, "F1 routed read g35", SearchOptions{
			Columns: []string{"id", "routing_id"}, Condition: "routing_id = '200000035'", OrderBy: "id ASC",
		}, "200000035", true, 10)
		if n := env.updateBoth(t, "F1 update g35", "routing_id = '200000035'",
			map[string]any{"mtime": 1800000000}, "200000035"); n != 10 {
			t.Fatalf("F1 update g35: expected 10 rows affected, got %d", n)
		}
		env.runBoth(t, "F1 delete g31", func(p *BulkProcessor) error {
			return p.DeleteSyncV2(eqFlipFileStr(31), "200000031")
		})
		env.assertSearchEqual(t, "F1 g31 gone", SearchOptions{
			Columns: []string{"id"}, Condition: "routing_id = '200000031'", OrderBy: "id ASC",
		}, "200000031", true, 0)
		eqAssertScopedGroundTruth(t, env, 30, 39)
	}) {
		return
	}

	if !t.Run("F2_old_group_grows", func(t *testing.T) {
		// default is eqInstA, but tenant 30 is already home on eqInstMain
		newFile := int64(eqFlipFileBase + 300)
		env.insertBoth(t, fmt.Sprintf("%d", newFile), "200000030",
			eqRows("200000030", newFile, eqFlipGroupID, 1, eqFlipIDBase+300, 10))
		// version bump on tenant 30's original file supersedes its v1 rows
		env.insertBoth(t, eqFlipFileStr(30), "200000030", rowsFor(30, 2))
		env.flushBoth(t)

		if n := eqCountRoutingID(t, env.mainDB, eqTableSharded, "200000030"); n != 20 {
			t.Fatalf("200000030: expected 20 rows on control-plane database after growth, got %d", n)
		}
		if n := eqCountRoutingID(t, env.shardA, eqTableSharded, "200000030") + eqCountRoutingID(t, env.shardB, eqTableSharded, "200000030"); n != 0 {
			t.Fatalf("200000030: new rows must never land off its home instance, got %d off-main", n)
		}
		if inst, _ := eqInstanceForRoutingID(t, env.mainDB, eqTableSharded, "200000030"); inst != eqInstMain {
			t.Fatalf("200000030: mapping drifted to %s", inst)
		}
		env.assertSearchEqual(t, "F2 read g30", SearchOptions{
			Columns: []string{"id", "fileid", "version"}, Condition: "routing_id = '200000030'", OrderBy: "id ASC",
		}, "200000030", true, 20)
		eqAssertScopedGroundTruth(t, env, 30, 39)
	}) {
		return
	}

	if !t.Run("F3_flip_storm", func(t *testing.T) {
		stop := make(chan struct{})
		var flipWg sync.WaitGroup
		var flipMu sync.Mutex
		var flipErrs []error
		flipWg.Add(1)
		go func() {
			defer flipWg.Done()
			insts := []string{eqInstMain, eqInstA, eqInstB}
			i := 0
			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					if err := eqFlipSentinel(env.mainDB, eqTableSharded, insts[i%3]); err != nil {
						flipMu.Lock()
						flipErrs = append(flipErrs, err)
						flipMu.Unlock()
					}
					i++
				}
			}
		}()

		// register tenants 40..79 (40 tenants) via 8 goroutines, 5 tenants each
		const workers, perWorker = 8, 5
		var wg sync.WaitGroup
		var mu sync.Mutex
		var insErrs []error
		for w := range workers {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for k := range perWorker {
					idx := 40 + w*perWorker + k
					recs := eqRows(eqFlipTenant(idx), eqFlipFileID(idx), eqFlipGroupID, 1, eqFlipStartID(idx), 5)
					if err := env.sharded.InsertV2(eqFlipFileStr(idx), eqFlipTenant(idx), recs); err != nil {
						mu.Lock()
						insErrs = append(insErrs, err)
						mu.Unlock()
					}
				}
			}(w)
		}
		wg.Wait()
		time.Sleep(500 * time.Millisecond) // let the storm keep flipping a bit past the writes
		close(stop)
		flipWg.Wait()

		if err := env.sharded.Flush(); err != nil {
			t.Fatalf("F3 flush failed: %v", err)
		}
		if len(insErrs) > 0 {
			t.Fatalf("F3 InsertV2 returned %d errors, first: %v", len(insErrs), insErrs[0])
		}
		if len(flipErrs) > 0 {
			t.Fatalf("F3 transactional flip returned %d errors, first: %v", len(flipErrs), flipErrs[0])
		}
		eqAssertTenantInvariants(t, env, eqFlipTenantNames(40, 79), 5)
	}) {
		return
	}

	if !t.Run("F4_dual_process_race", func(t *testing.T) {
		p2 := newEqProcessor(t, eqTableSharded) // second SDK process, own router cache
		defer p2.Shutdown()
		if !p2.isSharded {
			t.Fatalf("second processor should be sharded")
		}

		const tenant = "200000080"
		fileP1 := int64(eqFlipFileBase + 800)
		fileP2 := int64(eqFlipFileBase + 801)

		stop := make(chan struct{})
		var flipWg sync.WaitGroup
		flipWg.Add(1)
		go func() {
			defer flipWg.Done()
			insts := []string{eqInstA, eqInstB}
			i := 0
			ticker := time.NewTicker(30 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					eqFlipSentinel(env.mainDB, eqTableSharded, insts[i%2])
					i++
				}
			}
		}()

		var wg sync.WaitGroup
		var mu sync.Mutex
		var errs []error
		writer := func(p *BulkProcessor, fileid int64, startID int) {
			defer wg.Done()
			recs := eqRows(tenant, fileid, eqFlipGroupID, 1, startID, 20)
			if err := p.InsertV2(fmt.Sprintf("%d", fileid), tenant, recs); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}
		wg.Add(2)
		go writer(env.sharded, fileP1, eqFlipIDBase+8000)
		go writer(p2, fileP2, eqFlipIDBase+8100)
		wg.Wait()
		close(stop)
		flipWg.Wait()

		if err := env.sharded.Flush(); err != nil {
			t.Fatalf("F4 p1 flush failed: %v", err)
		}
		if err := p2.Flush(); err != nil {
			t.Fatalf("F4 p2 flush failed: %v", err)
		}
		if len(errs) > 0 {
			t.Fatalf("F4 InsertV2 errors: %v", errs)
		}

		mapped, found := eqInstanceForRoutingID(t, env.mainDB, eqTableSharded, tenant)
		if !found {
			t.Fatalf("200000080: expected exactly one mapping row, found none")
		}
		dbByInst := map[string]*sql.DB{eqInstMain: env.mainDB, eqInstA: env.shardA, eqInstB: env.shardB}
		for inst, db := range dbByInst {
			n := eqCountRoutingID(t, db, eqTableSharded, tenant)
			want := 0
			if inst == mapped {
				want = 40 // union of both writers' distinct PKs (20 + 20, disjoint fileids)
			}
			if n != want {
				t.Fatalf("200000080: expected %d rows on %s (mapped=%s), got %d", want, inst, mapped, n)
			}
		}
	}) {
		return
	}

	if !t.Run("F5_misoperated_gap", func(t *testing.T) {
		eqDeleteSentinelT(t, env.mainDB, eqTableSharded)

		// brand-new tenant during the gap must fail loudly and write nothing
		const g81 = "200000081"
		f81 := int64(eqFlipFileBase + 810)
		err := env.sharded.InsertV2(fmt.Sprintf("%d", f81), g81,
			eqRows(g81, f81, eqFlipGroupID, 1, eqFlipIDBase+8200, 10))
		if !errors.Is(err, ErrNoDefaultInstance) {
			t.Fatalf("F5: InsertV2 during gap must wrap ErrNoDefaultInstance, got %v", err)
		}
		if err := env.sharded.Flush(); err != nil {
			t.Fatalf("F5 flush during gap failed: %v", err)
		}
		for _, db := range env.allDBs() {
			if n := eqCountRoutingID(t, db, eqTableSharded, g81); n != 0 {
				t.Fatalf("F5: g81 must have no rows during gap, found %d", n)
			}
		}
		if _, found := eqInstanceForRoutingID(t, env.mainDB, eqTableSharded, g81); found {
			t.Fatalf("F5: g81 must have no mapping row during gap")
		}

		// existing tenant 30 (cached) keeps working during the gap; mirror to
		// baseline so the finale equivalence stays intact
		gapFile := int64(eqFlipFileBase + 301)
		env.insertBoth(t, fmt.Sprintf("%d", gapFile), "200000030",
			eqRows("200000030", gapFile, eqFlipGroupID, 1, eqFlipIDBase+310, 10))
		env.flushBoth(t)
		if n := eqCountRoutingID(t, env.shardA, eqTableSharded, "200000030") + eqCountRoutingID(t, env.shardB, eqTableSharded, "200000030"); n != 0 {
			t.Fatalf("F5: g30 gap rows must stay on control-plane database, got %d off-main", n)
		}

		// restore the default and retry the gapped tenant
		eqFlipSentinelT(t, env.mainDB, eqTableSharded, eqInstMain)
		if err := env.sharded.InsertV2(fmt.Sprintf("%d", f81), g81,
			eqRows(g81, f81, eqFlipGroupID, 1, eqFlipIDBase+8200, 10)); err != nil {
			t.Fatalf("F5: g81 retry after restore failed: %v", err)
		}
		if err := env.sharded.Flush(); err != nil {
			t.Fatalf("F5 flush after restore failed: %v", err)
		}
		if inst, found := eqInstanceForRoutingID(t, env.mainDB, eqTableSharded, g81); !found || inst != eqInstMain {
			t.Fatalf("F5: g81 should map to restored default %s, got (%q,%v)", eqInstMain, inst, found)
		}
		if n := eqCountRoutingID(t, env.mainDB, eqTableSharded, g81); n != 10 {
			t.Fatalf("F5: g81 should have 10 rows on control-plane database after retry, got %d", n)
		}
	}) {
		return
	}

	if !t.Run("F6_flip_no_restart", func(t *testing.T) {
		// same long-lived processor, alive across every prior flip
		eqFlipSentinelT(t, env.mainDB, eqTableSharded, eqInstB)
		const g82 = "200000082"
		f82 := int64(eqFlipFileBase + 820)
		if err := env.sharded.InsertV2(fmt.Sprintf("%d", f82), g82,
			eqRows(g82, f82, eqFlipGroupID, 1, eqFlipIDBase+8300, 10)); err != nil {
			t.Fatalf("F6: InsertV2 g82 failed: %v", err)
		}
		// mapping is assertable immediately after InsertV2, before Flush
		if inst, found := eqInstanceForRoutingID(t, env.mainDB, eqTableSharded, g82); !found || inst != eqInstB {
			t.Fatalf("F6: g82 must resolve to freshly-flipped %s, got (%q,%v) -- default was cached", eqInstB, inst, found)
		}
		if err := env.sharded.Flush(); err != nil {
			t.Fatalf("F6 flush failed: %v", err)
		}
		if n := eqCountRoutingID(t, env.shardB, eqTableSharded, g82); n != 10 {
			t.Fatalf("F6: g82 should have 10 rows on shard B database, got %d", n)
		}
		if n := eqCountRoutingID(t, env.mainDB, eqTableSharded, g82) + eqCountRoutingID(t, env.shardA, eqTableSharded, g82); n != 0 {
			t.Fatalf("F6: g82 rows leaked off shard B database, got %d", n)
		}
	}) {
		return
	}

	t.Run("finale_scoped_ground_truth", func(t *testing.T) {
		eqFlipSentinelT(t, env.mainDB, eqTableSharded, eqInstMain)
		// baseline received identical writes only for F1/F2 tenants (30-39), so
		// restrict the comparison to those; F3-F6 tenants (40-82) exist only
		// on the candidate side.
		eqAssertScopedGroundTruth(t, env, 30, 39)
	})
}
