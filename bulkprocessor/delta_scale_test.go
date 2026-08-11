package bulkprocessor

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Scale test for instance routing: 1 main + 9 shard instances, a
// production-shaped canonical workload (sentinel flip per tenant block, version
// supersede, duplicate PKs), >=100k surviving rows, verified analytically
// (no baseline twin). See instance_routing.rfc.

const (
	scTable           = "routing_scale_sharded"
	scTenants         = 100
	scTenantsPerBlock = 10
	scFilesPerTenant  = 2
	scRowsPerCombo    = 1000 // 500 version-0 rows + 500 version-1 rows
	scDupPerHalf      = 50   // +10% duplicate-PK rows per version half
	scFileIDBase      = 300000
	scGroupIDBase     = 3000
)

// scInstanceDBs is the full 10-instance fleet configured through integration
// environment variables. Instance ids use the production bigint-in-string scheme.
var scInstanceDBs = []struct {
	db   string
	inst string
}{
	{eqDBMain, eqInstMain},
	{eqDBShardA, eqInstA},
	{eqDBShardB, eqInstB},
	{eqDBShardC, "4"},
	{eqDBShardD, "5"},
	{eqDBShardE, "6"},
	{eqDBShardF, "7"},
	{eqDBShardG, "8"},
	{eqDBShardH, "9"},
	{eqDBShardI, "10"},
}

// scFlipOrder: before tenant block b (tenants b*10..b*10+9) the sentinel is
// flipped to scFlipOrder[b], so each instance ends up owning 10 tenants.
// Order: shard_a..shard_i ("2".."10"), then main ("1") last.
var scFlipOrder = []string{
	"2", "3", "4", "5", "6",
	"7", "8", "9", "10", "1",
}

// Scale-suite routing ids use the 100000200+n block ("100000200" ..
// "100000299"), mirroring the production 1000002xx shape; disjoint from the
// CRUD suite's 100000000+n block (max 100000089).
const scTenantIDBase = 100000200

func scTenantName(tIdx int) string   { return fmt.Sprintf("%d", scTenantIDBase+tIdx) }
func scFileID(tIdx, fIdx int) int64  { return int64(scFileIDBase + tIdx*10 + fIdx) }
func scGroupID(tIdx int) int64       { return int64(scGroupIDBase + tIdx) }
func scBaseIDNum(tIdx, fIdx int) int { return tIdx*2000 + fIdx*1000 }
func scInstanceFor(tIdx int) string  { return scFlipOrder[tIdx/scTenantsPerBlock] }
func scFileIDStr(tIdx, fIdx int) string {
	return strconv.FormatInt(scFileID(tIdx, fIdx), 10)
}

const scTableConfigUpsert = `
INSERT INTO relyt_sys.relyt_loader_table_config (table_name, buffer_max_records, insert_into_batch_size)
VALUES ('%s', 5000, 500)
ON CONFLICT (table_name) DO UPDATE SET
    buffer_max_records = EXCLUDED.buffer_max_records,
    insert_into_batch_size = EXCLUDED.insert_into_batch_size;
`

// BootstrapScaleEnv extends the existing 3-instance equivalence environment to
// the full 10-instance fleet, idempotently: per-instance runtime + the
// routing_scale_sharded data table on all 10 DBs, registry rows for all 10
// instances, a per-table speedup config row, and the instance routing table
// with its sentinel on main. Global SDK_LOADER_CONFIG is left as the
// equivalence bootstrap set it.
func BootstrapScaleEnv(t *testing.T) {
	BootstrapEquivalenceEnv(t)
	for _, d := range scInstanceDBs {
		if d.db == "" {
			t.Skip("full instance-routing scale environment is not configured")
		}
	}

	for _, d := range scInstanceDBs {
		bootstrapInstanceRuntime(t, d.db, d.inst, scTable)
	}

	mainDB := eqOpenDB(t, eqDBMain)
	defer mainDB.Close()

	for _, d := range scInstanceDBs {
		connstr := eqRegistryConnStr(d.db)
		eqExec(t, mainDB, eqDBMain, "registry."+d.inst, fmt.Sprintf(eqUpsertRegistryTemplate, d.inst, connstr))
	}

	eqExec(t, mainDB, eqDBMain, "table config "+scTable, fmt.Sprintf(scTableConfigUpsert, scTable))
	eqExec(t, mainDB, eqDBMain, scTable+" instance routing table",
		fmt.Sprintf(eqInstanceRoutingTableDDLTemplate, scTable, eqInstMain))
}

// scCleanData truncates routing_scale_sharded on all 10 DBs and resets the routing
// table to sentinel-only, with the sentinel back on eqInstMain ("1").
func scCleanData(t *testing.T) {
	for _, d := range scInstanceDBs {
		db := eqOpenDB(t, d.db)
		func() {
			defer db.Close()
			eqExec(t, db, d.db, "truncate "+scTable, fmt.Sprintf("TRUNCATE TABLE public.%s", scTable))
		}()
	}

	mainDB := eqOpenDB(t, eqDBMain)
	defer mainDB.Close()
	eqExec(t, mainDB, eqDBMain, "clear tenant routing",
		fmt.Sprintf("DELETE FROM relyt_sys.%s_relyt_instance_routing WHERE routing_id <> '-1'", scTable))
	eqExec(t, mainDB, eqDBMain, "reset default instance sentinel",
		fmt.Sprintf("UPDATE relyt_sys.%s_relyt_instance_routing SET instance_id = '%s' WHERE routing_id = '-1'", scTable, eqInstMain))
}

type scGroup struct {
	fileID  string
	tenant  string
	records []EquivalenceRecord
}

// scBuildTenantGroups builds one tenant's four insert groups (2 fileids x 2
// version halves, v0 before v1): 500 base rows with ascending ids plus 10%
// duplicate-PK rows per half, shuffled deterministically via the shared rng.
func scBuildTenantGroups(rng *rand.Rand, tIdx int) []scGroup {
	tenant := scTenantName(tIdx)
	groupID := scGroupID(tIdx)
	half := scRowsPerCombo / 2
	groups := make([]scGroup, 0, scFilesPerTenant*2)
	for fIdx := range scFilesPerTenant {
		fileid := scFileID(tIdx, fIdx)
		base := scBaseIDNum(tIdx, fIdx)
		for v := range 2 {
			recs := make([]EquivalenceRecord, 0, half+scDupPerHalf)
			for i := range half {
				recs = append(recs, eqWorkloadRow(base+v*half+i, tenant, fileid, groupID, int64(v)))
			}
			for range scDupPerHalf {
				recs = append(recs, recs[rng.Intn(half)])
			}
			rng.Shuffle(len(recs), func(i, j int) { recs[i], recs[j] = recs[j], recs[i] })
			groups = append(groups, scGroup{
				fileID:  scFileIDStr(tIdx, fIdx),
				tenant:  tenant,
				records: recs,
			})
		}
	}
	return groups
}

// scSurvivingIDsDesc returns the analytically expected top-n surviving ids of
// one (tenant,fileid) combo in descending order: the version-1 half is ids
// base+500..base+999, so the sequence starts at base+999.
func scSurvivingIDsDesc(tIdx, fIdx, n int) []string {
	top := scBaseIDNum(tIdx, fIdx) + scRowsPerCombo - 1
	ids := make([]string, 0, n)
	for i := range n {
		ids = append(ids, eqID(top-i))
	}
	return ids
}

type scAgg struct {
	rows       int
	distinctPK int
	v0         int
}

// scScanDB returns per-routing_id aggregates for one instance DB in a single
// query: row count, distinct-PK count, and version-0 row count.
func scScanDB(t *testing.T, db *sql.DB, inst string) map[string]scAgg {
	t.Helper()
	query := fmt.Sprintf(`SELECT routing_id, count(*),
		count(DISTINCT fileid::text || ':' || id),
		sum(CASE WHEN version = 0 THEN 1 ELSE 0 END)
		FROM public.%s GROUP BY routing_id`, scTable)
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("aggregate scan failed on %s: %v", inst, err)
	}
	defer rows.Close()
	out := make(map[string]scAgg)
	for rows.Next() {
		var tenant string
		var agg scAgg
		if err := rows.Scan(&tenant, &agg.rows, &agg.distinctPK, &agg.v0); err != nil {
			t.Fatalf("aggregate scan failed on %s: %v", inst, err)
		}
		out[tenant] = agg
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("aggregate iteration failed on %s: %v", inst, err)
	}
	return out
}

func scColumnIndex(t *testing.T, res *SearchResult, name string) int {
	t.Helper()
	for i, col := range res.Columns {
		if col == name {
			return i
		}
	}
	t.Fatalf("result has no %q column: %v", name, res.Columns)
	return -1
}

func TestScaleSharded(t *testing.T) {
	eqSkipIfUnreachable(t)
	BootstrapScaleEnv(t)
	scCleanData(t)

	mainDB := eqOpenDB(t, eqDBMain)
	defer mainDB.Close()

	dbByInst := make(map[string]*sql.DB, len(scInstanceDBs))
	for _, d := range scInstanceDBs {
		db := eqOpenDB(t, d.db)
		defer db.Close()
		dbByInst[d.inst] = db
	}

	// restore the sentinel no matter how the test exits
	t.Cleanup(func() {
		if err := eqFlipSentinel(mainDB, scTable, eqInstMain); err != nil {
			t.Logf("cleanup: failed to restore sentinel: %v", err)
		}
	})

	// verification 8: every import error surfaces through this callback
	var cbMutex sync.Mutex
	var cbErrs []string
	config := Config{
		PostgreSQL: PostgreSQLConfig{
			Host:        eqHost,
			Port:        eqPort,
			Username:    eqUser,
			Password:    eqPassword,
			Database:    eqDBMain,
			Schema:      "public",
			Table:       scTable,
			MaxPoolSize: 5,
		},
		BatchSize:        10000,
		BatchImportSize:  2,
		FeedbackColumn:   "id",
		FileWriteTimeout: 2,
		BGWorkerInterval: 30,
		LogLevel:         LOG,
		ImportErrorCallback: func(fieldname string, values []string, err error, _ any) {
			cbMutex.Lock()
			cbErrs = append(cbErrs, fmt.Sprintf("%d %s records failed: %v", len(values), fieldname, err))
			cbMutex.Unlock()
		},
	}
	proc, err := New(config)
	if err != nil {
		t.Fatalf("failed to create processor: %v", err)
	}
	defer proc.Shutdown()
	if !proc.isSharded {
		t.Fatalf("%s: expected sharded processor", scTable)
	}
	assertNoCallbackErrors := func(stage string) {
		t.Helper()
		cbMutex.Lock()
		defer cbMutex.Unlock()
		if len(cbErrs) > 0 {
			t.Fatalf("%s: %d import error callbacks, first: %s", stage, len(cbErrs), cbErrs[0])
		}
	}

	// --- load: flip sentinel before each block of 10 tenants, then insert ---
	rng := rand.New(rand.NewSource(42))
	pushed := 0
	loadStart := time.Now()
	for tIdx := range scTenants {
		if tIdx%scTenantsPerBlock == 0 {
			eqFlipSentinelT(t, mainDB, scTable, scFlipOrder[tIdx/scTenantsPerBlock])
		}
		for _, g := range scBuildTenantGroups(rng, tIdx) {
			if err := proc.InsertV2(g.fileID, g.tenant, g.records); err != nil {
				t.Fatalf("InsertV2 failed for %s/%s: %v", g.tenant, g.fileID, err)
			}
			pushed += len(g.records)
		}
	}
	if err := proc.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	loadElapsed := time.Since(loadStart)
	assertNoCallbackErrors("after load+flush")

	wantPushed := scTenants * scFilesPerTenant * (scRowsPerCombo + 2*scDupPerHalf)
	if pushed != wantPushed {
		t.Fatalf("pushed %d records, want %d", pushed, wantPushed)
	}
	wantSurviving := scTenants * scFilesPerTenant * (scRowsPerCombo / 2)

	// --- verification 1: mapping rows match the flip schedule exactly ---
	t.Run("V1_mapping", func(t *testing.T) {
		rows, err := mainDB.Query(fmt.Sprintf(
			"SELECT routing_id, instance_id FROM relyt_sys.%s_relyt_instance_routing", scTable))
		if err != nil {
			t.Fatalf("routing table read failed: %v", err)
		}
		defer rows.Close()
		mapping := make(map[string]string)
		for rows.Next() {
			var rid, inst string
			if err := rows.Scan(&rid, &inst); err != nil {
				t.Fatalf("routing table scan failed: %v", err)
			}
			mapping[rid] = inst
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("routing table iteration failed: %v", err)
		}
		if len(mapping) != scTenants+1 {
			t.Fatalf("routing table has %d rows, want %d (tenants + sentinel)", len(mapping), scTenants+1)
		}
		if mapping[defaultRoutingSentinel] != eqInstMain {
			t.Fatalf("sentinel points at %q, want %q (last flip)", mapping[defaultRoutingSentinel], eqInstMain)
		}
		for tIdx := range scTenants {
			tenant := scTenantName(tIdx)
			if got, want := mapping[tenant], scInstanceFor(tIdx); got != want {
				t.Fatalf("%s: mapped to %q, want %q per flip schedule", tenant, got, want)
			}
		}
	})

	// --- verifications 2-4: placement, totals, dup PKs, version supersede ---
	// one aggregate query per DB covers all four checks
	perInst := make(map[string]map[string]scAgg, len(scInstanceDBs))
	t.Run("V2_V3_V4_placement_totals_versions", func(t *testing.T) {
		for _, d := range scInstanceDBs {
			perInst[d.inst] = scScanDB(t, dbByInst[d.inst], d.inst)
		}
		total := 0
		for inst, byTenant := range perInst {
			for tenant, agg := range byTenant {
				total += agg.rows
				id, err := strconv.Atoi(tenant)
				tIdx := id - scTenantIDBase
				if err != nil || tIdx < 0 || tIdx >= scTenants {
					t.Fatalf("unexpected routing_id %q on instance %s", tenant, inst)
				}
				if want := scInstanceFor(tIdx); inst != want {
					t.Fatalf("%s: %d rows on instance %s, but flip schedule maps it to %s", tenant, agg.rows, inst, want)
				}
				if want := scFilesPerTenant * scRowsPerCombo / 2; agg.rows != want {
					t.Fatalf("%s: %d rows on %s, want %d", tenant, agg.rows, inst, want)
				}
				if agg.distinctPK != agg.rows {
					t.Fatalf("%s: duplicate PKs on %s: %d rows, %d distinct PKs", tenant, inst, agg.rows, agg.distinctPK)
				}
				if agg.v0 != 0 {
					t.Fatalf("%s: %d version-0 rows survived on %s, delete_before_insert must supersede them", tenant, agg.v0, inst)
				}
			}
			// each instance owns exactly one block of 10 tenants
			if len(byTenant) != scTenants/len(scInstanceDBs) {
				t.Fatalf("instance %s holds %d tenants, want %d", inst, len(byTenant), scTenants/len(scInstanceDBs))
			}
		}
		if total != wantSurviving {
			t.Fatalf("cross-DB total is %d rows, want %d", total, wantSurviving)
		}
	})

	// --- verification 5: routed reads on 10 sampled tenants, one per instance ---
	// NOTE: the search UDF (_check_and_build_query, both the original
	// plpython3u in sql/udf.sql and the test reimplementation) caps the inner
	// match set at LIMIT 500, so count(*) OVER() can never report more than 500.
	// The per-tenant total of 1000 is therefore asserted as the sum of the two
	// per-fileid totals (each exactly 500, unaffected by the cap).
	t.Run("V5_routed_reads", func(t *testing.T) {
		for b := range scFlipOrder {
			tIdx := b*scTenantsPerBlock + 3
			tenant := scTenantName(tIdx)
			tenantTotal := 0
			for fIdx := range scFilesPerTenant {
				res, err := proc.SearchV2(&SearchOptions{
					Table:     scTable,
					Columns:   []string{"id", "count(*) OVER() AS total"},
					Condition: fmt.Sprintf("routing_id = '%s' AND fileid = %d", tenant, scFileID(tIdx, fIdx)),
					OrderBy:   "id DESC",
					Limit:     10,
					RoutingID: tenant,
				})
				if err != nil {
					t.Fatalf("%s fileid %d: routed SearchV2 failed: %v", tenant, scFileID(tIdx, fIdx), err)
				}
				if len(res.Rows) != 10 {
					t.Fatalf("%s fileid %d: got %d rows, want 10", tenant, scFileID(tIdx, fIdx), len(res.Rows))
				}
				totalIdx := scColumnIndex(t, res, "total")
				var comboTotal int
				if _, err := fmt.Sscanf(fmt.Sprintf("%v", res.Rows[0][totalIdx]), "%d", &comboTotal); err != nil {
					t.Fatalf("%s: cannot parse total %v: %v", tenant, res.Rows[0][totalIdx], err)
				}
				if comboTotal != scRowsPerCombo/2 {
					t.Fatalf("%s fileid %d: count(*) OVER() = %d, want %d", tenant, scFileID(tIdx, fIdx), comboTotal, scRowsPerCombo/2)
				}
				tenantTotal += comboTotal
				eqAssertIDs(t, fmt.Sprintf("%s fileid %d top-10 desc", tenant, scFileID(tIdx, fIdx)),
					eqIDSequence(t, res), scSurvivingIDsDesc(tIdx, fIdx, 10))
			}
			if tenantTotal != scFilesPerTenant*scRowsPerCombo/2 {
				t.Fatalf("%s: summed count(*) OVER() = %d, want %d", tenant, tenantTotal, scFilesPerTenant*scRowsPerCombo/2)
			}
		}
	})

	// --- verification 6: routed updates on 3 tenants (3 different instances) ---
	t.Run("V6_routed_updates", func(t *testing.T) {
		const newMtime = 1900000000
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		for _, tIdx := range []int{5, 45, 95} { // instances "2", "6", "1"
			tenant := scTenantName(tIdx)
			affected, err := proc.UpdateByQueryWithContextV2(ctx, &UpdateByQueryOptions{
				Table:     scTable,
				Condition: fmt.Sprintf("routing_id = '%s' AND fileid = %d", tenant, scFileID(tIdx, 0)),
				Updates:   map[string]any{"mtime": newMtime},
				RoutingID: tenant,
			})
			if err != nil {
				t.Fatalf("%s: routed update failed: %v", tenant, err)
			}
			if affected != int64(scRowsPerCombo/2) {
				t.Fatalf("%s: update affected %d rows, want %d", tenant, affected, scRowsPerCombo/2)
			}
			for _, d := range scInstanceDBs {
				got := eqScalarInt(t, dbByInst[d.inst], fmt.Sprintf(
					"SELECT count(*) FROM public.%s WHERE routing_id = $1 AND mtime = %d", scTable, newMtime), tenant)
				want := 0
				if d.inst == scInstanceFor(tIdx) {
					want = scRowsPerCombo / 2
				}
				if got != want {
					t.Fatalf("%s: %d updated rows on %s, want %d", tenant, got, d.inst, want)
				}
			}
		}
	})

	// --- verification 7: routed deletes ---
	t.Run("V7_deletes", func(t *testing.T) {
		// DeleteSyncV2 one (tenant,fileid) on 3 tenants across 3 instances
		for _, tIdx := range []int{7, 37, 97} { // instances "2", "5", "1"
			tenant := scTenantName(tIdx)
			db := dbByInst[scInstanceFor(tIdx)]
			if err := proc.DeleteSyncV2(scFileIDStr(tIdx, 0), tenant); err != nil {
				t.Fatalf("%s: DeleteSyncV2 failed: %v", tenant, err)
			}
			if n := eqScalarInt(t, db, fmt.Sprintf(
				"SELECT count(*) FROM public.%s WHERE routing_id = $1 AND fileid = %d", scTable, scFileID(tIdx, 0)), tenant); n != 0 {
				t.Fatalf("%s: %d rows of deleted fileid remain", tenant, n)
			}
			if n := eqScalarInt(t, db, fmt.Sprintf(
				"SELECT count(*) FROM public.%s WHERE routing_id = $1 AND fileid = %d", scTable, scFileID(tIdx, 1)), tenant); n != scRowsPerCombo/2 {
				t.Fatalf("%s: sibling fileid has %d rows, want %d (must be untouched)", tenant, n, scRowsPerCombo/2)
			}
		}

		// DeleteByGroupV2 empties one whole tenant (group_id is per-tenant)
		const groupTenantIdx = 50 // instance "7"
		tenant := scTenantName(groupTenantIdx)
		if err := proc.DeleteByGroupV2(strconv.FormatInt(scGroupID(groupTenantIdx), 10), tenant); err != nil {
			t.Fatalf("%s: DeleteByGroupV2 failed: %v", tenant, err)
		}
		if n := eqCountRoutingID(t, dbByInst[scInstanceFor(groupTenantIdx)], scTable, tenant); n != 0 {
			t.Fatalf("%s: %d rows remain after group delete", tenant, n)
		}
	})

	// --- verification 8: zero errors across the whole run ---
	assertNoCallbackErrors("end of run")

	// --- verification 9: summary ---
	perInstRows := make(map[string]int, len(scInstanceDBs))
	for inst, byTenant := range perInst {
		for _, agg := range byTenant {
			perInstRows[inst] += agg.rows
		}
	}
	t.Logf("scale summary: pushed %d records across %d tenants, %d surviving rows", pushed, scTenants, wantSurviving)
	t.Logf("scale summary: load+flush wall time %.1fs, %.0f pushed rows/s, %.0f surviving rows/s",
		loadElapsed.Seconds(), float64(pushed)/loadElapsed.Seconds(), float64(wantSurviving)/loadElapsed.Seconds())
	for _, d := range scInstanceDBs {
		t.Logf("scale summary: instance %-9s (%s): %d rows", d.inst, d.db, perInstRows[d.inst])
	}
}
