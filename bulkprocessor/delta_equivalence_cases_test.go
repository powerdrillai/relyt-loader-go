package bulkprocessor

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"
)

// Phase B: CRUD differential cases. Every operation runs against both the
// non-sharded baseline (routing_eq_plain) and the instance-sharded candidate
// (routing_eq_sharded); interface results and physical ground truth must match.
//
// Documented divergences (candidate-only carve-outs, everything else must
// match the baseline exactly):
//   1. V1 Insert is rejected (ErrShardedRequiresV2)
//   2. routing_id '-1' is reserved (ErrReservedRoutingID)
//   3. RoutingID is required on sharded search/update (ErrRoutingIDRequired)

const (
	eqTenants       = 12
	eqFilesPer      = 3
	eqRowsPerCombo  = 200 // 100 version-0 rows + 100 version-1 rows
	eqDupPerVersion = 10  // +10% duplicate-PK rows per version half
	eqFileIDBase    = 5000
	eqGroupIDBase   = 900
)

// Tenant routing ids are production-shaped positive bigints rendered as
// strings: this suite uses the 100000000+n block (tenant 3 = "100000003").
func eqTenantName(tIdx int) string { return fmt.Sprintf("%d", 100000000+tIdx) }

func eqFileID(tIdx, fIdx int) int64 { return int64(eqFileIDBase + tIdx*10 + fIdx) }

func eqID(n int) string { return fmt.Sprintf("%08d", n) }

// eqWorkloadRow builds one workload record. The vector's first component encodes
// idNum%1500 (exactly representable in fp16, distinct within any id window
// shorter than 1500) so vector distances are deterministic and tie-free.
func eqWorkloadRow(idNum int, tenant string, fileid, groupID, version int64) EquivalenceRecord {
	return EquivalenceRecord{
		ID:              eqID(idNum),
		RoutingID:       tenant,
		ChunkID:         idNum,
		ChunkType:       "text",
		UserID:          1000000,
		Creator:         1000000,
		Sharer:          1000000,
		FileID:          fileid,
		GroupID:         groupID,
		Ctime:           1640995200,
		Mtime:           1640995200,
		Y:               2022,
		Ym:              202201,
		Ymd:             20220101,
		Ext:             "txt",
		Fsize:           1024,
		ParentID:        1000000,
		Ftype:           "text",
		Version:         version,
		IndexUpdateTime: 1640995200,
		ExtGroup:        "text",
		Vector:          fmt.Sprintf("[%d,0.1,0.1,0.1]", idNum%1500),
	}
}

// eqRows builds n records with consecutive ids starting at startID.
func eqRows(tenant string, fileid, groupID, version int64, startID, n int) []EquivalenceRecord {
	records := make([]EquivalenceRecord, 0, n)
	for i := range n {
		records = append(records, eqWorkloadRow(startID+i, tenant, fileid, groupID, version))
	}
	return records
}

type eqGroup struct {
	fileID    string
	routingID string
	records   []EquivalenceRecord
}

// eqBuildWorkload ports the canonical workload generator pattern, scaled down: 12 tenants x 3
// fileids x 200 rows (version 0 first half, version 1 second half), globally
// ascending ids, +10% duplicate-PK rows, deterministic per-group shuffle.
// One group per (tenant,fileid,version), v0 group always before v1 group so
// the delete_before_insert version supersede is order-stable across buffer
// boundaries in both modes.
func eqBuildWorkload() []eqGroup {
	rng := rand.New(rand.NewSource(42))
	var groups []eqGroup
	idNum := 0
	for tIdx := range eqTenants {
		tenant := eqTenantName(tIdx)
		groupID := int64(eqGroupIDBase + tIdx)
		for fIdx := range eqFilesPer {
			fileid := eqFileID(tIdx, fIdx)
			half := eqRowsPerCombo / 2
			var byVersion [2][]EquivalenceRecord
			for i := range eqRowsPerCombo {
				version := int64(i / half)
				byVersion[version] = append(byVersion[version], eqWorkloadRow(idNum, tenant, fileid, groupID, version))
				idNum++
			}
			for v := range 2 {
				recs := byVersion[v]
				for range eqDupPerVersion {
					recs = append(recs, recs[rng.Intn(half)])
				}
				rng.Shuffle(len(recs), func(i, j int) { recs[i], recs[j] = recs[j], recs[i] })
				groups = append(groups, eqGroup{
					fileID:    strconv.FormatInt(fileid, 10),
					routingID: tenant,
					records:   recs,
				})
			}
		}
	}
	return groups
}

func eqIDList(startID, n int) []string {
	ids := make([]string, 0, n)
	for i := range n {
		ids = append(ids, eqID(startID+i))
	}
	return ids
}

type eqEnv struct {
	plain   *BulkProcessor
	sharded *BulkProcessor
	mainDB  *sql.DB
	shardA  *sql.DB
	shardB  *sql.DB
}

func (e *eqEnv) allDBs() []*sql.DB { return []*sql.DB{e.mainDB, e.shardA, e.shardB} }

// eqBoth runs the same operation against baseline and candidate and requires
// the same error/nil outcome (and success, since every case expects success).
func eqBoth[T any](t *testing.T, e *eqEnv, name string, op func(p *BulkProcessor, table string) (T, error)) (T, T) {
	t.Helper()
	baseVal, baseErr := op(e.plain, eqTablePlain)
	candVal, candErr := op(e.sharded, eqTableSharded)
	if (baseErr == nil) != (candErr == nil) {
		t.Fatalf("%s: error outcome diverged: baseline=%v candidate=%v", name, baseErr, candErr)
	}
	if baseErr != nil {
		t.Fatalf("%s: failed in both modes: baseline=%v candidate=%v", name, baseErr, candErr)
	}
	return baseVal, candVal
}

func (e *eqEnv) runBoth(t *testing.T, name string, op func(p *BulkProcessor) error) {
	t.Helper()
	eqBoth(t, e, name, func(p *BulkProcessor, _ string) (struct{}, error) {
		return struct{}{}, op(p)
	})
}

func (e *eqEnv) insertBoth(t *testing.T, fileID, routingID string, records []EquivalenceRecord) {
	t.Helper()
	e.runBoth(t, "InsertV2 "+routingID+"/"+fileID, func(p *BulkProcessor) error {
		return p.InsertV2(fileID, routingID, records)
	})
}

func (e *eqEnv) flushBoth(t *testing.T) {
	t.Helper()
	e.runBoth(t, "Flush", func(p *BulkProcessor) error { return p.Flush() })
}

// searchBoth runs the same SearchOptions template on both processors; the
// candidate additionally gets RoutingID set to candRoutingID (may be empty).
func (e *eqEnv) searchBoth(t *testing.T, name string, tmpl SearchOptions, candRoutingID string) (*SearchResult, *SearchResult) {
	t.Helper()
	return eqBoth(t, e, name, func(p *BulkProcessor, table string) (*SearchResult, error) {
		opts := tmpl
		opts.Table = table
		if p == e.sharded {
			opts.RoutingID = candRoutingID
		}
		return p.SearchV2(&opts)
	})
}

// eqNormalizeRows renders every result row as a canonical JSON string
// (map marshal sorts keys), so rows compare independent of column order.
func eqNormalizeRows(t *testing.T, res *SearchResult) []string {
	t.Helper()
	rows := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		m := make(map[string]any, len(res.Columns))
		for i, col := range res.Columns {
			m[col] = r[i]
		}
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("failed to normalize row: %v", err)
		}
		rows = append(rows, string(b))
	}
	return rows
}

// assertSearchEqual compares interface results: exact sequence when ordered
// (query has ORDER BY), set equality otherwise. wantRows=-1 skips count check.
// Returns the baseline rows (sorted when unordered).
func (e *eqEnv) assertSearchEqual(t *testing.T, name string, tmpl SearchOptions, candRoutingID string, ordered bool, wantRows int) []string {
	t.Helper()
	baseRes, candRes := e.searchBoth(t, name, tmpl, candRoutingID)
	baseRows := eqNormalizeRows(t, baseRes)
	candRows := eqNormalizeRows(t, candRes)
	if !ordered {
		sort.Strings(baseRows)
		sort.Strings(candRows)
	}
	if len(baseRows) != len(candRows) {
		t.Fatalf("%s: row count diverged: baseline=%d candidate=%d", name, len(baseRows), len(candRows))
	}
	for i := range baseRows {
		if baseRows[i] != candRows[i] {
			t.Fatalf("%s: row %d diverged:\n  baseline:  %s\n  candidate: %s", name, i, baseRows[i], candRows[i])
		}
	}
	if wantRows >= 0 && len(baseRows) != wantRows {
		t.Fatalf("%s: expected %d rows, got %d", name, wantRows, len(baseRows))
	}
	return baseRows
}

// eqIDSequence extracts the "id" column values in result order.
func eqIDSequence(t *testing.T, res *SearchResult) []string {
	t.Helper()
	idIdx := -1
	for i, col := range res.Columns {
		if col == "id" {
			idIdx = i
		}
	}
	if idIdx < 0 {
		if len(res.Rows) == 0 {
			return nil
		}
		t.Fatalf("result has no id column: %v", res.Columns)
	}
	ids := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		ids = append(ids, fmt.Sprintf("%v", r[idIdx]))
	}
	return ids
}

func eqAssertIDs(t *testing.T, name string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: expected %d ids, got %d (got=%v)", name, len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: id %d mismatch: got %s want %s", name, i, got[i], want[i])
		}
	}
}

// updateBoth runs the same UpdateByQueryWithContextV2 on both modes and
// requires equal rows-affected counts.
func (e *eqEnv) updateBoth(t *testing.T, name, condition string, updates map[string]any, candRoutingID string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	baseCount, candCount := eqBoth(t, e, name, func(p *BulkProcessor, table string) (int64, error) {
		opts := &UpdateByQueryOptions{Table: table, Condition: condition, Updates: updates}
		if p == e.sharded {
			opts.RoutingID = candRoutingID
		}
		return p.UpdateByQueryWithContextV2(ctx, opts)
	})
	if baseCount != candCount {
		t.Fatalf("%s: rows affected diverged: baseline=%d candidate=%d", name, baseCount, candCount)
	}
	return baseCount
}

// eqFetchRowSignatures returns one pipe-joined string per physical row.
func eqFetchRowSignatures(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()
	query := fmt.Sprintf(`SELECT concat_ws('|', id, routing_id, chunk_id, chunk_type, user_id,
		creator, sharer, fileid, group_id, ctime, mtime, y, ym, ymd, ext, fsize,
		parent_id, ftype, version, index_update_time, ext_group, vector::text)
		FROM public.%s`, table)
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("ground truth query failed on %s: %v", table, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("ground truth scan failed on %s: %v", table, err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("ground truth iteration failed on %s: %v", table, err)
	}
	return out
}

// eqCompareGroundTruth compares the full physical content of routing_eq_plain on
// main against the union of routing_eq_sharded across the three databases.
func (e *eqEnv) eqCompareGroundTruth(t *testing.T) {
	t.Helper()
	baseRows := eqFetchRowSignatures(t, e.mainDB, eqTablePlain)
	var candRows []string
	for _, db := range e.allDBs() {
		candRows = append(candRows, eqFetchRowSignatures(t, db, eqTableSharded)...)
	}
	sort.Strings(baseRows)
	sort.Strings(candRows)
	if len(baseRows) != len(candRows) {
		t.Fatalf("ground truth row count diverged: baseline=%d candidate=%d", len(baseRows), len(candRows))
	}
	for i := range baseRows {
		if baseRows[i] != candRows[i] {
			t.Fatalf("ground truth row %d diverged:\n  baseline:  %s\n  candidate: %s", i, baseRows[i], candRows[i])
		}
	}
}

func eqScalarInt(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("query %q failed: %v", query, err)
	}
	return n
}

func TestEquivalenceCRUD(t *testing.T) {
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

	// mapping plan: tenants 0-3 -> instance "1" (main), 4-7 -> "2" (shard a),
	// 8-11 -> "3" (shard b)
	tenantInstance := make(map[string]string)
	for tIdx := range eqTenants {
		inst := eqInstMain
		if tIdx >= 8 {
			inst = eqInstB
		} else if tIdx >= 4 {
			inst = eqInstA
		}
		tenant := eqTenantName(tIdx)
		tenantInstance[tenant] = inst
		_, err := env.mainDB.Exec(
			fmt.Sprintf("INSERT INTO relyt_sys.%s_relyt_instance_routing (routing_id, instance_id) VALUES ($1, $2) ON CONFLICT (routing_id) DO NOTHING", eqTableSharded),
			tenant, inst,
		)
		if err != nil {
			t.Fatalf("failed to pre-map %s: %v", tenant, err)
		}
	}

	env.plain = newEqProcessor(t, eqTablePlain)
	defer env.plain.Shutdown()
	env.sharded = newEqProcessor(t, eqTableSharded)
	defer env.sharded.Shutdown()
	if env.plain.isSharded || !env.sharded.isSharded {
		t.Fatalf("processor sharding modes wrong: plain=%v sharded=%v", env.plain.isSharded, env.sharded.isSharded)
	}

	workload := eqBuildWorkload()

	t.Run("C1_bulk_load", func(t *testing.T) {
		for _, g := range workload {
			env.insertBoth(t, g.fileID, g.routingID, g.records)
		}
		env.flushBoth(t)
		env.eqCompareGroundTruth(t)

		// delete_before_insert version supersede: per combo only the
		// version-1 half survives, so distinct surviving PKs = 100/combo.
		want := eqTenants * eqFilesPer * (eqRowsPerCombo / 2)
		got := eqScalarInt(t, env.mainDB, fmt.Sprintf("SELECT count(*) FROM public.%s", eqTablePlain))
		if got != want {
			t.Fatalf("expected %d surviving rows, got %d", want, got)
		}
		distinct := eqScalarInt(t, env.mainDB,
			fmt.Sprintf("SELECT count(*) FROM (SELECT DISTINCT routing_id, fileid, id FROM public.%s) x", eqTablePlain))
		if distinct != got {
			t.Fatalf("duplicate PKs after load: %d rows, %d distinct PKs", got, distinct)
		}
	})

	t.Run("C2_version_v0_then_v1", func(t *testing.T) {
		env.insertBoth(t, "7000", "100000020", eqRows("100000020", 7000, 920, 0, 91000, 20))
		env.flushBoth(t)
		env.insertBoth(t, "7000", "100000020", eqRows("100000020", 7000, 920, 1, 91020, 20))
		env.flushBoth(t)
		baseRes, _ := env.searchBoth(t, "C2 read", SearchOptions{
			Columns:   []string{"id", "version"},
			Condition: "routing_id = '100000020'",
			OrderBy:   "id ASC",
		}, "100000020")
		env.assertSearchEqual(t, "C2 rows equal", SearchOptions{
			Columns:   []string{"id", "version"},
			Condition: "routing_id = '100000020'",
			OrderBy:   "id ASC",
		}, "100000020", true, 20)
		eqAssertIDs(t, "C2 survivors", eqIDSequence(t, baseRes), eqIDList(91020, 20))
		env.eqCompareGroundTruth(t)
	})

	t.Run("C3_version_v1_then_v0", func(t *testing.T) {
		// both batches in one unflushed buffer: v0 dropped at enqueue
		env.insertBoth(t, "7001", "100000021", eqRows("100000021", 7001, 921, 1, 92020, 20))
		env.insertBoth(t, "7001", "100000021", eqRows("100000021", 7001, 921, 0, 92000, 20))
		env.flushBoth(t)
		baseRes, _ := env.searchBoth(t, "C3 read", SearchOptions{
			Columns:   []string{"id", "version"},
			Condition: "routing_id = '100000021'",
			OrderBy:   "id ASC",
		}, "100000021")
		env.assertSearchEqual(t, "C3 rows equal", SearchOptions{
			Columns:   []string{"id", "version"},
			Condition: "routing_id = '100000021'",
			OrderBy:   "id ASC",
		}, "100000021", true, 20)
		eqAssertIDs(t, "C3 survivors", eqIDSequence(t, baseRes), eqIDList(92020, 20))
		env.eqCompareGroundTruth(t)
	})

	t.Run("C4_reimport_version_bump", func(t *testing.T) {
		// fresh re-import of (tenant 0, 5000) with version 2 replaces all v1 rows
		env.insertBoth(t, "5000", "100000000", eqRows("100000000", 5000, 900, 2, 93000, 60))
		env.flushBoth(t)
		baseRes, _ := env.searchBoth(t, "C4 read", SearchOptions{
			Columns:   []string{"id", "version"},
			Condition: "routing_id = '100000000' AND fileid = 5000",
			OrderBy:   "id ASC",
		}, "100000000")
		env.assertSearchEqual(t, "C4 rows equal", SearchOptions{
			Columns:   []string{"id", "version"},
			Condition: "routing_id = '100000000' AND fileid = 5000",
			OrderBy:   "id ASC",
		}, "100000000", true, 60)
		eqAssertIDs(t, "C4 survivors", eqIDSequence(t, baseRes), eqIDList(93000, 60))
		env.eqCompareGroundTruth(t)
	})

	t.Run("C5_registration_unmapped_tenant", func(t *testing.T) {
		if _, found := eqInstanceForRoutingID(t, env.mainDB, eqTableSharded, "100000012"); found {
			t.Fatalf("100000012 must be unmapped before this case")
		}
		env.insertBoth(t, "7100", "100000012", eqRows("100000012", 7100, 912, 1, 94000, 40))
		env.flushBoth(t)
		env.assertSearchEqual(t, "C5 rows equal", SearchOptions{
			Columns:   []string{"id", "routing_id"},
			Condition: "routing_id = '100000012'",
			OrderBy:   "id ASC",
		}, "100000012", true, 40)

		inst, found := eqInstanceForRoutingID(t, env.mainDB, eqTableSharded, "100000012")
		if !found || inst != eqInstMain {
			t.Fatalf("100000012: expected mapping to default %s, got (%q, %v)", eqInstMain, inst, found)
		}
		if n := eqCountRoutingID(t, env.mainDB, eqTableSharded, "100000012"); n != 40 {
			t.Fatalf("100000012: expected 40 rows on control-plane database, got %d", n)
		}
		for _, db := range []*sql.DB{env.shardA, env.shardB} {
			if n := eqCountRoutingID(t, db, eqTableSharded, "100000012"); n != 0 {
				t.Fatalf("100000012: expected 0 rows on non-default instance, got %d", n)
			}
		}
		env.eqCompareGroundTruth(t)
	})

	t.Run("C6_read_after_flush", func(t *testing.T) {
		env.insertBoth(t, "7200", "100000005", eqRows("100000005", 7200, 905, 1, 95000, 20))
		env.flushBoth(t)
		baseRes, _ := env.searchBoth(t, "C6 read", SearchOptions{
			Columns:   []string{"id", "fileid"},
			Condition: "routing_id = '100000005' AND fileid = 7200",
			OrderBy:   "id ASC",
		}, "100000005")
		env.assertSearchEqual(t, "C6 rows equal", SearchOptions{
			Columns:   []string{"id", "fileid"},
			Condition: "routing_id = '100000005' AND fileid = 7200",
			OrderBy:   "id ASC",
		}, "100000005", true, 20)
		eqAssertIDs(t, "C6 rows visible", eqIDSequence(t, baseRes), eqIDList(95000, 20))
	})

	readColumns := []string{"id", "routing_id", "fileid", "version"}
	// id range crossing the tenant-3 (instance "1") / tenant-4 (instance "2")
	// boundary: last 50 surviving ids of tenant3/5032 plus first 50 of
	// tenant4/5040.
	crossCondition := "id >= '00002350' AND id <= '00002549'"
	// all 100 surviving ids of (tenant 3, 5032), resident on one instance
	tenant3Condition := "routing_id = '100000003' AND id >= '00002300' AND id <= '00002399'"

	t.Run("R1_point_lookup_routed", func(t *testing.T) {
		env.assertSearchEqual(t, "R1", SearchOptions{
			Columns:   readColumns,
			Condition: "routing_id = '100000006' AND fileid = 5060",
			OrderBy:   "id ASC",
		}, "100000006", true, 100)
	})

	t.Run("R2_routing_id_required", func(t *testing.T) {
		// RoutingID unset on the sharded candidate is an error; the baseline
		// runs the same query fine (RoutingID is a sharded-only requirement).
		opts := SearchOptions{
			Table:     eqTableSharded,
			Columns:   readColumns,
			Condition: "routing_id = '100000006' AND fileid = 5060",
			OrderBy:   "id ASC",
		}
		if _, err := env.sharded.SearchV2(&opts); !errors.Is(err, ErrRoutingIDRequired) {
			t.Fatalf("R2: expected ErrRoutingIDRequired, got %v", err)
		}
		baseRes, err := env.plain.SearchV2(&SearchOptions{
			Table:     eqTablePlain,
			Columns:   readColumns,
			Condition: "routing_id = '100000006' AND fileid = 5060",
			OrderBy:   "id ASC",
		})
		if err != nil {
			t.Fatalf("R2: baseline search failed: %v", err)
		}
		if len(baseRes.Rows) != 100 {
			t.Fatalf("R2: baseline expected 100 rows, got %d", len(baseRes.Rows))
		}
	})

	t.Run("R3_boundary_tenants", func(t *testing.T) {
		// crossCondition spans two instances; the union of the two per-tenant
		// routed queries must match the baseline's per-tenant results.
		total := 0
		for _, tenant := range []string{"100000003", "100000004"} {
			rows := env.assertSearchEqual(t, "R3 "+tenant, SearchOptions{
				Columns:   readColumns,
				Condition: fmt.Sprintf("routing_id = '%s' AND %s", tenant, crossCondition),
			}, tenant, false, -1)
			total += len(rows)
		}
		if total != 100 {
			t.Fatalf("R3: expected 100 rows across boundary tenants, got %d", total)
		}
	})

	t.Run("R4_order_limit_offset", func(t *testing.T) {
		env.assertSearchEqual(t, "R4", SearchOptions{
			Columns:   readColumns,
			Condition: tenant3Condition,
			OrderBy:   "id DESC",
			Limit:     50,
			Offset:    20,
		}, "100000003", true, 50)
	})

	t.Run("R5_count_over_total", func(t *testing.T) {
		rows := env.assertSearchEqual(t, "R5", SearchOptions{
			Columns:   []string{"id", "routing_id", "count(*) OVER() AS total"},
			Condition: tenant3Condition,
			OrderBy:   "id ASC",
			Limit:     30,
		}, "100000003", true, 30)
		for i, row := range rows {
			var m map[string]any
			if err := json.Unmarshal([]byte(row), &m); err != nil {
				t.Fatalf("R5: failed to decode row %d: %v", i, err)
			}
			if total, ok := m["total"].(float64); !ok || total != 100 {
				t.Fatalf("R5: row %d total=%v, want 100 (full pre-limit match count)", i, m["total"])
			}
		}
	})

	t.Run("R6_nonexistent_routing", func(t *testing.T) {
		// unmapped RoutingID: no mapping means no data, empty result
		env.assertSearchEqual(t, "R6 routed", SearchOptions{
			Columns:   readColumns,
			Condition: "routing_id = '100000077'",
			OrderBy:   "id ASC",
		}, "100000077", true, 0)
		// RoutingID unset: error on the sharded candidate
		if _, err := env.sharded.SearchV2(&SearchOptions{
			Table:     eqTableSharded,
			Columns:   readColumns,
			Condition: "routing_id = '100000077'",
			OrderBy:   "id ASC",
		}); !errors.Is(err, ErrRoutingIDRequired) {
			t.Fatalf("R6 unset: expected ErrRoutingIDRequired, got %v", err)
		}
	})

	t.Run("R7_vector_order", func(t *testing.T) {
		dsColumn := "vector <-> '[-1,0.1,0.1,0.1]' as ds"
		probe := SearchOptions{
			Table:     eqTablePlain,
			Columns:   []string{"id", dsColumn},
			Condition: "routing_id = '100000006'",
			OrderBy:   "ds ASC",
			Limit:     1,
		}
		if _, err := env.plain.SearchV2(&probe); err != nil {
			t.Skipf("vecf16 <-> unavailable, skipping vector case: %v", err)
		}

		baseRes, candRes := env.searchBoth(t, "R7 tenant", SearchOptions{
			Columns:   []string{"id", dsColumn},
			Condition: "routing_id = '100000006'",
			OrderBy:   "ds ASC",
			Limit:     20,
		}, "100000006")
		eqAssertIDs(t, "R7 tenant id sequence", eqIDSequence(t, candRes), eqIDSequence(t, baseRes))

		baseRes, candRes = env.searchBoth(t, "R7 boundary", SearchOptions{
			Columns:   []string{"id", dsColumn},
			Condition: "routing_id = '100000003' AND " + crossCondition,
			OrderBy:   "ds ASC",
			Limit:     20,
		}, "100000003")
		eqAssertIDs(t, "R7 boundary id sequence", eqIDSequence(t, candRes), eqIDSequence(t, baseRes))
		// model check: distance ascends with id%1500, so ids 2350..2369 win
		eqAssertIDs(t, "R7 boundary expected", eqIDSequence(t, baseRes), eqIDList(2350, 20))
	})

	t.Run("U1_update_routed", func(t *testing.T) {
		n := env.updateBoth(t, "U1", "routing_id = '100000001' AND fileid = 5010",
			map[string]any{"mtime": 1800000000}, "100000001")
		if n != 100 {
			t.Fatalf("U1: expected 100 rows affected, got %d", n)
		}
		env.eqCompareGroundTruth(t)
	})

	t.Run("U2_update_routing_id_required", func(t *testing.T) {
		// RoutingID unset on a sharded update is an error and no row anywhere
		// may change; the baseline is left untouched too so ground truth holds.
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		_, err := env.sharded.UpdateByQueryWithContextV2(ctx, &UpdateByQueryOptions{
			Table:     eqTableSharded,
			Condition: "routing_id = '100000005' AND fileid = 5050",
			Updates:   map[string]any{"mtime": 1810000000},
		})
		if !errors.Is(err, ErrRoutingIDRequired) {
			t.Fatalf("U2: expected ErrRoutingIDRequired, got %v", err)
		}
		for _, db := range env.allDBs() {
			if n := eqScalarInt(t, db, fmt.Sprintf(
				"SELECT count(*) FROM public.%s WHERE mtime = 1810000000", eqTableSharded)); n != 0 {
				t.Fatalf("U2: rejected update must change no rows, found %d changed", n)
			}
		}
		env.eqCompareGroundTruth(t)
	})

	t.Run("U3_update_zero_match", func(t *testing.T) {
		if n := env.updateBoth(t, "U3 routed", "routing_id = '100000001' AND fileid = 99999",
			map[string]any{"mtime": 1}, "100000001"); n != 0 {
			t.Fatalf("U3 routed: expected 0 rows affected, got %d", n)
		}
		// unmapped RoutingID: no mapping means no data, 0 rows affected
		if n := env.updateBoth(t, "U3 unmapped", "routing_id = '100000077'",
			map[string]any{"mtime": 1}, "100000077"); n != 0 {
			t.Fatalf("U3 unmapped: expected 0 rows affected, got %d", n)
		}
	})

	t.Run("D1_delete_sync", func(t *testing.T) {
		// one (tenant,fileid) per instance-resident tenant
		for _, d := range []struct{ fileID, tenant string }{
			{"5012", "100000001"}, {"5052", "100000005"}, {"5092", "100000009"},
		} {
			env.runBoth(t, "DeleteSyncV2 "+d.tenant, func(p *BulkProcessor) error {
				return p.DeleteSyncV2(d.fileID, d.tenant)
			})
			env.assertSearchEqual(t, "D1 gone "+d.tenant, SearchOptions{
				Columns:   readColumns,
				Condition: fmt.Sprintf("routing_id = '%s' AND fileid = %s", d.tenant, d.fileID),
				OrderBy:   "id ASC",
			}, d.tenant, true, 0)
		}
		// untouched sibling combo intact
		env.assertSearchEqual(t, "D1 intact", SearchOptions{
			Columns:   readColumns,
			Condition: "routing_id = '100000001' AND fileid = 5011",
			OrderBy:   "id ASC",
		}, "100000001", true, 100)
		env.eqCompareGroundTruth(t)
	})

	t.Run("D2_delete_async", func(t *testing.T) {
		for _, d := range []struct{ fileID, tenant string }{
			{"5021", "100000002"}, {"5061", "100000006"}, {"5101", "100000010"},
		} {
			env.runBoth(t, "DeleteV2 "+d.tenant, func(p *BulkProcessor) error {
				return p.DeleteV2(d.fileID, d.tenant)
			})
		}
		env.flushBoth(t)
		for _, d := range []struct{ fileID, tenant string }{
			{"5021", "100000002"}, {"5061", "100000006"}, {"5101", "100000010"},
		} {
			env.assertSearchEqual(t, "D2 gone "+d.tenant, SearchOptions{
				Columns:   readColumns,
				Condition: fmt.Sprintf("routing_id = '%s' AND fileid = %s", d.tenant, d.fileID),
				OrderBy:   "id ASC",
			}, d.tenant, true, 0)
		}
		env.eqCompareGroundTruth(t)
	})

	t.Run("D3_delete_by_group", func(t *testing.T) {
		// group_id is a per-tenant constant, so group delete is tenant-scoped
		for _, d := range []struct{ groupID, tenant string }{
			{"903", "100000003"}, {"907", "100000007"}, {"911", "100000011"},
		} {
			env.runBoth(t, "DeleteByGroupV2 "+d.tenant, func(p *BulkProcessor) error {
				return p.DeleteByGroupV2(d.groupID, d.tenant)
			})
			env.assertSearchEqual(t, "D3 gone "+d.tenant, SearchOptions{
				Columns:   readColumns,
				Condition: fmt.Sprintf("routing_id = '%s'", d.tenant),
				OrderBy:   "id ASC",
			}, d.tenant, true, 0)
		}
		env.assertSearchEqual(t, "D3 intact", SearchOptions{
			Columns:   readColumns,
			Condition: "routing_id = '100000002'",
			OrderBy:   "id ASC",
		}, "100000002", true, 200)
		env.eqCompareGroundTruth(t)
	})

	t.Run("D4_delete_reinsert_one_buffer", func(t *testing.T) {
		// insert A, delete, insert B in one unflushed buffer: buffer dedup
		// cancels A, the delete wipes pre-existing rows, B survives
		env.insertBoth(t, "5040", "100000004", eqRows("100000004", 5040, 904, 1, 96000, 20))
		env.runBoth(t, "D4 DeleteV2", func(p *BulkProcessor) error {
			return p.DeleteV2("5040", "100000004")
		})
		env.insertBoth(t, "5040", "100000004", eqRows("100000004", 5040, 904, 1, 97000, 20))
		env.flushBoth(t)
		baseRes, _ := env.searchBoth(t, "D4 read", SearchOptions{
			Columns:   readColumns,
			Condition: "routing_id = '100000004' AND fileid = 5040",
			OrderBy:   "id ASC",
		}, "100000004")
		env.assertSearchEqual(t, "D4 rows equal", SearchOptions{
			Columns:   readColumns,
			Condition: "routing_id = '100000004' AND fileid = 5040",
			OrderBy:   "id ASC",
		}, "100000004", true, 20)
		eqAssertIDs(t, "D4 survivors", eqIDSequence(t, baseRes), eqIDList(97000, 20))
		env.eqCompareGroundTruth(t)
	})

	t.Run("D5_delete_nonexistent", func(t *testing.T) {
		env.runBoth(t, "D5 sync", func(p *BulkProcessor) error {
			return p.DeleteSyncV2("111111", "100000088")
		})
		env.runBoth(t, "D5 async", func(p *BulkProcessor) error {
			return p.DeleteV2("111111", "100000089")
		})
		env.flushBoth(t)
		env.eqCompareGroundTruth(t)

		// deletes must never register mappings (invariant 3)
		for _, tenant := range []string{"100000088", "100000089"} {
			if inst, found := eqInstanceForRoutingID(t, env.mainDB, eqTableSharded, tenant); found {
				t.Fatalf("delete registered a mapping for %s -> %s; deletes must not register", tenant, inst)
			}
		}
	})

	t.Run("final_invariants", func(t *testing.T) {
		env.eqCompareGroundTruth(t)

		// candidate-only physical invariants
		instByDB := []struct {
			db   *sql.DB
			inst string
		}{
			{env.mainDB, eqInstMain},
			{env.shardA, eqInstA},
			{env.shardB, eqInstB},
		}

		tenantRows := make(map[string]map[string]int) // tenant -> instance -> rows
		pkSeen := make(map[string]bool)
		for _, d := range instByDB {
			rows, err := d.db.Query(fmt.Sprintf(
				"SELECT routing_id, fileid::text, id FROM public.%s", eqTableSharded))
			if err != nil {
				t.Fatalf("invariant scan failed on %s: %v", d.inst, err)
			}
			for rows.Next() {
				var tenant, fileid, id string
				if err := rows.Scan(&tenant, &fileid, &id); err != nil {
					rows.Close()
					t.Fatalf("invariant scan failed on %s: %v", d.inst, err)
				}
				if tenantRows[tenant] == nil {
					tenantRows[tenant] = make(map[string]int)
				}
				tenantRows[tenant][d.inst]++
				pk := tenant + "|" + fileid + "|" + id
				if pkSeen[pk] {
					t.Fatalf("duplicate PK across the 3-DB union: %s", pk)
				}
				pkSeen[pk] = true
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				t.Fatalf("invariant iteration failed on %s: %v", d.inst, err)
			}
			rows.Close()
		}

		for tenant, byInst := range tenantRows {
			if len(byInst) != 1 {
				t.Fatalf("tenant %s has rows on multiple instances: %v", tenant, byInst)
			}
			mapped, found := eqInstanceForRoutingID(t, env.mainDB, eqTableSharded, tenant)
			if !found {
				t.Fatalf("tenant %s has data but no mapping row", tenant)
			}
			for inst := range byInst {
				if inst != mapped {
					t.Fatalf("tenant %s mapped to %s but rows live on %s", tenant, mapped, inst)
				}
			}
		}
	})
}
