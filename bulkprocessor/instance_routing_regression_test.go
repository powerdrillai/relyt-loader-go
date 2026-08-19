package bulkprocessor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"
)

const (
	routingregressionTenantA    = "410000010"
	routingregressionTenantB    = "410000011"
	routingregressionMixedRoute = "410000012"
	routingregressionMixedOther = "410000013"

	routingregressionFileA     int64 = 61010
	routingregressionFileB     int64 = 61011
	routingregressionMixedFile int64 = 61012

	routingregressionAID  = "routingregression-a"
	routingregressionBID  = "routingregression-b"
	routingregressionAExt = "A-SEED"
	routingregressionBExt = "B-SECRET"
)

var routingregressionTenants = []string{
	routingregressionTenantA,
	routingregressionTenantB,
	routingregressionMixedRoute,
	routingregressionMixedOther,
}

// routingregressionCleanup removes only this test's namespaced rows and mappings from
// the shared equivalence environment. It intentionally never creates or drops
// any of the fixed equivalence databases.
func routingregressionCleanup(t *testing.T) {
	t.Helper()
	for _, dbName := range []string{eqDBMain, eqDBShardA, eqDBShardB} {
		db := eqOpenDB(t, dbName)
		for _, tenant := range routingregressionTenants {
			if _, err := db.Exec(fmt.Sprintf(
				"DELETE FROM public.%s WHERE routing_id = $1", eqTableSharded), tenant); err != nil {
				db.Close()
				t.Fatalf("cleanup tenant %s from %s: %v", tenant, dbName, err)
			}
		}
		if err := db.Close(); err != nil {
			t.Errorf("close cleanup connection to %s: %v", dbName, err)
		}
	}

	mainDB := eqOpenDB(t, eqDBMain)
	defer mainDB.Close()
	for _, tenant := range routingregressionTenants {
		if _, err := mainDB.Exec(fmt.Sprintf(
			"DELETE FROM relyt_sys.%s_relyt_instance_routing WHERE routing_id = $1", eqTableSharded), tenant); err != nil {
			t.Fatalf("cleanup mapping for tenant %s: %v", tenant, err)
		}
	}
}

func routingregressionMapColocatedTenants(t *testing.T, mainDB *sql.DB) {
	t.Helper()
	query := fmt.Sprintf(`
INSERT INTO relyt_sys.%s_relyt_instance_routing (routing_id, instance_id)
VALUES ($1, $3), ($2, $3)
ON CONFLICT (routing_id) DO UPDATE SET instance_id = EXCLUDED.instance_id`, eqTableSharded)
	if _, err := mainDB.Exec(query, routingregressionTenantA, routingregressionTenantB, eqInstA); err != nil {
		t.Fatalf("map routingregression tenants to the same instance: %v", err)
	}
}

func routingregressionSeedColocatedRows(t *testing.T, p *BulkProcessor) {
	t.Helper()
	a := eqMakeRecords(routingregressionTenantA, routingregressionFileA, routingregressionAID)
	a[0].Ext = routingregressionAExt
	a[0].Mtime = 10
	b := eqMakeRecords(routingregressionTenantB, routingregressionFileB, routingregressionBID)
	b[0].Ext = routingregressionBExt
	b[0].Mtime = 20

	if err := p.InsertV2(fmt.Sprint(routingregressionFileA), routingregressionTenantA, a); err != nil {
		t.Fatalf("seed tenant A: %v", err)
	}
	if err := p.InsertV2(fmt.Sprint(routingregressionFileB), routingregressionTenantB, b); err != nil {
		t.Fatalf("seed tenant B: %v", err)
	}
	if err := p.Flush(); err != nil {
		t.Fatalf("flush colocated seed rows: %v", err)
	}
}

type routingregressionState struct {
	Rows     []string
	Mappings []string
}

// routingregressionSnapshot captures full physical equivalence rows (including their database)
// plus control-plane mappings for the requested tenants.
func routingregressionSnapshot(t *testing.T, env *eqEnv, tenants ...string) routingregressionState {
	t.Helper()
	var state routingregressionState
	dbs := []struct {
		name string
		db   *sql.DB
	}{
		{eqDBMain, env.mainDB},
		{eqDBShardA, env.shardA},
		{eqDBShardB, env.shardB},
	}
	query := fmt.Sprintf(`SELECT concat_ws('|', id, routing_id, chunk_id, chunk_type, user_id,
		creator, sharer, fileid, group_id, ctime, mtime, y, ym, ymd, ext, fsize,
		parent_id, ftype, version, index_update_time, ext_group, vector::text)
		FROM public.%s WHERE routing_id = $1`, eqTableSharded)
	for _, d := range dbs {
		for _, tenant := range tenants {
			rows, err := d.db.Query(query, tenant)
			if err != nil {
				t.Fatalf("snapshot tenant %s from %s: %v", tenant, d.name, err)
			}
			for rows.Next() {
				var signature string
				if err := rows.Scan(&signature); err != nil {
					rows.Close()
					t.Fatalf("scan tenant %s from %s: %v", tenant, d.name, err)
				}
				state.Rows = append(state.Rows, d.name+"|"+signature)
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				t.Fatalf("iterate tenant %s from %s: %v", tenant, d.name, err)
			}
			rows.Close()
		}
	}
	for _, tenant := range tenants {
		instance, found := eqInstanceForRoutingID(t, env.mainDB, eqTableSharded, tenant)
		if found {
			state.Mappings = append(state.Mappings, tenant+"->"+instance)
		} else {
			state.Mappings = append(state.Mappings, tenant+"-><missing>")
		}
	}
	sort.Strings(state.Rows)
	sort.Strings(state.Mappings)
	return state
}

func routingregressionAssertStateEqual(t *testing.T, label string, got, want routingregressionState) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s changed state:\n got rows=%v mappings=%v\nwant rows=%v mappings=%v",
			label, got.Rows, got.Mappings, want.Rows, want.Mappings)
	}
}

func routingregressionAssertSearch(t *testing.T, label string, result *SearchResult, wantRows int) {
	t.Helper()
	if result == nil {
		t.Fatalf("%s returned nil result", label)
	}
	normalized := eqNormalizeRows(t, result)
	if len(result.Rows) != wantRows {
		t.Fatalf("%s returned %d rows, want %d: %v", label, len(result.Rows), wantRows, normalized)
	}
	if len(result.Rows) == 0 {
		return
	}

	routingIndex, idIndex := -1, -1
	for i, column := range result.Columns {
		switch column {
		case "routing_id":
			routingIndex = i
		case "id":
			idIndex = i
		}
	}
	if routingIndex < 0 || idIndex < 0 {
		t.Fatalf("%s result lacks tenant identity columns: %v", label, result.Columns)
	}
	for i, row := range result.Rows {
		if got := fmt.Sprint(row[routingIndex]); got != routingregressionTenantA {
			t.Fatalf("%s exposed non-A routing_id in row %d: got %q, rows=%v", label, i, got, normalized)
		}
		if got := fmt.Sprint(row[idIndex]); got != routingregressionAID {
			t.Fatalf("%s exposed non-A id in row %d: got %q, rows=%v", label, i, got, normalized)
		}
		for _, value := range row {
			switch fmt.Sprint(value) {
			case routingregressionTenantB, routingregressionBID, routingregressionBExt:
				t.Fatalf("%s exposed tenant B value %q in row %d: %v", label, value, i, normalized)
			}
		}
	}
}

func routingregressionUpdate(t *testing.T, p *BulkProcessor, condition, ext string) (int64, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return p.UpdateByQueryWithContextV2(ctx, &UpdateByQueryOptions{
		Table:     eqTableSharded,
		RoutingID: routingregressionTenantA,
		Condition: condition,
		Updates:   map[string]any{"ext": ext},
	})
}

func TestRoutingRegressionRoutingRegression(t *testing.T) {
	eqSkipIfUnreachable(t)
	BootstrapEquivalenceEnv(t)
	routingregressionCleanup(t)
	defer routingregressionCleanup(t)

	env := &eqEnv{
		mainDB: eqOpenDB(t, eqDBMain),
		shardA: eqOpenDB(t, eqDBShardA),
		shardB: eqOpenDB(t, eqDBShardB),
	}
	defer env.mainDB.Close()
	defer env.shardA.Close()
	defer env.shardB.Close()

	routingregressionMapColocatedTenants(t, env.mainDB)
	processor := newEqProcessor(t, eqTableSharded)
	defer processor.Shutdown()
	routingregressionSeedColocatedRows(t, processor)

	for _, tenant := range []string{routingregressionTenantA, routingregressionTenantB} {
		if instance, found := eqInstanceForRoutingID(t, env.mainDB, eqTableSharded, tenant); !found || instance != eqInstA {
			t.Fatalf("tenant %s is not colocated on %s: mapping=%q found=%v", tenant, eqInstA, instance, found)
		}
		if got := eqCountRoutingID(t, env.shardA, eqTableSharded, tenant); got != 1 {
			t.Fatalf("tenant %s: got %d rows on colocated shard, want 1", tenant, got)
		}
		if got := eqCountRoutingID(t, env.mainDB, eqTableSharded, tenant) +
			eqCountRoutingID(t, env.shardB, eqTableSharded, tenant); got != 0 {
			t.Fatalf("tenant %s: got %d rows outside colocated shard, want 0", tenant, got)
		}
	}

	t.Run("searches_are_tenant_scoped", func(t *testing.T) {
		cases := []struct {
			name      string
			condition string
			wantRows  int
		}{
			{name: "empty_condition", condition: "", wantRows: 1},
			{name: "B_selecting_condition", condition: "routing_id = '" + routingregressionTenantB + "'", wantRows: 0},
			{name: "OR_condition", condition: "routing_id = '" + routingregressionTenantA + "' OR routing_id = '" + routingregressionTenantB + "'", wantRows: 1},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := processor.SearchV2(&SearchOptions{
					Table:     eqTableSharded,
					Columns:   []string{"id", "routing_id", "ext"},
					Condition: tc.condition,
					OrderBy:   "id ASC",
					RoutingID: routingregressionTenantA,
				})
				if err != nil {
					t.Fatalf("RoutingID=A search failed: %v", err)
				}
				routingregressionAssertSearch(t, tc.name, result, tc.wantRows)
			})
		}
	})

	t.Run("updates_are_tenant_scoped", func(t *testing.T) {
		beforeB := routingregressionSnapshot(t, env, routingregressionTenantB)
		cases := []struct {
			name      string
			condition string
			ext       string
			wantCount int64
		}{
			{name: "empty_condition", condition: "", ext: "A-EMPTY", wantCount: 1},
			{name: "B_selecting_condition", condition: "routing_id = '" + routingregressionTenantB + "'", ext: "A-BSEL", wantCount: 0},
			{name: "OR_condition", condition: "routing_id = '" + routingregressionTenantA + "' OR routing_id = '" + routingregressionTenantB + "'", ext: "A-OR", wantCount: 1},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				count, err := routingregressionUpdate(t, processor, tc.condition, tc.ext)
				if err != nil {
					t.Fatalf("RoutingID=A update failed: %v", err)
				}
				if count != tc.wantCount {
					t.Fatalf("updated %d rows, want %d", count, tc.wantCount)
				}
				routingregressionAssertStateEqual(t, "tenant B", routingregressionSnapshot(t, env, routingregressionTenantB), beforeB)
			})
		}
	})

	t.Run("mixed_InsertV2_is_atomic_before_routing", func(t *testing.T) {
		mixed := append(
			eqMakeRecords(routingregressionMixedRoute, routingregressionMixedFile, "routingregression-mixed-a"),
			eqMakeRecords(routingregressionMixedOther, routingregressionMixedFile, "routingregression-mixed-b")...,
		)
		err := processor.InsertV2(fmt.Sprint(routingregressionMixedFile), routingregressionMixedRoute, mixed)
		if !errors.Is(err, ErrRoutingIDMismatch) {
			t.Fatalf("mixed batch: want ErrRoutingIDMismatch, got %v", err)
		}
		// Flush makes a partial-enqueue bug observable as physical rows.
		if err := processor.Flush(); err != nil {
			t.Fatalf("flush after rejected mixed batch: %v", err)
		}
		want := routingregressionState{
			Mappings: []string{
				routingregressionMixedOther + "-><missing>",
				routingregressionMixedRoute + "-><missing>",
			},
		}
		sort.Strings(want.Mappings)
		routingregressionAssertStateEqual(t, "rejected mixed InsertV2", routingregressionSnapshot(t, env,
			routingregressionMixedRoute, routingregressionMixedOther), want)
	})

	t.Run("routing_id_update_rejection_preserves_state", func(t *testing.T) {
		before := routingregressionSnapshot(t, env, routingregressionTenantA, routingregressionTenantB)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		count, err := processor.UpdateByQueryWithContextV2(ctx, &UpdateByQueryOptions{
			Table:     eqTableSharded,
			RoutingID: routingregressionTenantA,
			Condition: "",
			Updates:   map[string]any{"routing_id": routingregressionTenantB},
		})
		if !errors.Is(err, ErrRoutingIDUpdateForbidden) {
			t.Fatalf("routing_id update: want ErrRoutingIDUpdateForbidden, got count=%d err=%v", count, err)
		}
		if count != 0 {
			t.Fatalf("rejected routing_id update reported %d affected rows, want 0", count)
		}
		routingregressionAssertStateEqual(t, "rejected routing_id update",
			routingregressionSnapshot(t, env, routingregressionTenantA, routingregressionTenantB), before)
	})
}
