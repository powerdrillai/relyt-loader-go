package bulkprocessor

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

const (
	tenantisolationAExt = "ASEED"
	tenantisolationBExt = "BSECRET"
)

type tenantisolationRowState struct {
	ID        string
	RoutingID string
	Ext       string
	Mtime     int64
}

type tenantisolationEnv struct {
	tenantA string
	tenantB string
	idA     string
	idB     string

	// Set only after the two-row mapping transaction commits. Cleanup must not
	// remove a mapping that this invocation did not successfully create.
	ownsMappings bool
}

func tenantisolationUniqueTenants(t *testing.T) (string, string) {
	t.Helper()
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatalf("generate unique tenantisolation tenants: %v", err)
	}
	// routing_id is TEXT, but positive decimal strings also exercise the common
	// production convention. Sixty random bits make concurrent collisions
	// negligible without relying on fixed, broadly cleaned-up tenant IDs.
	base := binary.BigEndian.Uint64(random[:]) & ((uint64(1) << 60) - 1)
	return fmt.Sprintf("31%018d1", base), fmt.Sprintf("31%018d2", base)
}

// This test consumes the already provisioned equivalence integration cluster.
// It deliberately does not call BootstrapEquivalenceEnv: that helper replaces
// shared UDFs and mutates shared config/registry rows. Missing prerequisites
// mean this is not the dedicated integration environment and are a skip.
func tenantisolationRequireDedicatedEnvironment(t *testing.T) {
	t.Helper()
	for _, dbName := range []string{eqDBMain, eqDBShardA, eqDBShardB} {
		db, err := sql.Open("postgres", eqConnStr(dbName))
		if err != nil {
			t.Skipf("tenantisolation requires dedicated equivalence databases: open %s: %v", dbName, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := db.PingContext(ctx); err != nil {
			cancel()
			db.Close()
			t.Skipf("tenantisolation requires dedicated equivalence databases: ping %s: %v", dbName, err)
		}
		cancel()

		var tableReady, searchUDFReady, updateUDFReady bool
		err = db.QueryRow(`
SELECT
    to_regclass($1) IS NOT NULL,
    EXISTS (
        SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'relyt_sys' AND p.proname = 'get_columns_with_condition'
    ),
    EXISTS (
        SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'relyt_sys' AND p.proname = 'generate_update_by_query_sql'
    )`, "public."+eqTableSharded).Scan(&tableReady, &searchUDFReady, &updateUDFReady)
		db.Close()
		if err != nil || !tableReady || !searchUDFReady || !updateUDFReady {
			t.Skipf("tenantisolation requires pre-provisioned dedicated equivalence DB %s (table=%t search_udf=%t update_udf=%t err=%v)",
				dbName, tableReady, searchUDFReady, updateUDFReady, err)
		}
	}

	mainDB := eqOpenDB(t, eqDBMain)
	defer mainDB.Close()
	var routingReady, registryReady, configReady bool
	err := mainDB.QueryRow(`SELECT
    to_regclass($1) IS NOT NULL,
    to_regclass('relyt_sys.relyt_instance_registry') IS NOT NULL,
    to_regclass('relyt_sys.sdk_loader_config') IS NOT NULL`,
		"relyt_sys."+eqTableSharded+"_relyt_instance_routing").Scan(
		&routingReady, &registryReady, &configReady)
	if err != nil || !routingReady || !registryReady || !configReady {
		t.Skipf("tenantisolation requires pre-provisioned dedicated control plane (routing=%t registry=%t config=%t err=%v)",
			routingReady, registryReady, configReady, err)
	}
}

func (e *tenantisolationEnv) cleanup(t *testing.T) {
	t.Helper()
	for _, dbName := range []string{eqDBMain, eqDBShardA, eqDBShardB} {
		db, err := sql.Open("postgres", eqConnStr(dbName))
		if err != nil {
			t.Errorf("tenantisolation cleanup: open %s: %v", dbName, err)
			continue
		}
		_, execErr := db.Exec(fmt.Sprintf(
			"DELETE FROM public.%s WHERE routing_id IN ($1, $2)", eqTableSharded),
			e.tenantA, e.tenantB)
		if execErr != nil {
			t.Errorf("tenantisolation cleanup: delete rows from %s: %v", dbName, execErr)
		}
		db.Close()
	}
	if !e.ownsMappings {
		return
	}
	mainDB, err := sql.Open("postgres", eqConnStr(eqDBMain))
	if err != nil {
		t.Errorf("tenantisolation cleanup: open main for mappings: %v", err)
		return
	}
	defer mainDB.Close()
	if _, err := mainDB.Exec(fmt.Sprintf(
		"DELETE FROM relyt_sys.%s_relyt_instance_routing WHERE routing_id IN ($1, $2)", eqTableSharded),
		e.tenantA, e.tenantB); err != nil {
		t.Errorf("tenantisolation cleanup: delete mappings: %v", err)
	}
}

func (e *tenantisolationEnv) insertMappings(t *testing.T) {
	t.Helper()
	mainDB := eqOpenDB(t, eqDBMain)
	defer mainDB.Close()
	tx, err := mainDB.Begin()
	if err != nil {
		t.Fatalf("begin tenantisolation mapping transaction: %v", err)
	}
	mappingSQL := fmt.Sprintf(`
INSERT INTO relyt_sys.%s_relyt_instance_routing (routing_id, instance_id)
VALUES ($1, $3), ($2, $3)`, eqTableSharded)
	if _, err := tx.Exec(mappingSQL, e.tenantA, e.tenantB, eqInstA); err != nil {
		tx.Rollback()
		t.Fatalf("insert unique tenantisolation mappings: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tenantisolation mappings: %v", err)
	}
	e.ownsMappings = true

	for _, tenant := range []string{e.tenantA, e.tenantB} {
		var instance string
		err := mainDB.QueryRow(fmt.Sprintf(
			"SELECT instance_id FROM relyt_sys.%s_relyt_instance_routing WHERE routing_id = $1", eqTableSharded),
			tenant).Scan(&instance)
		if err != nil || instance != eqInstA {
			t.Fatalf("tenant %s mapping: got instance %q, err=%v; want %s", tenant, instance, err, eqInstA)
		}
	}
}

func (e *tenantisolationEnv) seedRows(t *testing.T) {
	t.Helper()
	db := eqOpenDB(t, eqDBShardA)
	defer db.Close()

	insert := fmt.Sprintf(`
INSERT INTO public.%s
    (id, routing_id, chunk_id, chunk_type, user_id, creator, sharer, fileid,
     group_id, ctime, mtime, y, ym, ymd, ext, fsize, parent_id, ftype,
     version, index_update_time, ext_group, vector)
VALUES
    ($1, $2, 1, 'text', 1001, 1001, 1001, 31901,
     1, 1700000000, $4, 2024, 202401, 20240101, $3, 1024, 0, 'doc',
     1, 1700000000, 'default', '[0.1,0.2,0.3,0.4]'::vecf16)`, eqTableSharded)

	for _, row := range []tenantisolationRowState{
		{ID: e.idA, RoutingID: e.tenantA, Ext: tenantisolationAExt, Mtime: 10},
		{ID: e.idB, RoutingID: e.tenantB, Ext: tenantisolationBExt, Mtime: 20},
	} {
		if _, err := db.Exec(insert, row.ID, row.RoutingID, row.Ext, row.Mtime); err != nil {
			t.Fatalf("seed tenant %s: %v", row.RoutingID, err)
		}
	}
}

func (e *tenantisolationEnv) resetRows(t *testing.T) {
	t.Helper()
	for _, dbName := range []string{eqDBMain, eqDBShardA, eqDBShardB} {
		db := eqOpenDB(t, dbName)
		if _, err := db.Exec(fmt.Sprintf(
			"DELETE FROM public.%s WHERE routing_id IN ($1, $2)", eqTableSharded),
			e.tenantA, e.tenantB); err != nil {
			db.Close()
			t.Fatalf("reset tenantisolation rows in %s: %v", dbName, err)
		}
		db.Close()
	}
	e.seedRows(t)
}

func (e *tenantisolationEnv) state(t *testing.T, routingID string) tenantisolationRowState {
	t.Helper()
	db := eqOpenDB(t, eqDBShardA)
	defer db.Close()

	var got tenantisolationRowState
	err := db.QueryRow(fmt.Sprintf(
		"SELECT id, routing_id, ext, mtime FROM public.%s WHERE routing_id = $1", eqTableSharded),
		routingID).Scan(&got.ID, &got.RoutingID, &got.Ext, &got.Mtime)
	if err != nil {
		t.Fatalf("read ground truth for tenant %s: %v", routingID, err)
	}
	return got
}

func (e *tenantisolationEnv) wantA() tenantisolationRowState {
	return tenantisolationRowState{ID: e.idA, RoutingID: e.tenantA, Ext: tenantisolationAExt, Mtime: 10}
}

func (e *tenantisolationEnv) wantB() tenantisolationRowState {
	return tenantisolationRowState{ID: e.idB, RoutingID: e.tenantB, Ext: tenantisolationBExt, Mtime: 20}
}

func (e *tenantisolationEnv) assertState(t *testing.T, wantA, wantB tenantisolationRowState) {
	t.Helper()
	if got := e.state(t, e.tenantA); got != wantA {
		t.Errorf("tenant A ground truth: got %+v, want %+v", got, wantA)
	}
	if got := e.state(t, e.tenantB); got != wantB {
		t.Errorf("tenant B was altered through tenant A options: got %+v, want %+v", got, wantB)
	}
}

func tenantisolationColumnIndex(t *testing.T, result *SearchResult, column string) int {
	t.Helper()
	if result == nil {
		t.Fatal("successful search returned nil result")
	}
	for i, got := range result.Columns {
		if got == column {
			return i
		}
	}
	t.Fatalf("successful search omitted required column %q: columns=%v", column, result.Columns)
	return -1
}

func (e *tenantisolationEnv) assertOneOuterARow(t *testing.T, result *SearchResult) {
	t.Helper()
	if result == nil {
		t.Fatal("successful search returned nil result")
	}
	if len(result.Rows) != 1 {
		t.Fatalf("successful search returned %d rows, want exactly tenant A's one row; columns=%v rows=%v",
			len(result.Rows), result.Columns, result.Rows)
	}
	idColumn := tenantisolationColumnIndex(t, result, "id")
	routingColumn := tenantisolationColumnIndex(t, result, "routing_id")
	row := result.Rows[0]
	if idColumn >= len(row) || routingColumn >= len(row) {
		t.Fatalf("successful search row is shorter than its columns: columns=%v row=%v", result.Columns, row)
	}
	if id, routingID := fmt.Sprint(row[idColumn]), fmt.Sprint(row[routingColumn]); id != e.idA || routingID != e.tenantA {
		t.Errorf("successful search outer row = (id=%q routing_id=%q), want tenant A (%q, %q)",
			id, routingID, e.idA, e.tenantA)
	}
}

func (e *tenantisolationEnv) assertNoTenantBValues(t *testing.T, result *SearchResult) {
	t.Helper()
	if result == nil {
		t.Fatal("successful search returned nil result")
	}
	for rowNumber, row := range result.Rows {
		for _, value := range row {
			text := fmt.Sprint(value)
			if text == e.tenantB || text == tenantisolationBExt || text == e.idB {
				t.Errorf("tenant A search exposed tenant B value %q in row %d: columns=%v row=%v",
					text, rowNumber, result.Columns, row)
			}
		}
	}
}

// Only explicit SQL-policy validation is a safe rejection. Operational errors,
// SQL syntax accidents, missing UDFs, router/connection failures, and context
// expiry are test failures rather than evidence of tenant isolation.
func tenantisolationRequireSafeSQLRejection(t *testing.T, operation string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected an explicit SQL-policy rejection", operation)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("%s failed due to context expiry, not safe SQL rejection: %v", operation, err)
	}
	// The production validator uses this narrow, validator-owned marker. Do not
	// accept generic words such as "condition" or "unsupported": wrappers and
	// drivers commonly use those for operational failures.
	if strings.Contains(strings.ToLower(err.Error()), "unsafe sharded sql") {
		return
	}
	t.Fatalf("%s failed for an unrelated/non-policy reason; require explicit unsafe-sharded-SQL validation: %v", operation, err)
}

func TestTenantIsolationTenantSQLIsolation(t *testing.T) {
	eqSkipIfUnreachable(t)
	tenantisolationRequireDedicatedEnvironment(t)

	tenantA, tenantB := tenantisolationUniqueTenants(t)
	env := &tenantisolationEnv{
		tenantA: tenantA,
		tenantB: tenantB,
		idA:     "tenantisolation-a-" + tenantA,
		idB:     "tenantisolation-b-" + tenantB,
	}
	// Install cleanup before any mapping/data operation that can fail.
	defer env.cleanup(t)
	env.insertMappings(t)
	env.seedRows(t)

	processor := newEqProcessor(t, eqTableSharded)
	// Registered after data/mapping cleanup, so LIFO defer order shuts the
	// processor/router down before mappings are removed.
	defer processor.Shutdown()

	t.Run("nested_boolean_and_comments_remain_scoped", func(t *testing.T) {
		options := &SearchOptions{
			Table:   eqTableSharded,
			Columns: []string{"id", "routing_id", "ext"},
			Condition: fmt.Sprintf(
				"((routing_id = '%s' AND (TRUE OR FALSE)) OR (routing_id = '%s' AND FALSE)) /* nested benign comment */",
				env.tenantA, env.tenantB),
			RoutingID: env.tenantA,
		}
		result, err := processor.SearchV2(options)
		if err != nil {
			t.Fatalf("ordinary nested tenant-A search was rejected: %v", err)
		}
		env.assertOneOuterARow(t, result)
		env.assertNoTenantBValues(t, result)
	})

	t.Run("benign_update_expression_remains_scoped", func(t *testing.T) {
		env.resetRows(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		options := &UpdateByQueryOptions{
			Table: eqTableSharded, RoutingID: env.tenantA,
			Condition: fmt.Sprintf("((routing_id = '%s' AND TRUE) OR FALSE) /* benign expression */", env.tenantA),
			Updates:   map[string]any{"mtime": "(mtime + 7)"},
		}
		count, err := processor.UpdateByQueryWithContextV2(ctx, options)
		if err != nil {
			t.Fatalf("benign tenant-A update expression was rejected: %v", err)
		}
		if count != 1 {
			t.Errorf("benign tenant-A update count=%d, want 1; SQL=%s", count, options.FinalSQL)
		}
		wantA := env.wantA()
		wantA.Mtime = 17
		env.assertState(t, wantA, env.wantB())
	})

	t.Run("line_comment_cannot_rebind_generated_predicate", func(t *testing.T) {
		condition := fmt.Sprintf(
			"((routing_id = '%s' /* nested branch */ AND (TRUE OR FALSE)))) OR (FALSE -- generated tenant predicate must not be rebound\n",
			env.tenantB)
		options := &SearchOptions{
			Table: eqTableSharded, Columns: []string{"id", "routing_id", "ext"},
			Condition: condition, RoutingID: env.tenantA,
		}
		result, err := processor.SearchV2(options)
		if err != nil {
			tenantisolationRequireSafeSQLRejection(t, "comment/grouping search", err)
			return
		}
		if len(result.Rows) != 0 {
			t.Errorf("secure intersection of tenant A with tenant-B-only condition returned %d rows, want 0; SQL=%s rows=%v",
				len(result.Rows), options.FinalSQL, result.Rows)
		}
		env.assertNoTenantBValues(t, result)
	})

	t.Run("computed_projection_cannot_read_other_tenant_subquery", func(t *testing.T) {
		options := &SearchOptions{
			Table: eqTableSharded,
			Columns: []string{
				"id",
				"routing_id",
				"upper(ext) AS computed_ext",
				fmt.Sprintf("(SELECT max(s.routing_id) FROM public.%s AS s WHERE s.routing_id = '%s') AS subquery_routing_id", eqTableSharded, env.tenantB),
				fmt.Sprintf("(SELECT max(s.ext) FROM public.%s AS s WHERE s.routing_id = '%s') AS subquery_ext", eqTableSharded, env.tenantB),
			},
			Condition: "((TRUE AND TRUE) OR FALSE) /* projection isolation */",
			RoutingID: env.tenantA,
		}
		result, err := processor.SearchV2(options)
		if err != nil {
			tenantisolationRequireSafeSQLRejection(t, "computed/subquery projection", err)
			return
		}
		env.assertOneOuterARow(t, result)
		env.assertNoTenantBValues(t, result)
	})

	t.Run("comment_grouping_update_cannot_alter_other_tenant", func(t *testing.T) {
		env.resetRows(t)
		condition := fmt.Sprintf(
			"((routing_id = '%s' /* nested branch */ AND (TRUE OR FALSE)))) OR (FALSE -- generated tenant predicate must not be rebound\n",
			env.tenantB)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		options := &UpdateByQueryOptions{
			Table: eqTableSharded, RoutingID: env.tenantA, Condition: condition,
			Updates: map[string]any{"mtime": "(mtime + 1000)"},
		}
		count, err := processor.UpdateByQueryWithContextV2(ctx, options)
		if err != nil {
			tenantisolationRequireSafeSQLRejection(t, "comment/grouping update", err)
		} else if count != 0 {
			t.Errorf("secure tenant-A intersection update count=%d, want 0; SQL=%s", count, options.FinalSQL)
		}
		env.assertState(t, env.wantA(), env.wantB())
	})

	t.Run("update_expression_cannot_read_other_tenant_subquery", func(t *testing.T) {
		env.resetRows(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		options := &UpdateByQueryOptions{
			Table: eqTableSharded, RoutingID: env.tenantA,
			Condition: "((TRUE AND TRUE) OR FALSE) /* expression isolation */",
			Updates: map[string]any{
				"ext": fmt.Sprintf("(SELECT max(s.ext) FROM public.%s AS s WHERE s.routing_id = '%s')", eqTableSharded, env.tenantB),
			},
		}
		count, err := processor.UpdateByQueryWithContextV2(ctx, options)
		if err != nil {
			tenantisolationRequireSafeSQLRejection(t, "cross-tenant update expression", err)
			env.assertState(t, env.wantA(), env.wantB())
			return
		}
		if count != 1 {
			t.Errorf("accepted tenant-A update expression count=%d, want 1; SQL=%s", count, options.FinalSQL)
		}
		gotA := env.state(t, env.tenantA)
		if gotA.RoutingID != env.tenantA || gotA.ID != env.idA {
			t.Errorf("accepted update changed tenant A identity: got %+v", gotA)
		}
		if gotA.Ext == tenantisolationBExt {
			t.Errorf("tenant A update expression read tenant B data: got tenant-A ext %q; SQL=%s", gotA.Ext, options.FinalSQL)
		}
		if gotB := env.state(t, env.tenantB); gotB != env.wantB() {
			t.Errorf("tenant B changed through tenant A update expression: got %+v, want %+v", gotB, env.wantB())
		}
	})

	t.Run("routing_id_is_immutable", func(t *testing.T) {
		env.resetRows(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		options := &UpdateByQueryOptions{
			Table: eqTableSharded, RoutingID: env.tenantA,
			Condition: "TRUE /* immutable tenant key */",
			Updates: map[string]any{
				"routing_id": fmt.Sprintf("(SELECT '%s')", env.tenantB),
			},
		}
		count, err := processor.UpdateByQueryWithContextV2(ctx, options)
		if !errors.Is(err, ErrRoutingIDUpdateForbidden) {
			t.Errorf("canonical routing_id update: want ErrRoutingIDUpdateForbidden, got count=%d err=%v", count, err)
		}
		env.assertState(t, env.wantA(), env.wantB())
	})
}
