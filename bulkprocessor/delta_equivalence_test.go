package bulkprocessor

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"strconv"
	"testing"

	_ "github.com/lib/pq" // PostgreSQL driver
)

// Phase A: bootstrap for instance-routing equivalence tests. See
// instance_routing.rfc for the design this exercises. This file contains only
// setup/bootstrap helpers plus one smoke test; the CRUD/lifecycle case suites
// live in separate files added by later phases.

var (
	eqHost     = os.Getenv("RELYT_INTEGRATION_HOST")
	eqPort, _  = strconv.Atoi(os.Getenv("RELYT_INTEGRATION_PORT"))
	eqUser     = os.Getenv("RELYT_INTEGRATION_USER")
	eqPassword = os.Getenv("RELYT_INTEGRATION_PASSWORD")
	eqDBMain   = os.Getenv("RELYT_INTEGRATION_MAIN_DATABASE")
	eqDBShardA = os.Getenv("RELYT_INTEGRATION_SHARD_A_DATABASE")
	eqDBShardB = os.Getenv("RELYT_INTEGRATION_SHARD_B_DATABASE")
	eqDBShardC = os.Getenv("RELYT_INTEGRATION_SHARD_C_DATABASE")
	eqDBShardD = os.Getenv("RELYT_INTEGRATION_SHARD_D_DATABASE")
	eqDBShardE = os.Getenv("RELYT_INTEGRATION_SHARD_E_DATABASE")
	eqDBShardF = os.Getenv("RELYT_INTEGRATION_SHARD_F_DATABASE")
	eqDBShardG = os.Getenv("RELYT_INTEGRATION_SHARD_G_DATABASE")
	eqDBShardH = os.Getenv("RELYT_INTEGRATION_SHARD_H_DATABASE")
	eqDBShardI = os.Getenv("RELYT_INTEGRATION_SHARD_I_DATABASE")
)

const (
	// Production instance ids are bigints rendered as strings; the SDK
	// treats them as opaque TEXT.
	eqInstMain = "1"
	eqInstA    = "2"
	eqInstB    = "3"

	eqTablePlain   = "routing_eq_plain"
	eqTableSharded = "routing_eq_sharded"

	// smoke-test routing ids: numeric strings in the 9000000xx block,
	// disjoint from the CRUD (1000000xx), flip (2000000xx), and scale
	// (1000002xx) suites.
	eqSmokePlain   = "900000001"
	eqSmokeTenantA = "900000002"
	eqSmokeTenantB = "900000003"
	eqSmokeTenantC = "900000004"
)

// EquivalenceRecord mirrors the 22-column equivalence schema documented in
// the production-shaped compatibility workload, PK (routing_id, fileid, id).
type EquivalenceRecord struct {
	ID              string `relyt:"id"`
	RoutingID       string `relyt:"routing_id"`
	ChunkID         int    `relyt:"chunk_id"`
	ChunkType       string `relyt:"chunk_type"`
	UserID          int64  `relyt:"user_id"`
	Creator         int64  `relyt:"creator"`
	Sharer          int64  `relyt:"sharer"`
	FileID          int64  `relyt:"fileid"`
	GroupID         int64  `relyt:"group_id"`
	Ctime           int64  `relyt:"ctime"`
	Mtime           int64  `relyt:"mtime"`
	Y               int    `relyt:"y"`
	Ym              int    `relyt:"ym"`
	Ymd             int    `relyt:"ymd"`
	Ext             string `relyt:"ext"`
	Fsize           int64  `relyt:"fsize"`
	ParentID        int64  `relyt:"parent_id"`
	Ftype           string `relyt:"ftype"`
	Version         int64  `relyt:"version"`
	IndexUpdateTime int64  `relyt:"index_update_time"`
	ExtGroup        string `relyt:"ext_group"`
	Vector          string `relyt:"vector"`
}

func eqConnStr(dbName string) string {
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(eqUser, eqPassword),
		Host:   net.JoinHostPort(eqHost, strconv.Itoa(eqPort)),
		Path:   dbName,
	}
	query := u.Query()
	query.Set("sslmode", "disable")
	u.RawQuery = query.Encode()
	return u.String()
}

func eqRegistryConnStr(dbName string) string {
	u := &url.URL{
		Scheme: "postgres",
		User:   url.User(eqUser),
		Host:   net.JoinHostPort(eqHost, strconv.Itoa(eqPort)),
		Path:   dbName,
	}
	return u.String()
}

// eqSkipIfUnreachable skips unless an explicit integration environment is
// configured and its control-plane database is reachable.
func eqSkipIfUnreachable(t *testing.T) {
	if eqHost == "" || eqPort <= 0 || eqUser == "" || eqDBMain == "" || eqDBShardA == "" || eqDBShardB == "" {
		t.Skip("instance-routing integration environment is not configured")
	}
	db, err := sql.Open("postgres", eqConnStr(eqDBMain))
	if err != nil {
		t.Skipf("relyt cluster unreachable: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Skipf("relyt cluster unreachable: %v", err)
	}
}

func eqExec(t *testing.T, db *sql.DB, dbName, label, query string) {
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("bootstrap[%s] %s failed: %v", dbName, label, err)
	}
}

func eqOpenDB(t *testing.T, dbName string) *sql.DB {
	db, err := sql.Open("postgres", eqConnStr(dbName))
	if err != nil {
		t.Fatalf("failed to open %s: %v", dbName, err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("failed to ping %s: %v", dbName, err)
	}
	return db
}

const eqInstanceIDFuncDDLTemplate = `
CREATE OR REPLACE FUNCTION relyt.instance_id() RETURNS text AS $$ SELECT '%s'::text $$ LANGUAGE sql IMMUTABLE;
`

// eqDeleteTablesUDFDDL is relyt_sys.delete_tables_with_condition and
// relyt_sys.delete_tables_with_group, copied verbatim from sql/udf.sql lines
// 1-63 (plain plpgsql, no environment dependency).
const eqDeleteTablesUDFDDL = `
CREATE OR REPLACE FUNCTION relyt_sys.delete_tables_with_condition(
    IN schema_name TEXT,
    IN main_table TEXT,
    IN file_id TEXT,
    IN routing_id TEXT,
    IN have_aux_table BOOLEAN DEFAULT TRUE,
    OUT deleted_count BIGINT
) RETURNS BIGINT AS $$
DECLARE
    main_count BIGINT := 0;
    aux_count BIGINT := 0;
    aux_table TEXT;
BEGIN
    aux_table := main_table || '_relyt_massive_group';

    -- delete main table
    EXECUTE format('DELETE FROM %I.%I WHERE routing_id = %L AND fileid = %L',
                  schema_name, main_table, routing_id, file_id);
    GET DIAGNOSTICS main_count = ROW_COUNT;

    -- delete aux table
    IF have_aux_table THEN
        EXECUTE format('DELETE FROM %I.%I WHERE routing_id = %L AND fileid = %L',
                      schema_name, aux_table, routing_id, file_id);
        GET DIAGNOSTICS aux_count = ROW_COUNT;
    END IF;

    -- return deleted total records
    deleted_count := main_count + aux_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION relyt_sys.delete_tables_with_group(
    IN schema_name TEXT,
    IN main_table TEXT,
    IN p_group_id TEXT,
    IN routing_id TEXT,
    IN have_aux_table BOOLEAN DEFAULT TRUE,
    OUT deleted_count BIGINT
) RETURNS BIGINT AS $$
DECLARE
    main_count BIGINT := 0;
    aux_count BIGINT := 0;
    aux_table TEXT;
BEGIN
    aux_table := main_table || '_relyt_massive_group';

    -- delete main table
    EXECUTE format('DELETE FROM %I.%I WHERE routing_id = %L AND group_id = %L',
                  schema_name, main_table, routing_id, p_group_id);
    GET DIAGNOSTICS main_count = ROW_COUNT;

    -- delete aux table
    IF have_aux_table THEN
        EXECUTE format('DELETE FROM %I.%I WHERE routing_id = %L AND group_id = %L',
                      schema_name, aux_table, routing_id, p_group_id);
        GET DIAGNOSTICS aux_count = ROW_COUNT;
    END IF;

    -- return deleted total records
    deleted_count := main_count + aux_count;
END;
$$ LANGUAGE plpgsql;
`

// eqCheckAndBuildQueryDDL: plpgsql reimplementation of
// relyt_sys._check_and_build_query. The original in sql/udf.sql is
// LANGUAGE plpython3u; restricted integration roles may have no USAGE on that
// untrusted language, and some compatible builds reject GRANT/ALTER on
// languages outright ("feature is not yet supported") even for a superuser
// target, so there is no way to grant it from this role. This reimplements
// the same query-building behavior (validated live against both plain and
// vector-ordered cases) so the unchanged, verbatim wrapper functions below
// (get_columns_with_condition / get_columns_sql_with_condition) keep working;
// only the 'query' key of its JSON result is consumed by callers.
const eqCheckAndBuildQueryDDL = `
CREATE OR REPLACE FUNCTION relyt_sys._check_and_build_query(
    schema_name TEXT,
    target_table_name TEXT,
    column_names TEXT[],
    condition TEXT DEFAULT NULL,
    order_by TEXT DEFAULT NULL,
    group_by TEXT DEFAULT NULL,
    having_con TEXT DEFAULT NULL,
    limit_count INTEGER DEFAULT NULL,
    offset_count INTEGER DEFAULT NULL,
    have_aux_table BOOLEAN DEFAULT TRUE
) RETURNS JSON AS $$
DECLARE
    col TEXT;
    m TEXT[];
    regular_columns TEXT[] := '{}';
    vector_columns TEXT[] := '{}';
    vector_aliases TEXT[] := '{}';
    has_count_over BOOLEAN := FALSE;
    count_over_alias TEXT := 'count';
    inner_select TEXT;
    where_clause TEXT := '';
    group_clause TEXT := '';
    having_clause TEXT := '';
    order_clause TEXT := '';
    vector_order_by TEXT := '';
    vector_order_clause TEXT := '';
    limit_clause TEXT := '';
    offset_clause TEXT := '';
    final_select TEXT;
    aux_table TEXT;
    query TEXT;
    part TEXT;
    part_col TEXT;
    part_dir TEXT;
BEGIN
    FOREACH col IN ARRAY COALESCE(column_names, '{}') LOOP
        col := trim(col);

        m := regexp_matches(col, '^count\(\*\)\s+OVER\(\)(?:\s+(?:AS\s+)?(\w+))?$', 'i');
        IF m IS NOT NULL THEN
            has_count_over := TRUE;
            IF m[1] IS NOT NULL THEN
                count_over_alias := lower(m[1]);
            END IF;
            CONTINUE;
        END IF;

        m := regexp_matches(col, '^vector\s*<->\s*.*\s+(?:AS\s+)?(\w+)$', 'i');
        IF m IS NOT NULL THEN
            vector_columns := array_append(vector_columns, col);
            vector_aliases := array_append(vector_aliases, m[1]);
            CONTINUE;
        END IF;

        IF col ~* '^count\(\*\)(?:\s+(?:AS\s+)?(\w+))?$' THEN
            RAISE EXCEPTION 'count(*) without OVER() is not supported';
        END IF;
        IF col = '*' THEN
            RAISE EXCEPTION '* is not supported in column list, please specify exact column names';
        END IF;

        regular_columns := array_append(regular_columns, col);
    END LOOP;

    IF array_length(vector_columns, 1) > 0 AND order_by IS NOT NULL AND trim(order_by) <> '' THEN
        FOR part IN SELECT trim(x) FROM unnest(string_to_array(order_by, ',')) x LOOP
            IF part = '' THEN CONTINUE; END IF;
            part_col := (regexp_matches(part, '^(\w+)(?:\s+(ASC|DESC|asc|desc))?$'))[1];
            part_dir := upper((regexp_matches(part, '^(\w+)(?:\s+(ASC|DESC|asc|desc))?$'))[2]);
            IF part_col = ANY(vector_aliases) THEN
                IF part_dir = 'DESC' THEN
                    RAISE EXCEPTION 'col % can not be sorted by DESC, only ASC is allowed', part_col;
                END IF;
                vector_order_by := CASE WHEN vector_order_by = '' THEN part_col || ' ASC' ELSE vector_order_by || ', ' || part_col || ' ASC' END;
            END IF;
        END LOOP;
    END IF;

    inner_select := array_to_string(regular_columns || vector_columns, ', ');
    IF inner_select = '' THEN inner_select := '*'; END IF;

    IF condition IS NOT NULL AND trim(condition) <> '' THEN where_clause := 'WHERE ' || condition; END IF;
    IF group_by IS NOT NULL AND trim(group_by) <> '' THEN group_clause := 'GROUP BY ' || group_by; END IF;
    IF having_con IS NOT NULL AND trim(having_con) <> '' THEN having_clause := 'HAVING ' || having_con; END IF;
    IF order_by IS NOT NULL AND trim(order_by) <> '' THEN order_clause := 'ORDER BY ' || order_by; END IF;
    IF vector_order_by <> '' THEN vector_order_clause := 'ORDER BY ' || vector_order_by; END IF;
    IF limit_count IS NOT NULL AND limit_count > 0 THEN limit_clause := 'LIMIT ' || limit_count; END IF;
    IF offset_count IS NOT NULL AND offset_count > 0 THEN offset_clause := 'OFFSET ' || offset_count; END IF;

    IF has_count_over THEN
        final_select := '*, COUNT(*) OVER() AS ' || count_over_alias;
    ELSE
        final_select := '*';
    END IF;

    IF have_aux_table THEN
        aux_table := target_table_name || '_relyt_massive_group';
        query := format(
            'WITH combined_data AS ((SELECT %s FROM %I.%I %s %s LIMIT 500) UNION ALL (SELECT %s FROM %I.%I %s %s LIMIT 500)) SELECT row_to_json(t) FROM (SELECT %s FROM combined_data %s %s %s) t',
            inner_select, schema_name, target_table_name, where_clause, vector_order_clause,
            inner_select, schema_name, aux_table, where_clause, vector_order_clause,
            final_select, order_clause, limit_clause, offset_clause
        );
    ELSE
        query := format(
            'WITH main_table AS (SELECT %s FROM %I.%I %s %s %s %s LIMIT 500) SELECT row_to_json(t) FROM (SELECT %s FROM main_table %s %s %s) t',
            inner_select, schema_name, target_table_name, where_clause, group_clause, having_clause, vector_order_clause,
            final_select, order_clause, limit_clause, offset_clause
        );
    END IF;

    RETURN json_build_object(
        'valid', true,
        'vector_columns', vector_columns,
        'regular_columns', regular_columns,
        'vector_order_by', vector_order_by,
        'query', query
    );
END;
$$ LANGUAGE plpgsql;
`

// eqSearchUpdateUDFDDL is get_columns_with_condition, get_columns_sql_with_condition,
// and generate_update_by_query_sql, copied verbatim from sql/udf.sql lines 270-434.
const eqSearchUpdateUDFDDL = `
CREATE OR REPLACE FUNCTION relyt_sys.get_columns_with_condition(
    schema_name TEXT,
    target_table_name TEXT,
    column_names TEXT[],
    condition TEXT DEFAULT NULL,
    order_by TEXT DEFAULT NULL,
    group_by TEXT DEFAULT NULL,
    having_con TEXT DEFAULT NULL,
    limit_count INTEGER DEFAULT NULL,
    offset_count INTEGER DEFAULT NULL,
    have_aux_table BOOLEAN DEFAULT TRUE
) RETURNS SETOF JSON AS $$
DECLARE
    query_sql TEXT;
    result_json TEXT;
    rec JSON;
    start_get_sql_time TIMESTAMP;
    get_sql_time TIMESTAMP;
    start_exec_time TIMESTAMP;
BEGIN
    IF (group_by IS NOT NULL AND group_by != '') OR (having_con IS NOT NULL AND having_con != '') THEN
        RAISE EXCEPTION 'group by and having parameters are not supported now';
    END IF;

    start_get_sql_time := clock_timestamp();

    SELECT relyt_sys._check_and_build_query(
        schema_name,
        target_table_name,
        column_names,
        condition,
        order_by,
        group_by,
        having_con,
        limit_count,
        offset_count,
        have_aux_table
    ) INTO result_json;

    query_sql := (result_json::json->>'query');

    get_sql_time := clock_timestamp();
    start_exec_time := clock_timestamp();

    FOR rec IN EXECUTE query_sql
    LOOP
        RETURN NEXT rec;
    END LOOP;

    RAISE LOG 'get_columns_with_condition_exec: get_sql_time: % ms, exec time: % ms', EXTRACT(EPOCH FROM (get_sql_time - start_get_sql_time)) * 1000, EXTRACT(EPOCH FROM (clock_timestamp() - start_exec_time)) * 1000;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION relyt_sys.get_columns_sql_with_condition(
    schema_name TEXT,
    target_table_name TEXT,
    column_names TEXT[],
    condition TEXT DEFAULT NULL,
    order_by TEXT DEFAULT NULL,
    group_by TEXT DEFAULT NULL,
    having_con TEXT DEFAULT NULL,
    limit_count INTEGER DEFAULT NULL,
    offset_count INTEGER DEFAULT NULL,
    have_aux_table BOOLEAN DEFAULT TRUE
) RETURNS TEXT AS $$
DECLARE
    query_sql TEXT;
    result_json TEXT;
    start_time TIMESTAMP;
BEGIN
    IF (group_by IS NOT NULL AND group_by != '') OR (having_con IS NOT NULL AND having_con != '') THEN
        RAISE EXCEPTION 'group by and having parameters are not supported now';
    END IF;

    start_time := clock_timestamp();

    SELECT relyt_sys._check_and_build_query(
        schema_name,
        target_table_name,
        column_names,
        condition,
        order_by,
        group_by,
        having_con,
        limit_count,
        offset_count,
        have_aux_table
    ) INTO result_json;

    query_sql := (result_json::json->>'query');

    RAISE LOG 'get_columns_sql_with_condition_exec: query_sql: %, exec time: % ms', query_sql, EXTRACT(EPOCH FROM (clock_timestamp() - start_time)) * 1000;

    RETURN query_sql;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION relyt_sys.generate_update_by_query_sql(
    IN schema_name TEXT,
    IN target_table_name TEXT,
    IN update_condition TEXT,
    IN update_fields JSONB,
    IN have_aux_table BOOLEAN DEFAULT TRUE,
    OUT generated_sql TEXT
) RETURNS TEXT AS $$
DECLARE
    aux_table TEXT;
    update_set_parts TEXT[] := '{}';
    update_set_clause TEXT;
    main_sql TEXT;
    aux_sql TEXT;
    field_name TEXT;
    field_value TEXT;
BEGIN
    FOR field_name, field_value IN SELECT key, value FROM jsonb_each_text(update_fields)
    LOOP
        IF field_value LIKE '(%)' OR field_value LIKE 'ARRAY[%]' OR field_value LIKE '[%]' THEN
            update_set_parts := array_append(update_set_parts, format('%I = %s', field_name, field_value));
        ELSE
            update_set_parts := array_append(update_set_parts, format('%I = %L', field_name, field_value));
        END IF;
    END LOOP;

    IF array_length(update_set_parts, 1) = 0 THEN
        RAISE EXCEPTION 'No valid update fields specified';
    END IF;

    aux_table := target_table_name || '_relyt_massive_group';

    update_set_clause := array_to_string(update_set_parts, ', ');

    IF update_condition IS NULL OR trim(update_condition) = '' THEN
        main_sql := format('UPDATE %I.%I SET %s',
                          schema_name, target_table_name, update_set_clause);
        aux_sql := format('UPDATE %I.%I SET %s',
                          schema_name, aux_table, update_set_clause);
    ELSE
        main_sql := format('UPDATE %I.%I SET %s WHERE %s',
                          schema_name, target_table_name, update_set_clause, update_condition);
        aux_sql := format('UPDATE %I.%I SET %s WHERE %s',
                          schema_name, aux_table, update_set_clause, update_condition);
    END IF;

    if have_aux_table then
        generated_sql := main_sql || '; ' || aux_sql;
    else
        generated_sql := main_sql;
    end if;

    RAISE LOG 'generate_update_by_query_sql: generated_sql: %', generated_sql;

END;
$$ LANGUAGE plpgsql;
`

// eqTableDDLTemplate creates the 22-column equivalence data table. No
// "<table>_relyt_massive_group" aux table is created: neither twin table has
// an aux routing table (<table>_relyt_routing), so hasRoutingTable/
// have_aux_table is always false for both and that companion table is never
// referenced by the UDFs above. A plain CREATE TABLE (no DISTRIBUTED BY) is
// used; verified live that Relyt accepts this dialect for a vecf16 column
// plus a composite primary key without requiring a distribution clause.
const eqTableDDLTemplate = `
CREATE TABLE IF NOT EXISTS public.%[1]s (
    id text NOT NULL,
    routing_id text NOT NULL,
    chunk_id integer NOT NULL,
    chunk_type text NOT NULL,
    user_id bigint NOT NULL,
    creator bigint NOT NULL,
    sharer bigint NOT NULL,
    fileid bigint NOT NULL,
    group_id bigint NOT NULL,
    ctime bigint NOT NULL,
    mtime bigint NOT NULL,
    y integer NOT NULL,
    ym integer NOT NULL,
    ymd integer NOT NULL,
    ext varchar(10) NOT NULL,
    fsize bigint NOT NULL,
    parent_id bigint NOT NULL,
    ftype varchar(50) NOT NULL,
    version bigint NOT NULL,
    index_update_time bigint NOT NULL,
    ext_group varchar(50) NOT NULL,
    vector vecf16(4) NOT NULL,
    PRIMARY KEY (routing_id, fileid, id)
);
`

// eqControlPlaneDDL creates the control-plane tables from sql/relyt-loader.sql,
// adapted for integration tests (not run verbatim): the "DISTRIBUTED NONE" clause is
// dropped because Relyt rejects it for a non-superuser role ("only superuser
// can create an entry policy"), and the REVOKE/GRANT-to-loader-user lines are
// skipped since loader-user does not exist in this environment.
const eqControlPlaneDDL = `
CREATE TABLE IF NOT EXISTS relyt_sys.SDK_LOADER_CONFIG (
    CONFIG_NAME TEXT PRIMARY KEY,
    CONFIG_VALUE TEXT
) USING heap;

CREATE TABLE IF NOT EXISTS relyt_sys.relyt_loader_checkpoint (
	process_id TEXT PRIMARY KEY,
	pg_table TEXT NOT NULL,
	status TEXT NOT NULL,
	start_time TIMESTAMP WITH TIME ZONE NOT NULL,
	last_insert_time TIMESTAMP WITH TIME ZONE,
	files_total INT DEFAULT 0,
	files_imported INT DEFAULT 0,
	file_details JSONB DEFAULT '[]'::jsonb,
	error_message TEXT,
	error_records INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS relyt_sys.relyt_loader_delta_checkpoint (
	process_id TEXT NOT NULL,
	pg_table TEXT NOT NULL,
	status TEXT NOT NULL,
	start_time TIMESTAMP WITH TIME ZONE NOT NULL,
	finish_time TIMESTAMP WITH TIME ZONE,
	filepath TEXT NOT NULL,
	error_message TEXT DEFAULT '',
	error_records INT DEFAULT 0,
    PRIMARY KEY (process_id, filepath)
) using heap;

CREATE TABLE IF NOT EXISTS relyt_sys.relyt_loader_table_config (
    table_name TEXT PRIMARY KEY,
    buffer_max_records INT,
    insert_into_batch_size INT,
	tuples_pre_partition INT,
	file_write_timeout INT,
	retry_sleep_max_time INT,
	update_on_conflict BOOLEAN
) using heap;

CREATE TABLE IF NOT EXISTS relyt_sys.relyt_instance_registry (
    instance_id TEXT PRIMARY KEY,
    connstr TEXT NOT NULL,
    status TEXT DEFAULT 'active',
    updated_at TIMESTAMPTZ DEFAULT now()
) using heap;
`

// eqSDKLoaderConfigRows holds the SDK_LOADER_CONFIG (name, value) pairs
// specified for the equivalence environment; ImportStrategy=0 (InsertOnConflict)
// and TuplesPrePartition=-1 together mean the write path never touches S3.
var eqSDKLoaderConfigRows = [][2]string{
	{"endpoint", "https://example.invalid"},
	{"region", "dummy-region"},
	{"bucket_name", "dummy-bucket"},
	{"prefix", "eq/data"},
	{"access_key", "integration-placeholder"},
	{"secret_key", "integration-placeholder"},
	{"concurrency", "4"},
	{"part_size", "5242880"},
	{"import_timeout", "300"},
	{"import_error_sleep_time", "2"},
	{"enable_dual_buffer", "true"},
	{"buffer_max_records", "2000"},
	{"tuples_pre_partition", "-1"},
	{"import_strategy", "0"},
	{"max_concurrent_workers", "1"},
	{"insert_into_batch_size", "200"},
	{"delete_before_insert", "true"},
	{"update_on_conflict", "true"},
	{"file_write_timeout", "2"},
	{"async_delete", "false"},
	{"task_timeout", "120"},
	{"retry_sleep_max_time", "2"},
}

const eqUpsertSDKLoaderConfigTemplate = `
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('%s', '%s')
ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
`

const eqUpsertRegistryTemplate = `
INSERT INTO relyt_sys.relyt_instance_registry (instance_id, connstr, status) VALUES ('%s', '%s', 'active')
ON CONFLICT (instance_id) DO UPDATE SET connstr = EXCLUDED.connstr, status = EXCLUDED.status;
`

// The DELETE between CREATE and the sentinel INSERT migrates test state left
// by runs that used the old inst-* id scheme: it drops every mapping (and a
// stale sentinel) referencing a removed registry row, then the sentinel is
// re-seeded with the current default. Idempotent; a no-op once migrated.
const eqInstanceRoutingTableDDLTemplate = `
CREATE TABLE IF NOT EXISTS relyt_sys.%[1]s_relyt_instance_routing (
    routing_id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL
) USING heap;
DELETE FROM relyt_sys.%[1]s_relyt_instance_routing WHERE instance_id LIKE 'inst-%%';
INSERT INTO relyt_sys.%[1]s_relyt_instance_routing (routing_id, instance_id) VALUES ('-1', '%[2]s')
ON CONFLICT (routing_id) DO NOTHING;
`

// bootstrapInstanceRuntime installs the per-instance runtime objects on one
// database, idempotently: the relyt/relyt_sys schemas, relyt.instance_id()
// returning instanceID, the delete UDFs, the search/update UDFs, and the given
// equivalence-shaped data tables. Shared by the 3-DB equivalence bootstrap and the
// 10-DB scale bootstrap (delta_scale_test.go).
//
// The search/update UDFs go on every instance, not just main:
// routeSearchJsonRows/updateByQuerySharded in instance_query.go call
// GetColumnsWithCondition/UpdateByQueryV2 directly on each instance's own
// client for a RoutingID-scoped query, never proxying through main, so a shard
// without them simply cannot serve routed search/update.
func bootstrapInstanceRuntime(t *testing.T, dbName, instanceID string, tables ...string) {
	db := eqOpenDB(t, dbName)
	defer db.Close()
	eqExec(t, db, dbName, "create schema relyt", "CREATE SCHEMA IF NOT EXISTS relyt")
	eqExec(t, db, dbName, "create schema relyt_sys", "CREATE SCHEMA IF NOT EXISTS relyt_sys")
	eqExec(t, db, dbName, "relyt.instance_id()", fmt.Sprintf(eqInstanceIDFuncDDLTemplate, instanceID))
	eqExec(t, db, dbName, "delete_tables udfs", eqDeleteTablesUDFDDL)
	eqExec(t, db, dbName, "_check_and_build_query", eqCheckAndBuildQueryDDL)
	eqExec(t, db, dbName, "search/update udfs", eqSearchUpdateUDFDDL)
	for _, table := range tables {
		eqExec(t, db, dbName, table+" table", fmt.Sprintf(eqTableDDLTemplate, table))
	}
}

// BootstrapEquivalenceEnv creates every schema/table/UDF/row this test suite
// needs, idempotently, across the configured control-plane and shard databases.
func BootstrapEquivalenceEnv(t *testing.T) {
	shardDBs := []struct {
		name string
		inst string
	}{
		{eqDBMain, eqInstMain},
		{eqDBShardA, eqInstA},
		{eqDBShardB, eqInstB},
	}

	for _, d := range shardDBs {
		bootstrapInstanceRuntime(t, d.name, d.inst, eqTableSharded)
	}

	mainDB := eqOpenDB(t, eqDBMain)
	defer mainDB.Close()

	eqExec(t, mainDB, eqDBMain, "routing_eq_plain table", fmt.Sprintf(eqTableDDLTemplate, eqTablePlain))
	eqExec(t, mainDB, eqDBMain, "control plane tables", eqControlPlaneDDL)

	// Test-only migration from the old inst-* id scheme: the UDFs above now
	// return numeric ids, so stale inst-* registry rows would fail instance-id
	// verification and spam reconnect logs. Production never deletes registry
	// rows; this test environment does, idempotently.
	eqExec(t, mainDB, eqDBMain, "drop stale inst-* registry rows",
		"DELETE FROM relyt_sys.relyt_instance_registry WHERE instance_id LIKE 'inst-%'")

	for _, kv := range eqSDKLoaderConfigRows {
		eqExec(t, mainDB, eqDBMain, "SDK_LOADER_CONFIG."+kv[0], fmt.Sprintf(eqUpsertSDKLoaderConfigTemplate, kv[0], kv[1]))
	}

	for _, d := range shardDBs {
		connstr := eqRegistryConnStr(d.name)
		eqExec(t, mainDB, eqDBMain, "registry."+d.inst, fmt.Sprintf(eqUpsertRegistryTemplate, d.inst, connstr))
	}

	eqExec(t, mainDB, eqDBMain, "routing_eq_sharded instance routing table",
		fmt.Sprintf(eqInstanceRoutingTableDDLTemplate, eqTableSharded, eqInstMain))
}

// eqCleanData truncates the twin data tables and resets routing state so
// each test run starts from a clean slate; mappings other than the sentinel
// are removed and the sentinel is reset back to eqInstMain ("1").
func eqCleanData(t *testing.T) {
	shardDBs := []string{eqDBMain, eqDBShardA, eqDBShardB}
	for _, name := range shardDBs {
		db := eqOpenDB(t, name)
		func() {
			defer db.Close()
			eqExec(t, db, name, "truncate routing_eq_sharded", fmt.Sprintf("TRUNCATE TABLE public.%s", eqTableSharded))
		}()
	}

	mainDB := eqOpenDB(t, eqDBMain)
	defer mainDB.Close()

	eqExec(t, mainDB, eqDBMain, "truncate routing_eq_plain", fmt.Sprintf("TRUNCATE TABLE public.%s", eqTablePlain))
	eqExec(t, mainDB, eqDBMain, "clear tenant routing",
		fmt.Sprintf("DELETE FROM relyt_sys.%s_relyt_instance_routing WHERE routing_id <> '-1'", eqTableSharded))
	eqExec(t, mainDB, eqDBMain, "reset default instance sentinel",
		fmt.Sprintf("UPDATE relyt_sys.%s_relyt_instance_routing SET instance_id = '%s' WHERE routing_id = '-1'", eqTableSharded, eqInstMain))
}

// newEqProcessor builds a BulkProcessor bound to the configured control-plane
// database. routing_eq_sharded comes up sharded (has an instance routing table);
// routing_eq_plain comes up non-sharded (no routing table of any kind).
func newEqProcessor(t *testing.T, table string) *BulkProcessor {
	config := Config{
		PostgreSQL: PostgreSQLConfig{
			Host:        eqHost,
			Port:        eqPort,
			Username:    eqUser,
			Password:    eqPassword,
			Database:    eqDBMain,
			Schema:      "public",
			Table:       table,
			MaxPoolSize: 5,
		},
		BatchSize:        5,
		BatchImportSize:  2,
		FeedbackColumn:   "id",
		FileWriteTimeout: 2,
		BGWorkerInterval: 30,
		LogLevel:         LOG,
	}

	processor, err := New(config)
	if err != nil {
		t.Fatalf("failed to create processor for table %s: %v", table, err)
	}
	return processor
}

func eqVector() string {
	return "[0.1,0.2,0.3,0.4]"
}

func eqMakeRecords(routingID string, fileID int64, ids ...string) []EquivalenceRecord {
	records := make([]EquivalenceRecord, 0, len(ids))
	for _, id := range ids {
		records = append(records, EquivalenceRecord{
			ID:              id,
			RoutingID:       routingID,
			ChunkID:         1,
			ChunkType:       "text",
			UserID:          1001,
			Creator:         1001,
			Sharer:          1001,
			FileID:          fileID,
			GroupID:         1,
			Ctime:           1700000000,
			Mtime:           1700000000,
			Y:               2024,
			Ym:              202401,
			Ymd:             20240101,
			Ext:             "txt",
			Fsize:           1024,
			ParentID:        0,
			Ftype:           "doc",
			Version:         1,
			IndexUpdateTime: 1700000000,
			ExtGroup:        "default",
			Vector:          eqVector(),
		})
	}
	return records
}

// eqCountRoutingID returns how many rows with the given routing_id exist in
// table on db (used as ground truth for physical shard placement).
func eqCountRoutingID(t *testing.T, db *sql.DB, table, routingID string) int {
	var count int
	err := db.QueryRow(fmt.Sprintf("SELECT count(*) FROM public.%s WHERE routing_id = $1", table), routingID).Scan(&count)
	if err != nil {
		t.Fatalf("count query failed on table %s: %v", table, err)
	}
	return count
}

// eqInstanceForRoutingID reads the mapping row directly, bypassing the SDK.
func eqInstanceForRoutingID(t *testing.T, db *sql.DB, table, routingID string) (string, bool) {
	var instanceID string
	err := db.QueryRow(fmt.Sprintf("SELECT instance_id FROM relyt_sys.%s_relyt_instance_routing WHERE routing_id = $1", table), routingID).Scan(&instanceID)
	if err == sql.ErrNoRows {
		return "", false
	}
	if err != nil {
		t.Fatalf("routing lookup failed for %s: %v", routingID, err)
	}
	return instanceID, true
}

// TestEquivalenceSmoke is a minimal end-to-end check that the bootstrap
// environment works: a non-sharded baseline processor and an instance-sharded
// candidate processor can both insert, search, and delete correctly, and
// tenant rows physically land only in their mapped shard database.
func TestEquivalenceSmoke(t *testing.T) {
	eqSkipIfUnreachable(t)
	BootstrapEquivalenceEnv(t)
	eqCleanData(t)

	mainDB := eqOpenDB(t, eqDBMain)
	defer mainDB.Close()
	shardADB := eqOpenDB(t, eqDBShardA)
	defer shardADB.Close()
	shardBDB := eqOpenDB(t, eqDBShardB)
	defer shardBDB.Close()

	// --- baseline: non-sharded routing_eq_plain ---
	plain := newEqProcessor(t, eqTablePlain)
	defer plain.Shutdown()

	if plain.isSharded {
		t.Fatalf("routing_eq_plain: expected non-sharded processor")
	}

	// fileID must match the record's numeric "fileid" column as a string:
	// relyt_sys.delete_tables_with_condition and DeleteOutdatedFiles both
	// match DeleteBeforeInsert/DeleteSyncV2's fileID parameter directly
	// against the bigint "fileid" column, so it cannot be an arbitrary label.
	plainRecords := eqMakeRecords(eqSmokePlain, 1001, "p1", "p2", "p3")
	if err := plain.InsertV2("1001", eqSmokePlain, plainRecords); err != nil {
		t.Fatalf("plain InsertV2 failed: %v", err)
	}
	if err := plain.Flush(); err != nil {
		t.Fatalf("plain Flush failed: %v", err)
	}

	plainResult, err := plain.SearchV2(&SearchOptions{
		Table:     eqTablePlain,
		Columns:   []string{"id"},
		Condition: "routing_id = '" + eqSmokePlain + "'",
		OrderBy:   "id ASC",
	})
	if err != nil {
		t.Fatalf("plain SearchV2 failed: %v", err)
	}
	if len(plainResult.Rows) != 3 {
		t.Fatalf("plain SearchV2: expected 3 rows, got %d", len(plainResult.Rows))
	}

	// --- candidate: instance-sharded routing_eq_sharded ---
	sharded := newEqProcessor(t, eqTableSharded)
	defer sharded.Shutdown()

	if !sharded.isSharded {
		t.Fatalf("routing_eq_sharded: expected sharded processor")
	}

	// pre-map tenant A -> eqInstA, tenant B -> eqInstB; tenant C is left unmapped.
	log.Printf("pre-mapping smoke tenants %s/%s before any insert", eqSmokeTenantA, eqSmokeTenantB)
	for routingID, instanceID := range map[string]string{eqSmokeTenantA: eqInstA, eqSmokeTenantB: eqInstB} {
		_, err := mainDB.Exec(
			fmt.Sprintf("INSERT INTO relyt_sys.%s_relyt_instance_routing (routing_id, instance_id) VALUES ($1, $2) ON CONFLICT (routing_id) DO NOTHING", eqTableSharded),
			routingID, instanceID,
		)
		if err != nil {
			t.Fatalf("failed to pre-map %s: %v", routingID, err)
		}
	}

	tenants := []string{eqSmokeTenantA, eqSmokeTenantB, eqSmokeTenantC}
	for i, tenant := range tenants {
		fileID := int64(2000 + i)
		records := eqMakeRecords(tenant, fileID, tenant+"-r1", tenant+"-r2")
		if err := sharded.InsertV2(fmt.Sprintf("%d", fileID), tenant, records); err != nil {
			t.Fatalf("sharded InsertV2 failed for %s: %v", tenant, err)
		}
	}
	if err := sharded.Flush(); err != nil {
		t.Fatalf("sharded Flush failed: %v", err)
	}

	// (c) tenant C must have acquired a mapping row, defaulting to eqInstMain.
	tenantCInstance, found := eqInstanceForRoutingID(t, mainDB, eqTableSharded, eqSmokeTenantC)
	if !found {
		t.Fatalf("tenant %s: expected a routing mapping to be registered on first insert", eqSmokeTenantC)
	}
	if tenantCInstance != eqInstMain {
		t.Fatalf("tenant %s: expected default instance %s, got %s", eqSmokeTenantC, eqInstMain, tenantCInstance)
	}

	// (b) ground truth: each tenant's rows physically live only in the expected database.
	expectedDB := map[string]*sql.DB{eqSmokeTenantA: shardADB, eqSmokeTenantB: shardBDB, eqSmokeTenantC: mainDB}
	otherDBs := map[string][]*sql.DB{
		eqSmokeTenantA: {mainDB, shardBDB},
		eqSmokeTenantB: {mainDB, shardADB},
		eqSmokeTenantC: {shardADB, shardBDB},
	}
	for _, tenant := range tenants {
		if got := eqCountRoutingID(t, expectedDB[tenant], eqTableSharded, tenant); got != 2 {
			t.Fatalf("%s: expected 2 rows in its own instance db, got %d", tenant, got)
		}
		for _, otherDB := range otherDBs[tenant] {
			if got := eqCountRoutingID(t, otherDB, eqTableSharded, tenant); got != 0 {
				t.Fatalf("%s: expected 0 rows in a non-owning instance db, got %d", tenant, got)
			}
		}
	}

	// (a) SearchV2 with RoutingID set routes to a single instance.
	for _, tenant := range tenants {
		res, err := sharded.SearchV2(&SearchOptions{
			Table:     eqTableSharded,
			Columns:   []string{"id"},
			Condition: "routing_id = '" + tenant + "'",
			OrderBy:   "id ASC",
			RoutingID: tenant,
		})
		if err != nil {
			t.Fatalf("sharded SearchV2 (RoutingID set) failed for %s: %v", tenant, err)
		}
		if len(res.Rows) != 2 {
			t.Fatalf("%s: SearchV2 (RoutingID set) expected 2 rows, got %d", tenant, len(res.Rows))
		}
	}

	// (a) SearchV2 with RoutingID unset on a sharded table is an error
	// (carve-out #3 in the Phase B header, delta_equivalence_cases_test.go).
	_, err = sharded.SearchV2(&SearchOptions{
		Table:     eqTableSharded,
		Columns:   []string{"id", "routing_id"},
		Condition: "routing_id = '" + eqSmokeTenantA + "'",
		OrderBy:   "id ASC",
	})
	if !errors.Is(err, ErrRoutingIDRequired) {
		t.Fatalf("SearchV2 (RoutingID unset) expected ErrRoutingIDRequired, got %v", err)
	}

	// DeleteSyncV2 one tenant, verify rows gone from its shard. tenant A's
	// fileID was 2000 (i=0 above).
	if err := sharded.DeleteSyncV2("2000", eqSmokeTenantA); err != nil {
		t.Fatalf("DeleteSyncV2 failed for %s: %v", eqSmokeTenantA, err)
	}
	if got := eqCountRoutingID(t, shardADB, eqTableSharded, eqSmokeTenantA); got != 0 {
		t.Fatalf("tenant %s: expected 0 rows after DeleteSyncV2, got %d", eqSmokeTenantA, got)
	}
}
