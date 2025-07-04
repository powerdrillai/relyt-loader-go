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
    query TEXT;
    result JSON;
    rec RECORD;
BEGIN

    -- build base query based on have_aux_table parameter
    IF have_aux_table THEN
        query := format('
            WITH combined_data AS (
                (SELECT * FROM %I.%I %s)
                UNION ALL
                (SELECT * FROM %I.%I %s)
            )
            SELECT %s FROM combined_data %s %s %s %s %s',
            -- main table
            schema_name,
            target_table_name,
            CASE WHEN condition IS NOT NULL AND condition != '' THEN format('WHERE %s', condition) ELSE '' END,
            -- aux table
            schema_name,
            target_table_name || '_relyt_massive_group',
            CASE WHEN condition IS NOT NULL AND condition != '' THEN format('WHERE %s', condition) ELSE '' END,
            -- window function
            CASE 
                WHEN array_length(column_names, 1) > 0 
                THEN array_to_string(column_names, ', ')
                ELSE '*'
            END,
            CASE WHEN group_by IS NOT NULL AND group_by != '' THEN format('GROUP BY %s', group_by) ELSE '' END,
            CASE WHEN having_con IS NOT NULL AND having_con != '' THEN format('HAVING %s', having_con) ELSE '' END,
            CASE WHEN order_by IS NOT NULL AND order_by != '' THEN format('ORDER BY %s', order_by) ELSE '' END,
            CASE WHEN limit_count IS NOT NULL AND limit_count > 0 THEN format('LIMIT %s', limit_count) ELSE '' END,
            CASE WHEN offset_count IS NOT NULL AND offset_count > 0 THEN format('OFFSET %s', offset_count) ELSE '' END
        );
    ELSE
        query := format('
            SELECT %s FROM %I.%I %s %s %s %s %s %s',
            CASE 
                WHEN array_length(column_names, 1) > 0 
                THEN array_to_string(column_names, ', ')
                ELSE '*'
            END,
            schema_name,
            target_table_name,
            CASE WHEN condition IS NOT NULL AND condition != '' THEN format('WHERE %s', condition) ELSE '' END,
            CASE WHEN group_by IS NOT NULL AND group_by != '' THEN format('GROUP BY %s', group_by) ELSE '' END,
            CASE WHEN having_con IS NOT NULL AND having_con != '' THEN format('HAVING %s', having_con) ELSE '' END,
            CASE WHEN order_by IS NOT NULL AND order_by != '' THEN format('ORDER BY %s', order_by) ELSE '' END,
            CASE WHEN limit_count IS NOT NULL AND limit_count > 0 THEN format('LIMIT %s', limit_count) ELSE '' END,
            CASE WHEN offset_count IS NOT NULL AND offset_count > 0 THEN format('OFFSET %s', offset_count) ELSE '' END
        );
    END IF;

    raise log 'get_columns_with_condition: query: %s', query;

    -- execute query and return result
    FOR rec IN EXECUTE query
    LOOP
        RETURN NEXT row_to_json(rec);
    END LOOP;
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
BEGIN

    -- build base query based on have_aux_table parameter
    IF have_aux_table THEN
        query_sql = format('
            WITH combined_data AS (
                (SELECT * FROM %I.%I %s)
                UNION ALL
                (SELECT * FROM %I.%I %s)
            )
            SELECT row_to_json(t)
            FROM (SELECT %s FROM combined_data %s %s %s %s %s) AS t',
            -- main table
            schema_name,
            target_table_name,
            CASE WHEN condition IS NOT NULL AND condition != '' THEN format('WHERE %s', condition) ELSE '' END,
            -- aux table
            schema_name,
            target_table_name || '_relyt_massive_group',
            CASE WHEN condition IS NOT NULL AND condition != '' THEN format('WHERE %s', condition) ELSE '' END,
            -- window function
            CASE 
                WHEN array_length(column_names, 1) > 0 
                THEN array_to_string(column_names, ', ')
                ELSE '*'
            END,
            CASE WHEN group_by IS NOT NULL AND group_by != '' THEN format('GROUP BY %s', group_by) ELSE '' END,
            CASE WHEN having_con IS NOT NULL AND having_con != '' THEN format('HAVING %s', having_con) ELSE '' END,
            CASE WHEN order_by IS NOT NULL AND order_by != '' THEN format('ORDER BY %s', order_by) ELSE '' END,
            CASE WHEN limit_count IS NOT NULL AND limit_count > 0 THEN format('LIMIT %s', limit_count) ELSE '' END,
            CASE WHEN offset_count IS NOT NULL AND offset_count > 0 THEN format('OFFSET %s', offset_count) ELSE '' END
        );
    ELSE
        query_sql = format('
            SELECT row_to_json(t)
            FROM (SELECT %s FROM %I.%I %s %s %s %s %s %s) AS t',
            CASE 
                WHEN array_length(column_names, 1) > 0 
                THEN array_to_string(column_names, ', ')
                ELSE '*'
            END,
            schema_name,
            target_table_name,
            CASE WHEN condition IS NOT NULL AND condition != '' THEN format('WHERE %s', condition) ELSE '' END,
            CASE WHEN group_by IS NOT NULL AND group_by != '' THEN format('GROUP BY %s', group_by) ELSE '' END,
            CASE WHEN having_con IS NOT NULL AND having_con != '' THEN format('HAVING %s', having_con) ELSE '' END,
            CASE WHEN order_by IS NOT NULL AND order_by != '' THEN format('ORDER BY %s', order_by) ELSE '' END,
            CASE WHEN limit_count IS NOT NULL AND limit_count > 0 THEN format('LIMIT %s', limit_count) ELSE '' END,
            CASE WHEN offset_count IS NOT NULL AND offset_count > 0 THEN format('OFFSET %s', offset_count) ELSE '' END
        );
    END IF;

    RETURN query_sql;
END;
$$ LANGUAGE plpgsql;