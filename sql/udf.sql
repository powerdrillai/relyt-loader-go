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
import re
import plpy
import json

def validate_columns(schema_name, target_table_name, column_names):
    if not column_names or len(column_names) == 0:
        return True, [], [], []
    
    count_over_columns_pos = []  # 记录COUNT(*) OVER()的位置和别名
    vector_columns = []
    regular_columns = []
    
    # 正则表达式模式
    count_over_pattern = r'^count\(\*\)\s+OVER\(\)(?:\s+(?:AS\s+)?(\w+))?$'
    count_pattern = r'^count\(\*\)(?:\s+(?:AS\s+)?(\w+))?$'
    star_pattern = r'^\*$'
    vector_pattern = r'^vector\s*<->\s*.*\s+(?:AS\s+)?(\w+)$'
    
    for i, col in enumerate(column_names):
        col = col.strip()
        
        # 检查是否为 COUNT(*) OVER() AS xxx 或 COUNT(*) OVER()
        count_match = re.match(count_over_pattern, col, re.IGNORECASE)
        if count_match:
            alias = count_match.group(1) if count_match.group(1) else "count"
            count_over_columns_pos.append((i, alias.lower()))  # 记录位置和别名
            continue
            
        # 检查是否为向量列 vector <-> ... AS xxx
        vector_match = re.match(vector_pattern, col, re.IGNORECASE)
        if vector_match:
            vector_columns.append(col)
            continue
        
        # 检查是否为 count(*) (不带OVER())
        count_match = re.match(count_pattern, col, re.IGNORECASE)
        if count_match:
            plpy.error(f"count(*) without OVER() is not supported")
        
        # 检查是否为 *
        star_match = re.match(star_pattern, col, re.IGNORECASE)
        if star_match:
            plpy.error(f"* is not supported in column list, please specify exact column names")
        
        # 其他列都认为是普通列
        regular_columns.append(col)
    
    return True, count_over_columns_pos, vector_columns, regular_columns

def validate_order_by_for_vector(order_by, vector_columns):
    if not order_by or not vector_columns:
        return True, ""
    
    # 提取向量列的别名
    vector_aliases = []
    for vector_col in vector_columns:
        vector_match = re.match(r'^vector\s*<->\s*.*\s+(?:AS\s+)?(\w+)$', vector_col, re.IGNORECASE)
        if vector_match:
            vector_aliases.append(vector_match.group(1))
    
    # 解析原始 order_by 并筛选出向量列
    new_order_parts = []
    for part in order_by.split(','):
        part = part.strip()
        if not part:
            continue
            
        # 提取列名和排序方向
        col_match = re.match(r'^(\w+)(?:\s+(ASC|DESC|asc|desc))?$', part)
        if not col_match:
            continue
            
        col_name, sort_dir = col_match.groups()
        sort_dir = sort_dir.upper() if sort_dir else None
        
        # 检查是否为向量列
        if col_name in vector_aliases:
            if sort_dir == 'DESC':
                plpy.error(f"col {col_name} can not be sorted by DESC, only ASC is allowed")
            
            # 强制使用 ASC
            new_order_parts.append(f"{col_name} ASC")
        # 非向量列不添加到new_order_parts中，直接忽略
    
    # 如果没有向量列在 order_by 中，返回空的 new_order_by，不报错
    if not new_order_parts:
        return True, ""
    
    # 构建新的 order_by 语句，只包含向量列
    new_order_by = ', '.join(new_order_parts)
    
    return True, new_order_by

def build_query(schema_name, target_table_name, column_names, condition, order_by, 
                vector_order_by, group_by, having_con, limit_count, offset_count,
                count_over_columns_pos, vector_columns, regular_columns, have_aux_table):
    
    # 构建 SELECT 子句
    select_parts = []
    if regular_columns:
        select_parts.append(', '.join(regular_columns))
    if vector_columns:
        select_parts.append(', '.join(vector_columns))
    
    inner_select = ', '.join(select_parts) if select_parts else '*'
    
    # 构建其他子句
    where_clause = f"WHERE {condition}" if condition and condition.strip() else ""
    group_clause = f"GROUP BY {group_by}" if group_by and group_by.strip() else ""
    having_clause = f"HAVING {having_con}" if having_con and having_con.strip() else ""
    order_clause = f"ORDER BY {order_by}" if order_by and order_by.strip() else ""
    vector_order_clause = f"ORDER BY {vector_order_by}" if vector_order_by and vector_order_by.strip() else ""
    limit_clause = f"LIMIT {limit_count}" if limit_count and limit_count > 0 else ""
    offset_clause = f"OFFSET {offset_count}" if offset_count and offset_count > 0 else ""

    # 构建最终的SELECT子句，将COUNT(*) OVER()列放在最后
    def build_final_select():
        if not count_over_columns_pos:
            return "*"
        
        # 先添加所有普通列和向量列
        final_select_parts = []
        final_select_parts.append("*")
        
        # 然后在最后添加所有COUNT(*) OVER()列
        for pos, alias in count_over_columns_pos:
            final_select_parts.append(f"COUNT(*) OVER() AS {alias}")
        
        return ', '.join(final_select_parts)
    
    final_select = build_final_select()
    
    if have_aux_table:
        # 带辅助表的查询：使用 UNION ALL
        aux_table = f"{target_table_name}_relyt_massive_group"
        query = f"""
            WITH combined_data AS (
                (SELECT {inner_select} FROM {schema_name}.{target_table_name} {where_clause} {vector_order_clause} LIMIT 500)
                UNION ALL
                (SELECT {inner_select} FROM {schema_name}.{aux_table} {where_clause} {vector_order_clause} LIMIT 500)
            )
            SELECT {final_select} FROM combined_data {order_clause} {limit_clause} {offset_clause}
        """
    else:
        # 不带辅助表的查询：单表查询
        query = f"""
            WITH main_table AS (
                SELECT {inner_select} FROM {schema_name}.{target_table_name} {where_clause} {group_clause} {having_clause} {vector_order_clause} LIMIT 500
            )
            SELECT {final_select} FROM main_table {order_clause} {limit_clause} {offset_clause}
        """
    
    return query

try:

    # 1. 验证列名
    valid, count_over_columns_pos, vector_columns, regular_columns = validate_columns(
        schema_name, target_table_name, column_names
    )
    
    # 2. 初始化 vector_order_by
    vector_order_by = ""
    if vector_columns:
        valid, vector_order_by = validate_order_by_for_vector(order_by, vector_columns)
    
    # 3. 构建查询字符串
    query = build_query(
        schema_name, target_table_name, column_names, condition, order_by,
        vector_order_by, group_by, having_con, limit_count, offset_count,
        count_over_columns_pos, vector_columns, regular_columns, have_aux_table
    )
    
    # 4. 返回结果
    result = {
        'valid': valid,
        'count_over_columns_pos': count_over_columns_pos,
        'vector_columns': vector_columns,
        'regular_columns': regular_columns,
        'vector_order_by': vector_order_by,
        'query': query
    }
    
    return json.dumps(result, ensure_ascii=False)
    
except Exception as e:
    plpy.error(f"_check_and_build_query error: {str(e)}")

$$ LANGUAGE plpython3u;

-- 不带辅助表的函数（使用plpython3u）
CREATE OR REPLACE FUNCTION relyt_sys.get_columns_with_condition_without_aux(
    schema_name TEXT,
    target_table_name TEXT,
    column_names TEXT[],
    condition TEXT DEFAULT NULL,
    order_by TEXT DEFAULT NULL,
    group_by TEXT DEFAULT NULL,
    having_con TEXT DEFAULT NULL,
    limit_count INTEGER DEFAULT NULL,
    offset_count INTEGER DEFAULT NULL
) RETURNS SETOF JSON AS $$
import re
import plpy
import json

try:
    import time
    
    # 记录prepare开始时间
    start_get_sql_time = time.time()
    
    # 使用公共函数进行验证和查询构建
    plan = plpy.prepare("""
        SELECT relyt_sys._check_and_build_query(
            $1, $2, $3::text[], $4, $5, $6, $7, $8, $9, $10
        ) as result
    """, ["text", "text", "text[]", "text", "text", "text", "text", "int", "int", "bool"])
    
    result_json = plpy.execute(plan, [
        schema_name, 
        target_table_name, 
        column_names, 
        condition if condition else None, 
        order_by if order_by else None, 
        group_by if group_by else None, 
        having_con if having_con else None, 
        limit_count if limit_count else None, 
        offset_count if offset_count else None, 
        False
    ])[0]['result']
    
    # 计算execute执行时间
    get_sql_time = time.time() - start_get_sql_time
    
    result_data = json.loads(result_json)
    query = result_data['query']

    # 记录execute开始时间
    start_execute_time = time.time()

    # 执行查询
    main_result = plpy.execute(query)

    # 计算execute执行时间
    execute_time = time.time() - start_execute_time
    plpy.log(f'get_columns_with_condition_without_aux_exec query: {query}, get sql time: {get_sql_time*1000:.3f} ms, execute time: {execute_time*1000:.3f} ms')
    
    # 处理结果
    for row in main_result:
        row_dict = dict(row)
        
        # 返回 JSON 格式
        yield json.dumps(row_dict, ensure_ascii=False)
    
except Exception as e:
    plpy.error(f"function execution error: {str(e)}")

$$ LANGUAGE plpython3u;

CREATE OR REPLACE FUNCTION relyt_sys.get_columns_with_condition_with_aux(
    schema_name TEXT,
    target_table_name TEXT,
    column_names TEXT[],
    condition TEXT DEFAULT NULL,
    order_by TEXT DEFAULT NULL,
    group_by TEXT DEFAULT NULL,
    having_con TEXT DEFAULT NULL,
    limit_count INTEGER DEFAULT NULL,
    offset_count INTEGER DEFAULT NULL
) RETURNS SETOF JSON AS $$
import re
import plpy
import json

try:
    import time

    # 记录prepare开始时间
    prepare_start_time = time.time()

    # 使用公共函数进行验证和查询构建

    plan = plpy.prepare("""
        SELECT relyt_sys._check_and_build_query(
            $1, $2, $3::text[], $4, $5, $6, $7, $8, $9, $10
        ) as result
    """, ["text", "text", "text[]", "text", "text", "text", "text", "int", "int", "bool"])
    
    # 计算prepare执行时间
    prepare_time = time.time() - prepare_start_time
    
    # 记录execute开始时间
    start_get_sql_time = time.time()
    
    result_json = plpy.execute(plan, [
        schema_name, 
        target_table_name, 
        column_names, 
        condition if condition else None, 
        order_by if order_by else None, 
        group_by if group_by else None, 
        having_con if having_con else None, 
        limit_count if limit_count else None, 
        offset_count if offset_count else None, 
        True
    ])[0]['result']

    # 计算execute执行时间
    get_sql_time = time.time() - start_get_sql_time

    result_data = json.loads(result_json)
    query = result_data['query']
    
    # 记录execute开始时间
    start_execute_time = time.time()

    # 执行查询
    main_result = plpy.execute(query)

    # 计算execute执行时间
    execute_time = time.time() - start_execute_time

    plpy.log(f'get_columns_with_condition_with_aux_exec: {query}, prepare get sql time: {prepare_time*1000:.3f} ms, get sql time: {get_sql_time*1000:.3f} ms, execute time: {execute_time*1000:.3f} ms')
    
    # 处理结果
    for row in main_result:
        row_dict = dict(row)
        
        # 返回 JSON 格式
        yield json.dumps(row_dict, ensure_ascii=False)
    
except Exception as e:
    plpy.error(f"function execution error: {str(e)}")

$$ LANGUAGE plpython3u;

-- main function, decide which function to call based on have_aux_table parameter
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
BEGIN
    -- 检查group_by和having_con参数
    IF (group_by IS NOT NULL AND group_by != '') OR (having_con IS NOT NULL AND having_con != '') THEN
        RAISE EXCEPTION 'group by and having parameters are not supported now';
    END IF;

    IF have_aux_table THEN
        RETURN QUERY SELECT * FROM relyt_sys.get_columns_with_condition_with_aux(
            schema_name, target_table_name, column_names, condition, order_by,
            group_by, having_con, limit_count, offset_count
        );
    ELSE
        RETURN QUERY SELECT * FROM relyt_sys.get_columns_with_condition_without_aux(
            schema_name, target_table_name, column_names, condition, order_by,
            group_by, having_con, limit_count, offset_count
        );
    END IF;
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
import time
import plpy
import json

try:
    
    # 检查group_by和having_con参数
    if (group_by is not None and group_by != '') or (having_con is not None and having_con != ''):
        plpy.error('group by and having parameters are not supported now')

    # 使用公共函数进行验证和查询构建
    plan = plpy.prepare("""
        SELECT relyt_sys._check_and_build_query(
            $1, $2, $3::text[], $4, $5, $6, $7, $8, $9, $10
        ) as result
    """, ["text", "text", "text[]", "text", "text", "text", "text", "int", "int", "bool"])
    
    result_json = plpy.execute(plan, [
        schema_name, 
        target_table_name, 
        column_names, 
        condition if condition else None, 
        order_by if order_by else None, 
        group_by if group_by else None, 
        having_con if having_con else None, 
        limit_count if limit_count else None, 
        offset_count if offset_count else None, 
        have_aux_table
    ])[0]['result']
    
    # 从JSON结果中提取query字段
    result_data = json.loads(result_json)
    query_sql = result_data['query']
    
    return query_sql
    
except Exception as e:
    plpy.error(f"function execution error: {str(e)}")

$$ LANGUAGE plpython3u;