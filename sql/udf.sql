CREATE OR REPLACE FUNCTION get_columns_with_condition(
    IN table_name TEXT,
    IN column_names TEXT[],
    IN condition TEXT,
    OUT result TEXT
) RETURNS SETOF TEXT AS $$
DECLARE
    query TEXT;
    col TEXT;
BEGIN
    -- 构造查询语句
    query := 'SELECT ';
    FOR col IN SELECT unnest(column_names) LOOP
        query := query || col || ', ';
    END LOOP;
    query := rtrim(query, ', ') || ' FROM ' || table_name;
    
    -- 添加条件
    IF condition IS NOT NULL AND condition <> '' THEN
        query := query || ' WHERE ' || condition;
    END IF;

    -- 执行查询并返回结果
    RETURN QUERY EXECUTE query;
END;
$$ LANGUAGE plpgsql;