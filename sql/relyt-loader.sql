-- the following sql must be executed before using this loader
-- create a type to represent the s3 config info
CREATE TYPE loader_s3_config AS (
    endpoint TEXT,
    region TEXT,
    bucket_name TEXT,
    prefix TEXT,
    access_key TEXT,
    secret_key TEXT,
    concurrency INT,
    part_size INT,
    import_timeout INT,
    import_error_sleep_time INT
);

-- create the LOADER_CONFIG function, return the s3 config info
CREATE OR REPLACE FUNCTION relyt_sys.LOADER_CONFIG()
RETURNS loader_s3_config
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT 
        's3.amazonaws.com'::TEXT AS endpoint,
        'us-west-2'::TEXT AS region,
        'your-bucket'::TEXT AS bucket_name,
        'import/data'::TEXT AS prefix,
        'your-access-key'::TEXT AS access_key,
        'your-secret-key'::TEXT AS secret_key,
        20 AS concurrency,
        5242880 AS part_size,
        1800 AS import_timeout,
        10 AS import_error_sleep_time
    ;
$$;
-- revoke the execute permission from public for safety
REVOKE EXECUTE ON FUNCTION relyt_sys.LOADER_CONFIG() FROM PUBLIC;
-- grant the execute permission to the role who runs the loader
GRANT EXECUTE ON FUNCTION relyt_sys.LOADER_CONFIG() TO loader-user;

-- example: how to update the config (only admin can update this function)
-- CREATE OR REPLACE FUNCTION relyt_sys.LOADER_CONFIG()
-- RETURNS loader_s3_config
-- LANGUAGE SQL
-- IMMUTABLE
-- AS $$
--     SELECT 
--         'cn.amazonaws.com'::TEXT AS endpoint,
--         'us-west-2'::TEXT AS region,
--         'your-bucket'::TEXT AS bucket_name,
--         'import/data'::TEXT AS prefix,
--         'your-access-key'::TEXT AS access_key,
--         'your-secret-key'::TEXT AS secret_key,
--         20 AS concurrency,
--         5242880 AS part_size,
--         1600 AS import_timeout,
--         10 AS import_error_sleep_time
--     ;
-- $$;

-- example: test the function
-- SELECT * FROM relyt_sys.LOADER_CONFIG(); 

-- checkpoint table
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
GRANT SELECT,INSERT ON relyt_sys.relyt_loader_checkpoint TO public;


-- delta checkpoint table
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
GRANT SELECT,INSERT ON relyt_sys.relyt_loader_delta_checkpoint TO public;

-- create a table to store the routing table
--
-- CREATE TABLE IF NOT EXISTS relyt_sys.XXXX_routing (
--     routing_id bigint PRIMARY KEY,
--     store_table_name TEXT NOT NULL
-- ) USING heap DISTRIBUTED NONE;
-- GRANT SELECT,INSERT ON relyt_sys.XXXX_routing TO public;
