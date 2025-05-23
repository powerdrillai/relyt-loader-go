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
    part_size INT
);

-- create the LOADER_CONFIG function, return the s3 config info
CREATE OR REPLACE FUNCTION LOADER_CONFIG()
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
        5242880 AS part_size
    ;
$$;
-- revoke the execute permission from public for safety
REVOKE EXECUTE ON FUNCTION LOADER_CONFIG() FROM PUBLIC;
-- grant the execute permission to the role who runs the loader
GRANT EXECUTE ON FUNCTION LOADER_CONFIG() TO loader-user;

-- example: how to update the config (only admin can update this function)
-- CREATE OR REPLACE FUNCTION LOADER_CONFIG()
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
--         5242880 AS part_size
--     ;
-- $$;

-- example: test the function
-- SELECT * FROM LOADER_CONFIG(); 

-- checkpoint table
CREATE TABLE IF NOT EXISTS relyt_loader_checkpoint (
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
GRANT SELECT,INSERT ON relyt_loader_checkpoint TO public;