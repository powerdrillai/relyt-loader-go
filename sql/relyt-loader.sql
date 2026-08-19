-- the following sql must be executed before using this loader
-- create a type to represent the s3 config info

CREATE TABLE IF NOT EXISTS relyt_sys.SDK_LOADER_CONFIG (
    CONFIG_NAME TEXT PRIMARY KEY,
    CONFIG_VALUE TEXT
) USING heap DISTRIBUTED NONE;

INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('endpoint', 's3.amazonaws.com') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('region', 'us-west-2') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('bucket_name', 'your-bucket') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('prefix', 'import/data') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('access_key', 'your-access-key') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('secret_key', 'your-secret-key') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('concurrency', '20') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('part_size', '5242880') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('import_timeout', '1800') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('import_error_sleep_time', '10') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('enable_dual_buffer', 'true') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('buffer_max_records', '5000') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
-- -1: disable upload data to s3
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('tuples_pre_partition', '-1') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
-- 0: insert on conflict, 1: copy from local, 2: copy from s3, 3: insert into from external table
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('import_strategy', '0') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('max_concurrent_workers', '1') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('insert_into_batch_size', '100') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('delete_before_insert', 'true') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('async_delete', 'false') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('file_write_timeout', '3') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
-- the server error infos will be skipped when retry to import the file, split by |
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('skip_server_error_infos', 'Bad literal|Dimensions|duplicate key value|invalid byte sequence') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('task_timeout', '120') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('update_on_conflict', 'true') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;
INSERT INTO relyt_sys.SDK_LOADER_CONFIG (CONFIG_NAME, CONFIG_VALUE) VALUES ('retry_sleep_max_time', '10') ON CONFLICT (CONFIG_NAME) DO UPDATE SET CONFIG_VALUE = EXCLUDED.CONFIG_VALUE;

-- revoke all permission from public
REVOKE ALL ON relyt_sys.SDK_LOADER_CONFIG FROM public;

-- grant the select and insert permission to loader-user
GRANT USAGE ON SCHEMA relyt_sys TO loader-user;
GRANT SELECT,INSERT,UPDATE ON relyt_sys.SDK_LOADER_CONFIG TO loader-user;

-- update the config
-- UPDATE relyt_sys.SDK_LOADER_CONFIG SET CONFIG_VALUE = '1' WHERE CONFIG_NAME = 'import_strategy';

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
GRANT SELECT,INSERT,UPDATE,DELETE ON relyt_sys.relyt_loader_delta_checkpoint TO public;

-- buffer_max_records and insert_into_batch_size per table
CREATE TABLE IF NOT EXISTS relyt_sys.relyt_loader_table_config (
    table_name TEXT PRIMARY KEY,
    buffer_max_records INT,
    insert_into_batch_size INT,
	tuples_pre_partition INT,
	file_write_timeout INT,
	retry_sleep_max_time INT,
	update_on_conflict BOOLEAN
) using heap;
GRANT SELECT,INSERT ON relyt_sys.relyt_loader_table_config TO public;

-- instance registry for instance-level sharding, connstr has no password
CREATE TABLE IF NOT EXISTS relyt_sys.relyt_instance_registry (
    instance_id TEXT PRIMARY KEY,
    connstr TEXT NOT NULL,
    status TEXT DEFAULT 'active',
    updated_at TIMESTAMPTZ DEFAULT now()
) using heap;
GRANT SELECT ON relyt_sys.relyt_instance_registry TO public;

-- create a table to store the routing table
--
-- CREATE TABLE IF NOT EXISTS relyt_sys.XXXX_relyt_routing (
--     routing_id bigint PRIMARY KEY,
--     store_table_name TEXT NOT NULL
-- ) USING heap DISTRIBUTED NONE;
-- GRANT SELECT,INSERT ON relyt_sys.XXXX_relyt_routing TO public;

-- create a table to store the instance routing table (instance-level sharding),
-- mappings are immutable so UPDATE is revoked. note REVOKE does not bind the
-- table owner: create the table under a separate owning role, or accept that
-- immutability is enforced by the application only.
-- the sentinel row routing_id = '-1' holds the default instance id for new
-- tenants; the SDK rejects '-1' as a real routing_id. to change the default,
-- DELETE + INSERT the sentinel row in one transaction; the role doing the flip
-- needs DELETE (or ownership) on the table, which the grants below do not give.
--
-- CREATE TABLE IF NOT EXISTS relyt_sys.XXXX_relyt_instance_routing (
--     routing_id TEXT PRIMARY KEY,
--     instance_id TEXT NOT NULL
-- ) USING heap DISTRIBUTED NONE;
-- GRANT SELECT,INSERT ON relyt_sys.XXXX_relyt_instance_routing TO public;
-- REVOKE UPDATE ON relyt_sys.XXXX_relyt_instance_routing FROM public;
-- INSERT INTO relyt_sys.XXXX_relyt_instance_routing VALUES ('-1', 'the-default-instance-id');
