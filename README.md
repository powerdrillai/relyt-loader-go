# Relyt Bulk Processor SDK for Go

A Go SDK for bulk importing data into PostgreSQL via S3 external tables.

## Features

- Stream data directly to S3 without local temporary files
- Create S3 external tables in PostgreSQL
- Import data from S3 to PostgreSQL using `INSERT INTO ... SELECT FROM` or `INSERT INTO ... SELECT FROM ON CONFLICT ...` for the table contains primary keys
- Configurable batch size
- Automatic splitting of files based on record count
- Asynchronous processing
- Unique process ID for each task to isolate files in S3
- **S3 configuration automatically loaded from the database**
- **Task progress tracking via checkpoint table**
- **Configurable error tolerance for handling failed records**

## Usage

```go
import "github.com/powerdrillai/relyt-sdk-go/bulkprocessor"

// Initialize the bulk processor - S3 configuration is automatically loaded from database
config := bulkprocessor.Config{
    // PostgreSQL configuration (required)
    PostgreSQL: bulkprocessor.PostgreSQLConfig{
        Host:     "localhost",
        Port:     5432,
        Username: "postgres",
        Password: "password",
        Database: "mydatabase",
        Table:    "mytable",
        Schema:   "public", // Defaults to public
    },
    BatchSize: 100000,      // Number of records per file
    Concurrency: 1,        // Number of concurrent imports
    MaxErrorRecords: 100,  // Number of error records to ignore (0 = no errors allowed)
}

processor, err := bulkprocessor.New(config)
if err != nil {
    log.Fatalf("Failed to create bulk processor: %v", err)
}
defer processor.Shutdown() // Ensure processor is shut down properly

// Insert data
type MyData struct {
    ID        int       `json:"id"`        // json tag is used to determine column names
    Name      string    `json:"name"`
    CreatedAt time.Time `json:"created_at"`
}

records := []MyData{
    {ID: 1, Name: "John", CreatedAt: time.Now()},
    {ID: 2, Name: "Jane", CreatedAt: time.Now()},
}

// You can call Insert multiple times to insert data in batches
err = processor.Insert(records)
if err != nil {
    log.Fatalf("Failed to insert data: %v", err)
}

// Get the unique processor ID
processId := processor.GetProcessId()
log.Printf("Task process ID: %s", processId)
// S3 files will be stored in: [prefix]/[date]/[processId]/[filename]

// When finished, flush all remaining data and wait for import to complete
err = processor.Flush()
if err != nil {
    log.Fatalf("Failed to flush data: %v", err)
}
```

## S3 Configuration via PostgreSQL

The SDK automatically loads S3 configuration from PostgreSQL. You need to create a user-defined function (UDF) in your database:

```sql
-- Create a type to represent S3 configuration
CREATE TYPE loader_s3_config AS (
    endpoint TEXT,
    region TEXT,
    bucket_name TEXT,
    prefix TEXT,
    access_key TEXT,
    secret_key TEXT
);

-- Create the LOADER_CONFIG function
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
        'your-secret-key'::TEXT AS secret_key
    ;
$$;
```

## Checkpoint Tracking

The SDK automatically creates and maintains a checkpoint table in PostgreSQL to track task progress:

```sql
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
```

The checkpoint table provides the following information:

1. **Task Status**: RUNNING, COMPLETED, or FAILED
2. **Timestamps**: Start time and last successful insert time
3. **File Counts**: Total files generated and files successfully imported
4. **File Details**: JSON array containing detailed information about each file
   - File IDs and S3 keys/URLs
   - Creation and import timestamps
   - Number of records in each file
   - File status (CREATED, FROZEN, IMPORTING, IMPORTED, ERROR)
5. **Error Records**: Count of records that were skipped due to errors

This checkpoint table is useful for:

- Monitoring progress of long-running import tasks
- Identifying failed imports and their causes
- Resuming interrupted tasks
- Tracking S3 file usage and cleanup

You can query the checkpoint table directly:

```sql
-- Get overview of all tasks
SELECT 
    process_id, pg_table, status, 
    start_time, last_insert_time,
    files_total, files_imported,
    error_records
FROM relyt_loader_checkpoint;

-- Get details of a specific task
SELECT * FROM relyt_loader_checkpoint
WHERE process_id = 'your-process-id';

-- Get file details for a task
SELECT 
    file_id, status, s3_key, num_records, created_at, imported_at
FROM relyt_loader_checkpoint,
     jsonb_array_elements(file_details) AS file
WHERE process_id = 'your-process-id';
```

## Error Handling and Tolerance

The SDK provides flexible error handling with the `MaxErrorRecords` configuration option. This determines how many records with errors can be ignored during processing:

```go
config := bulkprocessor.Config{
    // ... other configuration options ...
    MaxErrorRecords: 100, // Allow up to 100 error records
}
```

When errors are encountered during processing:

1. If the total number of error records (including the current one) is less than or equal to `MaxErrorRecords`:
   - The error is logged to stderr
   - The error count is incremented in the checkpoint table
   - Processing continues with the next record
   
2. If the error count would exceed `MaxErrorRecords`:
   - The error is returned to the caller
   - Processing is halted

This feature is useful for:
- Handling occasional bad records without failing entire jobs
- Maintaining a balance between data quality and processing robustness
- Tracking the number of skipped records for later analysis

The default value for `MaxErrorRecords` is 0, which means no errors are tolerated.

## How It Works

1. On initialization, the SDK connects to PostgreSQL and calls the `LOADER_CONFIG()` function
2. The S3 configuration is retrieved and used for all S3 operations
3. The SDK generates a unique process ID for the task and creates a checkpoint record
4. When you call `Insert`, data is streamed directly to S3
5. When the record count reaches `BatchSize`, the file is frozen and queued for import
6. A background thread processes the queue of files:
   - Creates a temporary S3 external table in PostgreSQL
   - Executes INSERT...SELECT to import the data
   - Drops the external table when done
7. The checkpoint table is updated at each step of the process
8. Calling `Flush` waits for all pending files to be imported
9. On shutdown, the task status is marked as COMPLETED

## Requirements

- PostgreSQL must have access to the S3 storage
- The `LOADER_CONFIG()` function must be created in your database
- The `relyt_loader_checkpoint` table must be created in your database
- Structs must use `json:"column_name"` tags to specify column names