package bulkprocessor

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

const (
	routingTableSuffix = "_relyt_routing"
	auxTableSuffix     = "_relyt_massive_group"
)

// PostgreSQLClient handles interactions with PostgreSQL
type PostgreSQLClient struct {
	pool   *pgxpool.Pool
	config PostgreSQLConfig
}

// FileCheckpointInfo represents a file's status in the checkpoint
type FileCheckpointInfo struct {
	S3Key       string            `json:"s3_key"`
	NumRecords  int               `json:"num_records"`
	CreatedAt   time.Time         `json:"created_at"`
	ImportedAt  time.Time         `json:"imported_at,omitempty"`
	Status      string            `json:"status"` // CREATED, FROZEN, IMPORTING, IMPORTED, ERROR
	ErrorReason string            `json:"error_reason,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"` // Additional metadata for the file
}

// CheckpointStatus represents the status of a process in the checkpoint table
type CheckpointStatus string

const (
	CheckpointStatusRunning   CheckpointStatus = "RUNNING"
	CheckpointStatusCompleted CheckpointStatus = "COMPLETED"
	CheckpointStatusFailed    CheckpointStatus = "FAILED"
	CheckpointStatusCancelled CheckpointStatus = "CANCELLED"
)

// TableColumn represents a column in a PostgreSQL table
type TableColumn struct {
	Name       string `json:"name"`
	DataType   string `json:"data_type"`
	IsNullable bool   `json:"is_nullable"`
	ColumnType string `json:"column_type"` // PostgreSQL完整类型定义
}

// NewPostgreSQLClient creates a new PostgreSQL client
func NewPostgreSQLClient(config PostgreSQLConfig) (*PostgreSQLClient, error) {
	connString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?pool_max_conns=%d",
		config.Username,
		config.Password,
		config.Host,
		config.Port,
		config.Database,
		config.MaxPoolSize,
	)

	// Add SSL mode=disable if needed
	// connString += "?sslmode=disable"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create PostgreSQL connection pool")
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		return nil, errors.Wrap(err, "failed to ping PostgreSQL server")
	}

	return &PostgreSQLClient{
		pool:   pool,
		config: config,
	}, nil
}

// Close closes the PostgreSQL connection
func (c *PostgreSQLClient) Close() {
	if c.pool != nil {
		c.pool.Close()
	}
}

// InitializeCheckpoint initializes a new process in the checkpoint table
func (c *PostgreSQLClient) InitializeCheckpoint(ctx context.Context, processId string, pgTable string) error {
	sqlStatement := `
	INSERT INTO relyt_sys.relyt_loader_checkpoint
	(process_id, pg_table, status, start_time, files_total, files_imported, file_details, error_records)
	VALUES ($1, $2, $3, $4, 0, 0, '[]'::jsonb, 0)
	`

	_, err := c.pool.Exec(ctx, sqlStatement, processId, pgTable, string(CheckpointStatusRunning), time.Now())
	if err != nil {
		return errors.Wrap(err, "failed to initialize checkpoint")
	}

	return nil
}

// UpdateCheckpointLastInsert updates the last insert time in the checkpoint
func (c *PostgreSQLClient) UpdateCheckpointLastInsert(ctx context.Context, processId string) error {
	sqlStatement := `
	UPDATE relyt_sys.relyt_loader_checkpoint
	SET last_insert_time = $1
	WHERE process_id = $2
	`

	_, err := c.pool.Exec(ctx, sqlStatement, time.Now(), processId)
	if err != nil {
		return errors.Wrap(err, "failed to update checkpoint last insert time")
	}

	return nil
}

// UpdateCheckpointFile adds or updates a file in the checkpoint
func (c *PostgreSQLClient) UpdateCheckpointFile(ctx context.Context, processId string, fileInfo FileCheckpointInfo) error {
	// First, get current file_details
	var fileDetails []FileCheckpointInfo
	sqlSelect := `
	SELECT file_details FROM relyt_sys.relyt_loader_checkpoint
	WHERE process_id = $1
	`

	var fileDetailsJSON []byte
	err := c.pool.QueryRow(ctx, sqlSelect, processId).Scan(&fileDetailsJSON)
	if err != nil {
		return errors.Wrap(err, "failed to get file details from checkpoint")
	}

	// Parse existing file details
	if err := json.Unmarshal(fileDetailsJSON, &fileDetails); err != nil {
		return errors.Wrap(err, "failed to parse file details")
	}

	// Update or add the file info
	found := false
	for i, f := range fileDetails {
		if f.S3Key == fileInfo.S3Key {
			fileDetails[i] = fileInfo
			found = true
			break
		}
	}

	if !found {
		fileDetails = append(fileDetails, fileInfo)
	}

	// Serialize back to JSON
	updatedFileDetailsJSON, err := json.Marshal(fileDetails)
	if err != nil {
		return errors.Wrap(err, "failed to serialize file details")
	}

	// Update checkpoint record
	sqlUpdate := `
	UPDATE relyt_sys.relyt_loader_checkpoint
	SET file_details = $1,
	    files_total = $2,
	    files_imported = (
		SELECT COUNT(*) FROM jsonb_array_elements($1)
		WHERE (value->>'status')::text = 'IMPORTED'
	    )
	WHERE process_id = $3
	`

	_, err = c.pool.Exec(ctx, sqlUpdate, updatedFileDetailsJSON, len(fileDetails), processId)
	if err != nil {
		return errors.Wrap(err, "failed to update checkpoint file details")
	}

	return nil
}

// UpdateCheckpointStatus updates the status of a process in the checkpoint
func (c *PostgreSQLClient) UpdateCheckpointStatus(ctx context.Context, processId string, status CheckpointStatus, errorMsg string) error {
	sqlStatement := `
	UPDATE relyt_sys.relyt_loader_checkpoint
	SET status = $1, 
	    error_message = $2
	WHERE process_id = $3
	`

	_, err := c.pool.Exec(ctx, sqlStatement, string(status), errorMsg, processId)
	if err != nil {
		return errors.Wrap(err, "failed to update checkpoint status")
	}

	return nil
}

// GetLoadConfigFromDB retrieves loader configuration from the database
func (c *PostgreSQLClient) GetLoadConfigFromDB(ctx context.Context, config *Config) (*S3Config, error) {
	var s3Config S3Config
	var rawSkipServerErrorInfos string

	// Query the SDK_LOADER_CONFIG table
	sqlStatement := `
	SELECT 
		MAX(CASE WHEN CONFIG_NAME = 'endpoint' THEN CONFIG_VALUE END) as endpoint,
		MAX(CASE WHEN CONFIG_NAME = 'region' THEN CONFIG_VALUE END) as region,
		MAX(CASE WHEN CONFIG_NAME = 'bucket_name' THEN CONFIG_VALUE END) as bucket_name,
		MAX(CASE WHEN CONFIG_NAME = 'prefix' THEN CONFIG_VALUE END) as prefix,
		MAX(CASE WHEN CONFIG_NAME = 'access_key' THEN CONFIG_VALUE END) as access_key,
		MAX(CASE WHEN CONFIG_NAME = 'secret_key' THEN CONFIG_VALUE END) as secret_key,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'concurrency' THEN CONFIG_VALUE END)::INT, 20) as concurrency,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'part_size' THEN CONFIG_VALUE END)::BIGINT, 5242880) as part_size,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'import_timeout' THEN CONFIG_VALUE END)::INT, 1800) as import_timeout,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'import_error_sleep_time' THEN CONFIG_VALUE END)::INT, 10) as import_error_sleep_time,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'enable_dual_buffer' THEN CONFIG_VALUE END)::BOOLEAN, true) as enable_dual_buffer,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'buffer_max_records' THEN CONFIG_VALUE END)::INT, 5000) as buffer_max_records,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'tuples_pre_partition' THEN CONFIG_VALUE END)::INT, 5000) as tuples_pre_partition,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'import_strategy' THEN CONFIG_VALUE END)::INT, 2) as import_strategy,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'max_concurrent_workers' THEN CONFIG_VALUE END)::INT, 1) as max_concurrent_workers,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'insert_into_batch_size' THEN CONFIG_VALUE END)::INT, 100) as insert_into_batch_size,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'delete_before_insert' THEN CONFIG_VALUE END)::BOOLEAN, true) as delete_before_insert,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'update_on_conflict' THEN CONFIG_VALUE END)::BOOLEAN, true) as update_on_conflict,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'file_write_timeout' THEN CONFIG_VALUE END)::INT, 3) as file_write_timeout,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'async_delete' THEN CONFIG_VALUE END)::BOOLEAN, false) as async_delete,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'skip_server_error_infos' THEN CONFIG_VALUE END)::TEXT, 'Bad literal|Dimensions|duplicate key value|invalid byte sequence') as skip_server_error_infos,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'task_timeout' THEN CONFIG_VALUE END)::INT, 120) as task_timeout
	FROM relyt_sys.SDK_LOADER_CONFIG
	`

	row := c.pool.QueryRow(ctx, sqlStatement)

	err := row.Scan(
		&s3Config.Endpoint,
		&s3Config.Region,
		&s3Config.BucketName,
		&s3Config.Prefix,
		&s3Config.AccessKey,
		&s3Config.SecretKey,
		&s3Config.Concurrency,
		&s3Config.PartSize,
		&config.ImportTimeout,
		&config.ImportErrorSleepTime,
		&config.EnableDualBuffer,
		&config.BufferMaxRecords,
		&config.TuplesPrePartition,
		&config.ImportStrategy,
		&config.MaxConcurrentWorkers,
		&config.InsertIntoBatchSize,
		&config.DeleteBeforeInsert,
		&config.UpdateOnConflict,
		&config.FileWriteTimeout,
		&config.AsyncDelete,
		&rawSkipServerErrorInfos,
		&config.TaskTimeout,
	)

	config.SkipServerErrorInfos = strings.Split(rawSkipServerErrorInfos, "|")

	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve S3 configuration from database")
	}

	if config.TuplesPrePartition < 0 && config.ImportStrategy == CopyFromS3 {
		log.Printf("TuplesPrePartition is less than 0, set to 5000")
		config.TuplesPrePartition = 5000
	}

	return &s3Config, nil
}

func (c *PostgreSQLClient) UpdateLoadConfig(ctx context.Context, config *Config) error {
	var rawSkipServerErrorInfos string
	sqlStatement := `
		SELECT
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'import_timeout' THEN CONFIG_VALUE END)::INT, 1800) as import_timeout,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'import_error_sleep_time' THEN CONFIG_VALUE END)::INT, 10) as import_error_sleep_time,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'buffer_max_records' THEN CONFIG_VALUE END)::INT, 5000) as buffer_max_records,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'tuples_pre_partition' THEN CONFIG_VALUE END)::INT, -1) as tuples_pre_partition,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'import_strategy' THEN CONFIG_VALUE END)::INT, 0) as import_strategy,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'max_concurrent_workers' THEN CONFIG_VALUE END)::INT, 1) as max_concurrent_workers,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'insert_into_batch_size' THEN CONFIG_VALUE END)::INT, 100) as insert_into_batch_size,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'delete_before_insert' THEN CONFIG_VALUE END)::BOOLEAN, true) as delete_before_insert,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'update_on_conflict' THEN CONFIG_VALUE END)::BOOLEAN, true) as update_on_conflict,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'file_write_timeout' THEN CONFIG_VALUE END)::INT, 3) as file_write_timeout,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'async_delete' THEN CONFIG_VALUE END)::BOOLEAN, false) as async_delete,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'skip_server_error_infos' THEN CONFIG_VALUE END)::TEXT, 'Bad literal|Dimensions|duplicate key value|invalid byte sequence') as skip_server_error_infos,
		COALESCE(MAX(CASE WHEN CONFIG_NAME = 'task_timeout' THEN CONFIG_VALUE END)::INT, 120) as task_timeout
	FROM relyt_sys.SDK_LOADER_CONFIG
	`

	row := c.pool.QueryRow(ctx, sqlStatement)
	err := row.Scan(
		&config.ImportTimeout,
		&config.ImportErrorSleepTime,
		&config.BufferMaxRecords,
		&config.TuplesPrePartition,
		&config.ImportStrategy,
		&config.MaxConcurrentWorkers,
		&config.InsertIntoBatchSize,
		&config.DeleteBeforeInsert,
		&config.UpdateOnConflict,
		&config.FileWriteTimeout,
		&config.AsyncDelete,
		&rawSkipServerErrorInfos,
		&config.TaskTimeout,
	)

	if err != nil {
		return errors.Wrap(err, "failed to update load config")
	}

	if config.TuplesPrePartition < 0 && config.ImportStrategy == CopyFromS3 {
		log.Printf("TuplesPrePartition is less than 0, set to 5000")
		config.TuplesPrePartition = 5000
	}

	config.SkipServerErrorInfos = strings.Split(rawSkipServerErrorInfos, "|")

	return nil
}

func (c *PostgreSQLClient) HasRoutingTable(ctx context.Context, routingTableName string) (bool, error) {
	sqlStatement := fmt.Sprintf(`SELECT COUNT(*) FROM pg_tables WHERE tablename = '%s' and schemaname = 'relyt_sys'`, routingTableName)

	var count int64
	err := c.pool.QueryRow(ctx, sqlStatement).Scan(&count)
	if err != nil {
		return false, errors.Wrap(err, "failed to check if routing table exists")
	}

	return count > 0, nil
}

func (c *PostgreSQLClient) RefreshRoutingTable(ctx context.Context, routingTableName string) (map[string]struct{}, error) {
	sqlStatement := fmt.Sprintf(`SELECT routing_id FROM relyt_sys.%s`, routingTableName)

	rows, err := c.pool.Query(ctx, sqlStatement)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query routing table")
	}
	defer rows.Close()

	routingMap := make(map[string]struct{})
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, errors.Wrap(err, "failed to scan routing table row")
		}
		routingMap[id] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating routing table rows")
	}

	return routingMap, nil
}

// ConvertDBConfigToS3Config is no longer needed and removed

// CreateExternalTable creates an S3 external table in PostgreSQL
func (c *PostgreSQLClient) CreateExternalTable(ctx context.Context, s3URL, tableName string, columnNames []string, s3Config S3Config) error {
	// Get the table schema to ensure column types match
	tableSchema, err := c.GetTableSchema(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get table schema for creating external table")
	}

	// Create a map of column names to their definitions
	columnMap := make(map[string]string)
	for _, col := range tableSchema {
		columnMap[col.Name] = fmt.Sprintf("%s %s", col.Name, col.ColumnType)
	}

	// Build column definitions based on the provided column names
	// but use the types from the actual table schema
	columnDefs := make([]string, 0, len(columnNames))
	for _, name := range columnNames {
		if def, exists := columnMap[name]; exists {
			columnDefs = append(columnDefs, def)
		} else {
			return fmt.Errorf("column '%s' does not exist in target table", name)
		}
	}

	// Create temporary external table, we have a retry mechanism in the import process
	// , so we use IF NOT EXISTS to avoid error when retrying
	sqlStatement := fmt.Sprintf(`CREATE EXTERNAL TABLE %s.%s (
		%s
	)
	LOCATION('%s 
          accessid=%s
          secret=%s 
          region=%s 
          version=2')
	FORMAT 'CSV'
	(delimiter ',' null 'null')
	`, c.config.Schema, tableName, strings.Join(columnDefs, ",\n"), s3URL, s3Config.AccessKey, s3Config.SecretKey, s3Config.Region)

	_, err = c.pool.Exec(ctx, sqlStatement)
	if err != nil {
		return errors.Wrap(err, "failed to create external table")
	}

	return nil
}

// GetTablePrimaryKeys retrieves the primary key columns of a table
func (c *PostgreSQLClient) GetTablePrimaryKeys(ctx context.Context) ([]string, error) {
	// Parse the schema and table name
	parts := strings.Split(c.config.Table, ".")
	tableName := parts[0]
	if len(parts) > 1 {
		tableName = parts[1]
	}

	// Query to get primary key columns
	sqlStatement := `
	SELECT a.attname
	FROM pg_index i
	JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
	WHERE i.indrelid = (SELECT oid FROM pg_class WHERE relname = $1 
					   AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $2))
	  AND i.indisprimary
	ORDER BY a.attnum
	`

	rows, err := c.pool.Query(ctx, sqlStatement, tableName, c.config.Schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve primary key columns")
	}
	defer rows.Close()

	var pkColumns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, errors.Wrap(err, "failed to scan primary key column")
		}
		pkColumns = append(pkColumns, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating over primary key columns")
	}

	return pkColumns, nil
}

// ImportFromExternalTable imports data from external table to target table
func (c *PostgreSQLClient) ImportFromExternalTable(ctx context.Context, externalTableName string, columns []string, updateOnConflict bool, isAuxFile bool) error {
	// Get primary key columns to handle conflicts
	pkColumns, err := c.GetTablePrimaryKeys(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get primary key columns")
	}

	// Get target table name
	targetTable := c.config.Table
	if isAuxFile {
		targetTable = fmt.Sprintf("%s%s", c.config.Table, auxTableSuffix)
	}

	// Import data from external table to target table
	columnsList := strings.Join(columns, ", ")

	var sqlStatement string
	if len(pkColumns) > 0 {
		// Build the ON CONFLICT clause
		conflictColumns := strings.Join(pkColumns, ", ")

		// For the SELECT statement, we need to handle duplicate keys in the external table
		// Use DISTINCT ON to get only one row per primary key combination
		distinctOnClause := fmt.Sprintf("DISTINCT ON (%s)", conflictColumns)

		// Build the WHERE clause to ensure all columns are not null
		wherePartsForSelect := make([]string, 0, len(pkColumns))
		for _, pk := range pkColumns {
			wherePartsForSelect = append(wherePartsForSelect, fmt.Sprintf("%s IS NOT NULL", pk))
		}
		whereClauseForSelect := ""
		if len(wherePartsForSelect) > 0 {
			whereClauseForSelect = "WHERE " + strings.Join(wherePartsForSelect, " AND ")
		}

		if updateOnConflict {
			// Build the update set clause (set each column to excluded.column)
			updateSetParts := make([]string, 0, len(columns))
			for _, col := range columns {
				// Skip primary key columns in the update part
				isPK := false
				for _, pk := range pkColumns {
					if pk == col {
						isPK = true
						break
					}
				}
				if !isPK {
					updateSetParts = append(updateSetParts, fmt.Sprintf("%s = excluded.%s", col, col))
				}
			}

			// If there are non-PK columns to update
			if len(updateSetParts) > 0 {
				updateSet := strings.Join(updateSetParts, ", ")
				sqlStatement = fmt.Sprintf(`INSERT INTO %s.%s (%s)
				SELECT %s %s FROM %s.%s %s
				ON CONFLICT (%s) DO UPDATE SET %s`,
					c.config.Schema, targetTable, columnsList,
					distinctOnClause, columnsList, c.config.Schema, externalTableName, whereClauseForSelect,
					conflictColumns, updateSet)
			} else {
				// All columns are primary keys, do nothing on conflict
				sqlStatement = fmt.Sprintf(`INSERT INTO %s.%s (%s)
				SELECT %s %s FROM %s.%s %s
				ON CONFLICT (%s) DO NOTHING`,
					c.config.Schema, targetTable, columnsList,
					distinctOnClause, columnsList, c.config.Schema, externalTableName, whereClauseForSelect,
					conflictColumns)
			}
		} else {
			// Do nothing on conflict (as per configuration)
			sqlStatement = fmt.Sprintf(`INSERT INTO %s.%s (%s)
			SELECT %s %s FROM %s.%s %s
			ON CONFLICT (%s) DO NOTHING`,
				c.config.Schema, targetTable, columnsList,
				distinctOnClause, columnsList, c.config.Schema, externalTableName, whereClauseForSelect,
				conflictColumns)
		}
	} else {
		// No primary key, use standard INSERT with GROUP BY to avoid duplicates
		// In this case, we use GROUP BY all columns to eliminate exact duplicates
		// ERROR: could not identify an equality operator for type vecf16
		sqlStatement = fmt.Sprintf(`INSERT INTO %s.%s (%s)
		SELECT %s FROM %s.%s`,
			c.config.Schema, targetTable, columnsList,
			columnsList, c.config.Schema, externalTableName)
	}

	_, err = c.pool.Exec(ctx, sqlStatement)
	if err != nil {
		return err
	}

	return nil
}

// DropExternalTable drops the external table
func (c *PostgreSQLClient) DropExternalTable(ctx context.Context, tableName string) error {
	sqlStatement := fmt.Sprintf(`DROP FOREIGN TABLE IF EXISTS %s.%s`,
		c.config.Schema, tableName)

	_, err := c.pool.Exec(ctx, sqlStatement)
	if err != nil {
		return err
	}

	return nil
}

// ExecuteSQL executes a SQL statement
func (c *PostgreSQLClient) ExecuteSQL(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return c.pool.Query(ctx, sql, args...)
}

// UpdateCheckpointErrorRecords increments the error records count in the checkpoint
func (c *PostgreSQLClient) UpdateCheckpointErrorRecords(ctx context.Context, processId string, count int) error {
	sqlStatement := `
	UPDATE relyt_sys.relyt_loader_checkpoint
	SET error_records = error_records + $1
	WHERE process_id = $2
	`

	_, err := c.pool.Exec(ctx, sqlStatement, count, processId)
	if err != nil {
		return err
	}

	return nil
}

// GetCheckpointErrorRecords gets the number of error records in the checkpoint
func (c *PostgreSQLClient) GetCheckpointErrorRecords(ctx context.Context, processId string) (int, error) {
	sqlStatement := `
	SELECT error_records FROM relyt_sys.relyt_loader_checkpoint
	WHERE process_id = $1
	`

	var errorRecords int
	err := c.pool.QueryRow(ctx, sqlStatement, processId).Scan(&errorRecords)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get error records count from checkpoint")
	}

	return errorRecords, nil
}

// DeleteCheckpoint deletes checkpoint records for the given process ID
func (c *PostgreSQLClient) DeleteCheckpoint(ctx context.Context, processId string) error {
	sqlStatement := `
	DELETE FROM relyt_sys.relyt_loader_checkpoint
	WHERE process_id = $1
	`

	_, err := c.pool.Exec(ctx, sqlStatement, processId)
	if err != nil {
		return err
	}

	return nil
}

// GetTableSchema retrieves the schema of a PostgreSQL table
func (c *PostgreSQLClient) GetTableSchema(ctx context.Context) ([]TableColumn, error) {
	// Parse the schema and table name
	parts := strings.Split(c.config.Table, ".")
	tableName := parts[0]
	if len(parts) > 1 {
		tableName = parts[1]
	}

	// Query to get column information using pg_catalog views
	// This is similar to what psql \d+ command uses internally
	sqlStatement := `
	SELECT 
		a.attname as column_name,
		pg_catalog.format_type(a.atttypid, a.atttypmod) as column_type,
		CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END as is_nullable,
		pg_catalog.format_type(a.atttypid, NULL) as data_type
	FROM pg_catalog.pg_attribute a
	JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
	JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
	LEFT JOIN pg_catalog.pg_description d ON d.objoid = c.oid AND d.objsubid = a.attnum
	WHERE n.nspname = $1
	  AND c.relname = $2
	  AND a.attnum > 0  -- Skip system columns
	  AND NOT a.attisdropped  -- Skip dropped columns
	ORDER BY a.attnum
	`

	rows, err := c.pool.Query(ctx, sqlStatement, c.config.Schema, tableName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve table schema")
	}
	defer rows.Close()

	var columns []TableColumn
	for rows.Next() {
		var col TableColumn
		var isNullable string

		if err := rows.Scan(&col.Name, &col.ColumnType, &isNullable, &col.DataType); err != nil {
			return nil, errors.Wrap(err, "failed to scan column data")
		}

		col.IsNullable = isNullable == "YES"
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating over columns")
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns found for table %s.%s", c.config.Schema, tableName)
	}
	return columns, nil
}

// Insert a delta checkpoint record
func (c *PostgreSQLClient) InsertDeltaCheckpoint(ctx context.Context, processId string, pgTable string, filePath string) error {
	sqlStatement := `
	INSERT INTO relyt_sys.relyt_loader_delta_checkpoint
	(process_id, pg_table, status, start_time, finish_time, filepath)
	VALUES ($1, $2, $3, $4, $5, $6)
	`

	_, err := c.pool.Exec(ctx, sqlStatement, processId, pgTable, string(CheckpointStatusRunning), time.Now(), nil, filePath)
	if err != nil {
		return errors.Wrap(err, "failed to insert delta checkpoint")
	}

	return nil
}

// Update delta checkpoint record
func (c *PostgreSQLClient) UpdateDeltaCheckpointStatus(ctx context.Context, processId string, filePaths []string, status CheckpointStatus, errorRecords int, errorMessage string) error {
	var placeholders []string
	for i := range filePaths {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+6)) // $6, $7, ...
	}

	sqlStatement := fmt.Sprintf(`
	UPDATE relyt_sys.relyt_loader_delta_checkpoint
	SET finish_time = $1, status = $2, error_message = $3, error_records = $4
	WHERE process_id = $5 and filepath in (%s)
	`, strings.Join(placeholders, ", "))

	args := make([]interface{}, len(filePaths)+5)
	args[0] = time.Now()
	args[1] = string(status)
	args[2] = errorMessage
	args[3] = errorRecords
	args[4] = processId

	for i, filePath := range filePaths {
		args[i+5] = filePath
	}

	_, err := c.pool.Exec(ctx, sqlStatement, args...)
	if err != nil {
		return errors.Wrap(err, "failed to update delta checkpoint")
	}

	return nil
}

// Delete delta checkpoint record
func (c *PostgreSQLClient) DeleteDeltaCheckpointByProcessIdAndFilepaths(ctx context.Context, processId string, filePaths []string) error {

	if len(filePaths) == 0 {
		return nil
	}

	var placeholders []string
	for i := range filePaths {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+2)) // $2, $3, ...
	}

	sqlStatement := fmt.Sprintf(`
	DELETE FROM relyt_sys.relyt_loader_delta_checkpoint
	WHERE process_id = $1 and filepath in (%s)
	`, strings.Join(placeholders, ", "))

	args := make([]interface{}, len(filePaths)+1)
	args[0] = processId
	for i, filePath := range filePaths {
		args[i+1] = filePath
	}

	_, err := c.pool.Exec(ctx, sqlStatement, args...)
	if err != nil {
		return errors.Wrap(err, "failed to delete delta checkpoint")
	}

	return nil
}

// DeleteCompletedDeltaCheckpoint deletes completed delta checkpoint records that are older than the given interval(hours)
func (c *PostgreSQLClient) DeleteCompletedDeltaCheckpoint(ctx context.Context, interval_hours int) error {
	sqlStatement := `
	DELETE FROM relyt_sys.relyt_loader_delta_checkpoint
	WHERE status = 'COMPLETED' AND finish_time < $1
	`

	_, err := c.pool.Exec(ctx, sqlStatement, time.Now().Add(-time.Duration(interval_hours)*time.Hour))
	if err != nil {
		return errors.Wrap(err, "failed to delete completed delta checkpoint")
	}

	return nil
}

// Delete delta checkpoint record
func (c *PostgreSQLClient) DeleteDeltaCheckpointByProcessId(ctx context.Context, processId string) error {
	sqlStatement := `
	DELETE FROM relyt_sys.relyt_loader_delta_checkpoint
	WHERE process_id = $1
	`

	_, err := c.pool.Exec(ctx, sqlStatement, processId)
	if err != nil {
		return errors.Wrap(err, "failed to delete delta checkpoint")
	}

	return nil
}

// CreateRoutingTableTrigger creates a trigger to notify changes in routing table
func (c *PostgreSQLClient) CreateRoutingTableTrigger(ctx context.Context, routingTable string, channelName string) error {
	// Create trigger function
	createTriggerFunc := fmt.Sprintf(
		`CREATE OR REPLACE FUNCTION relyt_sys.notify_routing_table_change()
		 RETURNS trigger AS $$
		 BEGIN
		 	PERFORM pg_notify('%s', 'routing table changed');
		 	RETURN NEW;
		 END;
		 $$ LANGUAGE plpgsql;`, channelName)

	// Create trigger
	createTrigger := fmt.Sprintf(`
	DROP TRIGGER IF EXISTS routing_table_change_trigger ON relyt_sys.%s;
	CREATE TRIGGER routing_table_change_trigger
	AFTER INSERT OR UPDATE OR DELETE ON relyt_sys.%s
	FOR EACH ROW EXECUTE FUNCTION relyt_sys.notify_routing_table_change();
	`, routingTable, routingTable)

	// Execute create trigger function
	if _, err := c.pool.Exec(ctx, createTriggerFunc); err != nil {
		return errors.Wrap(err, "failed to create trigger function")
	}

	// Execute create trigger
	if _, err := c.pool.Exec(ctx, createTrigger); err != nil {
		return errors.Wrap(err, "failed to create trigger")
	}

	return nil
}

func (c *PostgreSQLClient) DeleteTablesWithCondition(ctx context.Context, schema, table, fileID, routingID string, haveAuxTable bool) (int, error) {
	sqlStatement := `
	SELECT relyt_sys.delete_tables_with_condition(
		$1,  -- schema_name
		$2,  -- main_table
		$3,  -- file_id
		$4,  -- routing_id
		$5  -- have_aux_table
	)`

	var result int
	err := c.pool.QueryRow(ctx, sqlStatement, schema, table, fileID, routingID, haveAuxTable).Scan(&result)
	if err != nil {
		return 0, errors.Wrap(err, "failed to delete tables with condition")
	}

	return result, nil
}

func (c *PostgreSQLClient) GetColumnsWithCondition(ctx context.Context, args ...interface{}) (pgx.Rows, string, error) {
	getSQLStatement := `
		SELECT * FROM relyt_sys.get_columns_sql_with_condition(
			$1,  -- schema_name
			$2,  -- target_table_name
			$3,  -- column_names
			$4,  -- condition
			$5,  -- order_by
			$6,  -- group_by
			$7,  -- having
			$8,  -- limit_count
			$9,  -- offset_count
			$10 -- have_aux_table
		)`

	// 根据args生成完整的sql
	sqlRow := c.pool.QueryRow(ctx, getSQLStatement, args...)
	var finalSQL string
	err := sqlRow.Scan(&finalSQL)
	if err != nil {
		return nil, "", errors.Wrap(err, "failed to get final sql")
	}

	rows, err := c.pool.Query(ctx, finalSQL)
	if err != nil {
		return nil, finalSQL, errors.Wrap(err, "failed to get columns with condition")
	}

	return rows, finalSQL, nil
}

// CopyFromLocalOnConflict copies data from a local file to a PostgreSQL table within a transaction
func (c *PostgreSQLClient) CopyFromLocalOnConflict(ctx context.Context, tx pgx.Tx, filePath, targetTable string, columnNames []string, updateOnConflict bool) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	conflictClause := ""

	if updateOnConflict {
		conflictClause = "DO ON CONFLICT DO UPDATE"
	} else {
		conflictClause = "DO ON CONFLICT DO NOTHING"
	}

	copyCommand := fmt.Sprintf(
		"COPY %s.%s (%s) FROM STDIN WITH (FORMAT csv, HEADER false, NULL 'null') %s",
		c.config.Schema,
		targetTable,
		strings.Join(columnNames, ", "),
		conflictClause,
	)

	if _, err := tx.Conn().PgConn().CopyFrom(ctx, file, copyCommand); err != nil {
		return err
	}

	return nil
}

func (c *PostgreSQLClient) InsertIntoOnConflictFromLocal(ctx context.Context, tx pgx.Tx, filePath, targetTable string, columnNames []string, updateOnConflict bool, insertIntoSize int) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	// Get primary key columns to handle conflicts
	pkColumns, err := c.GetTablePrimaryKeys(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get primary key columns")
	}

	// Create CSV reader
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // Allow variable number of fields

	// Build the base INSERT statement
	columnsList := strings.Join(columnNames, ", ")

	var sqlStatement string
	if len(pkColumns) > 0 && updateOnConflict {
		// Build the ON CONFLICT clause
		conflictColumns := strings.Join(pkColumns, ", ")

		// Build the update set clause (set each column to excluded.column)
		updateSetParts := make([]string, 0, len(columnNames))
		for _, col := range columnNames {
			// Skip primary key columns in the update part
			isPK := false
			for _, pk := range pkColumns {
				if pk == col {
					isPK = true
					break
				}
			}
			if !isPK {
				updateSetParts = append(updateSetParts, fmt.Sprintf("%s = excluded.%s", col, col))
			}
		}

		// If there are non-PK columns to update
		if len(updateSetParts) > 0 {
			updateSet := strings.Join(updateSetParts, ", ")
			sqlStatement = fmt.Sprintf(`INSERT INTO %s.%s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s`,
				c.config.Schema, targetTable, columnsList, "%s", conflictColumns, updateSet)
		} else {
			// All columns are primary keys, do nothing on conflict
			sqlStatement = fmt.Sprintf(`INSERT INTO %s.%s (%s) VALUES %s ON CONFLICT (%s) DO NOTHING`,
				c.config.Schema, targetTable, columnsList, "%s", conflictColumns)
		}
	} else if len(pkColumns) > 0 {
		// Do nothing on conflict (as per configuration)
		conflictColumns := strings.Join(pkColumns, ", ")
		sqlStatement = fmt.Sprintf(`INSERT INTO %s.%s (%s) VALUES %s ON CONFLICT (%s) DO NOTHING`,
			c.config.Schema, targetTable, columnsList, "%s", conflictColumns)
	} else {
		// No primary key, use standard INSERT
		sqlStatement = fmt.Sprintf(`INSERT INTO %s.%s (%s) VALUES %s`,
			c.config.Schema, targetTable, columnsList, "%s")
	}

	// Read and process rows in batches
	lineNum := 0
	batch := make([][]string, 0, insertIntoSize)

	for {
		lineNum++
		record, err := reader.Read()
		if err == io.EOF {
			// Process remaining batch
			if len(batch) > 0 {
				if err := c.executeBatchInsert(ctx, tx, sqlStatement, batch, lineNum-len(batch)); err != nil {
					return err
				}
			}
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read CSV line %d: %w", lineNum, err)
		}

		batch = append(batch, record)

		// Execute batch when it reaches the size limit
		if len(batch) >= insertIntoSize {
			if err := c.executeBatchInsert(ctx, tx, sqlStatement, batch, lineNum-len(batch)+1); err != nil {
				return err
			}
			batch = batch[:0] // Reset batch
		}
	}

	return nil
}

// executeBatchInsert executes a batch insert with the given records
func (c *PostgreSQLClient) executeBatchInsert(ctx context.Context, tx pgx.Tx, sqlTemplate string, batch [][]string, startLineNum int) error {
	if len(batch) == 0 {
		return nil
	}

	// Build VALUES clause for the batch
	valuesParts := make([]string, len(batch))
	allArgs := make([]interface{}, 0, len(batch)*len(batch[0]))
	argIndex := 1

	for i, record := range batch {
		placeholders := make([]string, len(record))
		for j := range record {
			placeholders[j] = fmt.Sprintf("$%d", argIndex)
			allArgs = append(allArgs, record[j])
			argIndex++
		}
		valuesParts[i] = fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))
	}

	valuesClause := strings.Join(valuesParts, ", ")
	sqlStatement := fmt.Sprintf(sqlTemplate, valuesClause)

	// Execute the batch insert
	_, err := tx.Exec(ctx, sqlStatement, allArgs...)
	if err != nil {
		return fmt.Errorf("failed to execute batch INSERT starting at line %d: %w", startLineNum, err)
	}

	return nil
}

func (c *PostgreSQLClient) CopyFromS3OnConflict(ctx context.Context, tx pgx.Tx, s3URL, targetTable string,
	columnNames []string, updateOnConflict bool, s3Config S3Config) error {

	var copySQL string
	if updateOnConflict {
		copySQL = fmt.Sprintf(`
			COPY %s.%s (%s) FROM '%s'
			ACCESS_KEY_ID '%s'
			SECRET_ACCESS_KEY '%s'
			(FORMAT csv, HEADER false, NULL 'null')
			DO ON CONFLICT DO UPDATE;`,
			c.config.Schema, targetTable, strings.Join(columnNames, ", "), s3URL,
			s3Config.AccessKey, s3Config.SecretKey)
	} else {
		copySQL = fmt.Sprintf(`
			COPY %s.%s (%s) FROM '%s'
			ACCESS_KEY_ID '%s'
			SECRET_ACCESS_KEY '%s'
			(FORMAT csv, HEADER false, NULL 'null')
			DO ON CONFLICT DO NOTHING;`,
			c.config.Schema, targetTable, strings.Join(columnNames, ", "), s3URL,
			s3Config.AccessKey, s3Config.SecretKey)
	}

	_, err := tx.Exec(ctx, copySQL)
	if err != nil {
		return fmt.Errorf("failed to execute COPY FROM S3: %w", err)
	}

	return nil
}

func (c *PostgreSQLClient) InsertIntoFromExternalTable(ctx context.Context, tx pgx.Tx, s3URL, batchDir, targetTable string, columnNames []string, updateOnConflict bool, s3Config S3Config) error {
	// Generate a unique table name for the external table
	externalTableName := fmt.Sprintf("ext_%s_%s",
		strings.ReplaceAll(c.config.Table, ".", "_"),
		batchDir)

	// Drop external table first using transaction, because we have a retry mechanism in the import process
	dropSQL := fmt.Sprintf(`DROP FOREIGN TABLE IF EXISTS %s.%s`, c.config.Schema, externalTableName)
	_, err := tx.Exec(ctx, dropSQL)
	if err != nil {
		return errors.Wrap(err, "failed to drop existing external table")
	}

	// Create external table using transaction with column names (types will be taken from target table)
	// Get the table schema to ensure column types match
	tableSchema, err := c.GetTableSchema(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get table schema for creating external table")
	}

	// Create a map of column names to their definitions
	columnMap := make(map[string]string)
	for _, col := range tableSchema {
		columnMap[col.Name] = fmt.Sprintf("%s %s", col.Name, col.ColumnType)
	}

	// Build column definitions based on the provided column names
	// but use the types from the actual table schema
	columnDefs := make([]string, 0, len(columnNames))
	for _, name := range columnNames {
		if def, exists := columnMap[name]; exists {
			columnDefs = append(columnDefs, def)
		} else {
			return fmt.Errorf("column '%s' does not exist in target table", name)
		}
	}

	// Create external table using transaction
	createSQL := fmt.Sprintf(`CREATE EXTERNAL TABLE %s.%s (
		%s
	)
	LOCATION('%s 
          accessid=%s
          secret=%s 
          region=%s 
          version=2
		  clean_invalid_encoding=on')
	FORMAT 'CSV'
	(delimiter ',' null 'null')
	`, c.config.Schema, externalTableName, strings.Join(columnDefs, ",\n"), s3URL, s3Config.AccessKey, s3Config.SecretKey, s3Config.Region)

	_, err = tx.Exec(ctx, createSQL)
	if err != nil {
		return errors.Wrap(err, "failed to create external table")
	}

	// Import data from external table to target table using transaction
	// Get primary key columns to handle conflicts
	pkColumns, err := c.GetTablePrimaryKeys(ctx)
	if err != nil {
		// Try to drop external table even if import failed using transaction
		if _, dropErr := tx.Exec(ctx, dropSQL); dropErr != nil {
			log.Printf("Failed to drop external table after import error: %v", dropErr)
		}
		return errors.Wrap(err, "failed to get primary key columns")
	}

	// Import data from external table to target table
	columnsList := strings.Join(columnNames, ", ")

	var sqlStatement string
	if len(pkColumns) > 0 {
		// Build the ON CONFLICT clause
		conflictColumns := strings.Join(pkColumns, ", ")

		// For the SELECT statement, we need to handle duplicate keys in the external table
		// Use DISTINCT ON to get only one row per primary key combination
		distinctOnClause := fmt.Sprintf("DISTINCT ON (%s)", conflictColumns)

		// Build the WHERE clause to ensure all columns are not null
		wherePartsForSelect := make([]string, 0, len(pkColumns))
		for _, pk := range pkColumns {
			wherePartsForSelect = append(wherePartsForSelect, fmt.Sprintf("%s IS NOT NULL", pk))
		}
		whereClauseForSelect := ""
		if len(wherePartsForSelect) > 0 {
			whereClauseForSelect = "WHERE " + strings.Join(wherePartsForSelect, " AND ")
		}

		if updateOnConflict {
			// Build the update set clause (set each column to excluded.column)
			updateSetParts := make([]string, 0, len(columnNames))
			for _, col := range columnNames {
				// Skip primary key columns in the update part
				isPK := false
				for _, pk := range pkColumns {
					if pk == col {
						isPK = true
						break
					}
				}
				if !isPK {
					updateSetParts = append(updateSetParts, fmt.Sprintf("%s = excluded.%s", col, col))
				}
			}

			// If there are non-PK columns to update
			if len(updateSetParts) > 0 {
				updateSet := strings.Join(updateSetParts, ", ")
				sqlStatement = fmt.Sprintf(`INSERT INTO %s.%s (%s)
				SELECT %s %s FROM %s.%s %s
				ON CONFLICT (%s) DO UPDATE SET %s`,
					c.config.Schema, targetTable, columnsList,
					distinctOnClause, columnsList, c.config.Schema, externalTableName, whereClauseForSelect,
					conflictColumns, updateSet)
			} else {
				// All columns are primary keys, do nothing on conflict
				sqlStatement = fmt.Sprintf(`INSERT INTO %s.%s (%s)
				SELECT %s %s FROM %s.%s %s
				ON CONFLICT (%s) DO NOTHING`,
					c.config.Schema, targetTable, columnsList,
					distinctOnClause, columnsList, c.config.Schema, externalTableName, whereClauseForSelect,
					conflictColumns)
			}
		} else {
			// Do nothing on conflict (as per configuration)
			sqlStatement = fmt.Sprintf(`INSERT INTO %s.%s (%s)
			SELECT %s %s FROM %s.%s %s
			ON CONFLICT (%s) DO NOTHING`,
				c.config.Schema, targetTable, columnsList,
				distinctOnClause, columnsList, c.config.Schema, externalTableName, whereClauseForSelect,
				conflictColumns)
		}
	} else {
		// No primary key, use standard INSERT
		sqlStatement = fmt.Sprintf(`INSERT INTO %s.%s (%s)
		SELECT %s FROM %s.%s`,
			c.config.Schema, targetTable, columnsList,
			columnsList, c.config.Schema, externalTableName)
	}

	_, err = tx.Exec(ctx, sqlStatement)
	if err != nil {
		// Try to drop external table even if import failed using transaction
		if _, dropErr := tx.Exec(ctx, dropSQL); dropErr != nil {
			log.Printf("Failed to drop external table after import error: %v", dropErr)
		}
		return errors.Wrap(err, "failed to execute import from external table")
	}

	// Drop external table after successful import using transaction
	_, err = tx.Exec(ctx, dropSQL)
	if err != nil {
		return errors.Wrap(err, "failed to drop external table after import")
	}

	return nil
}

func (c *PostgreSQLClient) DeleteOutdatedFiles(ctx context.Context, tx pgx.Tx, table string, fileVersionMap map[RecordIndex]string) error {
	// 如果没有版本映射，不需要删除
	if len(fileVersionMap) == 0 {
		return nil
	}

	// 构建批量删除的 SQL 语句
	// 遍历 fileVersionMap 构建 AND/OR 条件
	var conditions []string
	var args []interface{}
	argIndex := 1

	for recordIndex, version := range fileVersionMap {
		// 每个条件组合：routing_id = ? AND fileid = ? AND version < ?
		condition := fmt.Sprintf("(routing_id = $%d AND fileid = $%d AND version < $%d)",
			argIndex, argIndex+1, argIndex+2)

		conditions = append(conditions, condition)
		args = append(args, recordIndex.routingID, recordIndex.fileID, version)
		argIndex += 3
	}

	// 使用 OR 连接所有条件组合
	whereClause := strings.Join(conditions, " OR ")
	sqlStatement := fmt.Sprintf("DELETE FROM %s.%s WHERE %s", c.config.Schema, table, whereClause)

	// 执行批量删除
	_, err := tx.Exec(ctx, sqlStatement, args...)
	if err != nil {
		return err
	}

	return nil
}
