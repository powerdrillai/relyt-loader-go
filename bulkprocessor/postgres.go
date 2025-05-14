package bulkprocessor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
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
	connString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		config.Username,
		config.Password,
		config.Host,
		config.Port,
		config.Database,
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
	INSERT INTO relyt_loader_checkpoint
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
	UPDATE relyt_loader_checkpoint
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
	SELECT file_details FROM relyt_loader_checkpoint
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
	UPDATE relyt_loader_checkpoint
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
	UPDATE relyt_loader_checkpoint
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

// GetS3ConfigFromDB retrieves S3 configuration from the database
func (c *PostgreSQLClient) GetS3ConfigFromDB(ctx context.Context) (*S3Config, error) {
	var s3Config S3Config

	// Query the LOADER_CONFIG function
	sqlStatement := `
	SELECT 
		endpoint, 
		region, 
		bucket_name, 
		prefix, 
		access_key, 
		secret_key, 
		concurrency, 
		part_size
	FROM LOADER_CONFIG()
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
	)

	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve S3 configuration from database")
	}

	return &s3Config, nil
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

	// Create temporary external table
	sqlStatement := fmt.Sprintf(`CREATE EXTERNAL TABLE %s.%s (
		%s
	)
	LOCATION('%s 
          accessid=%s
          secret=%s 
          region=%s 
          version=2')
	FORMAT 'CSV'`, c.config.Schema, tableName, strings.Join(columnDefs, ",\n"), s3URL, s3Config.AccessKey, s3Config.SecretKey, s3Config.Region)

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
func (c *PostgreSQLClient) ImportFromExternalTable(ctx context.Context, externalTableName string, columns []string) error {
	// Get primary key columns to handle conflicts
	pkColumns, err := c.GetTablePrimaryKeys(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get primary key columns")
	}

	// Import data from external table to target table
	columnsList := strings.Join(columns, ", ")

	var sqlStatement string
	if len(pkColumns) > 0 {
		// Build the ON CONFLICT clause
		conflictColumns := strings.Join(pkColumns, ", ")

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
			SELECT %s FROM %s.%s
			ON CONFLICT (%s) DO UPDATE SET %s`,
				c.config.Schema, c.config.Table, columnsList,
				columnsList, c.config.Schema, externalTableName,
				conflictColumns, updateSet)
		} else {
			// All columns are primary keys, do nothing on conflict
			sqlStatement = fmt.Sprintf(`INSERT INTO %s.%s (%s)
			SELECT %s FROM %s.%s
			ON CONFLICT (%s) DO NOTHING`,
				c.config.Schema, c.config.Table, columnsList,
				columnsList, c.config.Schema, externalTableName,
				conflictColumns)
		}
	} else {
		// No primary key, use standard INSERT
		sqlStatement = fmt.Sprintf(`INSERT INTO %s.%s (%s)
		SELECT %s FROM %s.%s`,
			c.config.Schema, c.config.Table, columnsList,
			columnsList, c.config.Schema, externalTableName)
	}

	_, err = c.pool.Exec(ctx, sqlStatement)
	if err != nil {
		return errors.Wrap(err, "failed to load data from external table")
	}

	return nil
}

// DropExternalTable drops the external table
func (c *PostgreSQLClient) DropExternalTable(ctx context.Context, tableName string) error {
	sqlStatement := fmt.Sprintf(`DROP FOREIGN TABLE IF EXISTS %s.%s`,
		c.config.Schema, tableName)

	_, err := c.pool.Exec(ctx, sqlStatement)
	if err != nil {
		return errors.Wrap(err, "failed to drop external table")
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
	UPDATE relyt_loader_checkpoint
	SET error_records = error_records + $1
	WHERE process_id = $2
	`

	_, err := c.pool.Exec(ctx, sqlStatement, count, processId)
	if err != nil {
		return errors.Wrap(err, "failed to update checkpoint error records count")
	}

	return nil
}

// GetCheckpointErrorRecords gets the current error records count from the checkpoint
func (c *PostgreSQLClient) GetCheckpointErrorRecords(ctx context.Context, processId string) (int, error) {
	sqlStatement := `
	SELECT error_records FROM relyt_loader_checkpoint
	WHERE process_id = $1
	`

	var errorRecords int
	err := c.pool.QueryRow(ctx, sqlStatement, processId).Scan(&errorRecords)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get checkpoint error records count")
	}

	return errorRecords, nil
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
