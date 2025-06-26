package bulkprocessor

import "os"

type ImportErrorHandler func(fieldname string, values []string, err error, resources interface{})

// S3Config represents the configuration for S3
type S3Config struct {
	Endpoint    string // S3 endpoint (e.g., s3.amazonaws.com)
	Region      string // AWS region
	BucketName  string // S3 bucket name
	Prefix      string // Key prefix for S3 objects
	AccessKey   string // AWS access key
	SecretKey   string // AWS secret key
	Concurrency int    // Number of concurrent uploads
	PartSize    int64  // S3 multipart upload part size in bytes
}

// PostgreSQLConfig represents the configuration for PostgreSQL
type PostgreSQLConfig struct {
	Host     string // PostgreSQL host
	Port     int    // PostgreSQL port
	Username string // PostgreSQL username
	Password string // PostgreSQL password
	Database string // PostgreSQL database name
	Table    string // Target table name
	Schema   string // Schema name (default: public)
}

// Config represents the configuration for the bulk processor
type Config struct {
	// S3 configuration is now always loaded from database through PostgreSQL connection
	PostgreSQL           PostgreSQLConfig // PostgreSQL configuration
	BatchSize            int              // Number of records per file
	BatchImportSize      int              // Number of files to import in a single batch (default: 1)
	MaxErrorRecords      int              // Maximum number of error records to ignore (default: 0)
	UpdateOnConflict     bool             // Whether to update or do nothing on primary key conflict (true=update, false=do nothing, default: true)
	FlushSleepTime       int              // Sleep time in milliseconds between processing iterations (default: 10)
	FeedbackColumn       string           // Column name for error messages (default: "") when import failed
	ImportErrorCallback  ImportErrorHandler
	CallbackResource     interface{}
	FileWriteTimeout     int    // a new file opened for a limited time to write, default: 10 seconds
	BGWorkerInterval     int    // GC interval in seconds, default: 60 seconds
	ImportTimeout        int    // S3 import timeout in seconds
	ImportErrorSleepTime int    // S3 import error sleep time in seconds
	EnableDualBuffer     bool   // enable dual buffer, default: true
	BufferMaxRecords     int    // buffer max records, default: 1000
	UseInsertOnConflict  bool   // use insert on conflict, default: true
	LocalFilePrefix      string // local file prefix, default: "/tmp"
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.PostgreSQL.Host == "" {
		return ErrPostgreSQLHostRequired
	}
	if c.PostgreSQL.Port == 0 {
		c.PostgreSQL.Port = 5432 // Default PostgreSQL port
	}
	if c.PostgreSQL.Username == "" {
		return ErrPostgreSQLUsernameRequired
	}
	if c.PostgreSQL.Database == "" {
		return ErrPostgreSQLDatabaseRequired
	}
	if c.PostgreSQL.Table == "" {
		return ErrPostgreSQLTableRequired
	}
	if c.PostgreSQL.Schema == "" {
		c.PostgreSQL.Schema = "public" // Default schema
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 10000 // Default batch size
	}
	if c.BatchImportSize <= 0 {
		c.BatchImportSize = 10 // Default to importing 10 files at a time
	}
	if c.MaxErrorRecords < 0 {
		c.MaxErrorRecords = 0 // Default to not ignoring any errors
	}
	if !c.UpdateOnConflict {
		c.UpdateOnConflict = true // Default to update on conflict
	}
	if c.FlushSleepTime <= 0 {
		c.FlushSleepTime = 10 // Default sleep time to 10ms
	}
	if c.FileWriteTimeout <= 0 {
		c.FileWriteTimeout = 6 // Default auto flush interval to 6 seconds
	}
	if c.BGWorkerInterval <= 0 {
		c.BGWorkerInterval = 60 // Default GC interval to 60 seconds
	}
	if c.ImportTimeout <= 0 {
		c.ImportTimeout = 1800 // Default import timeout to 1800 seconds
	}
	if c.ImportErrorSleepTime <= 0 {
		c.ImportErrorSleepTime = 60 // Default import error sleep time to 60 seconds
	}

	if c.BufferMaxRecords <= 0 {
		c.BufferMaxRecords = 1000
	}

	if !c.EnableDualBuffer {
		c.EnableDualBuffer = true
	}

	if c.LocalFilePrefix == "" {
		c.LocalFilePrefix = os.TempDir()
	}

	if !c.UseInsertOnConflict {
		c.UseInsertOnConflict = true
	}

	return nil
}
