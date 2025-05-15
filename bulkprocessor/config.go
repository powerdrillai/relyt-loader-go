package bulkprocessor

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
	PostgreSQL       PostgreSQLConfig // PostgreSQL configuration
	BatchSize        int              // Number of records per file
	BatchImportSize  int              // Number of files to import in a single batch (default: 1)
	Concurrency      int              // Number of concurrent imports (default: 1)
	TmpDir           string           // Temporary directory for local files (default: os.TempDir())
	MaxErrorRecords  int              // Maximum number of error records to ignore (default: 0)
	UpdateOnConflict bool             // Whether to update or do nothing on primary key conflict (true=update, false=do nothing, default: true)
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
	if c.Concurrency <= 0 {
		c.Concurrency = 1 // Default concurrency
	}
	if c.TmpDir == "" {
		c.TmpDir = "" // Will use os.TempDir() in processor
	}
	if c.MaxErrorRecords < 0 {
		c.MaxErrorRecords = 0 // Default to not ignoring any errors
	}
	// UpdateOnConflict is true by default (update records on primary key conflict)
	// bool defaults to false in Go, so we need to explicitly set it to true if not set
	if !c.UpdateOnConflict {
		c.UpdateOnConflict = true // Default to update on conflict
	}
	return nil
}
