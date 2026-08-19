package bulkprocessor

import "errors"

// Common errors
var (
	// Configuration errors
	ErrS3BucketNameRequired       = errors.New("S3 bucket name is required")
	ErrS3RegionRequired           = errors.New("S3 region is required")
	ErrS3CredentialsRequired      = errors.New("S3 access key and secret key are required")
	ErrPostgreSQLHostRequired     = errors.New("PostgreSQL host is required")
	ErrPostgreSQLUsernameRequired = errors.New("PostgreSQL username is required")
	ErrPostgreSQLDatabaseRequired = errors.New("PostgreSQL database is required")
	ErrPostgreSQLTableRequired    = errors.New("PostgreSQL table is required")

	// Runtime errors
	ErrNotInitialized              = errors.New("bulk processor not initialized")
	ErrInvalidInput                = errors.New("invalid input: must be a slice of structs")
	ErrEmptyInput                  = errors.New("empty input: nothing to process")
	ErrS3UploadFailed              = errors.New("failed to upload to S3")
	ErrPostgreSQLConnectionFailed  = errors.New("failed to connect to PostgreSQL")
	ErrExternalTableCreationFailed = errors.New("failed to create external table")
	ErrDataImportFailed            = errors.New("failed to load data to PostgreSQL")
	ErrProcessorClosed             = errors.New("processor is closed")

	// Instance sharding errors
	ErrBothRoutingTables        = errors.New("table has both aux routing and instance routing tables, they are mutually exclusive")
	ErrNoDefaultInstance        = errors.New("no default instance configured: sentinel row '-1' missing in instance routing table")
	ErrShardedRequiresV2        = errors.New("instance-sharded tables only support the V2 (dual buffer) write path")
	ErrReservedRoutingID        = errors.New("routing_id '-1' is reserved as the default-instance sentinel")
	ErrRoutingIDRequired        = errors.New("sharded table requires RoutingID on search/update options")
	ErrRoutingIDMismatch        = errors.New("record routing_id does not match InsertV2 routingID")
	ErrRoutingColumnRequired    = errors.New("instance-sharded records require a routing_id column")
	ErrRoutingIDUpdateForbidden = errors.New("routing_id cannot be updated on an instance-sharded table")
	ErrUnsafeShardedSQL         = errors.New("unsafe sharded SQL fragment")
	ErrShardedPrimaryKey        = errors.New("instance-sharded table primary key must include routing_id")
	ErrPrimaryKeyColumnRequired = errors.New("record type is missing a primary-key column")
	ErrRecordTypeMismatch       = errors.New("record type does not match processor record type")
	ErrForeignShardedTable      = errors.New("sharded table requires a processor configured for that table")
)
