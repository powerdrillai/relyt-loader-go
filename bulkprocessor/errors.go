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
)
