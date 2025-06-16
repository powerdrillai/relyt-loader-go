module github.com/powerdrillai/relyt-loader-go

go 1.22.0

require (
	github.com/aws/aws-sdk-go v1.50.20
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.5.3
	github.com/pkg/errors v0.9.1
)

require (
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.14.3 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.3 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/lib/pq v1.10.9 // indirect
	golang.org/x/crypto v0.20.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)

replace github.com/powerdrillai/relyt-loader-go/bulkprocessor v1.0.5 => ./bulkprocessor
