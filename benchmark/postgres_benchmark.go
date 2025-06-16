package benchmark

import (
	"context"
	"testing"
	
	"github.com/powerdrillai/relyt-loader-go/bulkprocessor"
)

func BenchmarkDeleteDeltaCheckpointByProcessIdAndFilepaths(b *testing.B) {
	ctx := context.Background()
	
	// Setup test database connection
	config := bulkprocessor.PostgreSQLConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "test_db",
		User:     "test_user",
		Password: "test_password",
	}
	
	client, err := bulkprocessor.NewPostgreSQLClient(config)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	// Prepare test data
	processId := "test_process_123"
	filePaths := []string{
		"path/to/file1.csv",
		"path/to/file2.csv",
		"path/to/file3.csv",
	}

	// Benchmark the function
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := client.DeleteDeltaCheckpointByProcessIdAndFilepaths(ctx, processId, filePaths); err != nil {
			b.Fatal(err)
		}
	}
}
