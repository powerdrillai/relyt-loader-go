package bulkprocessor

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
)

type TestDataWithAuxV2 struct {
	ID        int    `relyt:"id"`
	FileID    int    `relyt:"fileid"`
	RoutingID string `relyt:"routing_id"`
	Ext       string `relyt:"ext"`
	Vector    string `relyt:"vector"`
}

// TestDataWithCopyOnConflict 包含版本字段的测试数据结构
type TestDataWithCopyOnConflict struct {
	ID        int    `relyt:"id"`
	FileID    int    `relyt:"fileid"`
	RoutingID string `relyt:"routing_id"`
	Version   int    `relyt:"version"`
	Ext       string `relyt:"ext"`
	Vector    string `relyt:"vector"`
}

func NewProcessorV2(dbconfig DatabaseConfig, fileTimeout int, bufferSize int, tablename ...string) *BulkProcessor {
	// open a error.log
	logFile, err := os.OpenFile("/tmp/error.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}

	tableName := "test_routing_data_v2"
	if len(tablename) > 0 {
		tableName = tablename[0]
	}

	// Create user-defined resource structure
	resources := &ErrorHandlerResources{
		LogFile: logFile,
	}

	// initialize config
	config := Config{
		// PostgreSQL config (required)
		PostgreSQL: PostgreSQLConfig{
			Host:     dbconfig.Host, // use your own host
			Port:     dbconfig.Port,
			Username: dbconfig.Username,
			Password: dbconfig.Password, // use your own password
			Database: dbconfig.Database, // use your own database
			Table:    tableName,
			Schema:   "public",
		},
		BatchSize:           10, // number of records per file
		BatchImportSize:     2,
		FeedbackColumn:      "id", // column name for error messages
		ImportErrorCallback: WriteErrorsToFiles,
		CallbackResource:    resources,
		FileWriteTimeout:    fileTimeout, // set file write timeout
		BGWorkerInterval:    10,          // set GC interval
	}

	// create processor
	processor, err := New(config)

	if bufferSize > 0 {
		processor.config.BufferMaxRecords = bufferSize
	}

	log.Printf("processor created, buffer max records: %d", processor.config.BufferMaxRecords)

	if err != nil {
		log.Fatalf("failed to create processor: %v", err)
	}
	return processor
}

func CreateTestDataTaleWithAuxV2(db *sql.DB) error {
	// This function is a placeholder for creating the test table in PostgreSQL.
	// You can implement the logic to create the necessary table structure here.
	// For example, you might use a SQL command like:
	// CREATE TABLE test_data (id SERIAL PRIMARY KEY, ext TEXT, vector TEXT);
	log.Println("Creating test table with auxin PostgreSQL...")
	query := `
	CREATE TABLE IF NOT EXISTS test_routing_data_v2 (
		id bigint NOT NULL PRIMARY KEY,
		fileid bigint NOT NULL,
		routing_id text NOT NULL,
		ext text NOT NULL,
		vector vecf16(3) NOT NULL
	);
	CREATE TABLE IF NOT EXISTS test_routing_data_v2_relyt_massive_group (
		id bigint NOT NULL PRIMARY KEY,
		fileid bigint NOT NULL,
		routing_id text NOT NULL,
		ext text NOT NULL,
		vector vecf16(3) NOT NULL
	);
	CREATE TABLE IF NOT EXISTS relyt_sys.test_routing_data_v2_relyt_routing (
		routing_id text PRIMARY KEY,
		store_table_name TEXT NOT NULL
	) USING heap DISTRIBUTED NONE;
	`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create test table: %w", err)
	}
	log.Println("Test table created successfully.")
	return nil
}

func TruncateTestDataTableWithAuxV2(db *sql.DB) error {
	// This function is a placeholder for truncating the test table in PostgreSQL.
	// You can implement the logic to truncate the table here.
	log.Println("Truncating test tables in PostgreSQL...")
	query := `
	TRUNCATE TABLE test_routing_data_v2;
	TRUNCATE TABLE test_routing_data_v2_relyt_massive_group;
	TRUNCATE TABLE relyt_sys.test_routing_data_v2_relyt_routing;
	`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to truncate test tables: %w", err)
	}
	log.Println("Test tables truncated successfully.")
	return nil
}

func InitTestDataTableWithAuxV2(db *sql.DB) error {
	log.Println("Initializing test data in PostgreSQL...")

	// insert data to test_routing_data
	query := `
	INSERT INTO test_routing_data_v2 (id, fileid, routing_id, ext, vector)
	VALUES ($1, $2, $3, $4, $5);
	`
	// create random number generator for vector values
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// insert 100 records for routing_id 100 and 110
	id := 0
	for _, routingID := range []int{100, 110} {
		for i := 0; i < 100; i++ {
			fileID := routingID
			ext := fmt.Sprintf("ext_%d", id)
			vector := fmt.Sprintf("[%f,%f,%f]", r.Float32(), r.Float32(), r.Float32())

			_, err := db.Exec(query, id, fileID, routingID, ext, vector)
			if err != nil {
				return fmt.Errorf("failed to insert data for routing_id %d: %w", routingID, err)
			}
			id++
		}
	}

	// insert 10 records for routing_id 120, 130, 140
	for _, routingID := range []int{120, 130, 140} {
		for i := 0; i < 10; i++ {
			fileID := routingID
			ext := fmt.Sprintf("ext_%d", id)
			vector := fmt.Sprintf("[%f,%f,%f]", r.Float32(), r.Float32(), r.Float32())

			_, err := db.Exec(query, id, fileID, routingID, ext, vector)
			if err != nil {
				return fmt.Errorf("failed to insert data for routing_id %d: %w", routingID, err)
			}
			id++
		}
	}

	log.Println("Test data initialized successfully.")
	return nil
}

func InitTestRoutingTable(db *sql.DB, tableName ...string) error {
	log.Println("Initializing test routing table in PostgreSQL...")

	tablename := "test_routing_data_v2"
	if len(tableName) > 0 {
		tablename = tableName[0]
	}
	query := fmt.Sprintf(`
	INSERT INTO relyt_sys.%s_relyt_routing (routing_id, store_table_name)
	VALUES ('100', '%s');
	`, tablename, tablename)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to insert data for test routing table: %w", err)
	}
	log.Println("Test routing table initialized successfully.")
	return nil
}

func GetCountFromTestDataTableWithAuxV2(db *sql.DB, auxtable bool) (int, error) {
	// This function retrieves the count of records in all test tables.
	log.Println("Counting records in test tables...")
	table := "test_routing_data_v2"
	if auxtable {
		table = "test_routing_data_v2_relyt_massive_group"
	}
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)

	var count int
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count records: %w", err)
	}
	return count, nil
}

func CreateTestDataTaleWithOutAux(db *sql.DB) error {
	// This function is a placeholder for creating the test table in PostgreSQL.
	// You can implement the logic to create the necessary table structure here.
	// For example, you might use a SQL command like:
	// CREATE TABLE test_data (id SERIAL PRIMARY KEY, ext TEXT, vector TEXT);
	log.Println("Creating test table with auxin PostgreSQL...")
	query := `
	CREATE TABLE IF NOT EXISTS test_routing_data_without_aux (
		id bigint NOT NULL PRIMARY KEY,
		fileid bigint NOT NULL,
		routing_id text NOT NULL,
		ext text NOT NULL,
		vector vecf16(3) NOT NULL
	);
	`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create test table: %w", err)
	}
	log.Println("Test table created successfully.")
	return nil
}

func TruncateTestDataTaleWithOutAux(db *sql.DB) error {
	// This function is a placeholder for creating the test table in PostgreSQL.
	// You can implement the logic to create the necessary table structure here.
	// For example, you might use a SQL command like:
	// CREATE TABLE test_data (id SERIAL PRIMARY KEY, ext TEXT, vector TEXT);
	log.Println("Truncating test table with auxin PostgreSQL...")
	query := `
	TRUNCATE TABLE test_routing_data_without_aux;
	`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create test table: %w", err)
	}
	log.Println("Test table created successfully.")
	return nil
}

func GetCountFromTestDataTableWithoutAux(db *sql.DB) (int, error) {
	// This function retrieves the count of records in all test tables.
	log.Println("Counting records in test tables...")
	table := "test_routing_data_without_aux"
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)

	var count int
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count records: %w", err)
	}
	return count, nil
}

func CreateTestDataTaleWithCopyOnConflict(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS test_routing_data_copy_on_conflict (
		id bigint not null,
		fileid bigint not null,
		routing_id text not null,
		version bigint not null,
		ext text not null,
		vector vecf16(3) not null,
		PRIMARY KEY (routing_id, fileid, id)
	);
	`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create test table: %w", err)
	}
	log.Println("Test table created successfully.")
	return nil

}

func TruncateTestDataTableWithCopyOnConflict(db *sql.DB) error {
	log.Println("Truncating test table with copy on conflict in PostgreSQL...")
	query := `
	TRUNCATE TABLE test_routing_data_copy_on_conflict;
	`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to truncate test table: %w", err)
	}
	log.Println("Test table truncated successfully.")
	return nil
}

// GetCountFromTestDataTableWithCopyOnConflict 获取测试表的记录数
func GetCountFromTestDataTableWithCopyOnConflict(db *sql.DB, auxtable bool) (int, error) {
	log.Println("Counting records in test tables with copy on conflict...")
	table := "test_routing_data_copy_on_conflict"
	if auxtable {
		table = "test_routing_data_copy_on_conflict_relyt_massive_group"
	}
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)

	var count int
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count records: %w", err)
	}
	return count, nil
}

// TestInsertWithBufferBasic:
// buffer_max_records to 10
func TestBufferInsertBasic(t *testing.T) {
	// Initialize database connection
	dbConfig := InitDatabaseConfig("127.0.0.1", 7000, "postgres", "", "postgres")
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	} else {
		err := CreateTestDataTaleWithAuxV2(db)
		if err != nil {
			log.Fatalf("failed to create test table: %v", err)
			return
		}
		err = TruncateTestDataTableWithAuxV2(db)
		if err != nil {
			log.Fatalf("failed to truncate test table: %v", err)
			return
		}
		err = InitTestRoutingTable(db)
		if err != nil {
			log.Fatalf("failed to init test routing table: %v", err)
			return
		}

		err = ClearRelytCheckpointTable(db)
		if err != nil {
			log.Fatalf("failed to clear relyt_checkpoint table: %v", err)
			return
		}
	}
	defer db.Close()

	fileTimeout := 3 // set file write timeout to 2 seconds
	processor := NewProcessorV2(dbConfig, fileTimeout, 0)

	filePath := "../examples/data/test_insert_v2_basic.csv"

	batchSize := 5
	var tests []TestDataWithAuxV2

	csvFile, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("failed to open csv file: %v", err)
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)
	csvReader.FieldsPerRecord = -1
	csvReader.Comma = '\t'
	csvReader.ReuseRecord = true

	i := 0
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf("failed to read csv file: %v", err)
		}
		id, err := strconv.Atoi(record[0])
		if err != nil {
			t.Errorf("failed to parse id: %v", err)
		}
		fileID, err := strconv.Atoi(record[1])
		if err != nil {
			t.Errorf("failed to parse file_id: %v", err)
		}
		routingID := record[2]
		if err != nil {
			t.Errorf("failed to parse routing_id: %v", err)
		}
		if len(record) < 5 {
			t.Errorf("record does not contain enough fields: %v", record)
		} else {
			log.Printf("record %d: id=%d, file_id=%d, routing_id=%s, ext=%s, vector=%s", i, id, fileID, routingID, record[3], record[4])
		}
		ext := record[3]
		vector := record[4]
		tests = append(tests, TestDataWithAuxV2{
			ID:        id,
			FileID:    fileID,
			RoutingID: routingID,
			Ext:       ext,
			Vector:    vector,
		})
		i++

		// insert batch
		if i%batchSize == 0 {
			log.Printf("insert batch %d, contains %d records", i/batchSize, len(tests))
			err := processor.InsertV2(fmt.Sprintf("%d", fileID), routingID, tests)
			if err != nil {
				t.Errorf("failed to insert data: %v", err)
			}

			tests = nil // clear the list, prepare for the next batch
		}

		if i == 5 {
			err := processor.DeleteV2(fmt.Sprintf("%d", fileID), routingID)
			log.Printf("delete data: fileID=%d, routingID=%s", fileID, routingID)
			if err != nil {
				t.Errorf("failed to delete data: %v", err)
			}
		}
	}

	processor.Flush()

	log.Println("waiting for import to complete...")
	time.Sleep(time.Duration(10) * time.Second)

	// Check the count of records in the test table
	mainCount, err := GetCountFromTestDataTableWithAuxV2(db, false)
	if err != nil {
		t.Errorf("failed to get count from test table: %v", err)
	}
	if mainCount != 15 {
		t.Errorf("main table expected 15 records, but got %d", mainCount)
	}
	auxCount, err := GetCountFromTestDataTableWithAuxV2(db, true)
	if err != nil {
		t.Errorf("failed to get count from test table: %v", err)
	}
	if auxCount != 5 {
		t.Errorf("aux table expected 5 records, but got %d", auxCount)
	}

	log.Println("test finished, shutdown the processor...")
	processor.Shutdown()
}

func TestBufferInsertWithSomeErrors(t *testing.T) {
	// Initialize database connection
	dbConfig := InitDatabaseConfig("127.0.0.1", 7000, "postgres", "", "postgres")
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	} else {
		err := CreateTestDataTaleWithAuxV2(db)
		if err != nil {
			log.Fatalf("failed to create test table: %v", err)
			return
		}
		err = TruncateTestDataTableWithAuxV2(db)
		if err != nil {
			log.Fatalf("failed to truncate test table: %v", err)
			return
		}
	}
	defer db.Close()

	processor := NewProcessorV2(dbConfig, 6, 10)
	defer processor.Shutdown()

	filePath := "../examples/data/test_multiple_s3_file_error_v2.csv"

	batchSize := 10
	var tests []TestDataWithAuxV2

	csvFile, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("failed to open csv file: %v", err)
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)
	csvReader.FieldsPerRecord = -1
	csvReader.Comma = '\t'
	csvReader.ReuseRecord = true

	i := 0
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf("failed to read csv file: %v", err)
		}
		id, err := strconv.Atoi(record[0])
		if err != nil {
			t.Errorf("failed to parse id: %v", err)
		}
		fileID, err := strconv.Atoi(record[1])
		if err != nil {
			t.Errorf("failed to parse file_id: %v", err)
		}
		routingID := record[2]
		if err != nil {
			t.Errorf("failed to parse routing_id: %v", err)
		}
		if len(record) < 5 {
			t.Errorf("record does not contain enough fields: %v", record)
		} else {
			log.Printf("record %d: id=%d, file_id=%d, routing_id=%s, ext=%s, vector=%s", i, id, fileID, routingID, record[3], record[4])
		}
		ext := record[3]
		vector := record[4]
		tests = append(tests, TestDataWithAuxV2{
			ID:        id,
			FileID:    fileID,
			RoutingID: routingID,
			Ext:       ext,
			Vector:    vector,
		})
		i++

		// insert batch
		if i%batchSize == 0 {
			err := processor.InsertV2(fmt.Sprintf("%d", fileID), routingID, tests)
			if err != nil {
				t.Errorf("failed to insert data: %v", err)
			}
			tests = nil // clear the list, prepare for the next batch
		}
	}

	// refresh all data and wait for import to complete
	log.Println("refreshing data and waiting for import to complete...")
	err = processor.Flush()
	if err != nil {
		log.Fatalf("failed to refresh data: %v", err)
	}

	processor.Shutdown()
	// Check the count of records in the test table
	count, err := GetCountFromTestDataTableWithAuxV2(db, false)
	if err != nil {
		t.Errorf("failed to get count from test table: %v", err)
	} else {
		log.Printf("Counted %d records in test table.", count)
		if count != 10 {
			t.Errorf("expected %d records, but got %d", 10, count)
		}
	}
}

func TestBufferInsertWithSleep(t *testing.T) {
	// Initialize database connection
	dbConfig := InitDatabaseConfig("127.0.0.1", 7000, "postgres", "", "postgres")
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	} else {
		err := CreateTestDataTaleWithAuxV2(db)
		if err != nil {
			log.Fatalf("failed to create test table: %v", err)
			return
		}
		err = TruncateTestDataTableWithAuxV2(db)
		if err != nil {
			log.Fatalf("failed to truncate test table: %v", err)
			return
		}
	}
	defer db.Close()

	fileTimeout := 3 // set file write timeout to 3 seconds
	processor := NewProcessorV2(dbConfig, fileTimeout, 0)
	defer processor.Shutdown()

	filePath := "../examples/data/test_insert_v2_basic.csv"

	// we write 5 records per batch and we will sleep for 2 seconds after each batch insert,
	// even though we set BatchSize = 10 at NewProcessorV2, we will have 2 files after we write
	// 15 records because the file timeout, and then import thread will import these
	// 2 files to PostgreSQL in parallel.
	batchSize := 5
	var tests []TestDataWithAuxV2

	csvFile, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("failed to open csv file: %v", err)
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)
	csvReader.FieldsPerRecord = -1
	csvReader.Comma = '\t'
	csvReader.ReuseRecord = true

	i := 0
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf("failed to read csv file: %v", err)
		}
		id, err := strconv.Atoi(record[0])
		if err != nil {
			t.Errorf("failed to parse id: %v", err)
		}
		fileID, err := strconv.Atoi(record[1])
		if err != nil {
			t.Errorf("failed to parse file_id: %v", err)
		}
		routingID := record[2]
		if len(record) < 5 {
			t.Errorf("record does not contain enough fields: %v", record)
		} else {
			log.Printf("record %d: id=%d, file_id=%d, routing_id=%s, ext=%s, vector=%s", i, id, fileID, routingID, record[3], record[4])
		}
		ext := record[3]
		vector := record[4]
		tests = append(tests, TestDataWithAuxV2{
			ID:        id,
			FileID:    fileID,
			RoutingID: routingID,
			Ext:       ext,
			Vector:    vector,
		})
		i++

		// insert batch
		if i%batchSize == 0 {
			log.Printf("insert batch %d, contains %d records", i/batchSize, len(tests))
			err := processor.InsertV2(fmt.Sprintf("%d", fileID), routingID, tests)
			time.Sleep(time.Duration(fileTimeout+1) * time.Second) // sleep for 2 seconds before next insert
			if err != nil {
				t.Errorf("failed to insert data: %v", err)
			}
			tests = nil // clear the list, prepare for the next batch
			// Check the count of records in the test table
			count, err := GetCountFromTestDataTableWithAuxV2(db, false)
			if err != nil {
				t.Errorf("failed to get count from test table: %v", err)
			} else {
				log.Printf("Counted %d records in test table.", count)
				if count != i {
					t.Errorf("expected %d records, but got %d", i, count)
				}
			}

		}
	}

	if len(tests) > 0 {
		log.Printf("insert batch %d, contains %d records", i/batchSize, len(tests))
		// Get fileID and routingID from the first record in tests
		lastFileID := fmt.Sprintf("%d", tests[0].FileID)
		lastRoutingID := tests[0].RoutingID
		err := processor.InsertV2(lastFileID, lastRoutingID, tests)
		if err != nil {
			t.Errorf("failed to insert data: %v", err)
		}
	}
	// refresh all data and wait for import to complete
	log.Println("refreshing data and waiting for import to complete...")
	err = processor.Flush()

	time.Sleep(time.Duration(fileTimeout) * time.Second)

	if err != nil {
		log.Fatalf("failed to refresh data: %v", err)
	}

	// Check the count of records in the test table
	count, err := GetCountFromTestDataTableWithAuxV2(db, false)
	if err != nil {
		t.Errorf("failed to get count from test table: %v", err)
	} else {
		log.Printf("Counted %d records in test table.", count)
		if count != i {
			t.Errorf("expected %d records, but got %d", i, count)
		}
	}
}

func TestBufferInsertWithPgRecovery(t *testing.T) {
	// Initialize database connection
	dbConfig := InitDatabaseConfig("127.0.0.1", 7000, "postgres", "", "postgres")
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	} else {
		err := CreateTestDataTaleWithAuxV2(db)
		if err != nil {
			log.Fatalf("failed to create test table: %v", err)
			return
		}
		err = TruncateTestDataTableWithAuxV2(db)
		if err != nil {
			log.Fatalf("failed to truncate test table: %v", err)
			return
		}
	}
	defer db.Close()

	fileTimeout := 6 // set file write timeout to 6 seconds
	processor := NewProcessorV2(dbConfig, fileTimeout, 0)
	defer processor.Shutdown()

	filePath := "../examples/data/test_insert_v2_basic.csv"

	// we write 5 records per batch and we will sleep for 2 seconds after each batch insert,
	// even though we set BatchSize = 10 at NewProcessorV2, we will have 2 files after we write
	// 15 records because the file timeout, and then import thread will import these
	// 2 files to PostgreSQL in parallel.
	batchSize := 5
	s3BatchSize := 10
	var tests []TestDataWithAuxV2

	csvFile, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("failed to open csv file: %v", err)
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)
	csvReader.FieldsPerRecord = -1
	csvReader.Comma = '\t'
	csvReader.ReuseRecord = true

	i := 0
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf("failed to read csv file: %v", err)
		}
		id, err := strconv.Atoi(record[0])
		if err != nil {
			t.Errorf("failed to parse id: %v", err)
		}
		fileID, err := strconv.Atoi(record[1])
		if err != nil {
			t.Errorf("failed to parse file_id: %v", err)
		}
		routingID := record[2]
		if len(record) < 5 {
			t.Errorf("record does not contain enough fields: %v", record)
		} else {
			log.Printf("record %d: id=%d, file_id=%d, routing_id=%s, ext=%s, vector=%s", i, id, fileID, routingID, record[3], record[4])
		}
		ext := record[3]
		vector := record[4]
		tests = append(tests, TestDataWithAuxV2{
			ID:        id,
			FileID:    fileID,
			RoutingID: routingID,
			Ext:       ext,
			Vector:    vector,
		})
		i++

		// insert batch
		if i%batchSize == 0 {
			log.Printf("insert batch %d, contains %d records", i/batchSize, len(tests))
			err := processor.InsertV2(fmt.Sprintf("%d", fileID), routingID, tests)
			if err != nil {
				t.Errorf("failed to insert data: %v", err)
			}
			if i == s3BatchSize {
				// shut down the pg-server
				log.Printf("shutting down the pg-server...")
				cmd := exec.Command("/bin/sh", "-c", "source /workspace/phoenix/neon/pg_install/v12/greenplum_path.sh; source /workspace/phoenix/gpAux/gpdemo/gpdemo-env.sh;gpstop -ia;")
				err := cmd.Run()
				if err != nil {
					t.Errorf("failed to kill pg-server: %v", err)
				}
				log.Printf("finish shut down the pg-server...")
			}
			time.Sleep(time.Duration(fileTimeout/2) * time.Second) // sleep for 3 seconds before next insert
			if i == s3BatchSize {
				// start the pg-server
				log.Printf("starting the pg-server...")
				cmd := exec.Command("/bin/sh", "-c", "source /workspace/phoenix/neon/pg_install/v12/greenplum_path.sh; source /workspace/phoenix/gpAux/gpdemo/gpdemo-env.sh;gpstart -a;")
				err := cmd.Run()
				if err != nil {
					t.Errorf("failed to start pg-server: %v", err)
				}
				// 2 times of import error sleep time
				log.Printf("sleep for %d seconds before next insert", processor.config.ImportErrorSleepTime*2)
				time.Sleep(time.Duration(processor.config.ImportErrorSleepTime*2) * time.Second)
			}
			tests = nil // clear the list, prepare for the next batch
			if i == s3BatchSize {
				// Check the count of records in the test table
				count, err := GetCountFromTestDataTableWithAuxV2(db, false)
				if err != nil {
					t.Errorf("failed to get count from test table: %v", err)
				} else {
					log.Printf("Counted %d records in test table.", count)
					if count != s3BatchSize {
						t.Errorf("expected %d records, but got %d", s3BatchSize, count)
					}
				}
			}
		}
	}

	if len(tests) > 0 {
		log.Printf("insert batch %d, contains %d records", i/batchSize, len(tests))
		// Get fileID and routingID from the first record in tests
		lastFileID := fmt.Sprintf("%d", tests[0].FileID)
		lastRoutingID := tests[0].RoutingID
		err := processor.InsertV2(lastFileID, lastRoutingID, tests)
		if err != nil {
			t.Errorf("failed to insert data: %v", err)
		}
	}
	// refresh all data and wait for import to complete
	log.Println("refreshing data and waiting for import to complete...")
	err = processor.Flush()
	if err != nil {
		log.Fatalf("failed to refresh data: %v", err)
	}

	// Check the count of records in the test table
	count, err := GetCountFromTestDataTableWithAuxV2(db, false)
	if err != nil {
		t.Errorf("failed to get count from test table: %v", err)
	} else {
		log.Printf("Counted %d records in test table.", count)
		if count != i {
			t.Errorf("expected %d records, but got %d", i, count)
		}
	}
}

// TestBufferInsertWithMigration:
// 1. migrate data to test_routing_data_v2_relyt_massive_group from test_routing_data_v2
// 2. restart the pg-server, check the ListenThread will be reconnnected.
// 3. after migrate, new data will be inserted by routing table
// 4. check after insert, the data in relyt_loader_delta_checkpoint been gc
// 5. check in inserting, after fileTimeout, the data will be flushed by the AutoFlushThread
// 6. check delete data in the main table and aux table
func TestBufferInsertWithMigration(t *testing.T) {
	// Initialize database connection
	dbConfig := InitDatabaseConfig("127.0.0.1", 7000, "postgres", "", "postgres")
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	} else {
		err := CreateTestDataTaleWithAuxV2(db)
		if err != nil {
			log.Fatalf("failed to create test table: %v", err)
			return
		}
		err = TruncateTestDataTableWithAuxV2(db)
		if err != nil {
			log.Fatalf("failed to truncate test table: %v", err)
			return
		}

		err = InitTestDataTableWithAuxV2(db)
		if err != nil {
			log.Fatalf("failed to init test table: %v", err)
			return
		}

		err = ClearRelytCheckpointTable(db)
		if err != nil {
			log.Fatalf("failed to clear relyt_checkpoint table: %v", err)
			return
		}
	}
	defer db.Close()

	fileTimeout := 3 // set file write timeout to 3 seconds
	processor := NewProcessorV2(dbConfig, fileTimeout, 0)
	defer processor.Shutdown()

	filePath := "../examples/data/test_migration_v2.csv"
	migrateTool := "../migrate/migrate_data.py"

	batchSize := 5
	s3BatchSize := 10
	needMigrate := true
	needRestart := true
	var tests []TestDataWithAuxV2

	csvFile, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("failed to open csv file: %v", err)
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)
	csvReader.FieldsPerRecord = -1
	csvReader.Comma = '\t'
	csvReader.ReuseRecord = true

	i := 0
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf("failed to read csv file: %v", err)
		}
		id, err := strconv.Atoi(record[0])
		if err != nil {
			t.Errorf("failed to parse id: %v", err)
		}
		fileID, err := strconv.Atoi(record[1])
		if err != nil {
			t.Errorf("failed to parse file_id: %v", err)
		}
		routingID := record[2]
		if len(record) < 5 {
			t.Errorf("record does not contain enough fields: %v", record)
		} else {
			log.Printf("record %d: id=%d, file_id=%d, routing_id=%s, ext=%s, vector=%s", i, id, fileID, routingID, record[3], record[4])
		}
		ext := record[3]
		vector := record[4]
		tests = append(tests, TestDataWithAuxV2{
			ID:        id,
			FileID:    fileID,
			RoutingID: routingID,
			Ext:       ext,
			Vector:    vector,
		})
		i++

		// insert batch
		if i%batchSize == 0 {
			log.Printf("insert batch %d, contains %d records", i/batchSize, len(tests))
			err := processor.InsertV2(fmt.Sprintf("%d", fileID), routingID, tests)
			if err != nil {
				t.Errorf("failed to insert data: %v", err)
			}
			// 1. restart the pg-server, check the ListenThread will be reconnnected.
			if i == batchSize && needRestart {
				needRestart = false
				log.Printf("restart the pg-server")
				cmd := exec.Command("/bin/sh", "-c", "source /workspace/phoenix/neon/pg_install/v12/greenplum_path.sh; source /workspace/phoenix/gpAux/gpdemo/gpdemo-env.sh;gpstop -air;")
				err := cmd.Run()
				if err != nil {
					t.Errorf("failed to start pg-server: %v", err)
				}
				log.Printf("finish restart the pg-server...")
				time.Sleep(time.Duration(fileTimeout/2) * time.Second) // sleep for 2 seconds before migrate data
			}
			// 2. migrate data to test_routing_data_v2_relyt_massive_group
			if i == s3BatchSize && needMigrate {
				needMigrate = false
				log.Printf("migrate data to test_routing_data_v2_relyt_massive_group")
				cmd := exec.Command("python3", migrateTool, "--tables", "public.test_routing_data_v2", "--threshold", "100")
				err := cmd.Run()
				if err != nil {
					t.Errorf("failed to migrate data: %v", err)
				}
			}

			// ref test_migration_v2.csv, top 26 records are routing_id=100 and routing_id=110, here we wait for the data to be migrated.
			if i == 26 {
				time.Sleep(time.Duration(fileTimeout) * time.Second)
				auxCount, err := GetCountFromTestDataTableWithAuxV2(db, true)
				if err != nil {
					t.Errorf("failed to get count from test table: %v", err)
				}
				if auxCount != 220 {
					t.Errorf("expected %d records in aux table, but got %d", 220, auxCount)
				}
			}
			tests = nil // clear the list, prepare for the next batch
		}
	}

	if len(tests) > 0 {
		log.Printf("insert batch %d, contains %d records", i/batchSize, len(tests))
		// Get fileID and routingID from the first record in tests
		lastFileID := fmt.Sprintf("%d", tests[0].FileID)
		lastRoutingID := tests[0].RoutingID
		err := processor.InsertV2(lastFileID, lastRoutingID, tests)
		if err != nil {
			t.Errorf("failed to insert data: %v", err)
		}
	}

	processor.Flush()

	log.Println("waiting for import to complete...")
	time.Sleep(time.Duration(10) * time.Second)

	// delete sync file id = 100, routing id = 100
	err = processor.DeleteSyncV2("100", "100")
	if err != nil {
		t.Errorf("failed to delete data: %v", err)
	}

	// delete sync file id = 120, routing id = 120
	err = processor.DeleteSyncV2("120", "120")
	if err != nil {
		t.Errorf("failed to delete data: %v", err)
	}

	// Check the count of records in the test table
	mainCount, err := GetCountFromTestDataTableWithAuxV2(db, false)
	if err != nil {
		t.Errorf("failed to get count from test table: %v", err)
	}
	auxCount, err := GetCountFromTestDataTableWithAuxV2(db, true)
	if err != nil {
		t.Errorf("failed to get count from test table: %v", err)
	} else {
		log.Printf("Counted %d records in main table, %d records in aux table.", mainCount, auxCount)
		if mainCount+auxCount != 142 {
			t.Errorf("expected %d records, but got %d", 142, mainCount+auxCount)
		}
		if auxCount != 115 {
			t.Errorf("expected %d records in aux table, but got %d", 115, auxCount)
		}
		if mainCount != 27 {
			t.Errorf("expected %d records in main table, but got %d", 27, mainCount)
		}
	}

	checkpointCount, err := GetRelytCheckpointTable(db)
	if err != nil {
		t.Errorf("failed to get count from relyt_checkpoint table: %v", err)
	}
	if checkpointCount != 0 {
		t.Errorf("expected %d records in relyt_checkpoint table, but got %d", 0, checkpointCount)
	}

	log.Println("test finished, shutdown the processor...")
	processor.Shutdown()
}

// TestBufferInsertWithMixedOperations:
// test the mixed operations of insert and delete
func TestBufferInsertWithMixedOperations(t *testing.T) {
	// Initialize database connection
	dbConfig := InitDatabaseConfig("127.0.0.1", 7000, "postgres", "", "postgres")
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	} else {
		err := CreateTestDataTaleWithAuxV2(db)
		if err != nil {
			log.Fatalf("failed to create test table: %v", err)
			return
		}
		err = TruncateTestDataTableWithAuxV2(db)
		if err != nil {
			log.Fatalf("failed to truncate test table: %v", err)
			return
		}
		err = InitTestRoutingTable(db)
		if err != nil {
			log.Fatalf("failed to init test routing table: %v", err)
			return
		}

		err = ClearRelytCheckpointTable(db)
		if err != nil {
			log.Fatalf("failed to clear relyt_checkpoint table: %v", err)
			return
		}
	}
	defer db.Close()

	fileTimeout := 3 // set file write timeout to 3 seconds
	processor := NewProcessorV2(dbConfig, fileTimeout, 0)
	defer processor.Shutdown()

	filePath := "../examples/data/test_insert_v2_basic.csv"

	batchSize := 5
	var tests []TestDataWithAuxV2

	csvFile, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("failed to open csv file: %v", err)
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)
	csvReader.FieldsPerRecord = -1
	csvReader.Comma = '\t'
	csvReader.ReuseRecord = true

	i := 0
	insertCount := 0
	deleteCount := 0
	lastFileID := ""
	lastRoutingID := ""

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf("failed to read csv file: %v", err)
		}
		id, err := strconv.Atoi(record[0])
		if err != nil {
			t.Errorf("failed to parse id: %v", err)
		}
		fileID, err := strconv.Atoi(record[1])
		if err != nil {
			t.Errorf("failed to parse file_id: %v", err)
		}
		routingID := record[2]
		if len(record) < 5 {
			t.Errorf("record does not contain enough fields: %v", record)
		} else {
			log.Printf("record %d: id=%d, file_id=%d, routing_id=%s, ext=%s, vector=%s", i, id, fileID, routingID, record[3], record[4])
		}
		ext := record[3]
		vector := record[4]
		tests = append(tests, TestDataWithAuxV2{
			ID:        id,
			FileID:    fileID,
			RoutingID: routingID,
			Ext:       ext,
			Vector:    vector,
		})
		i++

		// insert batch
		if i%batchSize == 0 {
			log.Printf("insert batch %d, contains %d records", i/batchSize, len(tests))
			err := processor.InsertV2(fmt.Sprintf("%d", fileID), routingID, tests)
			if err != nil {
				t.Errorf("failed to insert data: %v", err)
			}
			insertCount += len(tests)
			lastFileID = fmt.Sprintf("%d", fileID)
			lastRoutingID = routingID

			// Mixed operations: execute delete operations under specific conditions
			if i == 10 {
				// Delete data for the first batch
				log.Printf("Executing delete operation #1: fileID=%s, routingID=%s", lastFileID, lastRoutingID)
				err := processor.DeleteV2(lastFileID, lastRoutingID)
				if err != nil {
					t.Errorf("failed to delete data: %v", err)
				}
				deleteCount++
			} else if i == 20 {
				// Delete data for the second batch
				log.Printf("Executing delete operation #2: fileID=%s, routingID=%s", lastFileID, lastRoutingID)
				err := processor.DeleteV2(lastFileID, lastRoutingID)
				if err != nil {
					t.Errorf("failed to delete data: %v", err)
				}
				deleteCount++
			} else if i == 30 {
				// Delete data for the third batch
				log.Printf("Executing delete operation #3: fileID=%s, routingID=%s", lastFileID, lastRoutingID)
				err := processor.DeleteV2(lastFileID, lastRoutingID)
				if err != nil {
					t.Errorf("failed to delete data: %v", err)
				}
				deleteCount++
			}

			tests = nil // clear the list, prepare for the next batch
		}
	}

	if len(tests) > 0 {
		log.Printf("insert final batch, contains %d records", len(tests))
		// Get fileID and routingID from the first record in tests
		lastFileID = fmt.Sprintf("%d", tests[0].FileID)
		lastRoutingID = tests[0].RoutingID
		err := processor.InsertV2(lastFileID, lastRoutingID, tests)
		if err != nil {
			t.Errorf("failed to insert data: %v", err)
		}
		insertCount += len(tests)
	}

	processor.Flush()

	log.Println("waiting for import to complete...")
	time.Sleep(time.Duration(10) * time.Second)

	// Check the count of records in the test table
	mainCount, err := GetCountFromTestDataTableWithAuxV2(db, false)
	if err != nil {
		t.Errorf("failed to get count from test table: %v", err)
	}
	auxCount, err := GetCountFromTestDataTableWithAuxV2(db, true)
	if err != nil {
		t.Errorf("failed to get count from test table: %v", err)
	}

	totalCount := mainCount + auxCount
	log.Printf("Final results:")
	log.Printf("- Total inserted: %d records", insertCount)
	log.Printf("- Total deleted: %d operations", deleteCount)
	log.Printf("- Main table: %d records", mainCount)
	log.Printf("- Aux table: %d records", auxCount)
	log.Printf("- Total remaining: %d records", totalCount)

	if totalCount != 10 {
		t.Errorf("Expected total count to be 10, but got %d", totalCount)
	}

	// Verify results: due to delete operations, the final record count should be less than the insert count
	if totalCount >= insertCount {
		t.Errorf("Expected total count to be less than insert count due to delete operations, but got total=%d, insert=%d", totalCount, insertCount)
	}

	// Verify that delete operations were actually executed
	if deleteCount == 0 {
		t.Errorf("Expected delete operations to be executed, but got 0")
	}

	log.Println("Mixed operations test finished, shutdown the processor...")
	processor.Shutdown()
}

// TestBufferInsertWithOffset:
// test the offset tracking functionality
func TestBufferInsertWithOffset(t *testing.T) {
	// Initialize database connection
	dbConfig := InitDatabaseConfig("127.0.0.1", 7000, "postgres", "", "postgres")
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	} else {
		err := CreateTestDataTaleWithAuxV2(db)
		if err != nil {
			log.Fatalf("failed to create test table: %v", err)
			return
		}
		err = TruncateTestDataTableWithAuxV2(db)
		if err != nil {
			log.Fatalf("failed to truncate test table: %v", err)
			return
		}
		err = InitTestRoutingTable(db)
		if err != nil {
			log.Fatalf("failed to init test routing table: %v", err)
			return
		}

		err = ClearRelytCheckpointTable(db)
		if err != nil {
			log.Fatalf("failed to clear relyt_checkpoint table: %v", err)
			return
		}
	}
	defer db.Close()

	fileTimeout := 3 // set file write timeout to 3 seconds
	processor := NewProcessorV2(dbConfig, fileTimeout, 0)
	defer processor.Shutdown()

	// Test data structure
	type TestRecordWithOffset struct {
		ID        int    `relyt:"id"`
		FileID    int    `relyt:"fileid"`
		RoutingID string `relyt:"routing_id"`
		Name      string `relyt:"ext"`
		Data      string `relyt:"vector"`
	}

	// Test 1: Insert records with different offsets
	log.Println("Test 1: Inserting records with different offsets...")

	// Insert batch 1 with offset 100
	records1 := []TestRecordWithOffset{
		{ID: 1, FileID: 100, RoutingID: "100", Name: "record1", Data: "[0.1,0.2,0.3]"},
		{ID: 2, FileID: 100, RoutingID: "100", Name: "record2", Data: "[0.4,0.5,0.6]"},
	}
	err = processor.InsertV2("100", "100", records1, 100)
	if err != nil {
		t.Errorf("Failed to insert records with offset 100: %v", err)
	}

	// Check current max offset
	currentOffset := processor.GetMaxOffset()
	log.Printf("Current max offset after first insert: %d", currentOffset)
	if currentOffset != 0 {
		t.Errorf("Expected max offset to be 0 before flush, got %d", currentOffset)
	}

	// Insert batch 2 with offset 200
	records2 := []TestRecordWithOffset{
		{ID: 3, FileID: 101, RoutingID: "101", Name: "record3", Data: "[0.7,0.8,0.9]"},
		{ID: 4, FileID: 101, RoutingID: "101", Name: "record4", Data: "[1.0,1.1,1.2]"},
	}
	err = processor.InsertV2("101", "101", records2, 200)
	if err != nil {
		t.Errorf("Failed to insert records with offset 200: %v", err)
	}

	// Insert batch 3 with offset 150 (lower than previous)
	records3 := []TestRecordWithOffset{
		{ID: 5, FileID: 102, RoutingID: "102", Name: "record5", Data: "[1.3,1.4,1.5]"},
	}
	err = processor.InsertV2("102", "102", records3, 150)
	if err != nil {
		t.Errorf("Failed to insert records with offset 150: %v", err)
	}

	// Test 2: Delete operation with offset
	log.Println("Test 2: Testing delete operation with offset...")
	err = processor.DeleteV2("100", "100", 300)
	if err != nil {
		t.Errorf("Failed to delete record with offset 300: %v", err)
	}

	time.Sleep(time.Duration(fileTimeout+1) * time.Second)
	currentOffset = processor.GetMaxOffset()
	log.Printf("Current max offset after first insert: %d", currentOffset)
	if currentOffset != 300 {
		t.Errorf("Expected max offset to be 300 after delete, got %d", currentOffset)
	}

	// Test 3: Insert without offset (should default to 0)
	log.Println("Test 3: Testing insert without offset...")
	records4 := []TestRecordWithOffset{
		{ID: 6, FileID: 103, RoutingID: "103", Name: "record6", Data: "[1.6,1.7,1.8]"},
	}
	err = processor.InsertV2("103", "103", records4)
	if err != nil {
		t.Errorf("Failed to insert records without offset: %v", err)
	}

	// Flush all pending data
	log.Println("Flushing all pending data...")
	err = processor.Flush()
	if err != nil {
		t.Errorf("Failed to flush: %v", err)
	}

	// Check final max offset after flush
	finalOffset := processor.GetMaxOffset()
	log.Printf("Final max offset after flush: %d", finalOffset)

	// The final max offset should be 300 (the highest offset from all operations)
	if finalOffset != 300 {
		t.Errorf("Expected final max offset to be 300, got %d", finalOffset)
	}

	// Check the count of records in the test table
	mainCount, err := GetCountFromTestDataTableWithAuxV2(db, false)
	if err != nil {
		t.Errorf("Failed to get count from main table: %v", err)
	}
	auxCount, err := GetCountFromTestDataTableWithAuxV2(db, true)
	if err != nil {
		t.Errorf("Failed to get count from aux table: %v", err)
	}

	totalCount := mainCount + auxCount
	log.Printf("Final results:")
	log.Printf("- Main table: %d records", mainCount)
	log.Printf("- Aux table: %d records", auxCount)
	log.Printf("- Total remaining: %d records", totalCount)
	log.Printf("- Final max offset: %d", finalOffset)

	// Verify that records were imported (excluding the deleted one)
	expectedCount := 4
	if totalCount != expectedCount {
		t.Errorf("Expected total count to be %d, but got %d", expectedCount, totalCount)
	}

	log.Println("Offset tracking test finished successfully!")
}

func TestBufferDeleteSync(t *testing.T) {
	dbConfig := InitDatabaseConfig("127.0.0.1", 7000, "postgres", "", "postgres")
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	}
	defer db.Close()

	err = CreateTestDataTaleWithOutAux(db)
	if err != nil {
		log.Fatalf("failed to create test table: %v", err)
		return
	}

	err = TruncateTestDataTaleWithOutAux(db)
	if err != nil {
		log.Fatalf("failed to truncate test table: %v", err)
		return
	}

	err = ClearRelytCheckpointTable(db)
	if err != nil {
		log.Fatalf("failed to clear relyt_checkpoint table: %v", err)
		return
	}

	processor := NewProcessorV2(dbConfig, 3, 0, "test_routing_data_without_aux")
	defer processor.Shutdown()

	filePath := "../examples/data/test_insert_v2_basic.csv"

	batchSize := 5
	var tests []TestDataWithAuxV2

	csvFile, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("failed to open csv file: %v", err)
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)
	csvReader.FieldsPerRecord = -1
	csvReader.Comma = '\t'
	csvReader.ReuseRecord = true

	i := 0

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf("failed to read csv file: %v", err)
		}
		id, err := strconv.Atoi(record[0])
		if err != nil {
			t.Errorf("failed to parse id: %v", err)
		}
		fileID, err := strconv.Atoi(record[1])
		if err != nil {
			t.Errorf("failed to parse file_id: %v", err)
		}
		routingID := record[2]
		if len(record) < 5 {
			t.Errorf("record does not contain enough fields: %v", record)
		} else {
			log.Printf("record %d: id=%d, file_id=%d, routing_id=%s, ext=%s, vector=%s", i, id, fileID, routingID, record[3], record[4])
		}
		ext := record[3]
		vector := record[4]
		tests = append(tests, TestDataWithAuxV2{
			ID:        id,
			FileID:    fileID,
			RoutingID: routingID,
			Ext:       ext,
			Vector:    vector,
		})
		i++

		// insert batch
		if i%batchSize == 0 {
			log.Printf("insert batch %d, contains %d records", i/batchSize, len(tests))
			err := processor.InsertV2(fmt.Sprintf("%d", fileID), routingID, tests)
			if err != nil {
				t.Errorf("failed to insert data: %v", err)
			}

			tests = nil // clear the list, prepare for the next batch
		}
	}

	time.Sleep(time.Duration(2) * time.Second)
	processor.Flush()

	mainCount, err := GetCountFromTestDataTableWithoutAux(db)
	if err != nil {
		t.Errorf("failed to get count from test table: %v", err)
	}
	if mainCount != i {
		t.Errorf("expected main table count to be %d, got %d", i, mainCount)
	}

	log.Printf("main table count: %d", mainCount)
	// delete records async(fileID=100, routingID=100)
	err = processor.DeleteSyncV2(fmt.Sprintf("%d", 100), "100")
	if err != nil {
		t.Errorf("failed to delete data: %v", err)
	}

	// delete records async(fileID=100, routingID=100)
	err = processor.DeleteSyncV2(fmt.Sprintf("%d", 110), "110")
	if err != nil {
		t.Errorf("failed to delete data: %v", err)
	}

	// select count(*) from test_routing_data where fileid = 120 and routing_id = 120
	searchOptions := &SearchOptions{
		Columns:   []string{"count(*)"},
		Condition: "fileid = $1 and routing_id = $2",
	}

	results, err := processor.SearchV2(searchOptions, "120", "120")
	if err != nil {
		t.Errorf("failed to search data: %v", err)
	}

	log.Printf("results: %v", results)

	// Check the count of records in the test table
	mainCount, err = GetCountFromTestDataTableWithoutAux(db)
	if err != nil {
		t.Errorf("failed to get count from test table: %v", err)
	}

	log.Printf("main table count: %d", mainCount)

	if mainCount != 5 {
		t.Errorf("expected main table count to be 5, got %d", mainCount)
	}
}

func TestBufferInsertWithDuplicate(t *testing.T) {
	// Initialize database connection
	dbConfig := InitDatabaseConfig("127.0.0.1", 7000, "postgres", "", "postgres")
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	} else {
		err := CreateTestDataTaleWithAuxV2(db)
		if err != nil {
			log.Fatalf("failed to create test table: %v", err)
			return
		}
		err = TruncateTestDataTableWithAuxV2(db)
		if err != nil {
			log.Fatalf("failed to truncate test table: %v", err)
			return
		}
		err = InitTestRoutingTable(db)
		if err != nil {
			log.Fatalf("failed to init test routing table: %v", err)
			return
		}

		err = ClearRelytCheckpointTable(db)
		if err != nil {
			log.Fatalf("failed to clear relyt_checkpoint table: %v", err)
			return
		}
	}
	defer db.Close()

	fileTimeout := 3 // set file write timeout to 3 seconds
	processor := NewProcessorV2(dbConfig, fileTimeout, 0)
	defer processor.Shutdown()

	filePath := "../examples/data/test_duplicate_v2.csv"

	batchSize := 5
	var tests []TestDataWithAuxV2

	csvFile, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("failed to open csv file: %v", err)
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)
	csvReader.FieldsPerRecord = -1
	csvReader.Comma = '\t'
	csvReader.ReuseRecord = true

	i := 0

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf("failed to read csv file: %v", err)
		}
		id, err := strconv.Atoi(record[0])
		if err != nil {
			t.Errorf("failed to parse id: %v", err)
		}
		fileID, err := strconv.Atoi(record[1])
		if err != nil {
			t.Errorf("failed to parse file_id: %v", err)
		}
		routingID := record[2]
		if len(record) < 5 {
			t.Errorf("record does not contain enough fields: %v", record)
		} else {
			log.Printf("record %d: id=%d, file_id=%d, routing_id=%s, ext=%s, vector=%s", i, id, fileID, routingID, record[3], record[4])
		}
		ext := record[3]
		vector := record[4]
		tests = append(tests, TestDataWithAuxV2{
			ID:        id,
			FileID:    fileID,
			RoutingID: routingID,
			Ext:       ext,
			Vector:    vector,
		})
		i++

		// insert batch
		if i%batchSize == 0 {
			log.Printf("insert batch %d, contains %d records", i/batchSize, len(tests))
			err := processor.InsertV2(fmt.Sprintf("%d", fileID), routingID, tests)
			if err != nil {
				t.Errorf("failed to insert data: %v", err)
			}
			tests = nil // clear the list, prepare for the next batch
		}
	}

	for _, test := range tests {
		log.Printf("insert batch %d, contains %d records", i/batchSize, len(tests))
		fileID := test.FileID
		routingID := test.RoutingID
		err := processor.InsertV2(fmt.Sprintf("%d", fileID), routingID, []TestDataWithAuxV2{test})
		if err != nil {
			t.Errorf("failed to insert data: %v", err)
		}
	}

	// Flush all pending data
	log.Println("Flushing all pending data...")
	time.Sleep(time.Duration(5) * time.Second)
	err = processor.Flush()
	if err != nil {
		t.Errorf("Failed to flush: %v", err)
	}

	// Check the count of records in the test tables
	mainCount, err := GetCountFromTestDataTableWithAuxV2(db, false)
	if err != nil {
		t.Errorf("Failed to get count from main table: %v", err)
	}
	auxCount, err := GetCountFromTestDataTableWithAuxV2(db, true)
	if err != nil {
		t.Errorf("Failed to get count from aux table: %v", err)
	}

	// Check that we have some records remaining
	if mainCount+auxCount != 26 {
		t.Errorf("Expected 26 records, got %d", mainCount+auxCount)
	}

	// Check that the records are correct
	searchOptions := &SearchOptions{
		Columns:   []string{"fileid", "routing_id"},
		Condition: "id in (1001, 1025)",
		OrderBy:   "id ASC",
	}
	results, err := processor.SearchV2(searchOptions)
	if err != nil {
		t.Errorf("failed to search data: %v", err)
	}

	row0Str := fmt.Sprintf("%v", results.Rows[0])
	row1Str := fmt.Sprintf("%v", results.Rows[1])
	expectedRow0Str := "[120 120]"
	expectedRow1Str := "[170 170]"

	if row0Str != expectedRow0Str {
		t.Errorf("expected results: %v, got: %v", expectedRow0Str, row0Str)
	}

	if row1Str != expectedRow1Str {
		t.Errorf("expected results: %v, got: %v", expectedRow1Str, row1Str)
	}

	log.Printf("results: %v", results)
}

func TestBufferInsertWithCopyOnConflict(t *testing.T) {
	// Initialize database connection
	dbConfig := InitDatabaseConfig("127.0.0.1", 7000, "postgres", "", "postgres")
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	} else {
		err := CreateTestDataTaleWithCopyOnConflict(db)
		if err != nil {
			log.Fatalf("failed to create test table: %v", err)
			return
		}
		err = TruncateTestDataTableWithCopyOnConflict(db)
		if err != nil {
			log.Fatalf("failed to truncate test table: %v", err)
			return
		}

		err = ClearRelytCheckpointTable(db)
		if err != nil {
			log.Fatalf("failed to clear relyt_checkpoint table: %v", err)
			return
		}
	}
	defer db.Close()

	fileTimeout := 3 // set file write timeout to 3 seconds
	processor := NewProcessorV2(dbConfig, fileTimeout, 0, "test_routing_data_copy_on_conflict")
	defer processor.Shutdown()

	filePath := "../examples/data/test_copy_on_conflict.csv"

	batchSize := 1
	var tests []TestDataWithCopyOnConflict

	csvFile, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("failed to open csv file: %v", err)
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)
	csvReader.FieldsPerRecord = -1
	csvReader.Comma = '\t'
	csvReader.ReuseRecord = true

	i := 0
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Errorf("failed to read csv file: %v", err)
		}

		if len(record) < 6 {
			t.Errorf("record does not contain enough fields: %v", record)
			continue
		}

		id, err := strconv.Atoi(record[0])
		if err != nil {
			t.Errorf("failed to parse id: %v", err)
		}
		fileID, err := strconv.Atoi(record[1])
		if err != nil {
			t.Errorf("failed to parse file_id: %v", err)
		}
		routingID := record[2]
		version, err := strconv.Atoi(record[3])
		if err != nil {
			t.Errorf("failed to parse version: %v", err)
		}
		ext := record[4]
		vector := record[5]

		tests = append(tests, TestDataWithCopyOnConflict{
			ID:        id,
			FileID:    fileID,
			RoutingID: routingID,
			Version:   version,
			Ext:       ext,
			Vector:    vector,
		})
		i++

		// insert batch
		if i%batchSize == 0 {
			err := processor.InsertV2(fmt.Sprintf("%d", fileID), routingID, tests)
			if err != nil {
				t.Errorf("failed to insert data: %v", err)
			}
			tests = nil // clear the list, prepare for the next batch
		}
	}

	for _, test := range tests {
		fileID := test.FileID
		routingID := test.RoutingID
		err := processor.InsertV2(fmt.Sprintf("%d", fileID), routingID, []TestDataWithCopyOnConflict{test})
		if err != nil {
			t.Errorf("failed to insert data: %v", err)
		}
	}

	// Flush all pending data
	log.Println("Flushing all pending data...")
	time.Sleep(time.Duration(5) * time.Second)
	err = processor.Flush()
	if err != nil {
		t.Errorf("Failed to flush: %v", err)
	}

	// Check the count of records in the test tables
	count, err := GetCountFromTestDataTableWithCopyOnConflict(db, false)
	if err != nil {
		t.Errorf("Failed to get count from main table: %v", err)
	}
	if count != 11 {
		t.Errorf("Expected 11 records, got %d", count)
	}

	processor.DeleteSyncV2(fmt.Sprintf("%d", 110), "110")

	// Check the count of records in the test tables
	count, err = GetCountFromTestDataTableWithCopyOnConflict(db, false)
	if err != nil {
		t.Errorf("Failed to get count from main table: %v", err)
	}
	if count != 9 {
		t.Errorf("Expected 9 records, got %d", count)
	}

	// Check that the records are correct
	searchOptions := &SearchOptions{
		Columns:   []string{"fileid", "routing_id", "ext"},
		Condition: "id = 1009",
	}
	results, err := processor.SearchV2(searchOptions)
	if err != nil {
		t.Errorf("failed to search data: %v", err)
	}

	row0Str := fmt.Sprintf("%v", results.Rows[0])
	expectedRow0Str := "[120 120 ext_1005_120_120_v2_duplicate5]"

	if row0Str != expectedRow0Str {
		t.Errorf("expected results: %v, got: %v", expectedRow0Str, row0Str)
	}

	log.Printf("results: %v", results)
}
