package bulkprocessor

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"database/sql"

	_ "github.com/lib/pq" // PostgreSQL driver
)

type TestData struct {
	ID     int    `relyt:"id"`
	Ext    string `relyt:"ext"`
	Vector string `relyt:"vector"`
}

type TestDataWithAux struct {
	ID        int    `relyt:"id"`
	RoutingID int    `relyt:"routing_id"`
	Ext       string `relyt:"ext"`
	Vector    string `relyt:"vector"`
}

// 定义一个结构体，包含数据库连接信息
type DatabaseConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	Database string
}

// define a struct to hold error handler resources
type ErrorHandlerResources struct {
	// add any resources needed for error handling, e.g., database connection, logger, etc.
	LogFile *os.File
}

func WriteErrorsToFiles(fieldname string, values []string, err error, resources any) {
	res := resources.(*ErrorHandlerResources)
	feedbackKeysString := fmt.Sprintf("failed %s is [%s] with error: %v.", fieldname, strings.Join(values, ","), err)
	res.LogFile.WriteString("Error: " + feedbackKeysString + "\n")
	log.Printf("Error: %s", feedbackKeysString)
}

func NewProcessor(dbconfig DatabaseConfig, fileTimeout int, extinfo string) *BulkProcessor {
	// open a error.log
	logFile, err := os.OpenFile("/tmp/error.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}

	// 创建用户定义的资源结构体
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

	if extinfo == "null" {
		log.Printf("table name is set to null")
	} else if extinfo == "auxtest" {
		config.PostgreSQL.Table = "test_routing_data"
	} else if extinfo == "content_personal_vector_semantic_insight_vector_bge_m3_dense" {
		config.PostgreSQL.Table = "content_personal_vector_semantic_insight_vector_bge_m3_dense"
	} else if extinfo == "importtimeout" {
		config.ImportTimeout = 5
		config.ImportErrorSleepTime = 5
	} else {
		config.PostgreSQL.Table = "test_data"
	}

	// create processor
	processor, err := New(config)
	if err != nil {
		log.Fatalf("failed to create processor: %v", err)
	}
	return processor
}

func InitDatabaseConfig(host string, port int, username, password, database string) DatabaseConfig {
	return DatabaseConfig{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
		Database: database,
	}
}

func integrationDatabaseConfig(t *testing.T) DatabaseConfig {
	t.Helper()
	port, err := strconv.Atoi(os.Getenv("RELYT_LEGACY_TEST_PORT"))
	if err != nil || port <= 0 {
		t.Skip("legacy database integration environment is not configured")
	}
	config := InitDatabaseConfig(
		os.Getenv("RELYT_LEGACY_TEST_HOST"), port,
		os.Getenv("RELYT_LEGACY_TEST_USER"),
		os.Getenv("RELYT_LEGACY_TEST_PASSWORD"),
		os.Getenv("RELYT_LEGACY_TEST_DATABASE"),
	)
	if config.Host == "" || config.Username == "" || config.Database == "" {
		t.Skip("legacy database integration environment is not configured")
	}
	return config
}

func SetupDataBase(config DatabaseConfig) (*sql.DB, error) {
	// Construct the connection string
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		config.Host, config.Port, config.Username, config.Password, config.Database)

	// Open a database connection
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Check if the connection is alive
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	log.Println("Database connection established successfully.")
	return db, nil
}

func CreateTestDataTale(db *sql.DB) error {
	// This function is a placeholder for creating the test table in PostgreSQL.
	// You can implement the logic to create the necessary table structure here.
	// For example, you might use a SQL command like:
	// CREATE TABLE test_data (id SERIAL PRIMARY KEY, ext TEXT, vector TEXT);
	log.Println("Creating test table in PostgreSQL...")
	query := `
	CREATE TABLE IF NOT EXISTS test_data (
		id bigint NOT NULL PRIMARY KEY,
		ext text,
		vector vecf16(3) NOT NULL
	);`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create test table: %w", err)
	}
	log.Println("Test table created successfully.")
	return nil
}

func TruncateTestDataTable(db *sql.DB) error {
	// This function is a placeholder for truncating the test table in PostgreSQL.
	// You can implement the logic to truncate the table here.
	log.Println("Truncating test table in PostgreSQL...")
	query := `TRUNCATE TABLE test_data;`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to truncate test table: %w", err)
	}
	log.Println("Test table truncated successfully.")
	return nil
}

func GetCountFromTestDataTable(db *sql.DB) (int, error) {
	// This function retrieves the count of records in the test table.
	log.Println("Counting records in test table...")
	query := `SELECT COUNT(*) FROM test_data;`
	var count int
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count records: %w", err)
	}
	log.Printf("Counted %d records in test table.", count)
	return count, nil
}

func CreateTestDataTaleWithAux(db *sql.DB) error {
	// This function is a placeholder for creating the test table in PostgreSQL.
	// You can implement the logic to create the necessary table structure here.
	// For example, you might use a SQL command like:
	// CREATE TABLE test_data (id SERIAL PRIMARY KEY, ext TEXT, vector TEXT);
	log.Println("Creating test table with auxin PostgreSQL...")
	query := `
	CREATE TABLE IF NOT EXISTS test_routing_data (
		id bigint NOT NULL PRIMARY KEY,
		routing_id bigint NOT NULL,
		ext text,
		vector vecf16(3) NOT NULL
	);
	CREATE TABLE IF NOT EXISTS test_routing_data_relyt_massive_group (
		id bigint NOT NULL PRIMARY KEY,
		routing_id bigint NOT NULL,
		ext text,
		vector vecf16(3) NOT NULL
	);
	CREATE TABLE IF NOT EXISTS relyt_sys.test_routing_data_relyt_routing (
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

func TruncateTestDataTableWithAux(db *sql.DB) error {
	// This function is a placeholder for truncating the test table in PostgreSQL.
	// You can implement the logic to truncate the table here.
	log.Println("Truncating test tables in PostgreSQL...")
	query := `
	TRUNCATE TABLE test_routing_data;
	TRUNCATE TABLE test_routing_data_relyt_massive_group;
	TRUNCATE TABLE relyt_sys.test_routing_data_relyt_routing;
	`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to truncate test tables: %w", err)
	}
	log.Println("Test tables truncated successfully.")
	return nil
}

// InitTestDataTableWithAux:
// routing_id 100 and 110 each insert 100 records
// routing_id 120, 130, 140 each insert 10 records
func InitTestDataTableWithAux(db *sql.DB) error {
	log.Println("Initializing test data in PostgreSQL...")

	// insert data to test_routing_data
	query := `
	INSERT INTO test_routing_data (id, routing_id, ext, vector)
	VALUES ($1, $2, $3, $4);
	`

	// create random number generator for vector values
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// insert 100 records for routing_id 100 and 110
	id := 0
	for _, routingID := range []int{100, 110} {
		for range 100 {
			ext := fmt.Sprintf("ext_%d", id)
			vector := fmt.Sprintf("[%f,%f,%f]", r.Float32(), r.Float32(), r.Float32())

			_, err := db.Exec(query, id, routingID, ext, vector)
			if err != nil {
				return fmt.Errorf("failed to insert data for routing_id %d: %w", routingID, err)
			}
			id++
		}
	}

	// insert 10 records for routing_id 120, 130, 140
	for _, routingID := range []int{120, 130, 140} {
		for range 10 {
			ext := fmt.Sprintf("ext_%d", id)
			vector := fmt.Sprintf("[%f,%f,%f]", r.Float32(), r.Float32(), r.Float32())

			_, err := db.Exec(query, id, routingID, ext, vector)
			if err != nil {
				return fmt.Errorf("failed to insert data for routing_id %d: %w", routingID, err)
			}
			id++
		}
	}

	log.Println("Test data initialized successfully.")
	return nil
}

func GetCountFromTestDataTableWithAux(db *sql.DB, auxtable bool) (int, error) {
	// This function retrieves the count of records in all test tables.
	log.Println("Counting records in test tables...")
	table := "test_routing_data"
	if auxtable {
		table = "test_routing_data_relyt_massive_group"
	}
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)

	var count int
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count records: %w", err)
	}
	log.Printf("Counted %d records in test tables.", count)
	return count, nil
}

func ClearRelytCheckpointTable(db *sql.DB) error {
	query := `
	TRUNCATE TABLE relyt_sys.relyt_loader_delta_checkpoint;
	`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to truncate relyt_checkpoint table: %w", err)
	}
	return nil
}

func GetRelytCheckpointTable(db *sql.DB) (int, error) {
	query := `
	SELECT COUNT(*) FROM relyt_sys.relyt_loader_delta_checkpoint;
	`
	var count int
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get count from relyt_checkpoint table: %w", err)
	}
	return count, nil
}

// TestInsertWithSomeErrors test the case we have some error data from csv to insert,
// and we will write the error data to a file via the ImportErrorCallback function.
func TestInsertWithSomeErrors(t *testing.T) {
	// Initialize database connection
	dbConfig := integrationDatabaseConfig(t)
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	} else {
		err := CreateTestDataTale(db)
		if err != nil {
			log.Fatalf("failed to create test table: %v", err)
			return
		}
		err = TruncateTestDataTable(db)
		if err != nil {
			log.Fatalf("failed to truncate test table: %v", err)
			return
		}
	}
	defer db.Close()

	processor := NewProcessor(dbConfig, 6, "")
	defer processor.Shutdown()

	filePath := "../examples/data/test_multiple_s3_file_error.csv"

	batchSize := 10
	var tests []TestData

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
		if len(record) < 3 {
			t.Errorf("record does not contain enough fields: %v", record)
		} else {
			log.Printf("record %d: id=%d, ext=%s, vector=%s", i, id, record[1], record[2])
		}
		ext := record[1]
		vector := record[2]
		tests = append(tests, TestData{
			ID:     id,
			Ext:    ext,
			Vector: vector,
		})
		i++

		// insert batch
		if i%batchSize == 0 {
			err := processor.Insert(tests)
			if err != nil {
				t.Errorf("failed to insert data: %v", err)
			}
			tests = nil // clear the list, prepare for the next batch
		}
	}
	if len(tests) > 0 {
		err := processor.Insert(tests)
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
	count, err := GetCountFromTestDataTable(db)
	if err != nil {
		t.Errorf("failed to get count from test table: %v", err)
	} else {
		log.Printf("Counted %d records in test table.", count)
		if count != 21 {
			t.Errorf("expected %d records, but got %d", i, count)
		}
	}
}

// TestInsertWithSleep test the case we write data intermittently.
func TestInsertWithSleep(t *testing.T) {
	// Initialize database connection
	dbConfig := integrationDatabaseConfig(t)
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	} else {
		err := CreateTestDataTale(db)
		if err != nil {
			log.Fatalf("failed to create test table: %v", err)
			return
		}
		err = TruncateTestDataTable(db)
		if err != nil {
			log.Fatalf("failed to truncate test table: %v", err)
			return
		}
	}
	defer db.Close()

	fileTimeout := 3 // set file write timeout to 2 seconds
	processor := NewProcessor(dbConfig, fileTimeout, "")
	defer processor.Shutdown()

	filePath := "../examples/data/test_sleep.csv"

	// we write 5 records per batch and we will sleep for 2 seconds after each batch insert,
	// even though we set BatchSize = 10 at NewProcessor, we will have 2 files after we write
	// 15 records because the file timeout, and then import thread will import these
	// 2 files to PostgreSQL in parallel.
	batchSize := 5
	s3BatchSize := 10
	var tests []TestData

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
		if len(record) < 3 {
			t.Errorf("record does not contain enough fields: %v", record)
		} else {
			log.Printf("record %d: id=%d, ext=%s, vector=%s", i, id, record[1], record[2])
		}
		ext := record[1]
		vector := record[2]
		tests = append(tests, TestData{
			ID:     id,
			Ext:    ext,
			Vector: vector,
		})
		i++

		// insert batch
		if i%batchSize == 0 {
			log.Printf("insert batch %d, contains %d records", i/batchSize, len(tests))
			err := processor.Insert(tests)
			time.Sleep(time.Duration(fileTimeout-1) * time.Second) // sleep for 2 seconds before next insert
			if err != nil {
				t.Errorf("failed to insert data: %v", err)
			}
			tests = nil // clear the list, prepare for the next batch
			if i > s3BatchSize {
				// Check the count of records in the test table
				count, err := GetCountFromTestDataTable(db)
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
		err := processor.Insert(tests)
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
	count, err := GetCountFromTestDataTable(db)
	if err != nil {
		t.Errorf("failed to get count from test table: %v", err)
	} else {
		log.Printf("Counted %d records in test table.", count)
		if count != i {
			t.Errorf("expected %d records, but got %d", i, count)
		}
	}
}

// TestInsertWithPgRecovery test the case when pg is down.
func TestInsertWithPgRecovery(t *testing.T) {
	// Initialize database connection
	dbConfig := integrationDatabaseConfig(t)
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	} else {
		err := CreateTestDataTale(db)
		if err != nil {
			log.Fatalf("failed to create test table: %v", err)
			return
		}
		err = TruncateTestDataTable(db)
		if err != nil {
			log.Fatalf("failed to truncate test table: %v", err)
			return
		}
	}
	defer db.Close()

	fileTimeout := 6 // set file write timeout to 2 seconds
	processor := NewProcessor(dbConfig, fileTimeout, "")
	defer processor.Shutdown()

	filePath := "../examples/data/test_sleep.csv"

	// we write 5 records per batch and we will sleep for 2 seconds after each batch insert,
	// even though we set BatchSize = 10 at NewProcessor, we will have 2 files after we write
	// 15 records because the file timeout, and then import thread will import these
	// 2 files to PostgreSQL in parallel.
	batchSize := 5
	s3BatchSize := 10
	var tests []TestData

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
		if len(record) < 3 {
			t.Errorf("record does not contain enough fields: %v", record)
		} else {
			log.Printf("record %d: id=%d, ext=%s, vector=%s", i, id, record[1], record[2])
		}
		ext := record[1]
		vector := record[2]
		tests = append(tests, TestData{
			ID:     id,
			Ext:    ext,
			Vector: vector,
		})
		i++

		// insert batch
		if i%batchSize == 0 {
			log.Printf("insert batch %d, contains %d records", i/batchSize, len(tests))
			err := processor.Insert(tests)
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
			time.Sleep(time.Duration(fileTimeout/2) * time.Second) // sleep for 2 seconds before next insert
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
				count, err := GetCountFromTestDataTable(db)
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
		err := processor.Insert(tests)
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
	count, err := GetCountFromTestDataTable(db)
	if err != nil {
		t.Errorf("failed to get count from test table: %v", err)
	} else {
		log.Printf("Counted %d records in test table.", count)
		if count != i {
			t.Errorf("expected %d records, but got %d", i, count)
		}
	}
}

func TestInsertWithImportTimeout(t *testing.T) {
	// Initialize database connection
	dbConfig := integrationDatabaseConfig(t)
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	} else {
		err := CreateTestDataTale(db)
		if err != nil {
			log.Fatalf("failed to create test table: %v", err)
			return
		}
		err = TruncateTestDataTable(db)
		if err != nil {
			log.Fatalf("failed to truncate test table: %v", err)
			return
		}
	}
	defer db.Close()

	processor := NewProcessor(dbConfig, 6, "importtimeout")
	defer processor.Shutdown()

	filePath := "../examples/data/test_multiple_s3_file_error.csv"

	batchSize := 10
	var tests []TestData

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
		if len(record) < 3 {
			t.Errorf("record does not contain enough fields: %v", record)
		} else {
			log.Printf("record %d: id=%d, ext=%s, vector=%s", i, id, record[1], record[2])
		}
		ext := record[1]
		vector := record[2]
		tests = append(tests, TestData{
			ID:     id,
			Ext:    ext,
			Vector: vector,
		})
		i++

		// insert batch
		if i%batchSize == 0 {
			err := processor.Insert(tests)
			if err != nil {
				t.Errorf("failed to insert data: %v", err)
			}
			tests = nil // clear the list, prepare for the next batch
		}

		if i == batchSize {
			log.Printf("restart the pg-server")
			cmd := exec.Command("/bin/sh", "-c", "source /workspace/phoenix/neon/pg_install/v12/greenplum_path.sh; source /workspace/phoenix/gpAux/gpdemo/gpdemo-env.sh;gpstop -air;")
			err := cmd.Run()
			if err != nil {
				t.Errorf("failed to start pg-server: %v", err)
			}
			log.Printf("finish restart the pg-server...")
			time.Sleep(time.Duration(2) * time.Second) // sleep for 2 seconds before migrate data

			count, err := GetRelytCheckpointTable(db)
			if err != nil {
				t.Errorf("failed to get count from relyt_checkpoint table: %v", err)
			}
			if count == 0 {
				t.Errorf("expected records in relyt_checkpoint table, but got 0")
			}
		}
	}
	if len(tests) > 0 {
		err := processor.Insert(tests)
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
	count, err := GetCountFromTestDataTable(db)
	if err != nil {
		t.Errorf("failed to get count from test table: %v", err)
	} else {
		log.Printf("Counted %d records in test table.", count)
		if count != 21 {
			t.Errorf("expected %d records, but got %d", i, count)
		}
	}
}

// TestInsertWithMigration:
// 1. migrate data to test_routing_data_massive from test_routing_data
// 2. restart the pg-server, check the ListenThread will be reconnnected.
// 3. after migrate, new data will be inserted by routing table
// 4. check after insert, the data in relyt_loader_delta_checkpoint been gc
// 5. check in inserting, after fileTimeout, the data will be flushed by the AutoFlushThread
func TestInsertWithMigration(t *testing.T) {
	// Initialize database connection
	dbConfig := integrationDatabaseConfig(t)
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	} else {
		err := CreateTestDataTaleWithAux(db)
		if err != nil {
			log.Fatalf("failed to create test table: %v", err)
			return
		}
		err = TruncateTestDataTableWithAux(db)
		if err != nil {
			log.Fatalf("failed to truncate test table: %v", err)
			return
		}

		err = InitTestDataTableWithAux(db)
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

	fileTimeout := 3 // set file write timeout to 2 seconds
	processor := NewProcessor(dbConfig, fileTimeout, "auxtest")
	defer processor.Shutdown()

	filePath := "../examples/data/test_migration.csv"
	migrateTool := "../migrate/migrate_data.py"

	batchSize := 5
	s3BatchSize := 10
	needMigrate := true
	needRestart := true
	var tests []TestDataWithAux

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
		routingID, err := strconv.Atoi(record[1])
		if err != nil {
			t.Errorf("failed to parse routing_id: %v", err)
		}
		if len(record) < 4 {
			t.Errorf("record does not contain enough fields: %v", record)
		} else {
			log.Printf("record %d: id=%d, routing_id=%d, ext=%s, vector=%s", i, id, routingID, record[2], record[3])
		}
		ext := record[2]
		vector := record[3]
		tests = append(tests, TestDataWithAux{
			ID:        id,
			RoutingID: routingID,
			Ext:       ext,
			Vector:    vector,
		})
		i++

		// insert batch
		if i%batchSize == 0 {
			log.Printf("insert batch %d, contains %d records", i/batchSize, len(tests))
			err := processor.Insert(tests)
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
			// 2. migrate data to test_routing_data_massive
			if i == s3BatchSize && needMigrate {
				needMigrate = false
				log.Printf("migrate data to test_routing_data_massive")
				cmd := exec.Command("python3", migrateTool, "--tables", "public.test_routing_data", "--threshold", "100")
				err := cmd.Run()
				if err != nil {
					t.Errorf("failed to migrate data: %v", err)
				}
			}

			// ref test_migration.csv, top 26 records are routing_id=100 and routing_id=110, here we wait for the data to be migrated.
			if i == 26 {
				time.Sleep(time.Duration(fileTimeout) * time.Second)
				auxCount, err := GetCountFromTestDataTableWithAux(db, true)
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
		err := processor.Insert(tests)
		if err != nil {
			t.Errorf("failed to insert data: %v", err)
		}
	}

	processor.Flush()

	log.Println("waiting for import to complete...")
	time.Sleep(time.Duration(10) * time.Second)

	// Check the count of records in the test table
	mainCount, err := GetCountFromTestDataTableWithAux(db, false)
	if err != nil {
		t.Errorf("failed to get count from test table: %v", err)
	}
	auxCount, err := GetCountFromTestDataTableWithAux(db, true)
	if err != nil {
		t.Errorf("failed to get count from test table: %v", err)
	} else {
		log.Printf("Counted %d records in main table, %d records in aux table.", mainCount, auxCount)
		if mainCount+auxCount != 45+230 {
			t.Errorf("expected %d records, but got %d", 45+230, mainCount+auxCount)
		}
		if auxCount != 230 {
			t.Errorf("expected %d records in aux table, but got %d", 230, auxCount)
		}
		if mainCount != 45 {
			t.Errorf("expected %d records in main table, but got %d", 45, mainCount)
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
