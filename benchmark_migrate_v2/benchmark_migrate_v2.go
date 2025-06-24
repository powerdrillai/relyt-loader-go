package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"database/sql"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/powerdrillai/relyt-loader-go/bulkprocessor"
)

type TestDataWithAuxV2 struct {
	ID        int    `relyt:"id"`
	FileID    int    `relyt:"fileid"`
	RoutingID string `relyt:"routing_id"`
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

func WriteErrorsToFiles(fieldname string, values []string, err error, resources interface{}) {
	res := resources.(*ErrorHandlerResources)
	feedbackKeysString := fmt.Sprintf("failed %s is [%s] with error: %v.", fieldname, strings.Join(values, ","), err)
	res.LogFile.WriteString("Error: " + feedbackKeysString + "\n")
	log.Printf("Error: %s", feedbackKeysString)
}

func NewProcessorV2(dbconfig DatabaseConfig, fileTimeout int, bufferSize int) *bulkprocessor.BulkProcessor {
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
	config := bulkprocessor.Config{
		// PostgreSQL config (required)
		PostgreSQL: bulkprocessor.PostgreSQLConfig{
			Host:     dbconfig.Host,
			Port:     dbconfig.Port,
			Username: dbconfig.Username,
			Password: dbconfig.Password,
			Database: dbconfig.Database,
			Table:    "relyt_migrate_insert_benchmark_v2",
			Schema:   "public",
		},
		BatchSize:           100000, // number of records per file
		BatchImportSize:     3,
		FeedbackColumn:      "id", // column name for error messages
		ImportErrorCallback: WriteErrorsToFiles,
		CallbackResource:    resources,
		FileWriteTimeout:    fileTimeout, // set file write timeout
		EnableDualBuffer:    true,
	}

	// create processor
	processor, err := bulkprocessor.New(config)
	if err != nil {
		log.Fatalf("failed to create processor: %v", err)
	}

	log.Printf("processor created, buffer max records: %d", config.BufferMaxRecords)

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

func CreateTestDataTaleV2(db *sql.DB) error {
	log.Println("Creating relyt_migrate_insert_benchmark_v2 in PostgreSQL...")
	query := `
	CREATE TABLE IF NOT EXISTS relyt_migrate_insert_benchmark_v2 (
		id bigint NOT NULL PRIMARY KEY,
		fileid bigint NOT NULL,
		routing_id text NOT NULL,
		ext text,
		vector vecf16(3) NOT NULL
	) using heap;

	CREATE TABLE IF NOT EXISTS relyt_migrate_insert_benchmark_v2_relyt_massive_group (
		id bigint NOT NULL PRIMARY KEY,
		fileid bigint NOT NULL,
		routing_id text NOT NULL,
		ext text,
		vector vecf16(3) NOT NULL
	) using heap;

	CREATE TABLE IF NOT EXISTS relyt_sys.relyt_migrate_insert_benchmark_v2_relyt_routing (
		routing_id text PRIMARY KEY,
		store_table_name TEXT NOT NULL
	) USING heap DISTRIBUTED NONE;`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create relyt_migrate_insert_benchmark_v2: %w", err)
	}
	log.Println("relyt_migrate_insert_benchmark_v2 created successfully.")
	return nil
}

func TruncateTestDataTableV2(db *sql.DB) error {
	log.Println("Truncating tables in PostgreSQL...")
	query := `
	TRUNCATE TABLE relyt_migrate_insert_benchmark_v2;
	TRUNCATE TABLE relyt_migrate_insert_benchmark_v2_relyt_massive_group;
	TRUNCATE TABLE relyt_sys.relyt_migrate_insert_benchmark_v2_relyt_routing;`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to truncate tables: %w", err)
	}
	log.Println("Tables truncated successfully.")
	return nil
}

func InitRoutingTableV2(db *sql.DB) error {
	log.Println("Initializing routing table...")
	query := `
	INSERT INTO relyt_sys.relyt_migrate_insert_benchmark_v2_relyt_routing (routing_id, store_table_name) 
	VALUES 
		('100', 'relyt_migrate_insert_benchmark_v2_relyt_massive_group'),
		('200', 'relyt_migrate_insert_benchmark_v2_relyt_massive_group'),
		('300', 'relyt_migrate_insert_benchmark_v2_relyt_massive_group');`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to initialize routing table: %w", err)
	}
	log.Println("Routing table initialized successfully.")
	return nil
}

func GetCountFromMainTestDataTableV2(db *sql.DB) (int, error) {
	// This function retrieves the count of records in the relyt_migrate_insert_benchmark_v2.
	log.Println("Counting records in relyt_migrate_insert_benchmark_v2...")
	query := `SELECT COUNT(*) FROM relyt_migrate_insert_benchmark_v2;`
	var count int
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count records: %w", err)
	}
	log.Printf("Counted %d records in relyt_migrate_insert_benchmark_v2.", count)
	return count, nil
}

func GetCountFromAuxTestDataTableV2(db *sql.DB) (int, error) {
	// This function retrieves the count of records in the relyt_migrate_insert_benchmark_v2_relyt_massive_group.
	log.Println("Counting records in relyt_migrate_insert_benchmark_v2_relyt_massive_group...")
	query := `SELECT COUNT(*) FROM relyt_migrate_insert_benchmark_v2_relyt_massive_group;`
	var count int
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count records: %w", err)
	}
	log.Printf("Counted %d records in relyt_migrate_insert_benchmark_v2_relyt_massive_group.", count)
	return count, nil
}

func InsertDataV2(db *sql.DB, processor *bulkprocessor.BulkProcessor, filePath string, batchSize int, wg *sync.WaitGroup) error {
	defer wg.Done()

	var tests []TestDataWithAuxV2
	csvFile, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("failed to open csv file: %v", err)
	}
	log.Printf("opened csv file: %s", filePath)
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
			log.Fatalf("failed to read csv file: %v", err)
		}
		if len(record) < 5 {
			log.Fatalf("record does not contain enough fields: %v", record)
		}

		id, err := strconv.Atoi(record[0])
		if err != nil {
			log.Fatalf("failed to parse id: %v", err)
		}

		fileID, err := strconv.Atoi(record[1])
		if err != nil {
			log.Fatalf("failed to parse file_id: %v", err)
		}

		routingID := record[2]
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
				log.Fatalf("failed to insert data: %v", err)
			}

			tests = nil // clear the list, prepare for the next batch
		}
	}

	if len(tests) > 0 {
		// Get fileID and routingID from the first record in tests
		lastFileID := fmt.Sprintf("%d", tests[0].FileID)
		lastRoutingID := tests[0].RoutingID
		err := processor.InsertV2(lastFileID, lastRoutingID, tests)
		if err != nil {
			log.Fatalf("failed to insert data: %v", err)
		}
	}

	log.Printf("inserted %d records for %s", i, filePath)

	return nil
}

// fork 10 go routines to insert data use only one processor
func main() {
	// Initialize database connection
	writeWg := sync.WaitGroup{}
	dbConfig := InitDatabaseConfig("127.0.0.1", 7000, "postgres", "", "postgres")
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	} else {
		err = CreateTestDataTaleV2(db)
		if err != nil {
			log.Fatalf("failed to create test table: %v", err)
			return
		}
		err = TruncateTestDataTableV2(db)
		if err != nil {
			log.Fatalf("failed to truncate test table: %v", err)
			return
		}
		err = InitRoutingTableV2(db)
		if err != nil {
			log.Fatalf("failed to initialize routing table: %v", err)
			return
		}
	}
	defer db.Close()

	processor := NewProcessorV2(dbConfig, 30, 0)
	err = processor.Start()
	if err != nil {
		log.Fatalf("failed to start processor: %v", err)
	}

	if err != nil {
		log.Fatalf("failed to initialize routing table: %v", err)
		return
	}

	var filePath string
	batchSize := 10000
	for i := 0; i < 10; i++ {
		filePath = fmt.Sprintf("./benchmark_test_v2_a%c", 'a'+i)
		writeWg.Add(1)
		go InsertDataV2(db, processor, filePath, batchSize, &writeWg)
	}
	writeWg.Wait()

	processor.Flush()
	processor.Shutdown()

	mainTableCount, err := GetCountFromMainTestDataTableV2(db)
	if err != nil {
		log.Fatalf("failed to get count from main test table: %v", err)
		return
	}

	auxTableCount, err := GetCountFromAuxTestDataTableV2(db)
	if err != nil {
		log.Fatalf("failed to get count from aux test table: %v", err)
		return
	}

	log.Printf("main table count: %d, aux test table count: %d", mainTableCount, auxTableCount)
}
