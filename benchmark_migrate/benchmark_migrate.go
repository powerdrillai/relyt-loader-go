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

type TestData struct {
	ID      int    `relyt:"id"`
	GroupID int    `relyt:"group_id"`
	Ext     string `relyt:"ext"`
	Vector  string `relyt:"vector"`
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

func NewProcessor(dbconfig DatabaseConfig, fileTimeout int) *bulkprocessor.BulkProcessor {
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
			Table:    "relyt_migrate_insert_benchmark",
			Schema:   "public",
		},
		BatchSize:           100000, // number of records per file
		BatchImportSize:     3,
		FeedbackColumn:      "id", // column name for error messages
		ImportErrorCallback: WriteErrorsToFiles,
		CallbackResource:    resources,
		FileWriteTimeout:    fileTimeout, // set file write timeout
		RoutingColumn:       "group_id",  // routing column
	}

	// create processor
	processor, err := bulkprocessor.New(config)
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
	log.Println("Creating relyt_migrate_insert_benchmark in PostgreSQL...")
	query := `
	CREATE TABLE IF NOT EXISTS relyt_migrate_insert_benchmark (
		id bigint NOT NULL PRIMARY KEY,
		group_id bigint NOT NULL,
		ext text,
		vector vecf16(3) NOT NULL
	) using heap;

	CREATE TABLE IF NOT EXISTS relyt_migrate_insert_benchmark_relyt_massive (
		id bigint NOT NULL PRIMARY KEY,
		group_id bigint NOT NULL,
		ext text,
		vector vecf16(3) NOT NULL
	) using heap;

	CREATE TABLE IF NOT EXISTS relyt_sys.relyt_migrate_insert_benchmark_relyt_routing (
		group_id bigint PRIMARY KEY,
		store_table_name TEXT NOT NULL
	) USING heap DISTRIBUTED NONE;`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create relyt_migrate_insert_benchmark: %w", err)
	}
	log.Println("relyt_migrate_insert_benchmark created successfully.")
	return nil
}

func TruncateTestDataTable(db *sql.DB) error {
	log.Println("Truncating tables in PostgreSQL...")
	query := `
	TRUNCATE TABLE relyt_migrate_insert_benchmark;
	TRUNCATE TABLE relyt_migrate_insert_benchmark_relyt_massive;
	TRUNCATE TABLE relyt_sys.relyt_migrate_insert_benchmark_relyt_routing;`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to truncate tables: %w", err)
	}
	log.Println("Tables truncated successfully.")
	return nil
}

func InitRoutingTable(db *sql.DB) error {
	log.Println("Initializing routing table...")
	query := `
	INSERT INTO relyt_sys.relyt_migrate_insert_benchmark_relyt_routing (group_id, store_table_name) 
	VALUES 
		(100, 'relyt_migrate_insert_benchmark_relyt_massive'),
		(200, 'relyt_migrate_insert_benchmark_relyt_massive'),
		(300, 'relyt_migrate_insert_benchmark_relyt_massive');`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to initialize routing table: %w", err)
	}
	log.Println("Routing table initialized successfully.")
	return nil
}

func GetCountFromMainTestDataTable(db *sql.DB) (int, error) {
	// This function retrieves the count of records in the relyt_migrate_insert_benchmark.
	log.Println("Counting records in relyt_migrate_insert_benchmark...")
	query := `SELECT COUNT(*) FROM relyt_migrate_insert_benchmark;`
	var count int
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count records: %w", err)
	}
	log.Printf("Counted %d records in relyt_migrate_insert_benchmark.", count)
	return count, nil
}

func GetCountFromAuxTestDataTable(db *sql.DB) (int, error) {
	// This function retrieves the count of records in the relyt_migrate_insert_benchmark_relyt_massive.
	log.Println("Counting records in relyt_migrate_insert_benchmark_relyt_massive...")
	query := `SELECT COUNT(*) FROM relyt_migrate_insert_benchmark_relyt_massive;`
	var count int
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count records: %w", err)
	}
	log.Printf("Counted %d records in relyt_migrate_insert_benchmark_relyt_massive.", count)
	return count, nil
}

func InsertData(db *sql.DB, processor *bulkprocessor.BulkProcessor, filePath string, batchSize int, wg *sync.WaitGroup) error {
	defer wg.Done()

	var tests []TestData
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
		if len(record) < 4 {
			log.Fatalf("record does not contain enough fields: %v", record)
		}

		id, err := strconv.Atoi(record[0])
		if err != nil {
			log.Fatalf("failed to parse id: %v", err)
		}

		groupID, err := strconv.Atoi(record[1])
		if err != nil {
			log.Fatalf("failed to parse group_id: %v", err)
		}

		ext := record[2]
		vector := record[3]

		tests = append(tests, TestData{
			ID:      id,
			GroupID: groupID,
			Ext:     ext,
			Vector:  vector,
		})
		i++

		// insert batch
		if i%batchSize == 0 {
			err := processor.Insert(tests)
			if err != nil {
				log.Fatalf("failed to insert data: %v", err)
			}
			tests = nil // clear the list, prepare for the next batch
		}
	}

	if len(tests) > 0 {
		err := processor.Insert(tests)
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
		err = CreateTestDataTale(db)
		if err != nil {
			log.Fatalf("failed to create test table: %v", err)
			return
		}
		err = TruncateTestDataTable(db)
		if err != nil {
			log.Fatalf("failed to truncate test table: %v", err)
			return
		}
		err = InitRoutingTable(db)
		if err != nil {
			log.Fatalf("failed to initialize routing table: %v", err)
			return
		}
	}
	defer db.Close()

	processor := NewProcessor(dbConfig, 30)
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
		filePath = fmt.Sprintf("./benchmark_test_a%c", 'a'+i)
		writeWg.Add(1)
		go InsertData(db, processor, filePath, batchSize, &writeWg)
	}
	writeWg.Wait()

	processor.Flush()

	mainTableCount, err := GetCountFromMainTestDataTable(db)
	if err != nil {
		log.Fatalf("failed to get count from main test table: %v", err)
		return
	}

	auxTableCount, err := GetCountFromAuxTestDataTable(db)
	if err != nil {
		log.Fatalf("failed to get count from aux test table: %v", err)
		return
	}

	log.Printf("main table count: %d, aux test table count: %d", mainTableCount, auxTableCount)

	processor.Shutdown()
}
