package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/powerdrillai/relyt-loader-go/bulkprocessor"
)

// to run this example, you need to have a postgres database with the following table:
// CREATE TABLE  user_data (
//	id bigint NOT NULL PRIMARY KEY,
//	ext text,
//	vector vecf16(3) NOT NULL,
// )
// then run the following command:
// go run main.go data/test.csv

type UserData struct {
	ID     int    `json:"id"`
	Ext    string `json:"ext"`
	Vector string `json:"vector"`
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

// to run
func main() {
	// open a error.log
    logFile, err := os.OpenFile("/tmp/error.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
    if err != nil {
        log.Fatalf("Failed to open log file: %v", err)
    }
    defer logFile.Close()

    // 创建用户定义的资源结构体
    resources := &ErrorHandlerResources{
        LogFile: logFile,
    }

	// initialize config
	config := bulkprocessor.Config{
		// PostgreSQL config (required)
		PostgreSQL: bulkprocessor.PostgreSQLConfig{
			Host:     "127.0.0.1", // use your own host
			Port:     7000,
			Username: "postgres",
			Password: "",        // use your own password
			Database: "postgres", // use your own database
			Table:    "user_data",
			Schema:   "public",
		},
		BatchSize:       10, // number of records per file
		BatchImportSize: 2,
		FeedbackColumn:      "id", // column name for error messages
		ImportErrorCallback: WriteErrorsToFiles,
		CallbackResource: resources,
	}

	// create processor
	processor, err := bulkprocessor.New(config)
	if err != nil {
		log.Fatalf("failed to create processor: %v", err)
	}
	defer processor.Shutdown()

	// get and print the unique ID of the processor
	processId := processor.GetProcessId()
	log.Printf("start task, processId: %s", processId)
	// create example data
	log.Println("prepare to import data...")

	// read an csv file from the command line as an example
	if len(os.Args) < 2 {
		log.Fatalf("please provide the csv file path as a parameter")
	}

	filePath := os.Args[1]
	batchSize := 10
	var users []UserData

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
			log.Fatalf("failed to read csv file: %v", err)
		}
		id, err := strconv.Atoi(record[0])
		if err != nil {
			log.Fatalf("failed to parse id: %v", err)
		}
		if len(record) < 3 {
			log.Fatalf("record does not contain enough fields: %v", record)
		} else {
			log.Printf("record %d: id=%d, ext=%s, vector=%s", i, id, record[1], record[2])
		}
		ext := record[1]
		vector := record[2]
		users = append(users, UserData{
			ID:     id,
			Ext:    ext,
			Vector: vector,
		})
		i++

		// insert batch
		if i%batchSize == 0 {
			log.Printf("insert batch %d, contains %d records", i/batchSize, len(users))
			err := processor.Insert(users)
			if err != nil {
				log.Fatalf("failed to insert data: %v", err)
			}
			users = nil // clear the list, prepare for the next batch
		}
	}
	if len(users) > 0 {
		log.Printf("insert batch %d, contains %d records", i/batchSize, len(users))
		err := processor.Insert(users)
		if err != nil {
			log.Fatalf("failed to insert data: %v", err)
		}
	}

	// refresh all data and wait for import to complete
	log.Println("refreshing data and waiting for import to complete...")
	err = processor.Flush()
	if err != nil {
		log.Fatalf("failed to refresh data: %v", err)
	}
	log.Printf("data import completed, processId: %s", processId)
}
