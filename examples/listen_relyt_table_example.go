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
	
	log.Printf("data import completed, processId: %s", processId)
}

func listen() {
    // 连接到PostgreSQL
    connStr := "user=youruser dbname=yourdb sslmode=disable"
    pool, err := pgxpool.Connect(context.Background(), connStr)
    if err != nil {
        log.Fatalf("Unable to connect to database: %v\n", err)
    }
    defer pool.Close()

    // 创建一个通知通道
    conn, err := pool.Acquire(context.Background())
    if err != nil {
        log.Fatalf("Unable to acquire connection: %v\n", err)
    }
    defer conn.Release()

    // 监听通知
    err = conn.Listen("my_channel")
    if err != nil {
        log.Fatalf("Unable to listen to channel 'my_channel': %v\n", err)
    }

    // 启动一个goroutine来处理通知
    go func() {
        for {
            select {
            case notification := <-conn.Notification:
				// 更新本地的map
                fmt.Printf("Notification received: %s\n", notification.Extra)
            case <-time.After(10 * time.Second):
                fmt.Println("No notifications received in the last 10 seconds.")
                return
            }
        }
    }()

    // 阻塞主goroutine，以便通知处理goroutine可以运行
    time.Sleep(30 * time.Second)
}
