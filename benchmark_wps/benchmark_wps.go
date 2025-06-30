package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/powerdrillai/relyt-loader-go/bulkprocessor"
)

type ContentLinkVectorData struct {
	ID              string `relyt:"id"`
	RoutingID       string `relyt:"routing_id"`
	ChunkID         int    `relyt:"chunk_id"`
	ChunkType       string `relyt:"chunk_type"`
	UserID          int64  `relyt:"user_id"`
	Creator         int64  `relyt:"creator"`
	Sharer          int64  `relyt:"sharer"`
	FileID          int64  `relyt:"fileid"`
	GroupID         int64  `relyt:"group_id"`
	CTime           int64  `relyt:"ctime"`
	MTime           int64  `relyt:"mtime"`
	Y               int    `relyt:"y"`
	YM              int    `relyt:"ym"`
	YMD             int    `relyt:"ymd"`
	Ext             string `relyt:"ext"`
	FSize           int64  `relyt:"fsize"`
	ParentID        int64  `relyt:"parent_id"`
	FType           string `relyt:"ftype"`
	Version         int64  `relyt:"version"`
	IndexUpdateTime int64  `relyt:"index_update_time"`
	ExtGroup        string `relyt:"ext_group"`
	Vector          string `relyt:"vector"`
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
			Table:    "content_personal_vector_semantic_insight_vector_bge_m3_dense",
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

func GetCountFromTable(db *sql.DB) (int, error) {
	log.Println("Counting records in content_personal_vector_semantic_insight_vector_bge_m3_dense...")
	query := `SELECT COUNT(*) FROM content_personal_vector_semantic_insight_vector_bge_m3_dense;`
	var count int
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count records: %w", err)
	}
	log.Printf("Counted %d records in content_personal_vector_semantic_insight_vector_bge_m3_dense.", count)
	return count, nil
}

// fork multiple go routines to insert data use only one processor
func main() {
	// Initialize database connection
	dbConfig := InitDatabaseConfig("127.0.0.1", 5432, "postgres", "", "postgres")
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	}
	defer db.Close()

	// 优化处理器配置：增加文件写入超时时间以减少频繁刷新
	processor := NewProcessorV2(dbConfig, 60, 0) // 增加超时时间到60秒
	err = processor.Start()
	if err != nil {
		log.Fatalf("failed to start processor: %v", err)
	}

	// 配置
	dataDir := "./generated_data" // 数据目录
	// filePrefix := "wps_data_version_" // 文件名前缀
	filePrefix := "wps_batch_data_" // 文件名前缀
	multiThread := true             // 是否多线程
	totalVersions := 10             // 文件数
	totalThread := 10               // 设置线程数量

	if multiThread {
		log.Printf("Starting multi-threaded data insertion with %d threads for %d versions", totalThread, totalVersions)
	} else {
		log.Printf("Starting sequential data insertion for %d versions", totalVersions)
	}

	startTime := time.Now()

	if multiThread {
		// 多线程模式：按线程数量平均分配版本
		writeWg := sync.WaitGroup{}

		// 计算每个线程处理的版本数量
		versionsPerThread := totalVersions / totalThread
		remainingVersions := totalVersions % totalThread

		log.Printf("Each thread will process approximately %d versions", versionsPerThread)
		if remainingVersions > 0 {
			log.Printf("First %d threads will process 1 additional version", remainingVersions)
		}

		// 启动多个线程
		for threadID := 0; threadID < totalThread; threadID++ {
			writeWg.Add(1)

			// 计算当前线程处理的版本范围
			startVersion := threadID * versionsPerThread
			endVersion := startVersion + versionsPerThread

			// 前面的线程处理额外的版本（如果有余数）
			if threadID < remainingVersions {
				startVersion += threadID
				endVersion += threadID + 1
			} else {
				startVersion += remainingVersions
				endVersion += remainingVersions
			}

			log.Printf("Thread %d will process versions %d to %d", threadID, startVersion, endVersion-1)

			// 启动goroutine处理指定范围的版本
			go func(tid, start, end int) {
				defer writeWg.Done()
				log.Printf("Thread %d started, processing versions %d-%d", tid, start, end-1)

				for version := start; version < end; version++ {
					filePath := fmt.Sprintf("%s/%s%d.csv", dataDir, filePrefix, version)
					log.Printf("Thread %d processing version %d: %s", tid, version, filePath)

					err := insertDataForThread(processor, filePath, tid)
					if err != nil {
						log.Fatalf("Thread %d failed to insert data for version %d: %v", tid, version, err)
					}
				}

				log.Printf("Thread %d completed processing versions %d-%d", tid, start, end-1)
			}(threadID, startVersion, endVersion)
		}

		// 等待所有线程完成
		writeWg.Wait()
		log.Printf("All %d threads completed", totalThread)

	} else {
		// 单线程模式：顺序处理每个版本
		for version := 0; version < totalVersions; version++ {
			filePath := fmt.Sprintf("%s/%s%d.csv", dataDir, filePrefix, version)
			log.Printf("Processing version %d: %s", version, filePath)

			err := insertDataForThread(processor, filePath, 0)
			if err != nil {
				log.Fatalf("Failed to insert data for version %d: %v", version, err)
			}
		}
	}

	// 确保所有数据都被刷新到数据库
	log.Printf("All versions completed, flushing remaining data...")
	flushStartTime := time.Now()
	processor.Flush()
	flushDuration := time.Since(flushStartTime)
	log.Printf("Flush completed in %v", flushDuration)

	processor.Shutdown()

	totalDuration := time.Since(startTime)
	log.Printf("Total processing time: %v", totalDuration)

	tableCount, err := GetCountFromTable(db)
	if err != nil {
		log.Fatalf("failed to get count from table: %v", err)
	}

	log.Printf("content_personal_vector_semantic_insight_vector_bge_m3_dense table count: %d", tableCount)

	// 计算性能指标
	if totalDuration.Seconds() > 0 {
		recordsPerSecond := float64(tableCount) / totalDuration.Seconds()
		log.Printf("Performance: %.2f records/second", recordsPerSecond)
		log.Printf("Average time per version: %v", totalDuration/time.Duration(totalVersions))
		if multiThread {
			log.Printf("Threads used: %d", totalThread)
		}
	}
}

// 专门用于线程处理的函数，不使用WaitGroup参数
func insertDataForThread(processor *bulkprocessor.BulkProcessor, filePath string, threadID int) error {
	var records []ContentLinkVectorData
	csvFile, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open csv file: %w", err)
	}
	log.Printf("Thread %d opened csv file: %s", threadID, filePath)
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)
	csvReader.FieldsPerRecord = -1
	csvReader.ReuseRecord = true

	i := 0
	batchSize := 10000 // 增加批量大小到5万条记录
	var currentFileID string
	var currentRoutingID string

	// 预分配切片容量以减少内存重新分配
	records = make([]ContentLinkVectorData, 0, batchSize)

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read csv file: %w", err)
		}
		if len(record) < 22 {
			return fmt.Errorf("record does not contain enough fields: %v", record)
		}

		// 解析所有字段
		id := record[0]
		routingID := record[1]
		chunkID, _ := strconv.Atoi(record[2])
		userID, _ := strconv.ParseInt(record[4], 10, 64)
		creator, _ := strconv.ParseInt(record[5], 10, 64)
		sharer, _ := strconv.ParseInt(record[6], 10, 64)
		fileID, _ := strconv.ParseInt(record[7], 10, 64)
		groupID, _ := strconv.ParseInt(record[8], 10, 64)
		cTime, _ := strconv.ParseInt(record[9], 10, 64)
		mTime, _ := strconv.ParseInt(record[10], 10, 64)
		y, _ := strconv.Atoi(record[11])
		ym, _ := strconv.Atoi(record[12])
		ymd, _ := strconv.Atoi(record[13])
		fSize, _ := strconv.ParseInt(record[15], 10, 64)
		parentID, _ := strconv.ParseInt(record[16], 10, 64)
		version, _ := strconv.ParseInt(record[18], 10, 64)
		indexUpdateTime, _ := strconv.ParseInt(record[19], 10, 64)

		fileIDStr := fmt.Sprintf("%d", fileID)

		// 检查是否需要插入数据（保持顺序插入逻辑）
		needInsert := false
		if len(records) == 0 {
			// 第一条记录，初始化当前组合
			currentFileID = fileIDStr
			currentRoutingID = routingID
		} else if fileIDStr != currentFileID || routingID != currentRoutingID {
			// fileID或routingID发生变化，必须先插入当前批次以保持顺序
			needInsert = true
		} else if len(records) >= batchSize {
			// 达到批处理大小
			needInsert = true
		}

		// 如果需要插入，先插入当前批次
		if needInsert && len(records) > 0 {
			err = processor.InsertV2(currentFileID, currentRoutingID, records)
			if err != nil {
				return fmt.Errorf("failed to insert data: %w", err)
			}
			// 重新分配切片以避免内存泄漏
			records = make([]ContentLinkVectorData, 0, batchSize)
		}

		// 添加新记录
		records = append(records, ContentLinkVectorData{
			ID:              id,
			RoutingID:       routingID,
			ChunkID:         chunkID,
			ChunkType:       record[3],
			UserID:          userID,
			Creator:         creator,
			Sharer:          sharer,
			FileID:          fileID,
			GroupID:         groupID,
			CTime:           cTime,
			MTime:           mTime,
			Y:               y,
			YM:              ym,
			YMD:             ymd,
			Ext:             record[14],
			FSize:           fSize,
			ParentID:        parentID,
			FType:           record[17],
			Version:         version,
			IndexUpdateTime: indexUpdateTime,
			ExtGroup:        record[20],
			Vector:          record[21],
		})

		// 更新当前组合
		currentFileID = fileIDStr
		currentRoutingID = routingID
		i++

		// 每处理10万条记录输出一次进度信息
		if i%100000 == 0 {
			log.Printf("Thread %d processed %d records from %s", threadID, i, filePath)
		}
	}

	// 处理最后一批数据
	if len(records) > 0 {
		err = processor.InsertV2(currentFileID, currentRoutingID, records)
		if err != nil {
			return fmt.Errorf("failed to insert data: %w", err)
		}
	}

	log.Printf("Thread %d total inserted %d records for %s", threadID, i, filePath)
	return nil
}
