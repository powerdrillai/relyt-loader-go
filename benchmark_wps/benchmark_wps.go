package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"bytes"
	"encoding/csv"
	"io"

	"github.com/powerdrillai/relyt-loader-go/bulkprocessor"
)

// 全局对象池，用于复用ContentLinkVectorData结构体
var dataPool = sync.Pool{
	New: func() interface{} {
		return &ContentLinkVectorData{}
	},
}

// 获取数据对象池中的对象
func getDataObject() *ContentLinkVectorData {
	return dataPool.Get().(*ContentLinkVectorData)
}

// 归还数据对象到池中
func putDataObject(data *ContentLinkVectorData) {
	// 重置所有字段
	data.ID = ""
	data.RoutingID = ""
	data.ChunkID = 0
	data.ChunkType = ""
	data.UserID = 0
	data.Creator = 0
	data.Sharer = 0
	data.FileID = 0
	data.GroupID = 0
	data.CTime = 0
	data.MTime = 0
	data.Y = 0
	data.YM = 0
	data.YMD = 0
	data.Ext = ""
	data.FSize = 0
	data.ParentID = 0
	data.FType = ""
	data.Version = 0
	data.IndexUpdateTime = 0
	data.ExtGroup = ""
	data.Vector = ""

	dataPool.Put(data)
}

// 字节解析函数，避免字符串转换
func parseBytesToInt64(data []byte) (int64, error) {
	return strconv.ParseInt(string(data), 10, 64)
}

func parseBytesToInt(data []byte) (int, error) {
	return strconv.Atoi(string(data))
}

// 字节转字符串，复用缓冲区
func bytesToString(data []byte, buffer []byte) string {
	buffer = buffer[:0] // 清空但保留容量
	buffer = append(buffer, data...)
	return string(buffer)
}

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

func TruncateTable(db *sql.DB) error {
	query := `TRUNCATE TABLE content_personal_vector_semantic_insight_vector_bge_m3_dense;`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to truncate table: %w", err)
	}

	query = `TRUNCATE TABLE content_personal_vector_semantic_insight_vector_bge_m3_dense_relyt_massive_group;`
	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to truncate table: %w", err)
	}

	query = `TRUNCATE TABLE relyt_sys.content_personal_vector_semantic_insight_vector_bge_m3_dense_relyt_routing;`
	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to truncate table: %w", err)
	}

	query = `insert into relyt_sys.content_personal_vector_semantic_insight_vector_bge_m3_dense_relyt_routing values(1,'content_personal_vector_semantic_insight_vector_bge_m3_dense_relyt_massive_group');`
	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}

	return nil
}

// fork multiple go routines to insert data use only one processor
func main() {
	// Initialize database connection
	dbConfig := InitDatabaseConfig("127.0.0.1", 7000, "postgres", "", "postgres")
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	}
	defer db.Close()

	go func() {
		log.Println("Starting pprof server on localhost:6060")
		log.Println("Memory profiling available at:")
		log.Println("  - http://localhost:6060/debug/pprof/heap")
		log.Println("  - http://localhost:6060/debug/pprof/allocs")
		log.Println("  - http://localhost:6060/debug/pprof/goroutine")
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// 优化处理器配置：增加文件写入超时时间以减少频繁刷新
	processor := NewProcessorV2(dbConfig, 60, 0) // 增加超时时间到60秒
	err = processor.Start()
	if err != nil {
		log.Fatalf("failed to start processor: %v", err)
	}

	err = TruncateTable(db)
	if err != nil {
		log.Fatalf("failed to truncate table: %v", err)
	}

	// 配置
	dataDir := "./generated_data" // 数据目录
	// filePrefix := "wps_data_version_" // 文件名前缀
	filePrefix := "wps_batch_data_" // 文件名前缀
	multiThread := false            // 是否多线程
	totalVersions := 10             // 文件数
	totalThread := 1                // 设置线程数量

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

			err := insertDataForThreadV4(processor, filePath, 0)
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

// 使用csv文件，使用csvreader读取数据，然后插入到数据库, cpu和内存消耗多
func insertDataForThread(processor *bulkprocessor.BulkProcessor, filePath string, threadID int) error {
	// 使用更小的缓冲区打开文件（默认4KB足够CSV行读取）
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open csv file: %w", err)
	}
	defer file.Close()

	// 配置高性能 CSV 读取器
	csvReader := csv.NewReader(file)
	csvReader.ReuseRecord = true   // 复用行缓冲区
	csvReader.FieldsPerRecord = -1 // 允许变长字段
	csvReader.LazyQuotes = true    // 宽松解析引号

	// 预分配变量（避免重复声明）
	var (
		record      []string
		fileIDStr   string
		currentFile string
		currentRID  string
		batch       = make([]ContentLinkVectorData, 0, 10000) // 预分配批量缓冲区
		parseErrors int
	)

	// 对象池优化
	recordPool := sync.Pool{
		New: func() interface{} { return &ContentLinkVectorData{} },
	}

	for i := 0; ; i++ {
		// 读取一行（复用record内存）
		record, err = csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// 容忍少量解析错误（记录后跳过）
			if parseErrors++; parseErrors > 100 {
				return fmt.Errorf("too many CSV errors (%d): %w", parseErrors, err)
			}
			continue
		}
		if len(record) < 22 {
			continue
		}

		// 从对象池获取记录对象
		data := recordPool.Get().(*ContentLinkVectorData)

		// 直接解析关键字段（跳过非必要校验）
		fileID, _ := strconv.ParseInt(record[7], 10, 64)
		fileIDStr = record[7] // 直接复用CSV中的原始字符串

		// 填充数据（按字段顺序避免分支）
		data.ID = record[0]
		data.RoutingID = record[1]
		data.ChunkID, _ = strconv.Atoi(record[2])
		data.ChunkType = record[3]
		data.UserID, _ = strconv.ParseInt(record[4], 10, 64)
		data.Creator, _ = strconv.ParseInt(record[5], 10, 64)
		data.Sharer, _ = strconv.ParseInt(record[6], 10, 64)
		data.FileID = fileID
		data.GroupID, _ = strconv.ParseInt(record[8], 10, 64)
		data.CTime, _ = strconv.ParseInt(record[9], 10, 64)
		data.MTime, _ = strconv.ParseInt(record[10], 10, 64)
		data.Y, _ = strconv.Atoi(record[11])
		data.YM, _ = strconv.Atoi(record[12])
		data.YMD, _ = strconv.Atoi(record[13])
		data.Ext = record[14]
		data.FSize, _ = strconv.ParseInt(record[15], 10, 64)
		data.ParentID, _ = strconv.ParseInt(record[16], 10, 64)
		data.FType = record[17]
		data.Version, _ = strconv.ParseInt(record[18], 10, 64)
		data.IndexUpdateTime, _ = strconv.ParseInt(record[19], 10, 64)
		data.ExtGroup = record[20]
		data.Vector = record[21]

		// 批量插入逻辑
		if len(batch) == 0 {
			currentFile, currentRID = fileIDStr, data.RoutingID
		} else if fileIDStr != currentFile || data.RoutingID != currentRID || len(batch) >= cap(batch) {
			if err := processor.InsertV2(currentFile, currentRID, batch); err != nil {
				recordPool.Put(data)
				return fmt.Errorf("batch insert failed at line %d: %w", i, err)
			}
			batch = batch[:0] // 清空批次但保留底层数组
			currentFile, currentRID = fileIDStr, data.RoutingID
		}

		// 添加记录副本到批次
		batch = append(batch, *data)
		recordPool.Put(data)
	}

	// 插入最后一批
	if len(batch) > 0 {
		if err := processor.InsertV2(currentFile, currentRID, batch); err != nil {
			return fmt.Errorf("final insert failed: %w", err)
		}
	}

	log.Printf("Thread %d total inserted records for %s", threadID, filePath)
	return nil
}

// 从txt文件中读取数据，然后插入到数据库，使用字节分割函数，避免字符串转换
func insertDataForThreadV2(processor *bulkprocessor.BulkProcessor, filePath string, threadID int) error {
	txtFile, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open txt file: %w", err)
	}
	log.Printf("Thread %d opened txt file: %s", threadID, filePath)
	defer txtFile.Close()

	// 使用bufio.Scanner进行逐行读取，优化缓冲区
	scanner := bufio.NewScanner(txtFile)
	scanner.Buffer(make([]byte, 4096), 1<<20) // 初始4KB，最大1MB

	// 预分配数字解析的临时变量
	var (
		fileID, userID, creator, sharer, groupID, cTime, mTime, fSize, parentID, version, indexUpdateTime int64
		chunkID, y, ym, ymd                                                                               int
	)

	// 预分配字符串缓冲区，用于复用
	stringBuffer := make([]byte, 0, 1024)

	i := 0
	batchSize := 100 // 限制批次大小，减少内存压力

	// 逐行读取并处理
	for scanner.Scan() {
		lineBytes := scanner.Bytes() // 直接获取字节切片
		if len(lineBytes) == 0 {
			continue // 跳过空行
		}

		// 使用字节分割函数，避免字符串转换
		recordBytes := bytes.Split(lineBytes, []byte{'|'})
		if len(recordBytes) < 22 {
			continue
		}

		// 从对象池获取数据对象
		singleRecord := getDataObject()

		// 使用字节解析，避免字符串转换
		fileID, _ = parseBytesToInt64(recordBytes[7])
		userID, _ = parseBytesToInt64(recordBytes[4])
		creator, _ = parseBytesToInt64(recordBytes[5])
		sharer, _ = parseBytesToInt64(recordBytes[6])
		groupID, _ = parseBytesToInt64(recordBytes[8])
		cTime, _ = parseBytesToInt64(recordBytes[9])
		mTime, _ = parseBytesToInt64(recordBytes[10])
		y, _ = parseBytesToInt(recordBytes[11])
		ym, _ = parseBytesToInt(recordBytes[12])
		ymd, _ = parseBytesToInt(recordBytes[13])
		fSize, _ = parseBytesToInt64(recordBytes[15])
		parentID, _ = parseBytesToInt64(recordBytes[16])
		version, _ = parseBytesToInt64(recordBytes[18])
		indexUpdateTime, _ = parseBytesToInt64(recordBytes[19])
		chunkID, _ = parseBytesToInt(recordBytes[2])

		// 复用字符串缓冲区构建fileIDStr
		stringBuffer = stringBuffer[:0] // 清空但保留容量
		stringBuffer = strconv.AppendInt(stringBuffer, fileID, 10)
		fileIDStr := string(stringBuffer)

		// 填充数据对象 - 使用字节转字符串，复用缓冲区
		singleRecord.ID = string(recordBytes[0])
		singleRecord.RoutingID = string(recordBytes[1])
		singleRecord.ChunkID = chunkID
		singleRecord.ChunkType = string(recordBytes[3])
		singleRecord.UserID = userID
		singleRecord.Creator = creator
		singleRecord.Sharer = sharer
		singleRecord.FileID = fileID
		singleRecord.GroupID = groupID
		singleRecord.CTime = cTime
		singleRecord.MTime = mTime
		singleRecord.Y = y
		singleRecord.YM = ym
		singleRecord.YMD = ymd
		singleRecord.Ext = string(recordBytes[14])
		singleRecord.FSize = fSize
		singleRecord.ParentID = parentID
		singleRecord.FType = string(recordBytes[17])
		singleRecord.Version = version
		singleRecord.IndexUpdateTime = indexUpdateTime
		singleRecord.ExtGroup = string(recordBytes[20])
		singleRecord.Vector = string(recordBytes[21])

		// 立即插入单条记录 - 注意这里需要复制值，因为对象池中的对象会被重用
		recordCopy := *singleRecord
		err := processor.InsertV2(fileIDStr, singleRecord.RoutingID, []ContentLinkVectorData{recordCopy})
		if err != nil {
			putDataObject(singleRecord) // 归还数据对象
			return fmt.Errorf("failed to insert record %d: %w", i, err)
		}

		// 归还对象到池中
		putDataObject(singleRecord)

		i++

		// 每处理1万条记录输出一次进度信息
		if i%10000 == 0 {
			log.Printf("Thread %d processed %d records from %s", threadID, i, filePath)
		}

		// 内存监控和优化 - 减少GC频率
		if i%batchSize == 0 {
			runtime.GC()
		}
	}

	// 最终GC
	runtime.GC()

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read txt file: %w", err)
	}

	log.Printf("Thread %d total inserted %d records for %s", threadID, i, filePath)

	return nil
}

// 内存中生成数据，然后插入到数据库
func insertDataForThreadV3(processor *bulkprocessor.BulkProcessor, filePath string, threadID int) error {
	// 随机数生成器，使用线程ID作为种子确保不同线程生成不同的数据
	rand.Seed(time.Now().UnixNano() + int64(threadID))

	// 生成数据的数量，可以根据需要调整
	totalRecords := 80000 // 8万条记录

	log.Printf("Thread %d started generating %d random records", threadID, totalRecords)

	// 固定值 - 为了效率，除了主键外的所有字段都固定
	const (
		FIXED_CHUNK_TYPE        = "text"
		FIXED_USER_ID           = 1000000
		FIXED_CREATOR           = 1000000
		FIXED_SHARER            = 1000000
		FIXED_GROUP_ID          = 1000000
		FIXED_CTIME             = 1640995200 // 2022-01-01
		FIXED_MTIME             = 1640995200 // 2022-01-01
		FIXED_Y                 = 2022
		FIXED_YM                = 202201
		FIXED_YMD               = 20220101
		FIXED_EXT               = "txt"
		FIXED_FSIZE             = 1024
		FIXED_PARENT_ID         = 1000000
		FIXED_FTYPE             = "text"
		FIXED_INDEX_UPDATE_TIME = 1640995200
		FIXED_EXT_GROUP         = "text"
		RECORDS_PER_COMBINATION = 8000 // 每8000条数据重新生成一次(routing_id, fileid)组合
	)

	// 生成固定的1024维向量 - 使用最简单的方式
	vectorValues := make([]string, 1024)
	for k := 0; k < 1024; k++ {
		vectorValues[k] = "0.1" // 最简单的固定值
	}
	fixedVector := "[" + strings.Join(vectorValues, ",") + "]"

	var currentRoutingID string
	var currentFileID int64
	var currentVersion int64

	for i := 0; i < totalRecords; i++ {
		// 每RECORDS_PER_COMBINATION条数据重新生成一次(routing_id, fileid)组合
		if i%RECORDS_PER_COMBINATION == 0 {
			currentRoutingID = fmt.Sprintf("%d", i/(RECORDS_PER_COMBINATION*10))
			currentFileID = rand.Int63n(9000000000) + 1000000000 // 10位数字
		}

		// 每个(routing_id, fileid)组合内生成两个version
		// 前RECORDS_PER_COMBINATION/2条记录用version 0，后RECORDS_PER_COMBINATION/2条记录用version 1
		currentVersion = int64((i % RECORDS_PER_COMBINATION) / (RECORDS_PER_COMBINATION / 2))

		// 从对象池获取数据对象
		record := getDataObject()

		// 填充数据对象
		record.ID = fmt.Sprintf("%d", i) // 全局唯一ID，从0开始递增
		record.RoutingID = currentRoutingID
		record.ChunkID = i // 与ID相同
		record.ChunkType = FIXED_CHUNK_TYPE
		record.UserID = FIXED_USER_ID
		record.Creator = FIXED_CREATOR
		record.Sharer = FIXED_SHARER
		record.FileID = currentFileID
		record.GroupID = FIXED_GROUP_ID
		record.CTime = FIXED_CTIME
		record.MTime = FIXED_MTIME
		record.Y = FIXED_Y
		record.YM = FIXED_YM
		record.YMD = FIXED_YMD
		record.Ext = FIXED_EXT
		record.FSize = FIXED_FSIZE
		record.ParentID = FIXED_PARENT_ID
		record.FType = FIXED_FTYPE
		record.Version = currentVersion
		record.IndexUpdateTime = FIXED_INDEX_UPDATE_TIME
		record.ExtGroup = FIXED_EXT_GROUP
		record.Vector = fixedVector

		// 立即插入单条记录 - 复制值以避免对象池重用问题
		recordCopy := *record
		fileIDStr := fmt.Sprintf("%d", currentFileID)
		err := processor.InsertV2(fileIDStr, currentRoutingID, []ContentLinkVectorData{recordCopy})
		if err != nil {
			putDataObject(record) // 归还数据对象
			return fmt.Errorf("failed to insert record %d: %w", i, err)
		}

		// 归还对象到池中
		putDataObject(record)

		// 每处理1万条记录输出一次进度信息
		if (i+1)%10000 == 0 {
			log.Printf("Thread %d processed %d/%d records", threadID, i+1, totalRecords)
		}

		// 添加小延迟避免过度占用CPU
		if (i+1)%10000 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	log.Printf("Thread %d completed generating %d random records", threadID, totalRecords)
	return nil
}

// 字节流CSV解析函数，类似insertDataForThreadV2但处理CSV格式
func insertDataForThreadV4(processor *bulkprocessor.BulkProcessor, filePath string, threadID int) error {
	csvFile, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open csv file: %w", err)
	}
	log.Printf("Thread %d opened csv file: %s", threadID, filePath)
	defer csvFile.Close()

	// 使用bufio.Scanner进行逐行读取，优化缓冲区
	scanner := bufio.NewScanner(csvFile)
	scanner.Buffer(make([]byte, 4096), 1<<20) // 初始4KB，最大1MB

	// 预分配数字解析的临时变量
	var (
		fileID, userID, creator, sharer, groupID, cTime, mTime, fSize, parentID, version, indexUpdateTime int64
		chunkID, y, ym, ymd                                                                               int
	)

	// 预分配字符串缓冲区，用于复用
	stringBuffer := make([]byte, 0, 1024)

	i := 0
	batchSize := 100 // 限制批次大小，减少内存压力

	// 字节切片池，用于复用字节缓冲区
	bytesPool := sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 4096) // 初始4KB容量
		},
	}

	// 逐行读取并处理
	for scanner.Scan() {
		lineBytes := scanner.Bytes() // 直接获取字节切片
		if len(lineBytes) == 0 {
			continue // 跳过空行
		}

		// 从对象池获取字节切片
		recordBytes := bytesPool.Get().([]byte)
		recordBytes = recordBytes[:0] // 清空但保留容量

		// 解析CSV行，处理引号和逗号
		fields := parseCSVLine(lineBytes, recordBytes)
		if len(fields) < 22 {
			bytesPool.Put(recordBytes)
			continue
		}

		// 从对象池获取数据对象
		singleRecord := getDataObject()

		// 使用字节解析，避免字符串转换
		fileID, _ = parseBytesToInt64(fields[7])
		userID, _ = parseBytesToInt64(fields[4])
		creator, _ = parseBytesToInt64(fields[5])
		sharer, _ = parseBytesToInt64(fields[6])
		groupID, _ = parseBytesToInt64(fields[8])
		cTime, _ = parseBytesToInt64(fields[9])
		mTime, _ = parseBytesToInt64(fields[10])
		y, _ = parseBytesToInt(fields[11])
		ym, _ = parseBytesToInt(fields[12])
		ymd, _ = parseBytesToInt(fields[13])
		fSize, _ = parseBytesToInt64(fields[15])
		parentID, _ = parseBytesToInt64(fields[16])
		version, _ = parseBytesToInt64(fields[18])
		indexUpdateTime, _ = parseBytesToInt64(fields[19])
		chunkID, _ = parseBytesToInt(fields[2])

		// 复用字符串缓冲区构建fileIDStr
		stringBuffer = stringBuffer[:0] // 清空但保留容量
		stringBuffer = strconv.AppendInt(stringBuffer, fileID, 10)
		fileIDStr := string(stringBuffer)

		// 填充数据对象 - 使用字节转字符串，复用缓冲区
		singleRecord.ID = string(fields[0])
		singleRecord.RoutingID = string(fields[1])
		singleRecord.ChunkID = chunkID
		singleRecord.ChunkType = string(fields[3])
		singleRecord.UserID = userID
		singleRecord.Creator = creator
		singleRecord.Sharer = sharer
		singleRecord.FileID = fileID
		singleRecord.GroupID = groupID
		singleRecord.CTime = cTime
		singleRecord.MTime = mTime
		singleRecord.Y = y
		singleRecord.YM = ym
		singleRecord.YMD = ymd
		singleRecord.Ext = string(fields[14])
		singleRecord.FSize = fSize
		singleRecord.ParentID = parentID
		singleRecord.FType = string(fields[17])
		singleRecord.Version = version
		singleRecord.IndexUpdateTime = indexUpdateTime
		singleRecord.ExtGroup = string(fields[20])
		singleRecord.Vector = string(fields[21])

		// 立即插入单条记录 - 注意这里需要复制值，因为对象池中的对象会被重用
		recordCopy := *singleRecord
		err := processor.InsertV2(fileIDStr, singleRecord.RoutingID, []ContentLinkVectorData{recordCopy})
		if err != nil {
			putDataObject(singleRecord) // 归还数据对象
			bytesPool.Put(recordBytes)  // 归还字节切片
			return fmt.Errorf("failed to insert record %d: %w", i, err)
		}

		// 归还对象到池中
		putDataObject(singleRecord)
		bytesPool.Put(recordBytes)

		i++

		// 每处理1万条记录输出一次进度信息
		if i%10000 == 0 {
			log.Printf("Thread %d processed %d records from %s", threadID, i, filePath)
		}

		// 内存监控和优化 - 减少GC频率
		if i%batchSize == 0 {
			runtime.GC()
		}
	}

	// 最终GC
	runtime.GC()

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read csv file: %w", err)
	}

	log.Printf("Thread %d total inserted %d records for %s", threadID, i, filePath)

	return nil
}

// 高性能CSV行解析函数，处理引号和逗号
func parseCSVLine(line []byte, buffer []byte) [][]byte {
	var fields [][]byte
	fieldStart := 0
	inQuotes := false

	for i := 0; i < len(line); i++ {
		switch line[i] {
		case '"':
			if !inQuotes {
				inQuotes = true
				fieldStart = i + 1
			} else {
				// 检查是否是转义的引号
				if i+1 < len(line) && line[i+1] == '"' {
					// 转义的引号，跳过下一个
					i++
					continue
				} else {
					// 引号结束
					inQuotes = false
					// 提取字段（去掉引号）
					field := line[fieldStart:i]
					fields = append(fields, field)
					fieldStart = i + 1
				}
			}
		case ',':
			if !inQuotes {
				// 字段分隔符
				field := line[fieldStart:i]
				fields = append(fields, field)
				fieldStart = i + 1
			}
		}
	}

	// 添加最后一个字段
	if fieldStart < len(line) {
		field := line[fieldStart:]
		fields = append(fields, field)
	}

	return fields
}
