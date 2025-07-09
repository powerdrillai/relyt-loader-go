package bulkprocessor

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func setupTestDatabase(t *testing.T) (*sql.DB, *BulkProcessor) {
	dbConfig := InitDatabaseConfig("127.0.0.1", 7000, "postgres", "", "postgres")
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		t.Fatalf("failed to setup database: %v", err)
	}

	err = CreateTestDataTaleWithAux(db)
	if err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}

	err = TruncateTestDataTableWithAux(db)
	if err != nil {
		t.Fatalf("failed to truncate test table: %v", err)
	}

	err = InitTestTables(db)
	if err != nil {
		t.Fatalf("failed to init test data routing table: %v", err)
	}

	processor := NewProcessor(dbConfig, 6, "auxtest")
	return db, processor
}

func InitTestTables(db *sql.DB) error {
	log.Println("Initializing test data in PostgreSQL...")

	// insert data to test_routing_data_relyt_routing
	query := `
	INSERT INTO relyt_sys.test_routing_data_relyt_routing (routing_id, store_table_name)
	VALUES ($1, $2);
	`
	// 插入group_id为100的记录
	_, err := db.Exec(query, 100, "test_routing_data")
	if err != nil {
		return fmt.Errorf("failed to insert data to routing table for routing_id 100: %w", err)
	}

	// 插入group_id为110的记录
	_, err = db.Exec(query, 110, "test_routing_data")
	if err != nil {
		return fmt.Errorf("failed to insert data to routing table for routing_id 110: %w", err)
	}

	// insert data to test_routing_data
	query = `
	INSERT INTO test_routing_data (id, routing_id, ext, vector)
	VALUES 
	(5, 120, 'test5', '[0.5,0.6,0.7]'),
	(6, 120, 'test6', '[0.6,0.7,0.8]'),
	(7, 130, 'test7', '[0.7,0.8,0.9]'),
	(8, 130, 'test8', '[0.8,0.9,1.0]'),
	(9, 140, null, '[0.9,1.0,1.1]'),
	(10, 140, '', '[1.0,1.1,1.2]');
	`
	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to insert data to test_routing_data for routing_id 100: %w", err)
	}

	// insert data to test_routing_data_relyt_massive_group
	query = `
	INSERT INTO test_routing_data_relyt_massive_group (id, routing_id, ext, vector)
	VALUES 
	(1, 100, 'test1', '[0.1,0.2,0.3]'),
	(2, 100, 'test2', '[0.2,0.3,0.4]'),
	(3, 110, '', '[0.3,0.4,0.5]'),
	(4, 110, null, '[0.4,0.5,0.6]');
	`
	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to insert data to test_routing_data_relyt_massive_group for routing_id 100: %w", err)
	}

	return nil
}

func CreateTestWPSDataTaleWithAux(db *sql.DB, withAux bool) error {
	// This function is a placeholder for creating the test table in PostgreSQL.
	// You can implement the logic to create the necessary table structure here.
	// For example, you might use a SQL command like:
	// CREATE TABLE test_data (id SERIAL PRIMARY KEY, ext TEXT, vector TEXT);
	log.Println("Creating test table with auxin PostgreSQL...")
	query := `
		CREATE TABLE IF NOT EXISTS content_personal_vector_semantic_insight_vector_bge_m3_dense (
		id VARCHAR, -- 唯一标识（fileid_chunkid）
		routing_id VARCHAR NOT NULL, -- 对应 ES 的 routing
		chunk_id INT NOT NULL, -- 文档内容块的 ID
		chunk_type VARCHAR NOT NULL, -- 文档内容块的类型
		user_id BIGINT NOT NULL, -- 用户 ID
		creator BIGINT NOT NULL, -- 创建者 ID
		sharer BIGINT NOT NULL, -- 分享者 ID
		fileid BIGINT NOT NULL, -- 文档唯一标记
		group_id BIGINT NOT NULL, -- 圈子 ID
		ctime BIGINT NOT NULL, -- 创建时间
		mtime BIGINT NOT NULL, -- 最后修改时间
		y INT NOT NULL, -- 修改日期（年）
		ym INT NOT NULL, -- 修改日期（年月）
		ymd INT NOT NULL, -- 修改日期（年月日）
		ext VARCHAR(10) NOT NULL, -- 文件格式
		fsize BIGINT NOT NULL, -- 文件大小
		parent_id BIGINT NOT NULL, -- 目录 ID
		ftype VARCHAR(50) NOT NULL, -- 文件类型
		version BIGINT NOT NULL, -- 文件版本号
		index_update_time BIGINT NOT NULL, -- 全文更新时间
		ext_group VARCHAR(50) NOT NULL, -- 格式组
		vector vecf16(3) NOT NULL, -- 文档内容段的向量
		PRIMARY KEY (routing_id, fileid, id) 
		) using heap distributed BY (routing_id, fileid);

		ALTER TABLE content_personal_vector_semantic_insight_vector_bge_m3_dense ALTER COLUMN vector SET STORAGE PLAIN;
		CREATE INDEX insight_idx_fileid ON content_personal_vector_semantic_insight_vector_bge_m3_dense (fileid);
		CREATE INDEX insight_idx_group_id ON content_personal_vector_semantic_insight_vector_bge_m3_dense (group_id);
		CREATE INDEX insight_idx_vector ON content_personal_vector_semantic_insight_vector_bge_m3_dense 
		using vectors(vector vecf16_l2_ops) 
		WITH (options = $$
			optimizing.optimizing_threads = 3
			segment.max_growing_segment_size = 20000
			segment.max_sealed_segment_size = 10000000
			[indexing.hnsw]
			m=30
			ef_construction=100
			quantization.product.ratio = "x16"
		$$);

		CREATE TABLE IF NOT EXISTS content_personal_vector_semantic_insight_vector_bge_m3_dense_relyt_massive_group (
		id VARCHAR, -- 唯一标识（fileid_chunkid）
		routing_id VARCHAR NOT NULL, -- 对应 ES 的 routing
		chunk_id INT NOT NULL, -- 文档内容块的 ID
		chunk_type VARCHAR NOT NULL, -- 文档内容块的类型
		user_id BIGINT NOT NULL, -- 用户 ID
		creator BIGINT NOT NULL, -- 创建者 ID
		sharer BIGINT NOT NULL, -- 分享者 ID
		fileid BIGINT NOT NULL, -- 文档唯一标记
		group_id BIGINT NOT NULL, -- 圈子 ID
		ctime BIGINT NOT NULL, -- 创建时间
		mtime BIGINT NOT NULL, -- 最后修改时间
		y INT NOT NULL, -- 修改日期（年）
		ym INT NOT NULL, -- 修改日期（年月）
		ymd INT NOT NULL, -- 修改日期（年月日）
		ext VARCHAR(10) NOT NULL, -- 文件格式
		fsize BIGINT NOT NULL, -- 文件大小
		parent_id BIGINT NOT NULL, -- 目录 ID
		ftype VARCHAR(50) NOT NULL, -- 文件类型
		version BIGINT NOT NULL, -- 文件版本号
		index_update_time BIGINT NOT NULL, -- 全文更新时间
		ext_group VARCHAR(50) NOT NULL, -- 格式组
		vector vecf16(3) NOT NULL, -- 文档内容段的向量
		PRIMARY KEY (routing_id, fileid, id) 
		) using heap distributed BY (routing_id, fileid);

		ALTER TABLE content_personal_vector_semantic_insight_vector_bge_m3_dense_relyt_massive_group ALTER COLUMN vector SET STORAGE PLAIN;
		CREATE INDEX insight_idx_fileid_relyt_massive_group ON content_personal_vector_semantic_insight_vector_bge_m3_dense_relyt_massive_group (fileid);
		CREATE INDEX insight_idx_group_id_relyt_massive_group ON content_personal_vector_semantic_insight_vector_bge_m3_dense_relyt_massive_group (group_id);
		CREATE INDEX insight_idx_vector_relyt_massive_group ON content_personal_vector_semantic_insight_vector_bge_m3_dense_relyt_massive_group 
		using vectors(vector vecf16_l2_ops) 
		WITH (options = $$
			optimizing.optimizing_threads = 3
			segment.max_growing_segment_size = 20000
			segment.max_sealed_segment_size = 10000000
			[indexing.hnsw]
			m=30
			ef_construction=100
			quantization.product.ratio = "x16"
		$$);
	`

	if withAux {
		query += `
		CREATE TABLE IF NOT EXISTS relyt_sys.content_personal_vector_semantic_insight_vector_bge_m3_dense_relyt_routing (
		routing_id text PRIMARY KEY,
		store_table_name TEXT NOT NULL
	) USING heap DISTRIBUTED NONE;
	`
	}

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create test table: %w", err)
	}
	log.Println("Test table created successfully.")
	return nil
}

func DropTestWPSDataTaleWithAux(db *sql.DB) error {
	query := `
	DROP TABLE IF EXISTS content_personal_vector_semantic_insight_vector_bge_m3_dense;
	DROP TABLE IF EXISTS content_personal_vector_semantic_insight_vector_bge_m3_dense_relyt_massive_group;
	DROP TABLE IF EXISTS relyt_sys.content_personal_vector_semantic_insight_vector_bge_m3_dense_relyt_routing;
	`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to drop test table: %w", err)
	}
	log.Println("Test table dropped successfully.")
	return nil
}

func InsertTestWPSDataTaleWithAux(db *sql.DB) error {
	gen_bash := `
	python3 ../examples/gen_wps_data.py
	`
	_, err := exec.Command("bash", "-c", gen_bash).Output()
	if err != nil {
		return fmt.Errorf("failed to insert test data: %w", err)
	}
	log.Println("Test data inserted successfully.")
	return nil
}

// TestDeleteAndSearch 测试删除和搜索功能
func TestSearchBasic(t *testing.T) {
	// 初始化数据库连接
	db, processor := setupTestDatabase(t)
	defer db.Close()
	defer processor.Shutdown()

	// 测试搜索功能
	// 1. 测试基本搜索
	searchOptions := &SearchOptions{
		Columns:   []string{"id", "routing_id", "ext"},
		Condition: "id > $1",
		OrderBy:   "id ASC",
		Limit:     3,
	}
	result, err := processor.SearchV2(searchOptions, 2)
	if err != nil {
		t.Errorf("failed to search data: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows, but got %d", len(result.Rows))
	}

	// 2. 测试向量查询
	searchOptions = &SearchOptions{
		Columns:   []string{"id", " routing_id", "ext", "vector as distance"},
		Condition: "id > $1",
		OrderBy:   "id ASC",
		Limit:     2,
	}
	result, err = processor.SearchV2(searchOptions, 0)
	if err != nil {
		t.Errorf("failed to search with vector similarity: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, but got %d", len(result.Rows))
	}

	// 将结果转换为字符串进行比较
	row0Str := fmt.Sprintf("%v", result.Rows[0])
	row1Str := fmt.Sprintf("%v", result.Rows[1])
	expectedRow0Str := "[1 100 test1 [0.099975586, 0.19995117, 0.30004883]]"
	expectedRow1Str := "[2 100 test2 [0.19995117, 0.30004883, 0.39990234]]"

	if row0Str != expectedRow0Str {
		t.Errorf("result.Rows[0]: %v:%v, expected: %s", result.Columns, result.Rows[0], expectedRow0Str)
	}
	if row1Str != expectedRow1Str {
		t.Errorf("result.Rows[1]: %v:%v, expected: %s", result.Columns, result.Rows[1], expectedRow1Str)
	}

	// 3. 测试向量相似度搜索（使用参数化查询）
	searchOptions = &SearchOptions{
		Columns:   []string{"id", "routing_id", "ext", "vector <-> $2 as distance"},
		Condition: "id > $1",
		OrderBy:   "distance ASC",
		Limit:     2,
	}
	result, err = processor.SearchV2(searchOptions, 0, "[1,2,3]")
	if err != nil {
		t.Errorf("failed to search with vector similarity using parameters: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, but got %d", len(result.Rows))
	}

	// 将结果转换为字符串进行比较
	row0Str = fmt.Sprintf("%v", result.Rows[0])
	row1Str = fmt.Sprintf("%v", result.Rows[1])
	expectedRow0Str = "[10 140  4.05]"
	expectedRow1Str = "[9 140 <nil> 4.621504]"

	if row0Str != expectedRow0Str {
		t.Errorf("result.Rows[0]: %v:%v, expected: %s", result.Columns, result.Rows[0], expectedRow0Str)
	}
	if row1Str != expectedRow1Str {
		t.Errorf("result.Rows[1]: %v:%v, expected: %s", result.Columns, result.Rows[1], expectedRow1Str)
	}

}

func TestNewSearchFunc(t *testing.T) {
	dbConfig := InitDatabaseConfig("127.0.0.1", 7000, "postgres", "", "postgres")
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		t.Fatalf("failed to setup database: %v", err)
	}
	defer db.Close()
	if err != nil {
		t.Fatalf("failed to setup database: %v", err)
	}

	err = DropTestWPSDataTaleWithAux(db)
	if err != nil {
		t.Fatalf("failed to drop test table: %v", err)
	}

	err = CreateTestWPSDataTaleWithAux(db, true)
	if err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}

	err = InsertTestWPSDataTaleWithAux(db)
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	processor := NewProcessor(dbConfig, 6, "content_personal_vector_semantic_insight_vector_bge_m3_dense")
	defer processor.Shutdown()

	// test 1: select id, chunk_id from content_personal_vector_semantic_insight_vector_bge_m3_dense where group_id in (1, 11) and routing_id in ('1', '11') order by id asc limit 16;
	searchOptions := &SearchOptions{
		Columns:   []string{"id", "chunk_id"},
		Condition: "group_id IN (1, 11) AND routing_id IN ('1', '11')",
		OrderBy:   "id ASC",
		Limit:     16,
	}
	result, err := processor.SearchV2(searchOptions)
	if err != nil {
		t.Errorf("failed to search data: %v", err)
		return
	}

	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, but got %d", len(result.Rows))
	}

	log.Printf("TestNewSearchFunc test 1 result size: %d, result: %v", len(result.Rows), result)

	// test 2: with window function
	// select id, mtime, COUNT(*) OVER() AS total, vector <-> '[0.76,0.49, 0.67]' as score
	// from content_personal_vector_semantic_insight_vector_bge_m3_dense where group_id in (1, 11) and routing_id in ('1', '11') order by score ASC, mtime DESC LIMIT 16;
	searchOptions = &SearchOptions{
		Columns:   []string{"id", "group_id", "mtime", "COUNT(*) OVER()", "vector <-> '[0.76,0.49, 0.67]' as score"},
		Condition: "group_id IN (1, 11) AND routing_id IN ('1', '11')",
		OrderBy:   "score ASC, mtime DESC",
		Limit:     2,
	}
	result, err = processor.SearchV2(searchOptions)
	if err != nil {
		t.Errorf("failed to search data: %v", err)
		return
	}
	log.Printf("TestNewSearchFunc test 2 result size: %d, result: %v", len(result.Rows), result)

	// test 3: without window function
	// select id, mtime vector <-> '[0.76,0.49, 0.67]' as score
	// from content_personal_vector_semantic_insight_vector_bge_m3_dense where group_id in (1, 11) and routing_id in ('1', '11') order by score ASC, mtime DESC LIMIT 2;
	searchOptions = &SearchOptions{
		Columns:   []string{"id", "group_id", "mtime", "vector <-> '[0.76,0.49, 0.67]' as score"},
		Condition: "group_id IN (1, 11) AND routing_id IN ('1', '11')",
		OrderBy:   "score ASC, mtime DESC",
		Limit:     2,
	}
	result, err = processor.SearchV2(searchOptions)
	if err != nil {
		t.Errorf("failed to search data: %v", err)
		return
	}
	log.Printf("TestNewSearchFunc test 3 result size: %d, result: %v", len(result.Rows), result)

	// test 4: order by score desc
	// select id, mtime vector <-> '[0.76,0.49, 0.67]' as score
	// from content_personal_vector_semantic_insight_vector_bge_m3_dense where group_id in (1, 11) and routing_id in ('1', '11') order by score DESC LIMIT 2;
	searchOptions = &SearchOptions{
		Columns:   []string{"id", "group_id", "mtime", "vector <-> '[0.76,0.49, 0.67]' as score"},
		Condition: "group_id IN (1, 11) AND routing_id IN ('1', '11')",
		OrderBy:   "score DESC",
		Limit:     2,
	}

	_, err = processor.SearchV2(searchOptions)
	if err == nil {
		t.Errorf("expected error, but got nil")
		return
	}
	log.Printf("TestNewSearchFunc test 4 error: %v", err)

	// test 5: order by without vector
	// select id, mtime vector <-> '[0.76,0.49, 0.67]' as score
	// from content_personal_vector_semantic_insight_vector_bge_m3_dense where group_id in (1, 11) and routing_id in ('1', '11') order by mtime DESC LIMIT 2;
	searchOptions = &SearchOptions{
		Columns:   []string{"id", "group_id", "COUNT(*) OVER() as count", "mtime", "vector <-> '[0.76,0.49, 0.67]' as score"},
		Condition: "group_id IN (1, 11) AND routing_id IN ('1', '11')",
		OrderBy:   "mtime DESC",
		Limit:     2,
	}
	_, err = processor.SearchV2(searchOptions)
	if err != nil {
		t.Errorf("TestNewSearchFunc test 5 error: %v", err)
		return
	}
	log.Printf("TestNewSearchFunc test 5 result size: %d, result: %v", len(result.Rows), result)

	// test 6: group by not support
	// select group_id, count(*) from content_personal_vector_semantic_insight_vector_bge_m3_dense where group_id in (1, 11) and routing_id in ('1', '11') group by group_id;
	searchOptions = &SearchOptions{
		Columns:   []string{"group_id", "count(*)"},
		Condition: "group_id IN (1, 11) AND routing_id IN ('1', '11')",
		GroupBy:   "group_id",
	}
	_, err = processor.SearchV2(searchOptions)
	if err == nil {
		t.Errorf("TestNewSearchFunc test 6 error: %v", err)
		return
	}
	log.Printf("TestNewSearchFunc test 6 error: %v", err)

	// test 7: syntax error
	searchOptions = &SearchOptions{
		Columns:   []string{"id", "chunk_id"},
		Condition: "and group_id IN (1, 11) AND routing_id IN ('1', '11')",
		OrderBy:   "id ASC",
		Limit:     16,
	}
	rows, err := processor.SearchJsonRowsWithTimeoutV2(10000, searchOptions)
	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			log.Printf("TestNewSearchFunc test 7 PostgreSQL error: %s", pgErr.Message)
		} else {
			log.Printf("TestNewSearchFunc test 7 error: %v", err)
		}
		return
	}

	for rows.Next() {
		var jsonData []byte
		err = rows.Scan(&jsonData)
		if err != nil {
			log.Printf("TestNewSearchFunc test 7 scan error: %v", err)
			continue
		}

		// 解析 JSON 数据
		var row map[string]interface{}
		if err := json.Unmarshal(jsonData, &row); err != nil {
			log.Printf("TestNewSearchFunc test 7 JSON unmarshal error: %v", err)
			continue
		}

		// 方法1: 直接访问字段（推荐）
		if id, exists := row["id"]; exists {
			log.Printf("TestNewSearchFunc test 7 result: id=%v", id)
		}

		// 方法2: 类型断言获取具体类型
		if chunkId, exists := row["chunk_id"]; exists {
			if chunkIdInt, ok := chunkId.(float64); ok {
				// JSON 中的数字默认解析为 float64
				log.Printf("TestNewSearchFunc test 7 result: chunk_id=%d", int(chunkIdInt))
			} else {
				log.Printf("TestNewSearchFunc test 7 result: chunk_id=%v (type: %T)", chunkId, chunkId)
			}
		}

		// 方法3: 获取所有字段
		log.Printf("TestNewSearchFunc test 7 full result: %v", row)
	}

	if err := rows.Err(); err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			log.Printf("TestNewSearchFunc test 7 PostgreSQL error: %s", pgErr.Message)
		} else {
			log.Printf("TestNewSearchFunc test 7 error: %v", err)
		}
	}

	// test 8: count(*)
	searchOptions = &SearchOptions{
		Columns:   []string{"count(*)"},
		Condition: "group_id IN (1, 11) AND routing_id IN ('1', '11')",
	}
	_, err = processor.SearchJsonRowsWithTimeoutV2(10000, searchOptions)

	if err == nil {
		t.Errorf("TestNewSearchFunc test 8 error: %v", err)
		return
	}
	log.Printf("TestNewSearchFunc test 8 error: %v", err)

	// test 9: select * from content_personal_vector_semantic_insight_vector_bge_m3_dense where group_id in (1, 11) and routing_id in ('1', '11');
	searchOptions = &SearchOptions{
		Columns:   []string{"*"},
		Condition: "group_id IN (1, 11) AND routing_id IN ('1', '11')",
	}
	_, err = processor.SearchJsonRowsWithTimeoutV2(10000, searchOptions)
	if err != nil {
		log.Printf("TestNewSearchFunc test 9 error: %v", err)
	}

	// test 10: select id, group_id, mtime, count(*) over() as count, vector <-> '[0.76,0.49, 0.67]' as score from test_table where group_id in (1, 11) and routing_id in ('1', '11') order by score ASC, mtime DESC limit 2;
	searchOptions = &SearchOptions{
		Columns:   []string{"id", "group_id", "mtime", "COUNT(*) OVER() as count", "vector <-> '[0.76,0.49, 0.67]' as score"},
		Condition: "group_id IN (1, 11) AND routing_id IN ('1', '11')",
		OrderBy:   "score ASC, mtime DESC",
		Limit:     2,
	}
	rows, err = processor.SearchJsonRowsWithTimeoutV2(10000, searchOptions)
	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			log.Printf("TestNewSearchFunc test 10 PostgreSQL error: %s, %s", pgErr.Message, pgErr.Detail)
		} else {
			log.Printf("TestNewSearchFunc test 10 error: %v", err)
		}
		return
	}

	for rows.Next() {
		var jsonData []byte
		err = rows.Scan(&jsonData)
		if err != nil {
			t.Errorf("TestNewSearchFunc test 10 rows.Scan error: %v", err)

		}

		log.Printf("TestNewSearchFunc test 10 jsonData: %v", string(jsonData))

		// 解析 JSON 数据
		var row map[string]interface{}
		if err := json.Unmarshal(jsonData, &row); err != nil {
			t.Errorf("TestNewSearchFunc test 10 JSON unmarshal error: %v", err)
			continue
		}

		// 获取具体的字段值
		id, idExists := row["id"]
		chunkId, chunkIdExists := row["group_id"]
		mtime, mtimeExists := row["mtime"]
		count, countExists := row["count"]
		score, scoreExists := row["score"]

		if idExists && chunkIdExists && mtimeExists && scoreExists && countExists {
			log.Printf("TestNewSearchFunc test 10 result: id=%v, group_id=%v, mtime=%v, score=%v, count=%v", id, chunkId, mtime, score, count)
		} else {
			log.Printf("TestNewSearchFunc test 10 result: %v (missing fields)", row)
		}
	}

	if err := rows.Err(); err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			log.Printf("TestNewSearchFunc test 10 PostgreSQL error: %s, %s", pgErr.Message, pgErr.Detail)
		} else {
			log.Printf("TestNewSearchFunc test 10 error: %v", err)
		}
	}

	// test 11: select id, group_id, count(*) over() count, mtime, vector <-> '[0.76,0.49, 0.67]' score from test_table where group_id in (1, 11) and routing_id in ('1', '11') order by score ASC, mtime DESC limit 2;
	searchOptions = &SearchOptions{
		Columns:   []string{"id", "group_id", "COUNT(*) OVER() Count", "mtime", "vector <-> '[0.76,0.49, 0.67]' score"},
		Condition: "group_id IN (1, 11) AND routing_id IN ('1', '11')",
		OrderBy:   "score ASC, mtime DESC",
		Limit:     2,
	}
	rows, err = processor.SearchJsonRowsWithTimeoutV2(10000, searchOptions)
	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			log.Printf("TestNewSearchFunc test 11 PostgreSQL error: %s, %s", pgErr.Message, pgErr.Detail)
		} else {
			log.Printf("TestNewSearchFunc test 11 error: %v", err)
		}
		return
	}

	for rows.Next() {
		var jsonData []byte
		err = rows.Scan(&jsonData)
		if err != nil {
			t.Errorf("TestNewSearchFunc test 11 rows.Scan error: %v", err)

		}

		log.Printf("TestNewSearchFunc test 11 jsonData: %v", string(jsonData))
	}

	if err := rows.Err(); err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			log.Printf("TestNewSearchFunc test 11 PostgreSQL error: %s, %s", pgErr.Message, pgErr.Detail)
		} else {
			log.Printf("TestNewSearchFunc test 11 error: %v", err)
		}
	}
}

func TestSearchAdditional(t *testing.T) {
	db, processor := setupTestDatabase(t)
	defer db.Close()
	defer processor.Shutdown()
	// 1. 基本查询：select id, routing_id, ext, vector from test_routing_data where id in (1, 2, 5, 6) order by id asc limit 5
	searchOptions := &SearchOptions{
		Columns:   []string{"id", "routing_id", "ext", "vector"},
		Condition: "id in ($1)",
		OrderBy:   "id ASC",
		Limit:     5,
	}
	result, err := processor.SearchV2(searchOptions, []interface{}{1, 2, 5, 6})
	if err != nil {
		t.Errorf("failed to search data: %v", err)
	}
	if len(result.Rows) != 4 {
		t.Errorf("expected 4 rows, but got %d", len(result.Rows))
	}
	log.Printf("Test 1 result size: %d, result: %v", len(result.Rows), result)

	// 2. 带别名的列表达式：select id, routing_id, vector <-> '[1,2,3]' as distance from test_routing_data order by distance asc limit 3
	searchOptions = &SearchOptions{
		Columns: []string{"id", "routing_id", "vector <-> '[1,2,3]' as distance"},
		OrderBy: "distance ASC",
		Limit:   3,
	}
	result, err = processor.SearchV2(searchOptions)
	if err != nil {
		t.Errorf("failed to search data: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows, but got %d", len(result.Rows))
	}
	log.Printf("Test 2 result size: %d, result: %v", len(result.Rows), result)

	// 3. 复杂条件查询：select * from test_routing_data where id > 3 and routing_id = 100
	searchOptions = &SearchOptions{
		Columns:   []string{"*"},
		Condition: "id > $1 and routing_id = $2",
		OrderBy:   "id DESC",
		Limit:     5,
	}
	result, err = processor.SearchV2(searchOptions, 3, 100)
	if err != nil {
		t.Errorf("failed to search data: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows, but got %d", len(result.Rows))
	}
	log.Printf("Test 3 result size: %d, result: %v", len(result.Rows), result)

	// 4. 带计算和别名的列：select id, routing_id, (vector <-> '[1,2,3]') * 2 as double_distance from test_routing_data order by double_distance asc limit 3
	searchOptions = &SearchOptions{
		Columns: []string{"id", "routing_id", "(vector <-> '[1,2,3]') * 2 as double_distance"},
		OrderBy: "double_distance ASC",
		Limit:   3,
	}
	result, err = processor.SearchV2(searchOptions)
	if err != nil {
		t.Errorf("failed to search data: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows, but got %d", len(result.Rows))
	}
	expectedResult0 := "[10 140 8.100000381469727]"
	expectedResult1 := "[9 140 9.24300765991211]"
	expectedResult2 := "[8 130 10.500585556030273]"
	if fmt.Sprintf("%v", result.Rows[0]) != expectedResult0 {
		t.Errorf("expected result: %v, but got %v", expectedResult0, result.Rows[0])
	}
	if fmt.Sprintf("%v", result.Rows[1]) != expectedResult1 {
		t.Errorf("expected result: %v, but got %v", expectedResult1, result.Rows[1])
	}
	if fmt.Sprintf("%v", result.Rows[2]) != expectedResult2 {
		t.Errorf("expected result: %v, but got %v", expectedResult2, result.Rows[2])
	}

	// 5. 多条件组合查询：select id, routing_id routingid from test_routing_data where id in ($1) and routing_id > 100
	searchOptions = &SearchOptions{
		Columns:   []string{"id", "routing_id routingid"},
		Condition: "id in ($1) and routing_id > $2",
		OrderBy:   "id ASC",
		Limit:     10,
	}
	result, err = processor.SearchV2(searchOptions, []interface{}{1, 2, 3, 4, 5}, 100)
	if err != nil {
		t.Errorf("failed to search data: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows, but got %d", len(result.Rows))
	}
	log.Printf("Test 5 result size: %d, result: %v", len(result.Rows), result)

	// 6. 带分页的查询：select * from test_routing_data order by id desc limit 5 offset 2
	searchOptions = &SearchOptions{
		Columns: []string{"*"},
		OrderBy: "id DESC",
		Limit:   5,
		Offset:  2,
	}
	result, err = processor.SearchV2(searchOptions)
	if err != nil {
		t.Errorf("failed to search data: %v", err)
	}
	if len(result.Rows) != 5 {
		t.Errorf("expected 5 rows, but got %d", len(result.Rows))
	}
	log.Printf("Test 6 result size: %d, result: %v", len(result.Rows), result)

	// 7. 带 NULL 值处理的查询：select id, routing_id from test_routing_data where ext is not null
	searchOptions = &SearchOptions{
		Columns:   []string{"id", "routing_id"},
		Condition: "ext is not null",
		OrderBy:   "id ASC",
	}
	result, err = processor.SearchV2(searchOptions)
	if err != nil {
		t.Errorf("failed to search data: %v", err)
	}
	if len(result.Rows) != 8 {
		t.Errorf("expected 8 rows, but got %d", len(result.Rows))
	}
	log.Printf("Test 7 result size: %d, result: %v", len(result.Rows), result)

	// 8. 带数组参数的复杂条件：select * from test_routing_data where id in ($1) and routing_id in ($2)
	searchOptions = &SearchOptions{
		Columns:   []string{"*"},
		Condition: "id in ($1) and routing_id in ($2)",
		OrderBy:   "id ASC",
	}
	result, err = processor.SearchV2(searchOptions, []interface{}{1, 3, 5, 7}, []interface{}{100, 120})
	if err != nil {
		t.Errorf("failed to search data: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, but got %d", len(result.Rows))
	}
	log.Printf("Test 8 result size: %d, result: %v", len(result.Rows), result)

	// 9. group by: select routing_id, count(*) from test_routing_data group by routing_id
	searchOptions = &SearchOptions{
		Columns: []string{"routing_id", "count(*)"},
		GroupBy: "routing_id",
	}
	result, err = processor.SearchV2(searchOptions)
	if err != nil {
		t.Errorf("failed to search data: %v", err)
	}
	if len(result.Rows) != 5 {
		t.Errorf("expected 5 rows, but got %d", len(result.Rows))
	}

	log.Printf("Test 9 result size: %d, result: %v", len(result.Rows), result)

	// 10. having: select routing_id, count(*) from test_routing_data group by routing_id having count(*) > 1
	searchOptions = &SearchOptions{
		Columns: []string{"routing_id", "count(*)"},
		GroupBy: "routing_id",
		Having:  "count(*) > 2",
	}
	result, err = processor.SearchV2(searchOptions)
	if err != nil {
		t.Errorf("failed to search data: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows, but got %d", len(result.Rows))
	}

	log.Printf("Test 10 result size: %d, result: %v", len(result.Rows), result)

	// 11. 组合条件查询: select routing_id, count(*) from test_routing_data_relyt_massive_group where id > 0 and routing_id in (100,110,120) group by routing_id having count(*) > 0 order by routing_id asc limit 5 offset 1
	searchOptions = &SearchOptions{
		Columns:   []string{"routing_id", "COUNT(*)"},
		Condition: "id > $1 and routing_id in ($2)",
		GroupBy:   "routing_id",
		Having:    "count(*) > 0",
		OrderBy:   "routing_id ASC",
		Limit:     5,
		Offset:    1,
	}
	result, err = processor.SearchV2(searchOptions, 0, []interface{}{100, 110, 120})
	if err != nil {
		t.Errorf("failed to search data: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, but got %d", len(result.Rows))
	}
	log.Printf("Test 11 result size: %d, result: %v", len(result.Rows), result)

	// 12. count(*): select count(*) from test_routing_data where id in ($1) limit 5
	searchOptions = &SearchOptions{
		Columns:   []string{"count(*)"},
		Condition: "id in ($1)",
	}
	result, err = processor.SearchV2(searchOptions, []interface{}{1, 2, 3, 4, 5})
	if err != nil {
		t.Errorf("failed to search data: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 rows, but got %d", len(result.Rows))
	}
	log.Printf("Test 12 result size: %d, result: %v", len(result.Rows), result)

	// 13. select null: select id, routing_id, ext from test_routing_data where ext is null or ext = ''
	searchOptions = &SearchOptions{
		Columns:   []string{"id", "routing_id as routingid", "ext"},
		Condition: "ext is null or ext = ''",
	}
	result, err = processor.SearchV2(searchOptions)
	if err != nil {
		t.Errorf("failed to search data: %v", err)
	}

	// check result
	if len(result.Rows) != 4 {
		t.Errorf("expected 4 rows, but got %d", len(result.Rows))
	}
	log.Printf("Test 13 result size: %d, result: %v", len(result.Rows), result)

	columns := len(result.Columns)

	for _, row := range result.Rows {
		for i := 0; i < columns; i++ {
			log.Printf("Test 13 result column: %d, value: %v", i, row[i])
		}
	}

	// 14. select json: select id, routing_id, ext from test_routing_data where id in ($1) order by id asc
	searchOptions = &SearchOptions{
		Columns:   []string{"id", "routing_id", "ext"},
		Condition: "id in ($1)",
		OrderBy:   "id DESC",
	}
	jsonResults, err := processor.SearchJsonV2(searchOptions, []interface{}{1, 5, 2, 6})
	if err != nil {
		t.Errorf("Test 14 failed: %v", err)
	}
	if len(jsonResults) != 4 {
		t.Errorf("Test 14 expected 4 records, got %d", len(jsonResults))
	}

	// 解析每个JSON记录
	var jsonRecords []map[string]interface{}
	for _, jsonResult := range jsonResults {
		var record map[string]interface{}
		if err := json.Unmarshal(jsonResult, &record); err != nil {
			t.Errorf("Test 14 unmarshal failed: %v", err)
		}
		jsonRecords = append(jsonRecords, record)
	}

	log.Printf("Test 14 result column: %v", jsonRecords)

	// 15. select json: select id, routing_id, ext from test_routing_data where id in ($1) order by id asc
	jsonResults, err = processor.SearchJsonV2(searchOptions, []interface{}{999, 1000})
	if err != nil {
		t.Errorf("Test 15 failed: %v", err)
	}
	if len(jsonResults) != 0 {
		t.Errorf("Test 15 expected empty array, got %d records", len(jsonResults))
	}
	log.Printf("Test 15 result column: %v", jsonResults)

	// 16. select json: select id, routing_id, ext from test_routing_data where id in ($1) order by id asc
	rows, err := processor.SearchJsonRowsV2(searchOptions, []interface{}{1, 2, 5, 6})
	if err != nil {
		t.Errorf("Test 16 failed: %v", err)
	}
	defer rows.Close()
	var rowCount int
	for rows.Next() {
		var resultJSON []byte
		if err := rows.Scan(&resultJSON); err != nil {
			t.Errorf("Test 16 scan failed: %v", err)
			continue
		}
		var record map[string]interface{}
		if err := json.Unmarshal(resultJSON, &record); err != nil {
			t.Errorf("Test 16 unmarshal failed: %v", err)
			continue
		}
		rowCount++
		log.Printf("Test 16 row: %v", record)
	}
	if rowCount != 4 {
		t.Errorf("Test 16 expected 4 rows, got %d", rowCount)
	}

	// 17. select json: select id, routing_id, ext from test_routing_data where id in ($1) order by id asc
	rows, err = processor.SearchJsonRowsV2(searchOptions, []interface{}{999, 1000})
	if err != nil {
		t.Errorf("Test 17 failed: %v", err)
	}
	defer rows.Close()
	rowCount = 0
	for rows.Next() {
		rowCount++
	}
	if rowCount != 0 {
		t.Errorf("Test 17 expected 0 rows, got %d", rowCount)
	}
	log.Printf("Test 17 result column: %v", rowCount)

}
