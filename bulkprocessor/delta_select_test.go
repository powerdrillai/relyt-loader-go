package bulkprocessor

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"testing"
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

	// SearchJsonV2 测试1：基本功能
	searchOptions = &SearchOptions{
		Columns:   []string{"id", "routing_id", "ext"},
		Condition: "id in ($1)",
		OrderBy:   "id ASC",
	}
	jsonResults, err := processor.SearchJsonV2(searchOptions, []interface{}{1, 2, 5, 6})
	if err != nil {
		t.Errorf("SearchJsonV2 basic failed: %v", err)
	}
	if len(jsonResults) != 4 {
		t.Errorf("SearchJsonV2 basic expected 4 records, got %d", len(jsonResults))
	}

	// 解析每个JSON记录
	var jsonRecords []map[string]interface{}
	for _, jsonResult := range jsonResults {
		var record map[string]interface{}
		if err := json.Unmarshal(jsonResult, &record); err != nil {
			t.Errorf("SearchJsonV2 basic unmarshal failed: %v", err)
		}
		jsonRecords = append(jsonRecords, record)
	}

	log.Printf("SearchJsonV2 basic: %v", jsonRecords)

	// SearchJsonV2 测试2：空结果
	jsonResults, err = processor.SearchJsonV2(searchOptions, []interface{}{999, 1000})
	if err != nil {
		t.Errorf("SearchJsonV2 empty failed: %v", err)
	}
	if len(jsonResults) != 0 {
		t.Errorf("SearchJsonV2 empty expected empty array, got %d records", len(jsonResults))
	}
	log.Printf("SearchJsonV2 empty: %d records", len(jsonResults))

	// SearchJsonRowsV2 测试1：基本功能
	rows, err := processor.SearchJsonRowsV2(searchOptions, []interface{}{1, 2, 5, 6})
	if err != nil {
		t.Errorf("SearchJsonRowsV2 basic failed: %v", err)
	}
	defer rows.Close()
	var rowCount int
	for rows.Next() {
		var resultJSON []byte
		if err := rows.Scan(&resultJSON); err != nil {
			t.Errorf("SearchJsonRowsV2 basic scan failed: %v", err)
			continue
		}
		var record map[string]interface{}
		if err := json.Unmarshal(resultJSON, &record); err != nil {
			t.Errorf("SearchJsonRowsV2 basic unmarshal failed: %v", err)
			continue
		}
		rowCount++
		log.Printf("SearchJsonRowsV2 basic row: %v", record)
	}
	if rowCount != 4 {
		t.Errorf("SearchJsonRowsV2 basic expected 4 rows, got %d", rowCount)
	}

	// SearchJsonRowsV2 测试2：空结果
	rows, err = processor.SearchJsonRowsV2(searchOptions, []interface{}{999, 1000})
	if err != nil {
		t.Errorf("SearchJsonRowsV2 empty failed: %v", err)
	}
	defer rows.Close()
	rowCount = 0
	for rows.Next() {
		rowCount++
	}
	if rowCount != 0 {
		t.Errorf("SearchJsonRowsV2 empty expected 0 rows, got %d", rowCount)
	}
	log.Printf("SearchJsonRowsV2 empty: %d rows", rowCount)

}
