package bulkprocessor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
)

type TestDataV2 struct {
	ID        int            `json:"id"`
	Name      string         `json:"name"`
	Age       int            `json:"age"`
	Email     string         `json:"email"`
	Tags      []string       `json:"tags"`
	Metadata  map[string]any `json:"metadata"`
	IsActive  bool           `json:"is_active"`
	CreatedAt int64          `json:"created_at"`
}

func CreateTestUsersV2WithAux(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS test_users_v1 (
		id bigint NOT NULL PRIMARY KEY,
		fileid bigint NOT NULL,
		routing_id text NOT NULL,
		ext text NOT NULL,
		vector vecf16(3) NOT NULL,
		version bigint,
		tags TEXT[],
		metadata JSONB
	);

	CREATE TABLE IF NOT EXISTS test_users_v2 (
		id bigint NOT NULL PRIMARY KEY,
		fileid bigint NOT NULL,
		routing_id text NOT NULL,
		ext text NOT NULL,
		vector vecf16(3) NOT NULL,
		version bigint,
		tags TEXT[],
		metadata JSONB
	);

	INSERT INTO test_users_v1 VALUES
	(1, 100, '100', 'ext1', '[1,2,3]', 1, ARRAY['tag1', 'tag2'], '{"key1": "value1", "key2": "value2"}'),
	(2, 110, '110', 'ext2', '[4,5,6]', 1, ARRAY['tag3', 'tag4'], '{"key3": "value3", "key4": "value4"}'),
	(3, 120, '120', 'ext3', '[7,8,9]', 1, ARRAY['tag5', 'tag6'], '{"key5": "value5", "key6": "value6"}'),
	(4, 130, '130', 'ext4', '[10,11,12]', 1, ARRAY['tag7', 'tag8'], '{"key7": "value7", "key8": "value8"}'),
	(5, 140, '140', 'ext5', '[13,14,15]', 1, ARRAY['tag9', 'tag10'], '{"key9": "value9", "key10": "value10"}'),
	(6, 150, '150', 'ext6', '[16,17,18]', 1, ARRAY['tag11', 'tag12'], '{"key11": "value11", "key12": "value12"}'),
	(7, 160, '160', 'ext7', '[19,20,21]', 1, ARRAY['tag13', 'tag14'], '{"key13": "value13", "key14": "value14"}'),
	(8, 170, '170', 'ext8', '[22,23,24]', 1, ARRAY['tag15', 'tag16'], '{"key15": "value15", "key16": "value16"}'),
	(9, 180, '180', 'ext9', '[25,26,27]', 1, ARRAY['tag17', 'tag18'], '{"key17": "value17", "key18": "value18"}'),
	(10, 190, '190', 'ext10', '[28,29,30]', 1, ARRAY['tag19', 'tag20'], '{"key19": "value19", "key20": "value20"}');

	INSERT INTO test_users_v2 VALUES
	(4, 130, '130', 'ext4', '[10,11,12]', 1, ARRAY['tag7', 'tag8'], '{"key7": "value7", "key8": "value8"}'),
	(5, 140, '140', 'ext5', '[13,14,15]', 1, ARRAY['tag9', 'tag10'], '{"key9": "value9", "key10": "value10"}'),
	(6, 150, '150', 'ext6', '[16,17,18]', 1, ARRAY['tag11', 'tag12'], '{"key11": "value11", "key12": "value12"}'),
	(7, 160, '160', 'ext7', '[19,20,21]', 1, ARRAY['tag13', 'tag14'], '{"key13": "value13", "key14": "value14"}'),
	(8, 170, '170', 'ext8', '[22,23,24]', 1, ARRAY['tag15', 'tag16'], '{"key15": "value15", "key16": "value16"}'),
	(9, 180, '180', 'ext9', '[25,26,27]', 1, ARRAY['tag17', 'tag18'], '{"key17": "value17", "key18": "value18"}'),
	(10, 190, '190', 'ext10', '[28,29,30]', 1, ARRAY['tag19', 'tag20'], '{"key19": "value19", "key20": "value20"}');

	CREATE TABLE IF NOT EXISTS test_users_v2_relyt_massive_group (
		id bigint NOT NULL PRIMARY KEY,
		fileid bigint NOT NULL,
		routing_id text NOT NULL,
		ext text NOT NULL,
		vector vecf16(3) NOT NULL,
		version bigint,
		tags TEXT[],
		metadata JSONB
	);

	INSERT INTO test_users_v2_relyt_massive_group VALUES
	(1, 100, '100', 'ext1', '[1,2,3]', 1, ARRAY['tag1', 'tag2'], '{"key1": "value1", "key2": "value2"}'),
	(2, 110, '110', 'ext2', '[4,5,6]', 1, ARRAY['tag3', 'tag4'], '{"key3": "value3", "key4": "value4"}'),
	(3, 120, '120', 'ext3', '[7,8,9]', 1, ARRAY['tag5', 'tag6'], '{"key5": "value5", "key6": "value6"}');

	CREATE TABLE IF NOT EXISTS relyt_sys.test_users_v2_relyt_routing (
		routing_id text PRIMARY KEY,
		store_table_name text NOT NULL
	);

	INSERT INTO relyt_sys.test_users_v2_relyt_routing VALUES ('100', 'test_users_v2_relyt_massive_group');
	INSERT INTO relyt_sys.test_users_v2_relyt_routing VALUES ('110', 'test_users_v2_relyt_massive_group');
	INSERT INTO relyt_sys.test_users_v2_relyt_routing VALUES ('120', 'test_users_v2_relyt_massive_group');
	`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create test table: %w", err)
	}
	log.Println("Test table created successfully.")
	return nil
}

func DropTestUsersV2WithAux(db *sql.DB) error {
	log.Println("Dropping test table with aux in PostgreSQL...")
	query := `
	DROP TABLE IF EXISTS test_users_v1;
	DROP TABLE IF EXISTS test_users_v2;
	DROP TABLE IF EXISTS test_users_v2_relyt_massive_group;
	DROP TABLE IF EXISTS relyt_sys.test_users_v2_relyt_routing;
	`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to drop test table: %w", err)
	}
	log.Println("Test table dropped successfully.")
	return nil
}

func TestUpdateByQueryV2(t *testing.T) {
	dbConfig := InitDatabaseConfig("127.0.0.1", 7000, "postgres", "", "postgres")
	db, err := SetupDataBase(dbConfig)
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	}

	defer db.Close()

	err = DropTestUsersV2WithAux(db)
	if err != nil {
		log.Fatalf("failed to drop test table: %v", err)
	}

	err = CreateTestUsersV2WithAux(db)
	if err != nil {
		log.Fatalf("failed to create test table: %v", err)
	}

	processor := NewProcessor(dbConfig, 6, "null")
	defer processor.Shutdown()

	// update test_users_v2 set ext = 'updated_ext', version = 2 where id = 1;
	t.Run("UpdateNormalFields", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		options := &UpdateByQueryOptions{
			Table:     "test_users_v1",
			Condition: "id = $1",
			Updates: map[string]any{
				"ext":     "updated_ext",
				"version": 2,
			},
		}

		updatedCount, err := processor.UpdateByQueryWithContextV2(ctx, options, 1)
		if err != nil {
			t.Fatalf("Failed to update normal fields: %v", err)
		}

		if updatedCount != 1 {
			t.Errorf("Expected 1 record to be updated, got %d", updatedCount)
		}

		t.Logf("Updated %d records with normal fields", updatedCount)
		t.Logf("SQL: %s", options.FinalSQL)

		// 验证更新结果
		var ext string
		var version int64
		err = db.QueryRow("SELECT ext, version FROM test_users_v1 WHERE id = $1", 1).
			Scan(&ext, &version)
		if err != nil {
			t.Fatalf("Failed to verify update: %v", err)
		}

		if ext != "updated_ext" {
			t.Errorf("Expected ext to be 'updated_ext', got '%s'", ext)
		}
		if version != 2 {
			t.Errorf("Expected version to be 2, got %d", version)
		}
	})

	t.Run("UpdateNormalFieldsV2", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		options := &UpdateByQueryOptions{
			Table:     "test_users_v2",
			Condition: "id = $1",
			Updates: map[string]any{
				"ext":     "updated_ext",
				"version": 2,
			},
		}

		updatedCount, err := processor.UpdateByQueryWithContextV2(ctx, options, 1)
		if err != nil {
			t.Fatalf("Failed to update normal fields: %v", err)
		}

		if updatedCount != 1 {
			t.Errorf("Expected 1 record to be updated, got %d", updatedCount)
		}

		t.Logf("Updated %d records with normal fields", updatedCount)
		t.Logf("SQL: %s", options.FinalSQL)

		// 验证更新结果
		var ext string
		var version int64
		err = db.QueryRow("SELECT ext, version FROM test_users_v2_relyt_massive_group WHERE id = $1", 1).
			Scan(&ext, &version)
		if err != nil {
			t.Fatalf("Failed to verify update: %v", err)
		}

		if ext != "updated_ext" {
			t.Errorf("Expected ext to be 'updated_ext', got '%s'", ext)
		}
		if version != 2 {
			t.Errorf("Expected version to be 2, got %d", version)
		}
	})

	// update test_users_v2 set tags = ARRAY['updated_tag1', 'updated_tag2', 'updated_tag3'] where id = 2;
	t.Run("UpdateArrayFields", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		options := &UpdateByQueryOptions{
			Table:     "test_users_v2",
			Condition: "id = $1",
			Updates: map[string]any{
				"tags": "{updated_tag1, updated_tag2, updated_tag3}",
			},
		}

		updatedCount, err := processor.UpdateByQueryWithContextV2(ctx, options, 2)
		if err != nil {
			t.Fatalf("Failed to update array fields: %v", err)
		}

		if updatedCount != 1 {
			t.Errorf("Expected 1 record to be updated, got %d", updatedCount)
		}

		t.Logf("Updated %d records with array fields", updatedCount)

		// 验证更新结果
		var tags string
		err = db.QueryRow("SELECT tags::text FROM test_users_v2_relyt_massive_group WHERE id = $1", 2).
			Scan(&tags)
		if err != nil {
			t.Fatalf("Failed to verify array update: %v", err)
		}

		expectedTags := "{updated_tag1,updated_tag2,updated_tag3}"
		if len(tags) != len(expectedTags) {
			t.Errorf("Expected %d tags, got %d", len(expectedTags), len(tags))
		}
		if tags != expectedTags {
			t.Errorf("Expected tags to be '%s', got '%s'", expectedTags, tags)
		}
	})

	// update test_users_v2 set metadata = '{"status": "updated", "permissions": ["read", "write", "delete"], "updated_at": "2025-01-01 00:00:00"}' where id = 3;
	t.Run("UpdateJSONFields", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// 准备新的JSON数据
		newMetadata := map[string]any{
			"status":      "updated",
			"permissions": []string{"read", "write", "delete"},
			"updated_at":  time.Now().Unix(),
		}
		metadataJSON, _ := json.Marshal(newMetadata)

		options := &UpdateByQueryOptions{
			Table:     "test_users_v2",
			Condition: "id = $1",
			Updates: map[string]any{
				"metadata": string(metadataJSON),
			},
		}

		updatedCount, err := processor.UpdateByQueryWithContextV2(ctx, options, 3)
		if err != nil {
			t.Fatalf("Failed to update JSON fields: %v", err)
		}

		if updatedCount != 1 {
			t.Errorf("Expected 1 record to be updated, got %d", updatedCount)
		}

		t.Logf("Updated %d records with JSON fields", updatedCount)

		// 验证更新结果
		var metadataStr string
		err = db.QueryRow("SELECT metadata FROM test_users_v2_relyt_massive_group WHERE id = $1", 3).
			Scan(&metadataStr)
		if err != nil {
			t.Fatalf("Failed to verify JSON update: %v", err)
		}

		var actualMetadata map[string]any
		err = json.Unmarshal([]byte(metadataStr), &actualMetadata)
		if err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		if actualMetadata["status"] != "updated" {
			t.Errorf("Expected status to be 'updated', got '%v'", actualMetadata["status"])
		}

		permissions, ok := actualMetadata["permissions"].([]any)
		if !ok || len(permissions) != 3 {
			t.Errorf("Expected 3 permissions, got %v", permissions)
		}
	})

	// UPDATE public.test_users_v2 SET ext = 'vip_ext', tags = ARRAY['vip', 'premium', 'admin'], version = '3', metadata = '{"permissions":["read","write","delete","admin"],"status":"vip","upgraded_at":1756977125}' WHERE id > 1;
	t.Run("UpdateAllFieldTypes", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// 准备新的JSON数据
		newMetadata := map[string]any{
			"status":      "vip",
			"permissions": []string{"read", "write", "delete", "admin"},
			"upgraded_at": time.Now().Unix(),
		}
		metadataJSON, _ := json.Marshal(newMetadata)

		options := &UpdateByQueryOptions{
			Table:     "test_users_v2",
			Condition: "id > $1",
			Updates: map[string]any{
				"ext":      "vip_ext",
				"version":  3,
				"tags":     "ARRAY['vip', 'premium', 'admin']",
				"metadata": string(metadataJSON),
			},
		}

		updatedCount, err := processor.UpdateByQueryWithContextV2(ctx, options, 1)
		if err != nil {
			t.Fatalf("Failed to update all field types: %v", err)
		}

		if updatedCount != 2 { // id 2 and 3
			t.Errorf("Expected 2 records to be updated, got %d", updatedCount)
		}

		t.Logf("Updated %d records with all field types", updatedCount)

		// 验证更新结果
		rows, err := db.Query("SELECT id, ext, version, tags::text, metadata FROM test_users_v2_relyt_massive_group WHERE ext = 'vip_ext'")
		if err != nil {
			t.Fatalf("Failed to verify all field types update: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var id int64
			var ext string
			var version int64
			var tags string
			var metadataStr string

			err := rows.Scan(&id, &ext, &version, &tags, &metadataStr)
			if err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}

			if ext != "vip_ext" {
				t.Errorf("Expected ext to be 'vip_ext', got '%s'", ext)
			}
			if version != 3 {
				t.Errorf("Expected version to be 3, got %d", version)
			}

			expectedTags := "{vip,premium,admin}"
			if len(tags) != len(expectedTags) {
				t.Errorf("Expected %d tags, got %d", len(expectedTags), len(tags))
			}

			var metadata map[string]any
			err = json.Unmarshal([]byte(metadataStr), &metadata)
			if err != nil {
				t.Fatalf("Failed to unmarshal metadata: %v", err)
			}

			if metadata["status"] != "vip" {
				t.Errorf("Expected status to be 'vip', got '%v'", metadata["status"])
			}

			count++
		}

		if count != 2 {
			t.Errorf("Expected 2 records with ext 'vip_ext', got %d", count)
		}
	})

	// update test_users_v2 set tags = array_replace(tags, 'tag1', 'updated_tag1') where id = 1;
	t.Run("UpdateArrayWithFunctions", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// 使用array_replace函数更新数组
		options := &UpdateByQueryOptions{
			Table:     "test_users_v2",
			Condition: "id = $1",
			Updates: map[string]any{
				"tags": "array_replace(tags, $2, $3)",
			},
		}

		updatedCount, err := processor.UpdateByQueryWithContextV2(ctx, options, 1, "tag1", "updated_tag1")
		if err != nil {
			t.Fatalf("Failed to update array with function: %v", err)
		}

		log.Printf("result: %v", updatedCount)

		// 验证更新结果
		var tags string
		err = db.QueryRow("SELECT tags::text FROM test_users_v2_relyt_massive_group WHERE id = $1", 1).
			Scan(&tags)
		if err != nil {
			t.Fatalf("Failed to verify array function update: %v", err)
		}

		// 检查是否包含"updated_tag1"而不是"tag1"
		expectedTags := "{updated_tag1,tag2}"
		if tags != expectedTags {
			t.Errorf("Expected tags to be '%s', got '%s'", expectedTags, tags)
		}
	})

	t.Run("ErrorHandling", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// test empty updates
		options := &UpdateByQueryOptions{
			Table:     "test_users_v2",
			Condition: "id = $1",
			Updates:   map[string]any{},
		}

		_, err := processor.UpdateByQueryWithContextV2(ctx, options, 1)
		if err == nil {
			t.Error("Expected error for empty updates, got nil")
		}
		log.Printf("err: %v", err)

		// test non-existent table
		options2 := &UpdateByQueryOptions{
			Table:     "non_existent_table",
			Condition: "id = $1",
			Updates: map[string]any{
				"field": "value",
			},
		}

		_, err = processor.UpdateByQueryWithContextV2(ctx, options2, 1)
		if err == nil {
			t.Error("Expected error for non-existent table, got nil")
		}
		log.Printf("err: %v", err)

		// test empty updates
		options3 := &UpdateByQueryOptions{
			Table:     "test_users_v2",
			Condition: "id = $1",
		}

		_, err = processor.UpdateByQueryWithContextV2(ctx, options3, 1)
		if err == nil {
			t.Error("Expected error for empty updates, got nil")
		}
		log.Printf("err: %v", err)

		// empty table
		options4 := &UpdateByQueryOptions{
			Condition: "id = $1",
			Updates: map[string]any{
				"tags": "array_append(tags, $2)",
			},
		}

		_, err = processor.UpdateByQueryWithContextV2(ctx, options4, 1)
		if err == nil {
			t.Error("Expected error for syntax error, got nil")
		}
		log.Printf("err: %v", err)
	})

	// update test_users_v2 set tags = array_append(tags, 'new_tag') where id = 4;
	t.Run("UpdateArrayWithAppend", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		options := &UpdateByQueryOptions{
			Table:     "test_users_v2",
			Condition: "id = $1",
			Updates: map[string]any{
				"tags": "array_append(tags, $2)",
			},
		}

		updatedCount, err := processor.UpdateByQueryWithContextV2(ctx, options, 4, "new_tag")
		if err != nil {
			t.Fatalf("Failed to update array with append: %v", err)
		}

		t.Logf("Updated %d records with array_append", updatedCount)

		// 验证更新结果
		var tags string
		err = db.QueryRow("SELECT tags::text FROM test_users_v2 WHERE id = $1", 4).
			Scan(&tags)
		if err != nil {
			t.Fatalf("Failed to verify array append: %v", err)
		}

		expectedTags := "{vip,premium,admin,new_tag}"
		if tags != expectedTags {
			t.Errorf("Expected tags to be '%s', got '%s'", expectedTags, tags)
		}
	})

	// update test_users_v2 set tags = array_remove(tags, 'new_tag') where id = 5;
	t.Run("UpdateArrayWithRemove", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		options := &UpdateByQueryOptions{
			Table:     "test_users_v2",
			Condition: "id = $1",
			Updates: map[string]any{
				"tags": "array_remove(tags, $2)",
			},
		}

		updatedCount, err := processor.UpdateByQueryWithContextV2(ctx, options, 4, "new_tag")
		if err != nil {
			t.Fatalf("Failed to update array with remove: %v", err)
		}

		t.Logf("Updated %d records with array_remove", updatedCount)

		// 验证更新结果
		var tags string
		err = db.QueryRow("SELECT tags::text FROM test_users_v2 WHERE id = $1", 5).
			Scan(&tags)
		if err != nil {
			t.Fatalf("Failed to verify array remove: %v", err)
		}

		expectedTags := "{vip,premium,admin}"
		if tags != expectedTags {
			t.Errorf("Expected tags to be '%s', got '%s'", expectedTags, tags)
		}
	})

	// update test_users_v2 set version = 5 where tag2 = ANY(tags) AND id > 0;
	t.Run("UpdateWithAnyCondition", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		options := &UpdateByQueryOptions{
			Table:     "test_users_v2",
			Condition: "$1 = ANY(tags) AND id > $2",
			Updates: map[string]any{
				"version": 5,
			},
		}

		updatedCount, err := processor.UpdateByQueryWithContextV2(ctx, options, "tag2", 0)
		if err != nil {
			t.Fatalf("Failed to update with ANY condition: %v", err)
		}

		t.Logf("Updated %d records with ANY condition", updatedCount)

		// 验证更新结果
		var version int64
		err = db.QueryRow("SELECT version FROM test_users_v2_relyt_massive_group WHERE id = $1", 1).
			Scan(&version)
		if err != nil {
			t.Fatalf("Failed to verify ANY condition update: %v", err)
		}

		if version != 5 {
			t.Errorf("Expected version to be 5, got %d", version)
		}
	})

	// update test_users_v2 set tags = array_append(tags, 'popular') where array_length(tags, 1) > 1;
	t.Run("UpdateWithArrayLengthCondition", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		options := &UpdateByQueryOptions{
			Table:     "test_users_v2",
			Condition: "array_length(tags, 1) > $1",
			Updates: map[string]any{
				"tags": "array_append(tags, 'popular')",
			},
		}

		updatedCount, err := processor.UpdateByQueryWithContextV2(ctx, options, 1)
		if err != nil {
			t.Fatalf("Failed to update with array length condition: %v", err)
		}

		t.Logf("Updated %d records with array length condition", updatedCount)

		// 验证更新结果 - 检查是否有记录被更新
		if updatedCount > 0 {
			var count int64
			err := db.QueryRow("SELECT count(*) FROM test_users_v2 WHERE 'popular' = ANY(tags)").Scan(&count)
			if err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			if count != 7 {
				t.Errorf("Expected 1 record with 'popular' tag, got %d", count)
			}
		}
	})

	// update json fields
	// UPDATE public.test_users_v2 SET metadata = jsonb_set(metadata, '{key1}', '"{updated_value}"') WHERE id = 1 or id = 10
	t.Run("UpdateJSONFieldsUsingJSONBSetFunction", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		options := &UpdateByQueryOptions{
			Table:     "test_users_v2",
			Condition: "id = $1 or id = $2",
			Updates: map[string]any{
				"metadata": "jsonb_set(metadata, $3, $4)",
			},
		}

		updatedCount, err := processor.UpdateByQueryWithContextV2(ctx, options, 1, 10, "{key1}", "\"{updated_value}\"")
		log.Printf("sql: %s", options.FinalSQL)
		if err != nil {
			t.Fatalf("Failed to update JSON fields using jsonb_set function: %v", err)
		}

		t.Logf("Updated %d records with jsonb_set function", updatedCount)

		// 验证更新结果
		var metadataStr string
		err = db.QueryRow("SELECT metadata FROM test_users_v2_relyt_massive_group WHERE id = $1", 1).
			Scan(&metadataStr)
		if err != nil {
			t.Fatalf("Failed to verify jsonb_set function: %v", err)
		}

		var metadata map[string]any
		err = json.Unmarshal([]byte(metadataStr), &metadata)
		if err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		if metadata["key1"] != "{updated_value}" {
			t.Errorf("Expected key1 to be 'updated_value', got '%v'", metadata["key1"])
		}
	})

	// 测试更多数组函数
	t.Run("UpdateArrayWithPrepend", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		options := &UpdateByQueryOptions{
			Table:     "test_users_v2",
			Condition: "id = $1",
			Updates: map[string]any{
				"tags": "array_prepend($2, tags)",
			},
		}

		updatedCount, err := processor.UpdateByQueryWithContextV2(ctx, options, 2, "first_tag")
		if err != nil {
			t.Fatalf("Failed to update array with prepend: %v", err)
		}

		t.Logf("Updated %d records with array_prepend", updatedCount)

		// 验证更新结果
		var tags string
		err = db.QueryRow("SELECT tags::text FROM test_users_v2_relyt_massive_group WHERE id = $1", 2).
			Scan(&tags)
		if err != nil {
			t.Fatalf("Failed to verify array prepend: %v", err)
		}

		expectedTags := "{first_tag,vip,premium,admin,popular}"
		if tags != expectedTags {
			t.Errorf("Expected tags to be '%s', got '%s'", expectedTags, tags)
		}
	})

	// // 测试 JSON 插入函数
	t.Run("UpdateJSONWithInsert", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		options := &UpdateByQueryOptions{
			Table:     "test_users_v2",
			Condition: "id = $1",
			Updates: map[string]any{
				"metadata": "jsonb_insert(metadata, $2, $3)",
			},
		}

		updatedCount, err := processor.UpdateByQueryWithContextV2(ctx, options, 1, "{new_key}", "\"new_value\"")
		if err != nil {
			t.Fatalf("Failed to update JSON with insert: %v", err)
		}

		t.Logf("Updated %d records with jsonb_insert", updatedCount)

		// 验证更新结果
		var metadataStr string
		err = db.QueryRow("SELECT metadata FROM test_users_v2_relyt_massive_group WHERE id = $1", 1).
			Scan(&metadataStr)
		if err != nil {
			t.Fatalf("Failed to verify jsonb_insert: %v", err)
		}

		var metadata map[string]any
		err = json.Unmarshal([]byte(metadataStr), &metadata)
		if err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		if metadata["new_key"] != "new_value" {
			t.Errorf("Expected new_key to be 'new_value', got '%v'", metadata["new_key"])
		}
	})

	// // 测试复杂条件查询 - IN 操作符
	t.Run("UpdateWithInCondition", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		options := &UpdateByQueryOptions{
			Table:     "test_users_v2",
			Condition: "id IN ($1, $2, $3)",
			Updates: map[string]any{
				"version": 10,
			},
		}

		updatedCount, err := processor.UpdateByQueryWithContextV2(ctx, options, 1, 2, 3)
		if err != nil {
			t.Fatalf("Failed to update with IN condition: %v", err)
		}

		t.Logf("Updated %d records with IN condition", updatedCount)

		if updatedCount != 3 {
			t.Errorf("Expected 3 records to be updated, got %d", updatedCount)
		}

		// 验证更新结果
		var count int64
		err = db.QueryRow("SELECT count(*) FROM test_users_v2_relyt_massive_group WHERE version = 10").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to verify IN condition update: %v", err)
		}

		if count != 3 {
			t.Errorf("Expected 3 records with version 10, got %d", count)
		}
	})

	// 测试数组位置函数
	t.Run("UpdateArrayWithPosition", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		options := &UpdateByQueryOptions{
			Table:     "test_users_v2",
			Condition: "id = $1",
			Updates: map[string]any{
				"tags": "array_remove(tags, tags[array_position(tags, $2)])",
			},
		}

		updatedCount, err := processor.UpdateByQueryWithContextV2(ctx, options, 2, "first_tag")
		log.Printf("sql: %s", options.FinalSQL)
		if err != nil {
			t.Fatalf("Failed to update array with position: %v", err)
		}

		t.Logf("Updated %d records with array_position", updatedCount)

		// 验证更新结果
		var tags string
		err = db.QueryRow("SELECT tags::text FROM test_users_v2_relyt_massive_group WHERE id = $1", 2).
			Scan(&tags)
		if err != nil {
			t.Fatalf("Failed to verify array position: %v", err)
		}

		// 检查是否移除了 first_tag
		if strings.Contains(tags, "first_tag") {
			t.Errorf("Expected 'first_tag' to be removed, but tags still contain it: %s", tags)
		}
	})
}
