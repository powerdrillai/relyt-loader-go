.PHONY: test test-errors test-sleep test-recovery test-timeout test-migration

# 运行所有测试
test: test-errors test-sleep test-recovery test-timeout test-migration

# 测试错误处理
test-errors:
	go test -v ./bulkprocessor -run TestInsertWithSomeErrors

# 测试间歇性写入
test-sleep:
	go test -v ./bulkprocessor -run TestInsertWithSleep

# 测试PG恢复
test-recovery:
	go test -v ./bulkprocessor -run TestInsertWithPgRecovery

# 测试导入超时
test-timeout:
	go test -v ./bulkprocessor -run TestInsertWithImportTimeout

# 测试数据迁移
test-migration:
	go test -v ./bulkprocessor -run TestInsertWithMigration 