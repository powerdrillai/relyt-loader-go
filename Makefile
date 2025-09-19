.PHONY: test testv1 testv2 testselect

# 运行所有测试
all test: testv2 testselect

testv1:
	go test -v ./bulkprocessor -run TestInsertWithSomeErrors
	go test -v ./bulkprocessor -run TestInsertWithSleep
	go test -v ./bulkprocessor -run TestInsertWithPgRecovery
	go test -v ./bulkprocessor -run TestInsertWithImportTimeout
	go test -v ./bulkprocessor -run TestInsertWithMigration 

testv2:
	go test -v -count=1 ./bulkprocessor -run TestBufferInsertBasic
	go test -v -count=1 ./bulkprocessor -run TestBufferInsertWithSomeErrors
	go test -v -count=1 ./bulkprocessor -run TestBufferInsertWithSleep
	go test -v -count=1 ./bulkprocessor -run TestBufferInsertWithPgRecovery
	go test -v -count=1 ./bulkprocessor -run TestBufferInsertWithImportTimeout
	go test -v -count=1 ./bulkprocessor -run TestBufferInsertWithMigration
	go test -v -count=1 ./bulkprocessor -run TestBufferInsertWithMixedOperations
	go test -v -count=1 ./bulkprocessor -run TestBufferInsertWithOffset
	go test -v -count=1 ./bulkprocessor -run TestBufferDeleteSync
	go test -v -count=1 ./bulkprocessor -run TestBufferInsertWithDuplicate
	go test -v -count=1 ./bulkprocessor -run TestBufferInsertWithCopyOnConflict
	go test -v -count=1 ./bulkprocessor -run TestDeleteBeforeInsert
	go test -v -count=1 ./bulkprocessor -run TestErrorCaseEmptyTableName
	go test -v -count=1 ./bulkprocessor -run TestRealDelete
	go test -v -count=1 ./bulkprocessor -run TestDeleteGroupV2

testselect:
	go test -v -count=1 ./bulkprocessor -run TestSearchBasic
	go test -v -count=1 ./bulkprocessor -run TestSearchAdditional
	go test -v -count=1 ./bulkprocessor -run TestNewSearchFunc
	go test -v -count=1 ./bulkprocessor -run TestSearchMultipleTables