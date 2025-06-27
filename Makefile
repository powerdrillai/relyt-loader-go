.PHONY: test testv1 testv2 testselect

# 运行所有测试
all test: testv1 testv2 testselect

testv1:
	go test -v ./bulkprocessor -run TestInsertWithSomeErrors
	go test -v ./bulkprocessor -run TestInsertWithSleep
	go test -v ./bulkprocessor -run TestInsertWithPgRecovery
	go test -v ./bulkprocessor -run TestInsertWithImportTimeout
	go test -v ./bulkprocessor -run TestInsertWithMigration 

testv2:
	go test -v ./bulkprocessor -run TestBufferInsertBasic
	go test -v ./bulkprocessor -run TestBufferInsertWithSomeErrors
	go test -v ./bulkprocessor -run TestBufferInsertWithSleep
	go test -v ./bulkprocessor -run TestBufferInsertWithPgRecovery
	go test -v ./bulkprocessor -run TestBufferInsertWithImportTimeout
	go test -v ./bulkprocessor -run TestBufferInsertWithMigration
	go test -v ./bulkprocessor -run TestBufferInsertWithMixedOperations
	go test -v ./bulkprocessor -run TestBufferInsertWithOffset
	go test -v ./bulkprocessor -run TestBufferDeleteSync
	go test -v ./bulkprocessor -run TestBufferInsertWithDuplicate
testselect:
	go test -v ./bulkprocessor -run TestSearchBasic
	go test -v ./bulkprocessor -run TestSearchAdditional