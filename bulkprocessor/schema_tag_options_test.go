package bulkprocessor

import (
	"context"
	"reflect"
	"testing"
)

type tagoptionsTaggedRecord struct {
	ID        int64  `relyt:"id,omitempty"`
	RoutingID string `relyt:"routing_id,omitempty"`
	Version   int    `relyt:"version,string,omitempty"`
	Payload   string `relyt:"payload"`
}

// relyt tags follow the conventional "name,option,..." shape. Options are
// metadata, not part of the PostgreSQL identifier exposed by the reflection
// helpers, so every consumer of FieldInfo must see only the name before the
// first comma.
func TestTagOptionsRelytTagOptionsAreStrippedFromColumnMetadata(t *testing.T) {
	fields, err := GetStructFields(reflect.TypeOf(tagoptionsTaggedRecord{}))
	if err != nil {
		t.Fatalf("GetStructFields: %v", err)
	}

	wantNames := []string{"id", "routing_id", "version", "payload"}
	if got := GetColumnNames(fields); !reflect.DeepEqual(got, wantNames) {
		t.Errorf("column names include relyt tag options: got %q, want %q", got, wantNames)
	}

	wantDefinitions := []string{
		"id BIGINT",
		"routing_id TEXT",
		"version BIGINT",
		"payload TEXT",
	}
	if got := GetColumnDefinitions(fields); !reflect.DeepEqual(got, wantDefinitions) {
		t.Errorf("column definitions include relyt tag options: got %q, want %q", got, wantDefinitions)
	}

	for wantIndex, column := range wantNames {
		if got := GetColumnIndex(fields, column); got != wantIndex {
			t.Errorf("GetColumnIndex(%q) = %d, want %d", column, got, wantIndex)
		}
	}
}

// Exercise InsertV2 without starting workers or connecting to PostgreSQL. A
// primed instance-router cache makes successful sharded routing entirely local.
// With the bug, "routing_id,omitempty" cannot be found and InsertV2 returns
// ErrRoutingColumnRequired instead of accepting and routing the record.
func TestTagOptionsInsertV2RecognizesRoutingIDWithTagOptions(t *testing.T) {
	const (
		routingID  = "tenant-with-options"
		instanceID = "instance-a"
	)

	p := &BulkProcessor{
		config: Config{
			PostgreSQL:       PostgreSQLConfig{Table: "tagoptions_items"},
			EnableDualBuffer: true,
		},
		ctx:             context.Background(),
		isStarted:       true,
		isSharded:       true,
		routingColIndex: -1,
		versionColIndex: -1,
		feedFieldIndex:  -1,
		recordQueueV2:   make(chan *Record, 1),
		instanceRouter: &InstanceRouter{
			routingCache: map[string]string{routingID: instanceID},
		},
	}

	input := tagoptionsTaggedRecord{
		ID:        42,
		RoutingID: routingID,
		Version:   7,
		Payload:   "value",
	}
	if err := p.InsertV2("file-9", routingID, []tagoptionsTaggedRecord{input}); err != nil {
		t.Fatalf("InsertV2 did not recognize relyt:\"routing_id,omitempty\": %v", err)
	}

	if got := p.routingColIndex; got != 1 {
		t.Errorf("routingColIndex = %d, want 1", got)
	}
	if got := len(p.recordQueueV2); got != 1 {
		t.Fatalf("InsertV2 queued %d records, want 1", got)
	}

	record := <-p.recordQueueV2
	if record.RoutingID != routingID || record.InstanceID != instanceID {
		t.Errorf("queued routing metadata = (%q, %q), want (%q, %q)",
			record.RoutingID, record.InstanceID, routingID, instanceID)
	}
	wantValues := []string{"42", routingID, "7", "value"}
	if !reflect.DeepEqual(record.Values, wantValues) {
		t.Errorf("queued values = %q, want %q", record.Values, wantValues)
	}
}
