package bulkprocessor

import (
	"context"
	"errors"
	"testing"
)

// TestEmptyRows verifies the empty-result case used for an unregistered
// routing_id: emptyRows{}.Next() must be false immediately.
func TestEmptyRows(t *testing.T) {
	rows := emptyRows{}
	if rows.Next() {
		t.Fatalf("expected Next() == false for emptyRows")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("expected nil Err() for emptyRows, got %v", err)
	}
	if got := rows.RawValues(); got != nil {
		t.Fatalf("expected nil RawValues() for emptyRows, got %v", got)
	}
	rows.Close() // must be a no-op
}

func TestRoutingIDScopedCondition(t *testing.T) {
	tests := []struct {
		name      string
		condition string
		routingID string
		want      string
	}{
		{name: "empty", routingID: "100", want: "routing_id = '100'"},
		{name: "parenthesizes OR", condition: "state = 'new' OR state = 'ready'", routingID: "100",
			want: "(state = 'new' OR state = 'ready') AND routing_id = '100'"},
		{name: "escapes quote", condition: "id > 0", routingID: "tenant'one",
			want: "(id > 0) AND routing_id = 'tenant''one'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := routingIDScopedCondition(tt.condition, tt.routingID); got != tt.want {
				t.Fatalf("routingIDScopedCondition() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestValidateShardedUpdatesRejectsRoutingID(t *testing.T) {
	for _, column := range []string{"routing_id", "ROUTING_ID", " routing_id ", `"routing_id"`} {
		err := validateShardedUpdates(map[string]any{column: "200"})
		if !errors.Is(err, ErrRoutingIDUpdateForbidden) {
			t.Fatalf("column %q: expected ErrRoutingIDUpdateForbidden, got %v", column, err)
		}
	}
	if err := validateShardedUpdates(map[string]any{"state": "ready"}); err != nil {
		t.Fatalf("non-routing update rejected: %v", err)
	}
	for _, literal := range []string{"select -- literal", "(draft", " (select note)", "array[select]"} {
		if err := validateShardedUpdates(map[string]any{"state": literal}); err != nil {
			t.Fatalf("ordinary string literal %q rejected as SQL: %v", literal, err)
		}
	}
	for _, expression := range []string{"(select note)", "[select note]", "ARRAY[select note]"} {
		if err := validateShardedUpdates(map[string]any{"state": expression}); !errors.Is(err, ErrUnsafeShardedSQL) {
			t.Fatalf("raw expression %q: expected ErrUnsafeShardedSQL, got %v", expression, err)
		}
	}
}

func TestSQLCodeOnlyMatchesPostgresEscapeAndDollarQuotedStrings(t *testing.T) {
	for _, fragment := range []string{
		`note = E'escaped \' ) SELECT text' AND (state = 'ready')`,
		`note = $$) SELECT secret; -- still text$$ AND (state = 'ready')`,
		`note = $tag$) WITH hidden AS (SELECT 1);$tag$ AND (state = 'ready')`,
	} {
		if err := validateShardedSQLFragment("condition", fragment, true); err != nil {
			t.Errorf("valid PostgreSQL quoted fragment %q rejected: %v", fragment, err)
		}
	}
	for _, fragment := range []string{`note = E'unterminated \'`, `note = $tag$unterminated`} {
		if err := validateShardedSQLFragment("condition", fragment, true); !errors.Is(err, ErrUnsafeShardedSQL) {
			t.Errorf("unterminated PostgreSQL string %q: expected ErrUnsafeShardedSQL, got %v", fragment, err)
		}
	}
}

func TestValidateShardedSQLRejectsScopeEscapes(t *testing.T) {
	if err := validateShardedSQLFragment("condition", "TRUE) OR routing_id = 'other' OR (FALSE", false); !errors.Is(err, ErrUnsafeShardedSQL) {
		t.Fatalf("unbalanced condition: expected ErrUnsafeShardedSQL, got %v", err)
	}
	if err := validateShardedSQLFragment("projection", "(TABLE private_values)", true); !errors.Is(err, ErrUnsafeShardedSQL) {
		t.Fatalf("TABLE query expression: expected ErrUnsafeShardedSQL, got %v", err)
	}
	for _, fragment := range []string{"name = 'unterminated", `"unterminated`} {
		if err := validateShardedSQLFragment("condition", fragment, true); !errors.Is(err, ErrUnsafeShardedSQL) {
			t.Fatalf("unterminated quoted fragment %q: expected ErrUnsafeShardedSQL, got %v", fragment, err)
		}
	}
	fragments := []struct {
		name                        string
		orderBy, groupBy, havingSQL string
	}{
		{name: "order by", orderBy: "(SELECT secret FROM private_values)"},
		{name: "group by", groupBy: "(TABLE private_values)"},
		{name: "having", havingSQL: "EXISTS (WITH leaked AS (TABLE private_values) TABLE leaked)"},
	}
	for _, fragment := range fragments {
		err := validateShardedSearchSQL([]string{"id"}, "TRUE", fragment.orderBy, fragment.groupBy, fragment.havingSQL)
		if !errors.Is(err, ErrUnsafeShardedSQL) {
			t.Fatalf("%s query expression: expected ErrUnsafeShardedSQL, got %v", fragment.name, err)
		}
	}
}

func TestUpdateByQueryRejectsRoutingIDBeforeRouterAccess(t *testing.T) {
	p := &BulkProcessor{
		config:         Config{PostgreSQL: PostgreSQLConfig{Table: "items"}},
		isSharded:      true,
		instanceRouter: nil, // Deliberately nil: validation must happen first.
	}
	options := &UpdateByQueryOptions{
		Table:     "items",
		RoutingID: "100",
		Updates:   map[string]any{"routing_id": "200"},
	}
	_, err := p.UpdateByQueryWithContextV2(context.Background(), options)
	if !errors.Is(err, ErrRoutingIDUpdateForbidden) {
		t.Fatalf("expected ErrRoutingIDUpdateForbidden before router access, got %v", err)
	}
}

func TestValidateShardedRecordRoutingID(t *testing.T) {
	if err := validateShardedRecordRoutingID([]string{"1", "100"}, 1, "100"); err != nil {
		t.Fatalf("matching routing id rejected: %v", err)
	}
	if err := validateShardedRecordRoutingID([]string{"1", "200"}, 1, "100"); !errors.Is(err, ErrRoutingIDMismatch) {
		t.Fatalf("expected ErrRoutingIDMismatch, got %v", err)
	}
	if err := validateShardedRecordRoutingID([]string{"1"}, -1, "100"); !errors.Is(err, ErrRoutingColumnRequired) {
		t.Fatalf("expected ErrRoutingColumnRequired, got %v", err)
	}
}

func TestInsertV2ValidatesEntireShardedBatchBeforeRoutingOrEnqueue(t *testing.T) {
	type routedRecord struct {
		Ignored   string
		RoutingID string `relyt:"routing_id"`
	}
	p := &BulkProcessor{
		config: Config{
			PostgreSQL:       PostgreSQLConfig{Table: "items"},
			EnableDualBuffer: true,
		},
		isStarted:       true,
		isSharded:       true,
		routingColIndex: -1,
		feedFieldIndex:  -1,
		recordQueueV2:   make(chan *Record, 2),
		// Deliberately nil: validation must return before routing is attempted.
		instanceRouter: nil,
	}

	err := p.InsertV2("file-1", "100", []routedRecord{{RoutingID: "100"}, {RoutingID: "200"}})
	if !errors.Is(err, ErrRoutingIDMismatch) {
		t.Fatalf("expected ErrRoutingIDMismatch, got %v", err)
	}
	if got := len(p.recordQueueV2); got != 0 {
		t.Fatalf("validation failure partially enqueued %d records", got)
	}

	allNil := &BulkProcessor{
		config: Config{
			PostgreSQL:       PostgreSQLConfig{Table: "items"},
			EnableDualBuffer: true,
		},
		isStarted:       true,
		isSharded:       true,
		routingColIndex: -1,
		feedFieldIndex:  -1,
		instanceRouter:  nil,
	}
	if err := allNil.InsertV2("file-2", "100", []*routedRecord{nil}); !errors.Is(err, ErrEmptyInput) {
		t.Fatalf("all-nil batch: expected ErrEmptyInput before registration, got %v", err)
	}
}
