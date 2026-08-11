package bulkprocessor

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// emptyRows is a pgx.Rows with no rows, returned for an unregistered
// routing_id (no mapping means no data).
type emptyRows struct{}

func (emptyRows) Next() bool                                   { return false }
func (emptyRows) Scan(dest ...any) error                       { return errors.New("emptyRows has no rows") }
func (emptyRows) Values() ([]any, error)                       { return nil, errors.New("emptyRows has no rows") }
func (emptyRows) RawValues() [][]byte                          { return nil }
func (emptyRows) Close()                                       {}
func (emptyRows) Err() error                                   { return nil }
func (emptyRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (emptyRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (emptyRows) Conn() *pgx.Conn                              { return nil }

// routeSearchJsonRows handles instance-routed search for sharded tables.
// handled=false means the table is not sharded, so the caller runs its
// existing single-instance path unchanged. RoutingID is required.
// The processor's own table uses p.isSharded (fixed at startup), so warm
// processes never round-trip to main here; only foreign tables are checked
// against the database, and foreign sharded tables fail closed.
func (p *BulkProcessor) routeSearchJsonRows(ctx context.Context, options *SearchOptions,
	processedColumns []string, processedCondition, processedGroupBy, processedHaving string) (bool, pgx.Rows, error) {

	if !strings.EqualFold(options.Table, p.config.PostgreSQL.Table) {
		isSharded, err := p.CheckTableIsSharded(options.Table)
		if err != nil {
			// fail closed: never fall through to the unrouted path
			return true, nil, errors.Wrap(err, "failed to check if table is sharded")
		}
		if !isSharded {
			return false, nil, nil
		}
		return true, nil, errors.Wrapf(ErrForeignShardedTable,
			"search on sharded table %s (processor configured for table %q)",
			options.Table, p.config.PostgreSQL.Table)
	}
	if !p.isSharded {
		return false, nil, nil
	}
	if options.RoutingID == "" {
		return true, nil, errors.Wrapf(ErrRoutingIDRequired, "search on table %s", options.Table)
	}
	if err := validateShardedSearchSQL(processedColumns, processedCondition, options.OrderBy, processedGroupBy, processedHaving); err != nil {
		return true, nil, err
	}

	instanceID, found, err := p.instanceRouter.LookupInstance(ctx, options.RoutingID)
	if err != nil {
		return true, nil, err
	}
	if !found {
		// no mapping means no data
		return true, emptyRows{}, nil
	}
	client, err := p.instanceRouter.GetClient(ctx, instanceID)
	if err != nil {
		return true, nil, err
	}
	processedCondition = routingIDScopedCondition(processedCondition, options.RoutingID)
	params := shardedSearchParams(p.config.PostgreSQL.Schema, options.Table, processedColumns,
		processedCondition, options.OrderBy, processedGroupBy, processedHaving, options.Limit, options.Offset)
	rows, finalSQL, err := client.GetColumnsWithCondition(ctx, params...)
	options.FinalSQL = finalSQL
	if err != nil {
		return true, nil, errors.Wrap(err, "failed to get columns with condition")
	}
	return true, rows, nil
}

// shardedSearchParams builds get_columns_sql_with_condition args with
// have_aux_table always false (sharded tables never use aux routing).
func shardedSearchParams(schema, table string, columns []string, condition, orderBy, groupBy, having string, limit, offset int) []any {
	return []any{
		schema, table, pq.Array(columns), condition, orderBy, groupBy, having, limit, offset, false,
	}
}

// routingIDScopedCondition adds the tenant boundary enforced by the SDK. The
// caller's condition is always parenthesized so an OR cannot escape the scope.
func routingIDScopedCondition(condition, routingID string) string {
	routingCondition := "routing_id = " + pq.QuoteLiteral(routingID)
	if strings.TrimSpace(condition) == "" {
		return routingCondition
	}
	return "(" + condition + ") AND " + routingCondition
}

func sqlCodeOnly(fragment string) (string, bool) {
	var code strings.Builder
	for i := 0; i < len(fragment); {
		switch fragment[i] {
		case '\'':
			code.WriteByte(' ')
			i++
			closed := false
			for i < len(fragment) {
				if fragment[i] == '\'' {
					if i+1 < len(fragment) && fragment[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				i++
			}
			if !closed {
				return "", false
			}
		case '"':
			code.WriteByte(' ')
			i++
			closed := false
			for i < len(fragment) {
				if fragment[i] == '"' {
					if i+1 < len(fragment) && fragment[i+1] == '"' {
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				i++
			}
			if !closed {
				return "", false
			}
		case '-':
			if i+1 < len(fragment) && fragment[i+1] == '-' {
				return "", false
			}
			code.WriteByte(fragment[i])
			i++
		case '/':
			if i+1 < len(fragment) && fragment[i+1] == '*' {
				code.WriteByte(' ')
				i += 2
				for i+1 < len(fragment) && !(fragment[i] == '*' && fragment[i+1] == '/') {
					i++
				}
				if i+1 >= len(fragment) {
					return "", false
				}
				i += 2
				continue
			}
			code.WriteByte(fragment[i])
			i++
		default:
			code.WriteByte(fragment[i])
			i++
		}
	}
	return code.String(), true
}

func hasSQLKeyword(code, keyword string) bool {
	code = strings.ToLower(code)
	for i := 0; i+len(keyword) <= len(code); i++ {
		if code[i:i+len(keyword)] != keyword {
			continue
		}
		isIdent := func(b byte) bool {
			return b == '_' || b >= 'a' && b <= 'z' || b >= '0' && b <= '9'
		}
		if (i == 0 || !isIdent(code[i-1])) && (i+len(keyword) == len(code) || !isIdent(code[i+len(keyword)])) {
			return true
		}
	}
	return false
}

func validateShardedSQLFragment(kind, fragment string, rejectSubquery bool) error {
	code, ok := sqlCodeOnly(fragment)
	depth := 0
	for i := 0; ok && i < len(code); i++ {
		switch code[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				ok = false
			}
		}
	}
	if depth != 0 {
		ok = false
	}
	unsafeQueryExpression := rejectSubquery && (hasSQLKeyword(code, "select") ||
		hasSQLKeyword(code, "table") || hasSQLKeyword(code, "with") || hasSQLKeyword(code, "values"))
	if !ok || strings.Contains(code, ";") || unsafeQueryExpression {
		return errors.Wrapf(ErrUnsafeShardedSQL, "%s", kind)
	}
	return nil
}

func validateShardedSearchSQL(columns []string, condition, orderBy, groupBy, having string) error {
	for kind, fragment := range map[string]string{
		"condition": condition,
		"order by":  orderBy,
		"group by":  groupBy,
		"having":    having,
	} {
		if err := validateShardedSQLFragment(kind, fragment, true); err != nil {
			return err
		}
	}
	for _, column := range columns {
		if err := validateShardedSQLFragment("projection", column, true); err != nil {
			return err
		}
	}
	return nil
}

func isRawUpdateExpression(value string) bool {
	return strings.HasPrefix(value, "(") && strings.HasSuffix(value, ")") ||
		strings.HasPrefix(value, "ARRAY[") && strings.HasSuffix(value, "]") ||
		strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]")
}

func validateShardedUpdates(updates map[string]any) error {
	for column, value := range updates {
		column = strings.TrimSpace(column)
		if len(column) >= 2 && column[0] == '"' && column[len(column)-1] == '"' {
			column = strings.ReplaceAll(column[1:len(column)-1], `""`, `"`)
		}
		if strings.EqualFold(column, "routing_id") {
			return ErrRoutingIDUpdateForbidden
		}
		if expression, ok := value.(string); ok && isRawUpdateExpression(expression) {
			// Match generate_update_by_query_sql exactly: only complete `(…)`,
			// `ARRAY[…]`, and `[…]` values are emitted as SQL. Everything else
			// is escaped by the UDF as a string literal.
			if err := validateShardedSQLFragment("update expression", expression, true); err != nil {
				return err
			}
		}
	}
	return nil
}

// updateByQuerySharded routes an update to the one owning instance. Called
// only for the processor's own table. RoutingID is required; an unregistered
// RoutingID affects zero rows.
func (p *BulkProcessor) updateByQuerySharded(ctx context.Context, options *UpdateByQueryOptions,
	processedCondition string, processedUpdates map[string]any) (int64, error) {

	if options.RoutingID == "" {
		return 0, errors.Wrapf(ErrRoutingIDRequired, "update on table %s", options.Table)
	}
	if err := validateShardedSQLFragment("condition", processedCondition, true); err != nil {
		return 0, err
	}
	if err := validateShardedUpdates(processedUpdates); err != nil {
		return 0, errors.Wrapf(err, "update on table %s", options.Table)
	}

	instanceID, found, err := p.instanceRouter.LookupInstance(ctx, options.RoutingID)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, nil
	}
	client, err := p.instanceRouter.GetClient(ctx, instanceID)
	if err != nil {
		return 0, err
	}
	processedCondition = routingIDScopedCondition(processedCondition, options.RoutingID)
	count, finalSQL, err := client.UpdateByQueryV2(ctx, p.config.PostgreSQL.Schema, options.Table,
		processedCondition, processedUpdates, false)
	options.FinalSQL = finalSQL
	if err != nil {
		return 0, errors.Wrap(err, "failed to update by query")
	}
	return count, nil
}
