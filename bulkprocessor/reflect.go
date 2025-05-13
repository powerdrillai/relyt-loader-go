package bulkprocessor

import (
	"fmt"
	"reflect"
	"time"
)

// FieldInfo represents information about a field in a struct
type FieldInfo struct {
	Name     string       // Field name
	JSONName string       // JSON tag name
	Type     reflect.Type // Field type
	Index    int          // Field index
}

// GetStructFields returns information about the fields in a struct type
func GetStructFields(structType reflect.Type) ([]FieldInfo, error) {
	if structType.Kind() == reflect.Ptr {
		structType = structType.Elem()
	}

	if structType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct type, got %s", structType.Kind())
	}

	var fields []FieldInfo
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)

		// Skip unexported fields
		if field.PkgPath != "" {
			continue
		}

		// Get JSON tag
		jsonTag := field.Tag.Get("json")
		if jsonTag == "" || jsonTag == "-" {
			continue
		}

		// Handle JSON tag options (e.g., "name,omitempty")
		jsonName := jsonTag
		if comma := reflect.StructTag(jsonTag).Get(","); comma != "" {
			jsonName = jsonName[:len(jsonName)-len(comma)-1]
		}

		fields = append(fields, FieldInfo{
			Name:     field.Name,
			JSONName: jsonName,
			Type:     field.Type,
			Index:    i,
		})
	}

	return fields, nil
}

// GetFieldValues returns the values of the fields in a struct as strings
func GetFieldValues(obj interface{}, fields []FieldInfo) ([]string, error) {
	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct, got %s", val.Kind())
	}

	values := make([]string, len(fields))
	for i, field := range fields {
		fieldVal := val.Field(field.Index)
		values[i] = formatValue(fieldVal)
	}

	return values, nil
}

// formatValue converts a reflect.Value to a string representation
func formatValue(val reflect.Value) string {
	switch val.Kind() {
	case reflect.String:
		return val.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fmt.Sprintf("%d", val.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return fmt.Sprintf("%d", val.Uint())
	case reflect.Float32, reflect.Float64:
		return fmt.Sprintf("%f", val.Float())
	case reflect.Bool:
		return fmt.Sprintf("%t", val.Bool())
	case reflect.Ptr:
		if val.IsNil() {
			return ""
		}
		return formatValue(val.Elem())
	case reflect.Struct:
		// Handle special types like time.Time
		if val.Type() == reflect.TypeOf(time.Time{}) {
			t := val.Interface().(time.Time)
			return t.Format(time.RFC3339)
		}
		return fmt.Sprintf("%v", val.Interface())
	default:
		return fmt.Sprintf("%v", val.Interface())
	}
}

// GetColumnNames returns the column names from field info
func GetColumnNames(fields []FieldInfo) []string {
	columns := make([]string, len(fields))
	for i, field := range fields {
		columns[i] = field.JSONName
	}
	return columns
}

// GetColumnDefinitions returns the column definitions for PostgreSQL
func GetColumnDefinitions(fields []FieldInfo) []string {
	defs := make([]string, len(fields))
	for i, field := range fields {
		sqlType := getSQLType(field.Type)
		defs[i] = fmt.Sprintf("%s %s", field.JSONName, sqlType)
	}
	return defs
}

// getSQLType returns the SQL type for a Go type
func getSQLType(t reflect.Type) string {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.String:
		return "TEXT"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "BIGINT"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "BIGINT"
	case reflect.Float32, reflect.Float64:
		return "DOUBLE PRECISION"
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Struct:
		// Handle special types like time.Time
		if t == reflect.TypeOf(time.Time{}) {
			return "TIMESTAMP WITH TIME ZONE"
		}
		return "TEXT"
	default:
		return "TEXT"
	}
}
