package supabaseorm

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// FormatFilterValue formats a value for use in a filter
func FormatFilterValue(value interface{}) string {
	v := reflect.ValueOf(value)

	switch v.Kind() {
	case reflect.String:
		return fmt.Sprintf("\"%s\"", value)
	case reflect.Bool:
		return strconv.FormatBool(v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(v.Float(), 'f', -1, 64)
	case reflect.Slice, reflect.Array:
		if v.Type().Elem().Kind() == reflect.String {
			var quoted []string
			for i := 0; i < v.Len(); i++ {
				quoted = append(quoted, fmt.Sprintf("\"%s\"", v.Index(i).String()))
			}
			return fmt.Sprintf("{%s}", strings.Join(quoted, ","))
		}

		var items []string
		for i := 0; i < v.Len(); i++ {
			items = append(items, FormatFilterValue(v.Index(i).Interface()))
		}
		return fmt.Sprintf("{%s}", strings.Join(items, ","))
	default:
		return fmt.Sprintf("%v", value)
	}
}

// BuildFilterCondition builds a filter condition for the Supabase API
func BuildFilterCondition(column, operator string, value interface{}) string {
	formattedValue := FormatFilterValue(value)

	// Handle special operators
	switch operator {
	case "eq", "=":
		return fmt.Sprintf("%s=eq.%s", column, formattedValue)
	case "neq", "!=", "<>":
		return fmt.Sprintf("%s=neq.%s", column, formattedValue)
	case "gt", ">":
		return fmt.Sprintf("%s=gt.%s", column, formattedValue)
	case "gte", ">=":
		return fmt.Sprintf("%s=gte.%s", column, formattedValue)
	case "lt", "<":
		return fmt.Sprintf("%s=lt.%s", column, formattedValue)
	case "lte", "<=":
		return fmt.Sprintf("%s=lte.%s", column, formattedValue)
	case "like":
		return fmt.Sprintf("%s=like.%s", column, formattedValue)
	case "ilike":
		return fmt.Sprintf("%s=ilike.%s", column, formattedValue)
	case "in":
		return fmt.Sprintf("%s=in.%s", column, formattedValue)
	case "is":
		return fmt.Sprintf("%s=is.%s", column, formattedValue)
	default:
		return fmt.Sprintf("%s=%s.%s", column, operator, formattedValue)
	}
}

// ParseContentRange parses a Content-Range header
func ParseContentRange(contentRange string) (start, end, total int) {
	// Format: "items start-end/total"
	parts := strings.Split(contentRange, "/")
	if len(parts) != 2 {
		return 0, 0, 0
	}

	totalStr := parts[1]
	total, _ = strconv.Atoi(totalStr)

	rangeParts := strings.Split(parts[0], "-")
	if len(rangeParts) != 2 {
		return 0, 0, total
	}

	// Remove "items " prefix if present
	startStr := strings.TrimPrefix(rangeParts[0], "items ")

	start, _ = strconv.Atoi(startStr)
	end, _ = strconv.Atoi(rangeParts[1])

	return start, end, total
}
