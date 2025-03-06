package supabaseorm

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/go-resty/resty/v2"
)

// QueryBuilder builds and executes queries against the Supabase API
type QueryBuilder struct {
	client       *Client
	tableName    string
	method       string
	selectFields []string
	filters      []filter
	orderFields  []order
	limitValue   int
	offsetValue  int
	rangeValue   *rangeQuery
	headers      map[string]string
	joins        []join
	rawQuery     string
}

type filter struct {
	column    string
	operator  string
	value     interface{}
	isOr      bool
	isComplex bool
}

type order struct {
	column    string
	direction string
}

type rangeQuery struct {
	start int
	end   int
}

type join struct {
	foreignTable  string
	localColumn   string
	operator      string
	foreignColumn string
}

// Select specifies the columns to return
func (q *QueryBuilder) Select(columns ...string) *QueryBuilder {
	q.selectFields = columns
	return q
}

// Where adds a filter condition
func (q *QueryBuilder) Where(column, operator string, value interface{}) *QueryBuilder {
	q.filters = append(q.filters, filter{
		column:   column,
		operator: operator,
		value:    value,
	})
	return q
}

// OrWhere adds an OR filter condition
func (q *QueryBuilder) OrWhere(column, operator string, value interface{}) *QueryBuilder {
	q.filters = append(q.filters, filter{
		column:   column,
		operator: operator,
		value:    value,
		isOr:     true,
	})
	return q
}

// WhereRaw adds a raw filter condition
func (q *QueryBuilder) WhereRaw(condition string) *QueryBuilder {
	q.filters = append(q.filters, filter{
		column:    condition,
		isComplex: true,
	})
	return q
}

// Order adds an order clause
func (q *QueryBuilder) Order(column, direction string) *QueryBuilder {
	q.orderFields = append(q.orderFields, order{
		column:    column,
		direction: direction,
	})
	return q
}

// Limit sets the maximum number of rows to return
func (q *QueryBuilder) Limit(limit int) *QueryBuilder {
	q.limitValue = limit
	return q
}

// Offset sets the number of rows to skip
func (q *QueryBuilder) Offset(offset int) *QueryBuilder {
	q.offsetValue = offset
	return q
}

// Range sets the range of rows to return
func (q *QueryBuilder) Range(start, end int) *QueryBuilder {
	q.rangeValue = &rangeQuery{
		start: start,
		end:   end,
	}
	return q
}

// Header adds a custom header to the request
func (q *QueryBuilder) Header(key, value string) *QueryBuilder {
	if q.headers == nil {
		q.headers = make(map[string]string)
	}
	q.headers[key] = value
	return q
}

// Join adds a join clause to the query
// This uses the PostgREST foreign key join syntax
func (q *QueryBuilder) Join(foreignTable, localColumn, operator, foreignColumn string) *QueryBuilder {
	q.joins = append(q.joins, join{
		foreignTable:  foreignTable,
		localColumn:   localColumn,
		operator:      operator,
		foreignColumn: foreignColumn,
	})
	return q
}

// InnerJoin is a convenience method for Join with "eq" operator
func (q *QueryBuilder) InnerJoin(foreignTable, localColumn, foreignColumn string) *QueryBuilder {
	return q.Join(foreignTable, localColumn, "eq", foreignColumn)
}

// LeftJoin is a convenience method for left join
// Note: PostgREST doesn't directly support LEFT JOIN, but we can emulate it
func (q *QueryBuilder) LeftJoin(foreignTable, localColumn, foreignColumn string) *QueryBuilder {
	// Add the join
	q.Join(foreignTable, localColumn, "eq", foreignColumn)

	// Set the Prefer header to include nulls
	q.Header("Prefer", "missing=null")

	return q
}

// Raw sets a raw SQL query to be executed
// This uses the PostgREST RPC function call mechanism
func (q *QueryBuilder) Raw(query string) *QueryBuilder {
	q.rawQuery = query
	return q
}

// Get executes the query and returns the results
func (q *QueryBuilder) Get(result interface{}) error {
	return q.execute(result)
}

// First executes the query and returns the first result
func (q *QueryBuilder) First(result interface{}) error {
	q.Limit(1)
	return q.execute(result)
}

// Insert inserts a new record
func (q *QueryBuilder) Insert(data interface{}) error {
	q.method = http.MethodPost
	return q.execute(data)
}

// Update updates an existing record
func (q *QueryBuilder) Update(data interface{}) error {
	q.method = http.MethodPatch
	return q.execute(data)
}

// Delete deletes records
func (q *QueryBuilder) Delete() error {
	q.method = http.MethodDelete
	return q.execute(nil)
}

// Count returns the count of records
func (q *QueryBuilder) Count() (int, error) {
	q.Header("Prefer", "count=exact")

	var result json.RawMessage
	err := q.execute(&result)
	if err != nil {
		return 0, err
	}

	// Extract count from headers
	// This is a placeholder - in a real implementation, you'd extract the count from the response headers
	return 0, nil
}

// execute builds and executes the request
func (q *QueryBuilder) execute(data interface{}) error {
	var endpoint string

	// If it's a raw query, use the RPC endpoint
	if q.rawQuery != "" {
		// For raw SQL, we'll use the RPC endpoint
		// This assumes you have a function in your database that can execute the raw query
		endpoint = fmt.Sprintf("%s/rest/v1/rpc/execute_sql", q.client.GetBaseURL())

		// Set the method to POST for RPC calls
		q.method = http.MethodPost

		// Create the request body with the SQL query
		type sqlRequest struct {
			Query string `json:"query"`
		}

		data = sqlRequest{
			Query: q.rawQuery,
		}
	} else {
		// For normal queries, use the table endpoint
		endpoint = fmt.Sprintf("%s/rest/v1/%s", q.client.GetBaseURL(), q.tableName)
	}

	req := q.client.RawRequest()

	// Add custom headers
	for k, v := range q.headers {
		req.SetHeader(k, v)
	}

	// If it's not a raw query, build the query parameters
	if q.rawQuery == "" {
		// Build query parameters
		queryParams := url.Values{}

		// Add select fields
		if len(q.selectFields) > 0 {
			queryParams.Set("select", strings.Join(q.selectFields, ","))
		}

		// Add joins
		if len(q.joins) > 0 {
			// For each join, we need to modify the select parameter
			// to include the joined table columns
			var joinSelects []string

			for _, j := range q.joins {
				// Format: foreignTable(*)
				joinSelects = append(joinSelects, fmt.Sprintf("%s(*)", j.foreignTable))
			}

			// If we already have select fields, append the joins
			if len(q.selectFields) > 0 {
				queryParams.Set("select", fmt.Sprintf("%s,%s",
					queryParams.Get("select"),
					strings.Join(joinSelects, ",")))
			} else {
				// Otherwise, select all columns from the main table and the joined tables
				queryParams.Set("select", fmt.Sprintf("*,%s", strings.Join(joinSelects, ",")))
			}
		}

		// Add filters
		for _, f := range q.filters {
			if f.isComplex {
				// Handle raw conditions
				queryParams.Add("and", f.column)
			} else {
				// Handle standard conditions
				var condition string
				if f.isOr {
					condition = fmt.Sprintf("or(%s.%s.%v)", f.column, f.operator, f.value)
				} else {
					condition = fmt.Sprintf("%s.%s.%v", f.column, f.operator, f.value)
				}
				queryParams.Add("and", condition)
			}
		}

		// Add order
		if len(q.orderFields) > 0 {
			var orders []string
			for _, o := range q.orderFields {
				orders = append(orders, fmt.Sprintf("%s.%s", o.column, o.direction))
			}
			queryParams.Set("order", strings.Join(orders, ","))
		}

		// Add limit and offset
		if q.limitValue > 0 {
			queryParams.Set("limit", fmt.Sprintf("%d", q.limitValue))
		}

		if q.offsetValue > 0 {
			queryParams.Set("offset", fmt.Sprintf("%d", q.offsetValue))
		}

		// Add range header if specified
		if q.rangeValue != nil {
			req.SetHeader("Range", fmt.Sprintf("%d-%d", q.rangeValue.start, q.rangeValue.end))
		}

		// Set query parameters
		req.SetQueryParamsFromValues(queryParams)
	}

	var resp *resty.Response
	var err error

	switch q.method {
	case http.MethodGet:
		resp, err = req.Get(endpoint)
	case http.MethodPost:
		resp, err = req.SetBody(data).Post(endpoint)
	case http.MethodPatch:
		resp, err = req.SetBody(data).Patch(endpoint)
	case http.MethodDelete:
		resp, err = req.Delete(endpoint)
	default:
		return fmt.Errorf("unsupported HTTP method: %s", q.method)
	}

	if err != nil {
		return err
	}

	if resp.IsError() {
		return fmt.Errorf("API error: %s", resp.String())
	}

	// For methods that return data, unmarshal the response
	if q.method == http.MethodGet && data != nil {
		return json.Unmarshal(resp.Body(), data)
	}

	// For insert operations, update the ID of the inserted record
	if q.method == http.MethodPost && data != nil {
		return json.Unmarshal(resp.Body(), data)
	}

	return nil
}
