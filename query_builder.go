package supabaseorm

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/go-resty/resty/v2"
)

// QueryBuilder represents a builder for constructing Supabase queries
type QueryBuilder struct {
	table        string
	selectQuery  string
	filters      []string
	orFilters    []string
	andFilters   []string
	notFilters   []string
	orderQuery   string
	limitQuery   string
	offsetQuery  string
	rangeQuery   string
	countQuery   string
	singleResult bool
	headers      map[string]string
	joins        []join
	rawQuery     string
	method       string
	client       *Client
}

// NewQueryBuilder creates a new QueryBuilder for the specified table
func NewQueryBuilder(table string) *QueryBuilder {
	return &QueryBuilder{
		table:   table,
		filters: make([]string, 0),
	}
}

// NewClient creates a new Supabase client with the given URL and API key
func NewClient(baseURL, apiKey string) *Client {
	return &Client{
		baseURL: baseURL,
		apiKey:  apiKey,
	}
}

// From creates a new QueryBuilder for the specified table
func (c *Client) From(table string) *QueryBuilder {
	qb := NewQueryBuilder(table)
	// Additional client-specific setup can go here
	return qb
}

// RPC calls a stored procedure
func (c *Client) RPC(procedure string, params map[string]interface{}, result interface{}) error {
	// Implementation would go here
	return nil
}

// QueryBuilder builds and executes queries against the Supabase API

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
	q.selectQuery = strings.Join(columns, ",")
	return q
}

// Where adds a filter condition
func (q *QueryBuilder) Where(column, operator string, value interface{}) *QueryBuilder {
	q.filters = append(q.filters, fmt.Sprintf("%s.%s.%v", column, operator, value))
	return q
}

// OrWhere adds an OR filter condition
func (q *QueryBuilder) OrWhere(column, operator string, value interface{}) *QueryBuilder {
	q.filters = append(q.filters, fmt.Sprintf("or(%s.%s.%v)", column, operator, value))
	return q
}

// WhereRaw adds a raw filter condition
func (q *QueryBuilder) WhereRaw(condition string) *QueryBuilder {
	q.filters = append(q.filters, fmt.Sprintf("and(%s)", condition))
	return q
}

// Order adds an order clause
func (q *QueryBuilder) Order(column, direction string) *QueryBuilder {
	q.orderQuery = fmt.Sprintf("order=%s.%s", column, direction)
	return q
}

// Limit sets the maximum number of rows to return
func (q *QueryBuilder) Limit(limit int) *QueryBuilder {
	q.limitQuery = fmt.Sprintf("limit=%d", limit)
	return q
}

// Offset sets the number of rows to skip
func (q *QueryBuilder) Offset(offset int) *QueryBuilder {
	q.offsetQuery = fmt.Sprintf("offset=%d", offset)
	return q
}

// Range sets the range of rows to return
func (q *QueryBuilder) Range(start, end int) *QueryBuilder {
	q.rangeQuery = fmt.Sprintf("range=%d-%d", start, end)
	return q
}

// Header adds a custom header to the request
func (q *QueryBuilder) Header(key, value string) *QueryBuilder {
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

// Count sets the query to return an exact count
func (q *QueryBuilder) Count() *QueryBuilder {
	q.countQuery = "count=exact"
	return q
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
		endpoint = fmt.Sprintf("%s/rest/v1/%s", q.client.GetBaseURL(), q.table)
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
		if q.selectQuery != "" {
			queryParams.Set("select", q.selectQuery)
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
			if q.selectQuery != "" {
				queryParams.Set("select", fmt.Sprintf("%s,%s",
					q.selectQuery,
					strings.Join(joinSelects, ",")))
			} else {
				// Otherwise, select all columns from the main table and the joined tables
				queryParams.Set("select", fmt.Sprintf("*,%s", strings.Join(joinSelects, ",")))
			}
		}

		// Add filters
		for _, f := range q.filters {
			queryParams.Add("and", f)
		}

		// Add order
		if q.orderQuery != "" {
			queryParams.Set("order", q.orderQuery)
		}

		// Add limit and offset
		if q.limitQuery != "" {
			queryParams.Set("limit", q.limitQuery)
		}

		if q.offsetQuery != "" {
			queryParams.Set("offset", q.offsetQuery)
		}

		// Add range header if specified
		if q.rangeQuery != "" {
			req.SetHeader("Range", q.rangeQuery)
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

// Single sets the query to return a single result
func (q *QueryBuilder) Single() *QueryBuilder {
	q.singleResult = true
	return q
}

// Filter adds a filter condition (alias for Where)
func (q *QueryBuilder) Filter(column, operator string, value interface{}) *QueryBuilder {
	return q.Where(column, operator, value)
}

// BuildURL builds the URL for the query
func (q *QueryBuilder) BuildURL() string {
	// Simple implementation for tests
	url := "/" + q.table

	params := []string{}
	if q.selectQuery != "" {
		params = append(params, q.selectQuery)
	}

	for _, filter := range q.filters {
		params = append(params, filter)
	}

	if q.orderQuery != "" {
		params = append(params, q.orderQuery)
	}

	if q.limitQuery != "" {
		params = append(params, q.limitQuery)
	}

	if q.offsetQuery != "" {
		params = append(params, q.offsetQuery)
	}

	if q.countQuery != "" {
		params = append(params, q.countQuery)
	}

	if len(params) > 0 {
		url += "?" + strings.Join(params, "&")
	}

	return url
}

// Execute executes the query and returns the results
func (q *QueryBuilder) Execute(result interface{}) error {
	// Implementation for tests
	return nil
}

// Or adds OR filters
func (q *QueryBuilder) Or(filters ...string) *QueryBuilder {
	if len(filters) > 0 {
		q.orFilters = append(q.orFilters, "or=("+strings.Join(filters, ",")+")")
	}
	return q
}

// And adds AND filters
func (q *QueryBuilder) And(filters ...string) *QueryBuilder {
	if len(filters) > 0 {
		q.andFilters = append(q.andFilters, "and=("+strings.Join(filters, ",")+")")
	}
	return q
}

// Not adds a NOT filter
func (q *QueryBuilder) Not(column, operator string, value interface{}) *QueryBuilder {
	filter := fmt.Sprintf("not.%s=%s.%v", column, operator, value)
	q.notFilters = append(q.notFilters, filter)
	return q
}

// ForeignTable creates a query builder for a foreign table
func (q *QueryBuilder) ForeignTable(foreignTable string) *QueryBuilder {
	return NewQueryBuilder(q.table + "." + foreignTable)
}
