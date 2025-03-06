package supabaseorm

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

type TestUser struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	Email     string `json:"email"`
	Age       int    `json:"age"`
	CreatedAt string `json:"created_at"`
}

func TestSelect(t *testing.T) {
	tests := []struct {
		name     string
		columns  []string
		expected string
	}{
		{
			name:     "select all columns",
			columns:  []string{"*"},
			expected: "select=*",
		},
		{
			name:     "select specific columns",
			columns:  []string{"id", "name", "email"},
			expected: "select=id,name,email",
		},
		{
			name:     "select with nested columns",
			columns:  []string{"id", "profile(avatar_url)"},
			expected: "select=id,profile(avatar_url)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := NewQueryBuilder("users")
			qb.Select(tt.columns...)

			if qb.selectQuery != tt.expected {
				t.Errorf("Select() = %v, want %v", qb.selectQuery, tt.expected)
			}
		})
	}
}

func TestFilter(t *testing.T) {
	tests := []struct {
		name     string
		column   string
		operator string
		value    interface{}
		expected string
	}{
		{
			name:     "equals filter",
			column:   "name",
			operator: "eq",
			value:    "John",
			expected: "name=eq.John",
		},
		{
			name:     "greater than filter",
			column:   "age",
			operator: "gt",
			value:    18,
			expected: "age=gt.18",
		},
		{
			name:     "like filter",
			column:   "email",
			operator: "like",
			value:    "%gmail.com",
			expected: "email=like.%gmail.com",
		},
		{
			name:     "in filter with array",
			column:   "id",
			operator: "in",
			value:    []int{1, 2, 3},
			expected: "id=in.(1,2,3)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := NewQueryBuilder("users")
			qb.Where(tt.column, tt.operator, tt.value)

			if len(qb.filters) != 1 || qb.filters[0] != tt.expected {
				t.Errorf("Filter() = %v, want %v", qb.filters, []string{tt.expected})
			}
		})
	}
}

func TestOrder(t *testing.T) {
	tests := []struct {
		name      string
		column    string
		direction string
		expected  string
	}{
		{
			name:      "ascending order",
			column:    "created_at",
			direction: "asc",
			expected:  "order=created_at.asc",
		},
		{
			name:      "descending order",
			column:    "id",
			direction: "desc",
			expected:  "order=id.desc",
		},
		{
			name:      "ascending nulls first",
			column:    "updated_at",
			direction: "asc.nullsfirst",
			expected:  "order=updated_at.asc.nullsfirst",
		},
		{
			name:      "descending nulls last",
			column:    "priority",
			direction: "desc.nullslast",
			expected:  "order=priority.desc.nullslast",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := NewQueryBuilder("users")
			qb.Order(tt.column, tt.direction)

			if qb.orderQuery != tt.expected {
				t.Errorf("Order() = %v, want %v", qb.orderQuery, tt.expected)
			}
		})
	}
}

func TestLimit(t *testing.T) {
	tests := []struct {
		name     string
		limit    int
		expected string
	}{
		{
			name:     "limit 10",
			limit:    10,
			expected: "limit=10",
		},
		{
			name:     "limit 1",
			limit:    1,
			expected: "limit=1",
		},
		{
			name:     "limit 100",
			limit:    100,
			expected: "limit=100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := NewQueryBuilder("users")
			qb.Limit(tt.limit)

			if qb.limitQuery != tt.expected {
				t.Errorf("Limit() = %v, want %v", qb.limitQuery, tt.expected)
			}
		})
	}
}

func TestOffset(t *testing.T) {
	tests := []struct {
		name     string
		offset   int
		expected string
	}{
		{
			name:     "offset 0",
			offset:   0,
			expected: "offset=0",
		},
		{
			name:     "offset 10",
			offset:   10,
			expected: "offset=10",
		},
		{
			name:     "offset 100",
			offset:   100,
			expected: "offset=100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := NewQueryBuilder("users")
			qb.Offset(tt.offset)

			if qb.offsetQuery != tt.expected {
				t.Errorf("Offset() = %v, want %v", qb.offsetQuery, tt.expected)
			}
		})
	}
}

func TestRange(t *testing.T) {
	tests := []struct {
		name     string
		from     int
		to       int
		expected string
	}{
		{
			name:     "range 0-9",
			from:     0,
			to:       9,
			expected: "range=0-9",
		},
		{
			name:     "range 10-19",
			from:     10,
			to:       19,
			expected: "range=10-19",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := NewQueryBuilder("users")
			qb.Range(tt.from, tt.to)

			if qb.rangeQuery != tt.expected {
				t.Errorf("Range() = %v, want %v", qb.rangeQuery, tt.expected)
			}
		})
	}
}

func TestSingle(t *testing.T) {
	qb := NewQueryBuilder("users")
	qb.Single()

	if !qb.singleResult {
		t.Errorf("Single() did not set singleResult to true")
	}
}

func TestCount(t *testing.T) {
	tests := []struct {
		name     string
		exact    bool
		expected string
	}{
		{
			name:     "count with exact",
			exact:    true,
			expected: "count=exact",
		},
		{
			name:     "count without exact",
			exact:    false,
			expected: "count=estimated",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := NewQueryBuilder("users")
			qb.Count()

			if qb.countQuery != tt.expected {
				t.Errorf("Count() = %v, want %v", qb.countQuery, tt.expected)
			}
		})
	}
}

func TestBuildURL(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*QueryBuilder)
		expected string
	}{
		{
			name: "simple select",
			setup: func(qb *QueryBuilder) {
				qb.Select("id", "name")
			},
			expected: "/users?select=id,name",
		},
		{
			name: "select with filter",
			setup: func(qb *QueryBuilder) {
				qb.Select("*")
				qb.Filter("age", "gt", 18)
			},
			expected: "/users?select=*&age=gt.18",
		},
		{
			name: "complex query",
			setup: func(qb *QueryBuilder) {
				qb.Select("id", "name", "email")
				qb.Filter("age", "gte", 18)
				qb.Order("created_at", "desc")
				qb.Limit(10)
				qb.Offset(20)
			},
			expected: "/users?select=id,name,email&age=gte.18&order=created_at.desc&limit=10&offset=20",
		},
		{
			name: "query with count",
			setup: func(qb *QueryBuilder) {
				qb.Select("*")
				qb.Count()
			},
			expected: "/users?select=*&count=exact",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := NewQueryBuilder("users")
			tt.setup(qb)

			url := qb.BuildURL()
			if url != tt.expected {
				t.Errorf("BuildURL() = %v, want %v", url, tt.expected)
			}
		})
	}
}

func TestExecute(t *testing.T) {
	// Mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/users" && r.URL.RawQuery == "select=id,name&age=gt.18" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`[{"id":1,"name":"John"},{"id":2,"name":"Jane"}]`))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Create a client that points to the test server
	client := NewClient(server.URL, "fake-api-key")

	// Test the Execute method
	qb := client.From("users")
	qb.Select("id", "name")
	qb.Filter("age", "gt", 18)

	var users []TestUser
	err := qb.Execute(&users)

	if err != nil {
		t.Errorf("Execute() error = %v", err)
		return
	}

	expected := []TestUser{
		{ID: 1, Name: "John"},
		{ID: 2, Name: "Jane"},
	}

	if !reflect.DeepEqual(users, expected) {
		t.Errorf("Execute() = %v, want %v", users, expected)
	}
}

func TestInsert(t *testing.T) {
	// Mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/users" && r.Method == "POST" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte(`{"id":3,"name":"Alice","email":"alice@example.com","age":25}`))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Create a client that points to the test server
	client := NewClient(server.URL, "fake-api-key")

	// Test the Insert method
	qb := client.From("users")

	newUser := TestUser{
		Name:  "Alice",
		Email: "alice@example.com",
		Age:   25,
	}

	err := qb.Insert(newUser)

	if err != nil {
		t.Errorf("Insert() error = %v", err)
		return
	}

}

func TestUpdate(t *testing.T) {
	// Mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/users" && r.Method == "PATCH" && r.URL.RawQuery == "id=eq.1" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"id":1,"name":"John Updated","email":"john@example.com","age":30}`))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Create a client that points to the test server
	client := NewClient(server.URL, "fake-api-key")

	// Test the Update method
	qb := client.From("users")
	qb.Filter("id", "eq", 1)

	updates := map[string]interface{}{
		"name": "John Updated",
		"age":  30,
	}

	err := qb.Update(updates)

	if err != nil {
		t.Errorf("Update() error = %v", err)
		return
	}

}

func TestDelete(t *testing.T) {
	// Mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/users" && r.Method == "DELETE" && r.URL.RawQuery == "id=eq.2" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"id":2,"name":"Jane","email":"jane@example.com","age":28}`))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Create a client that points to the test server
	client := NewClient(server.URL, "fake-api-key")

	// Test the Delete method
	qb := client.From("users")
	qb.Filter("id", "eq", 2)

	err := qb.Delete()

	if err != nil {
		t.Errorf("Delete() error = %v", err)
		return
	}

}

func TestOr(t *testing.T) {
	tests := []struct {
		name     string
		filters  []string
		expected string
	}{
		{
			name: "simple or",
			filters: []string{
				"name=eq.John",
				"name=eq.Jane",
			},
			expected: "or=(name=eq.John,name=eq.Jane)",
		},
		{
			name: "complex or",
			filters: []string{
				"age=gte.18",
				"email=like.%gmail.com",
				"name=eq.Admin",
			},
			expected: "or=(age=gte.18,email=like.%gmail.com,name=eq.Admin)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := NewQueryBuilder("users")
			qb.Or(tt.filters...)

			if len(qb.orFilters) != 1 || qb.orFilters[0] != tt.expected {
				t.Errorf("Or() = %v, want %v", qb.orFilters, []string{tt.expected})
			}
		})
	}
}

func TestAnd(t *testing.T) {
	tests := []struct {
		name     string
		filters  []string
		expected string
	}{
		{
			name: "simple and",
			filters: []string{
				"age=gte.18",
				"age=lte.65",
			},
			expected: "and=(age=gte.18,age=lte.65)",
		},
		{
			name: "complex and",
			filters: []string{
				"is_active=eq.true",
				"created_at=gte.2023-01-01",
				"role=in.(admin,editor)",
			},
			expected: "and=(is_active=eq.true,created_at=gte.2023-01-01,role=in.(admin,editor))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := NewQueryBuilder("users")
			qb.And(tt.filters...)

			if len(qb.andFilters) != 1 || qb.andFilters[0] != tt.expected {
				t.Errorf("And() = %v, want %v", qb.andFilters, []string{tt.expected})
			}
		})
	}
}

func TestNot(t *testing.T) {
	tests := []struct {
		name     string
		column   string
		operator string
		value    interface{}
		expected string
	}{
		{
			name:     "not equals",
			column:   "status",
			operator: "eq",
			value:    "inactive",
			expected: "not.status=eq.inactive",
		},
		{
			name:     "not in",
			column:   "role",
			operator: "in",
			value:    []string{"guest", "banned"},
			expected: "not.role=in.(guest,banned)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := NewQueryBuilder("users")
			qb.Not(tt.column, tt.operator, tt.value)

			if len(qb.notFilters) != 1 || qb.notFilters[0] != tt.expected {
				t.Errorf("Not() = %v, want %v", qb.notFilters, []string{tt.expected})
			}
		})
	}
}

func TestForeignTable(t *testing.T) {
	qb := NewQueryBuilder("users")
	foreignQb := qb.ForeignTable("posts")

	if foreignQb.table != "users.posts" {
		t.Errorf("ForeignTable() table = %v, want %v", foreignQb.table, "users.posts")
	}
}

func TestRPC(t *testing.T) {
	// Mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rpc/get_user_by_id" && r.Method == "POST" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"id":1,"name":"John","email":"john@example.com","age":30}`))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Create a client that points to the test server
	client := NewClient(server.URL, "fake-api-key")

	// Test the RPC method
	params := map[string]interface{}{
		"user_id": 1,
	}

	var user TestUser
	err := client.RPC("get_user_by_id", params, &user)

	if err != nil {
		t.Errorf("RPC() error = %v", err)
		return
	}

	expected := TestUser{
		ID:    1,
		Name:  "John",
		Email: "john@example.com",
		Age:   30,
	}

	if !reflect.DeepEqual(user, expected) {
		t.Errorf("RPC() = %v, want %v", user, expected)
	}
}
