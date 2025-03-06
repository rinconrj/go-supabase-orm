package supabaseorm

import (
	"testing"
)

func TestJoin(t *testing.T) {
	client := &Client{
		baseURL: "https://example.com",
		apiKey:  "test-api-key",
	}

	qb := client.Table("users").
		Select("id", "name").
		InnerJoin("posts", "id", "user_id")

	// Check that the join was added correctly
	if len(qb.joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(qb.joins))
	}

	join := qb.joins[0]
	if join.foreignTable != "posts" {
		t.Errorf("Expected foreign table to be 'posts', got '%s'", join.foreignTable)
	}
	if join.localColumn != "id" {
		t.Errorf("Expected local column to be 'id', got '%s'", join.localColumn)
	}
	if join.operator != "eq" {
		t.Errorf("Expected operator to be 'eq', got '%s'", join.operator)
	}
	if join.foreignColumn != "user_id" {
		t.Errorf("Expected foreign column to be 'user_id', got '%s'", join.foreignColumn)
	}
}

func TestLeftJoin(t *testing.T) {
	client := &Client{
		baseURL: "https://example.com",
		apiKey:  "test-api-key",
	}

	qb := client.Table("users").
		Select("id", "name").
		LeftJoin("posts", "id", "user_id")

	// Check that the join was added correctly
	if len(qb.joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(qb.joins))
	}

	// Check that the Prefer header was set
	if qb.headers["Prefer"] != "missing=null" {
		t.Errorf("Expected Prefer header to be 'missing=null', got '%s'", qb.headers["Prefer"])
	}
}

func TestRaw(t *testing.T) {
	client := &Client{
		baseURL: "https://example.com",
		apiKey:  "test-api-key",
	}

	rawSQL := "SELECT * FROM users WHERE id = 1"
	qb := client.Table("").Raw(rawSQL)

	// Check that the raw query was set correctly
	if qb.rawQuery != rawSQL {
		t.Errorf("Expected raw query to be '%s', got '%s'", rawSQL, qb.rawQuery)
	}
}

func TestMultipleJoins(t *testing.T) {
	client := &Client{
		baseURL: "https://example.com",
		apiKey:  "test-api-key",
	}

	qb := client.Table("users").
		Select("id", "name").
		InnerJoin("posts", "id", "user_id").
		InnerJoin("comments", "id", "user_id")

	// Check that both joins were added correctly
	if len(qb.joins) != 2 {
		t.Errorf("Expected 2 joins, got %d", len(qb.joins))
	}

	// Check first join
	join1 := qb.joins[0]
	if join1.foreignTable != "posts" {
		t.Errorf("Expected first foreign table to be 'posts', got '%s'", join1.foreignTable)
	}

	// Check second join
	join2 := qb.joins[1]
	if join2.foreignTable != "comments" {
		t.Errorf("Expected second foreign table to be 'comments', got '%s'", join2.foreignTable)
	}
}
