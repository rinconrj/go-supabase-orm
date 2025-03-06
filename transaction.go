package supabaseorm

import (
	"fmt"
	"net/http"
)

// Transaction represents a database transaction
type Transaction struct {
	client *Client
}

// Begin starts a new transaction
func (c *Client) Begin() *Transaction {
	return &Transaction{
		client: c,
	}
}

// Table returns a new query builder for the specified table within the transaction
func (t *Transaction) Table(tableName string) *QueryBuilder {
	builder := &QueryBuilder{
		client: t.client,
		table:  tableName,
		method: http.MethodGet,
	}

	// Add transaction headers
	builder.Header("Prefer", "tx=commit")

	return builder
}

// Commit commits the transaction
// Note: In the current Supabase REST API, transactions are automatically committed
// This is a placeholder for future functionality
func (t *Transaction) Commit() error {
	return nil
}

// Rollback rolls back the transaction
// Note: In the current Supabase REST API, explicit rollbacks are not supported
// This is a placeholder for future functionality
func (t *Transaction) Rollback() error {
	return fmt.Errorf("rollback not supported in the current Supabase REST API")
}
