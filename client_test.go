package supabaseorm

import (
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	baseURL := "https://example.supabase.co"
	apiKey := "test-api-key"

	client := New(baseURL, apiKey)

	if client.baseURL != baseURL {
		t.Errorf("Expected baseURL to be %s, got %s", baseURL, client.baseURL)
	}

	if client.apiKey != apiKey {
		t.Errorf("Expected apiKey to be %s, got %s", apiKey, client.apiKey)
	}

	if client.httpClient == nil {
		t.Error("Expected httpClient to be initialized")
	}
}

func TestWithTimeout(t *testing.T) {
	baseURL := "https://example.supabase.co"
	apiKey := "test-api-key"
	timeout := 5 * time.Second

	client := New(baseURL, apiKey, WithTimeout(timeout))

	if client.httpClient.GetClient().Timeout != timeout {
		t.Errorf("Expected timeout to be %s, got %s", timeout, client.httpClient.GetClient().Timeout)
	}
}

func TestWithHeaders(t *testing.T) {
	baseURL := "https://example.supabase.co"
	apiKey := "test-api-key"
	headers := map[string]string{
		"X-Custom-Header": "test-value",
	}

	client := New(baseURL, apiKey, WithHeaders(headers))

	// Check if the custom header is set
	// The headers should be set on the client, not on individual requests
	// Let's check the client's default headers
	defaultHeaders := client.httpClient.Header
	headerValue := defaultHeaders.Get("X-Custom-Header")
	if headerValue != "test-value" {
		t.Errorf("Expected X-Custom-Header to be %s, got %s", "test-value", headerValue)
	}
}

func TestTable(t *testing.T) {
	baseURL := "https://example.supabase.co"
	apiKey := "test-api-key"
	tableName := "users"

	client := New(baseURL, apiKey)
	queryBuilder := client.Table(tableName)

	if queryBuilder.table != tableName {
		t.Errorf("Expected tableName to be %s, got %s", tableName, queryBuilder.table)
	}

	if queryBuilder.client != client {
		t.Error("Expected client to be the same instance")
	}
}
