package supabaseorm

import (
	"fmt"
	"net/http"
	"time"

	"github.com/go-resty/resty/v2"
)

// Client represents a Supabase client
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *resty.Client
	auth       *Auth
}

// ClientOption is a function that configures a Client
type ClientOption func(*Client)

// WithTimeout sets the timeout for the HTTP client
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.httpClient.SetTimeout(timeout)
	}
}

// WithHeaders sets additional headers for the HTTP client
func WithHeaders(headers map[string]string) ClientOption {
	return func(c *Client) {
		c.httpClient.SetHeaders(headers)
	}
}

// New creates a new Supabase client
func New(baseURL, apiKey string, options ...ClientOption) *Client {
	httpClient := resty.New()

	client := &Client{
		baseURL:    baseURL,
		apiKey:     apiKey,
		httpClient: httpClient,
	}

	// Set default headers
	client.httpClient.SetHeader("apikey", apiKey)
	client.httpClient.SetHeader("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	client.httpClient.SetHeader("Content-Type", "application/json")

	// Apply options
	for _, option := range options {
		option(client)
	}

	// Initialize auth
	client.auth = NewAuth(client)

	return client
}

// Table returns a new query builder for the specified table
func (c *Client) Table(tableName string) *QueryBuilder {
	return &QueryBuilder{
		client:    c,
		tableName: tableName,
		method:    http.MethodGet,
	}
}

// Auth returns the Auth instance for authentication operations
func (c *Client) Auth() *Auth {
	return c.auth
}

// RawRequest allows making raw HTTP requests to the Supabase API
func (c *Client) RawRequest() *resty.Request {
	return c.httpClient.R()
}

// GetBaseURL returns the base URL of the Supabase API
func (c *Client) GetBaseURL() string {
	return c.baseURL
}

// GetAPIKey returns the API key used for authentication
func (c *Client) GetAPIKey() string {
	return c.apiKey
}
