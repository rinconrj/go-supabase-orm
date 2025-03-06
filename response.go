package supabaseorm

import (
	"github.com/go-resty/resty/v2"
)

// Response wraps the Supabase API response
type Response struct {
	StatusCode int
	Headers    map[string]string
	Body       []byte
	Error      error
}

// NewResponse creates a new Response from a resty.Response
func NewResponse(resp *resty.Response, err error) *Response {
	if err != nil {
		return &Response{
			Error: err,
		}
	}

	headers := make(map[string]string)
	for k, v := range resp.Header() {
		if len(v) > 0 {
			headers[k] = v[0]
		}
	}

	return &Response{
		StatusCode: resp.StatusCode(),
		Headers:    headers,
		Body:       resp.Body(),
		Error:      nil,
	}
}

// IsError returns true if the response contains an error
func (r *Response) IsError() bool {
	if r.Error != nil {
		return true
	}
	return r.StatusCode >= 400
}

// GetContentRange parses the Content-Range header
func (r *Response) GetContentRange() (int, int, int) {
	// Parse Content-Range header (e.g., "0-9/42")
	// This is a placeholder - in a real implementation, you'd parse the header
	return 0, 0, 0
}
