package supabaseorm

import (
	"context"
	"testing"
)

func TestNewAuth(t *testing.T) {
	client := &Client{
		baseURL: "https://example.com",
		apiKey:  "test-api-key",
	}

	auth := NewAuth(client)

	if auth == nil {
		t.Error("Expected auth to be initialized")
	}

	if auth.client != client {
		t.Error("Expected auth.client to be the same as client")
	}
}

func TestClientAuth(t *testing.T) {
	client := New("https://example.com", "test-api-key")

	auth := client.Auth()

	if auth == nil {
		t.Error("Expected auth to be initialized")
	}

	if auth.client != client {
		t.Error("Expected auth.client to be the same as client")
	}
}

// Mock tests for auth methods
// In a real test, you would use a mock HTTP server to test the actual API calls

func TestSignUp(t *testing.T) {
	client := New("https://example.com", "test-api-key")
	auth := client.Auth()

	req := SignUpRequest{
		Email:    "test@example.com",
		Password: "password123",
	}

	// This will fail because we're not actually making an API call
	// but it tests that the method exists and takes the right parameters
	_, err := auth.SignUp(context.Background(), req)
	if err == nil {
		t.Error("Expected error when not making actual API call")
	}
}

func TestSignInWithPassword(t *testing.T) {
	client := New("https://example.com", "test-api-key")
	auth := client.Auth()

	req := SignInRequest{
		Email:    "test@example.com",
		Password: "password123",
	}

	// This will fail because we're not actually making an API call
	// but it tests that the method exists and takes the right parameters
	_, err := auth.SignInWithPassword(context.Background(), req)
	if err == nil {
		t.Error("Expected error when not making actual API call")
	}
}

func TestSignInWithOTP(t *testing.T) {
	client := New("https://example.com", "test-api-key")
	auth := client.Auth()

	req := SignInRequest{
		Email: "test@example.com",
	}

	// This will fail because we're not actually making an API call
	// but it tests that the method exists and takes the right parameters
	err := auth.SignInWithOTP(context.Background(), req)
	if err == nil {
		t.Error("Expected error when not making actual API call")
	}
}

func TestVerify(t *testing.T) {
	client := New("https://example.com", "test-api-key")
	auth := client.Auth()

	req := VerifyRequest{
		Email: "test@example.com",
		Token: "123456",
		Type:  MagicLinkType,
	}

	// This will fail because we're not actually making an API call
	// but it tests that the method exists and takes the right parameters
	_, err := auth.Verify(context.Background(), req)
	if err == nil {
		t.Error("Expected error when not making actual API call")
	}
}

func TestResetPassword(t *testing.T) {
	client := New("https://example.com", "test-api-key")
	auth := client.Auth()

	req := ResetPasswordRequest{
		Email: "test@example.com",
	}

	// This will fail because we're not actually making an API call
	// but it tests that the method exists and takes the right parameters
	err := auth.ResetPassword(context.Background(), req)
	if err == nil {
		t.Error("Expected error when not making actual API call")
	}
}

func TestUpdatePassword(t *testing.T) {
	client := New("https://example.com", "test-api-key")
	auth := client.Auth()

	req := UpdatePasswordRequest{
		Password: "newpassword123",
	}

	// This will fail because we're not actually making an API call
	// but it tests that the method exists and takes the right parameters
	err := auth.UpdatePassword(context.Background(), req, "test-token")
	if err == nil {
		t.Error("Expected error when not making actual API call")
	}
}

func TestRefreshToken(t *testing.T) {
	client := New("https://example.com", "test-api-key")
	auth := client.Auth()

	req := RefreshTokenRequest{
		RefreshToken: "test-refresh-token",
	}

	// This will fail because we're not actually making an API call
	// but it tests that the method exists and takes the right parameters
	_, err := auth.RefreshToken(context.Background(), req)
	if err == nil {
		t.Error("Expected error when not making actual API call")
	}
}

func TestGetUser(t *testing.T) {
	client := New("https://example.com", "test-api-key")
	auth := client.Auth()

	// This will fail because we're not actually making an API call
	// but it tests that the method exists and takes the right parameters
	_, err := auth.GetUser(context.Background(), "test-token")
	if err == nil {
		t.Error("Expected error when not making actual API call")
	}
}

func TestSignOut(t *testing.T) {
	client := New("https://example.com", "test-api-key")
	auth := client.Auth()

	// This will fail because we're not actually making an API call
	// but it tests that the method exists and takes the right parameters
	err := auth.SignOut(context.Background(), "test-token")
	if err == nil {
		t.Error("Expected error when not making actual API call")
	}
}

func TestAuthConstants(t *testing.T) {
	if MagicLinkType != "magiclink" {
		t.Errorf("Expected MagicLinkType to be 'magiclink', got '%s'", MagicLinkType)
	}

	if SMSType != "sms" {
		t.Errorf("Expected SMSType to be 'sms', got '%s'", SMSType)
	}

	if RecoveryType != "recovery" {
		t.Errorf("Expected RecoveryType to be 'recovery', got '%s'", RecoveryType)
	}
}
