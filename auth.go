package supabaseorm

import (
	"context"
	"fmt"
	"time"
)

// Auth provides methods for authentication with Supabase
type Auth struct {
	client *Client
}

// AuthResponse represents the response from authentication operations
type AuthResponse struct {
	AccessToken  string    `json:"access_token"`
	TokenType    string    `json:"token_type"`
	ExpiresIn    int       `json:"expires_in"`
	RefreshToken string    `json:"refresh_token"`
	User         User      `json:"user"`
	ExpiresAt    time.Time `json:"-"`
}

// User represents a Supabase user
type User struct {
	ID               string                 `json:"id"`
	Aud              string                 `json:"aud"`
	Role             string                 `json:"role"`
	Email            string                 `json:"email"`
	Phone            string                 `json:"phone"`
	EmailConfirmedAt time.Time              `json:"email_confirmed_at"`
	ConfirmedAt      time.Time              `json:"confirmed_at"`
	LastSignInAt     time.Time              `json:"last_sign_in_at"`
	AppMetadata      map[string]interface{} `json:"app_metadata"`
	UserMetadata     map[string]interface{} `json:"user_metadata"`
	CreatedAt        time.Time              `json:"created_at"`
	UpdatedAt        time.Time              `json:"updated_at"`
}

// SignUpRequest represents the request body for signing up
type SignUpRequest struct {
	Email        string                 `json:"email"`
	Password     string                 `json:"password"`
	Phone        string                 `json:"phone,omitempty"`
	UserMetadata map[string]interface{} `json:"data,omitempty"`
}

// SignInRequest represents the request body for signing in
type SignInRequest struct {
	Email      string `json:"email"`
	Password   string `json:"password,omitempty"`
	Phone      string `json:"phone,omitempty"`
	CreateUser bool   `json:"create_user,omitempty"`
}

// VerifyRequest represents the request body for verifying OTP
type VerifyRequest struct {
	Email string `json:"email"`
	Token string `json:"token"`
	Type  string `json:"type"`
}

// ResetPasswordRequest represents the request body for resetting password
type ResetPasswordRequest struct {
	Email string `json:"email"`
}

// UpdatePasswordRequest represents the request body for updating password
type UpdatePasswordRequest struct {
	Password string `json:"password"`
}

// RefreshTokenRequest represents the request body for refreshing token
type RefreshTokenRequest struct {
	RefreshToken string `json:"refresh_token"`
}

// NewAuth creates a new Auth instance
func NewAuth(client *Client) *Auth {
	return &Auth{
		client: client,
	}
}

// SignUp registers a new user
func (a *Auth) SignUp(ctx context.Context, req SignUpRequest) (*AuthResponse, error) {
	endpoint := fmt.Sprintf("%s/auth/v1/signup", a.client.baseURL)

	resp, err := a.client.httpClient.R().
		SetHeader("Content-Type", "application/json").
		SetBody(req).
		SetResult(&AuthResponse{}).
		Post(endpoint)

	if err != nil {
		return nil, err
	}

	if resp.IsError() {
		return nil, fmt.Errorf("auth error: %s", resp.String())
	}

	authResp, ok := resp.Result().(*AuthResponse)
	if !ok {
		return nil, fmt.Errorf("failed to parse auth response")
	}

	// Calculate expires_at
	authResp.ExpiresAt = time.Now().Add(time.Second * time.Duration(authResp.ExpiresIn))

	return authResp, nil
}

// SignInWithPassword authenticates a user with email and password
func (a *Auth) SignInWithPassword(ctx context.Context, req SignInRequest) (*AuthResponse, error) {
	endpoint := fmt.Sprintf("%s/auth/v1/token?grant_type=password", a.client.baseURL)

	resp, err := a.client.httpClient.R().
		SetHeader("Content-Type", "application/json").
		SetBody(req).
		SetResult(&AuthResponse{}).
		Post(endpoint)

	if err != nil {
		return nil, err
	}

	if resp.IsError() {
		return nil, fmt.Errorf("auth error: %s", resp.String())
	}

	authResp, ok := resp.Result().(*AuthResponse)
	if !ok {
		return nil, fmt.Errorf("failed to parse auth response")
	}

	// Calculate expires_at
	authResp.ExpiresAt = time.Now().Add(time.Second * time.Duration(authResp.ExpiresIn))

	return authResp, nil
}

// SignInWithOTP sends a one-time password to the user's email
func (a *Auth) SignInWithOTP(ctx context.Context, req SignInRequest) error {
	endpoint := fmt.Sprintf("%s/auth/v1/otp", a.client.baseURL)

	resp, err := a.client.httpClient.R().
		SetHeader("Content-Type", "application/json").
		SetBody(req).
		Post(endpoint)

	if err != nil {
		return err
	}

	if resp.IsError() {
		return fmt.Errorf("auth error: %s", resp.String())
	}

	return nil
}

// Verify verifies a one-time password
func (a *Auth) Verify(ctx context.Context, req VerifyRequest) (*AuthResponse, error) {
	endpoint := fmt.Sprintf("%s/auth/v1/verify", a.client.baseURL)

	resp, err := a.client.httpClient.R().
		SetHeader("Content-Type", "application/json").
		SetBody(req).
		SetResult(&AuthResponse{}).
		Post(endpoint)

	if err != nil {
		return nil, err
	}

	if resp.IsError() {
		return nil, fmt.Errorf("auth error: %s", resp.String())
	}

	authResp, ok := resp.Result().(*AuthResponse)
	if !ok {
		return nil, fmt.Errorf("failed to parse auth response")
	}

	// Calculate expires_at
	authResp.ExpiresAt = time.Now().Add(time.Second * time.Duration(authResp.ExpiresIn))

	return authResp, nil
}

// ResetPassword sends a password reset email
func (a *Auth) ResetPassword(ctx context.Context, req ResetPasswordRequest) error {
	endpoint := fmt.Sprintf("%s/auth/v1/recover", a.client.baseURL)

	resp, err := a.client.httpClient.R().
		SetHeader("Content-Type", "application/json").
		SetBody(req).
		Post(endpoint)

	if err != nil {
		return err
	}

	if resp.IsError() {
		return fmt.Errorf("auth error: %s", resp.String())
	}

	return nil
}

// UpdatePassword updates the user's password
func (a *Auth) UpdatePassword(ctx context.Context, req UpdatePasswordRequest, token string) error {
	endpoint := fmt.Sprintf("%s/auth/v1/user", a.client.baseURL)

	resp, err := a.client.httpClient.R().
		SetHeader("Content-Type", "application/json").
		SetHeader("Authorization", fmt.Sprintf("Bearer %s", token)).
		SetBody(req).
		Put(endpoint)

	if err != nil {
		return err
	}

	if resp.IsError() {
		return fmt.Errorf("auth error: %s", resp.String())
	}

	return nil
}

// RefreshToken refreshes the access token
func (a *Auth) RefreshToken(ctx context.Context, req RefreshTokenRequest) (*AuthResponse, error) {
	endpoint := fmt.Sprintf("%s/auth/v1/token?grant_type=refresh_token", a.client.baseURL)

	resp, err := a.client.httpClient.R().
		SetHeader("Content-Type", "application/json").
		SetBody(req).
		SetResult(&AuthResponse{}).
		Post(endpoint)

	if err != nil {
		return nil, err
	}

	if resp.IsError() {
		return nil, fmt.Errorf("auth error: %s", resp.String())
	}

	authResp, ok := resp.Result().(*AuthResponse)
	if !ok {
		return nil, fmt.Errorf("failed to parse auth response")
	}

	// Calculate expires_at
	authResp.ExpiresAt = time.Now().Add(time.Second * time.Duration(authResp.ExpiresIn))

	return authResp, nil
}

// GetUser gets the user information
func (a *Auth) GetUser(ctx context.Context, token string) (*User, error) {
	endpoint := fmt.Sprintf("%s/auth/v1/user", a.client.baseURL)

	resp, err := a.client.httpClient.R().
		SetHeader("Authorization", fmt.Sprintf("Bearer %s", token)).
		SetResult(&User{}).
		Get(endpoint)

	if err != nil {
		return nil, err
	}

	if resp.IsError() {
		return nil, fmt.Errorf("auth error: %s", resp.String())
	}

	user, ok := resp.Result().(*User)
	if !ok {
		return nil, fmt.Errorf("failed to parse user response")
	}

	return user, nil
}

// SignOut signs out the user
func (a *Auth) SignOut(ctx context.Context, token string) error {
	endpoint := fmt.Sprintf("%s/auth/v1/logout", a.client.baseURL)

	resp, err := a.client.httpClient.R().
		SetHeader("Authorization", fmt.Sprintf("Bearer %s", token)).
		Post(endpoint)

	if err != nil {
		return err
	}

	if resp.IsError() {
		return fmt.Errorf("auth error: %s", resp.String())
	}

	return nil
}

// MagicLinkType is the type for magic link authentication
const MagicLinkType = "magiclink"

// SMSType is the type for SMS authentication
const SMSType = "sms"

// RecoveryType is the type for recovery authentication
const RecoveryType = "recovery"
