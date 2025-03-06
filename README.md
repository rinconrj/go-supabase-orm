# Supabase ORM for Go

A lightweight ORM-like library for interacting with Supabase's RESTful API from Go applications.

## Features

- Fluent query builder interface for Supabase REST API
- Support for filtering, ordering, pagination, and selecting specific columns
- Support for table joins and relationships
- Support for raw SQL queries via RPC
- Complete authentication support (sign up, sign in, password reset, etc.)
- Automatic JSON marshaling/unmarshaling
- Type-safe operations
- Support for transactions (when available in Supabase)

## Installation

```bash
go get github.com/rinconrj/supabase-orm
```

## Usage

### Data Access

```go
package main

import (
    "fmt"
    "log"
    "time"

    supabaseorm "github.com/rinconrj/supabase-orm/src"
)

type User struct {
    ID        int       `json:"id"`
    Name      string    `json:"name"`
    Email     string    `json:"email"`
    CreatedAt time.Time `json:"created_at"`
    Posts     []Post    `json:"posts"` // Nested relationship
}

type Post struct {
    ID      int    `json:"id"`
    Title   string `json:"title"`
    Content string `json:"content"`
    UserID  int    `json:"user_id"`
}

func main() {
    // Initialize the client
    client := supabaseorm.New(
        "https://your-project.supabase.co",
        "your-supabase-api-key",
        supabaseorm.WithTimeout(10*time.Second),
    )

    // Query users with their posts (using join)
    var users []User
    err := client.
        Table("users").
        Select("id", "name", "email").
        InnerJoin("posts", "id", "user_id").
        Where("email", "like", "%@example.com").
        Order("created_at", "desc").
        Limit(10).
        Get(&users)

    if err != nil {
        log.Fatalf("Error querying users: %v", err)
    }

    for _, user := range users {
        fmt.Printf("User: %s (%s)\n", user.Name, user.Email)
        for _, post := range user.Posts {
            fmt.Printf("  - Post: %s\n", post.Title)
        }
    }

    // Execute a raw SQL query
    type Result struct {
        Count int `json:"count"`
    }

    var result Result
    err = client.
        Table("").
        Raw("SELECT COUNT(*) as count FROM users WHERE email LIKE '%@example.com'").
        Get(&result)

    if err != nil {
        log.Fatalf("Error executing raw query: %v", err)
    }

    fmt.Printf("User count: %d\n", result.Count)
}
```

### Authentication

```go
package main

import (
    "context"
    "fmt"
    "log"

    supabaseorm "github.com/rinconrj/supabase-orm/src"
)

func main() {
    // Initialize the client
    client := supabaseorm.New(
        "https://your-project.supabase.co",
        "your-supabase-api-key",
    )

    // Get the auth instance
    auth := client.Auth()

    // Sign up a new user
    signUpReq := supabaseorm.SignUpRequest{
        Email:    "test@example.com",
        Password: "password123",
        UserMetadata: map[string]interface{}{
            "name": "Test User",
            "age":  30,
        },
    }

    authResp, err := auth.SignUp(context.Background(), signUpReq)
    if err != nil {
        log.Fatalf("Error signing up: %v", err)
    }

    fmt.Printf("User signed up: %s\n", authResp.User.Email)
    fmt.Printf("Access token: %s\n", authResp.AccessToken)

    // Sign in with email and password
    signInReq := supabaseorm.SignInRequest{
        Email:    "test@example.com",
        Password: "password123",
    }

    authResp, err = auth.SignInWithPassword(context.Background(), signInReq)
    if err != nil {
        log.Fatalf("Error signing in: %v", err)
    }

    fmt.Printf("User signed in: %s\n", authResp.User.Email)
    fmt.Printf("Access token: %s\n", authResp.AccessToken)

    // Get user information
    user, err := auth.GetUser(context.Background(), authResp.AccessToken)
    if err != nil {
        log.Fatalf("Error getting user: %v", err)
    }

    fmt.Printf("User ID: %s\n", user.ID)
    fmt.Printf("User email: %s\n", user.Email)

    // Sign out
    err = auth.SignOut(context.Background(), authResp.AccessToken)
    if err != nil {
        log.Fatalf("Error signing out: %v", err)
    }

    fmt.Println("User signed out successfully")
}
```

## API Reference

### Client

```go
// Create a new client
client := supabaseorm.New(baseURL, apiKey)

// Create a new client with options
client := supabaseorm.New(
    baseURL,
    apiKey,
    supabaseorm.WithTimeout(10*time.Second),
    supabaseorm.WithHeaders(map[string]string{
        "X-Custom-Header": "value",
    }),
)
```

### Query Builder

```go
// Select specific columns
client.Table("users").Select("id", "name", "email")

// Filter records
client.Table("users").Where("name", "eq", "John")
client.Table("users").Where("age", "gt", 18)
client.Table("users").Where("email", "like", "%@example.com")

// Combine filters with AND
client.Table("users").
    Where("name", "eq", "John").
    Where("age", "gt", 18)

// Combine filters with OR
client.Table("users").
    Where("name", "eq", "John").
    OrWhere("name", "eq", "Jane")

// Order records
client.Table("users").Order("created_at", "desc")

// Limit and offset
client.Table("users").Limit(10).Offset(20)

// Range (for pagination)
client.Table("users").Range(0, 9) // First 10 records

// Get all records
var users []User
client.Table("users").Get(&users)

// Get first record
var user User
client.Table("users").First(&user)

// Insert a record
client.Table("users").Insert(&user)

// Update records
client.Table("users").
    Where("id", "eq", 1).
    Update(&user)

// Delete records
client.Table("users").
    Where("id", "eq", 1).
    Delete()

// Count records
count, err := client.Table("users").Count()
```

### Joins and Relationships

```go
// Join tables
client.Table("users").
    InnerJoin("posts", "id", "user_id").
    Get(&users)

// Custom join with operator
client.Table("users").
    Join("posts", "id", "eq", "user_id").
    Get(&users)

// Left join (emulated)
client.Table("users").
    LeftJoin("posts", "id", "user_id").
    Get(&users)
```

### Raw SQL Queries

```go
// Execute a raw SQL query
var result MyResultType
client.Table("").
    Raw("SELECT * FROM users WHERE email LIKE '%@example.com'").
    Get(&result)

// Raw query with parameters (use parameterized queries to prevent SQL injection)
client.Table("").
    Raw("SELECT * FROM users WHERE id = $1 AND active = $2").
    Get(&result)
```

### Authentication

```go
// Get the auth instance
auth := client.Auth()

// Sign up a new user
signUpReq := supabaseorm.SignUpRequest{
    Email:    "test@example.com",
    Password: "password123",
    UserMetadata: map[string]interface{}{
        "name": "Test User",
    },
}
authResp, err := auth.SignUp(context.Background(), signUpReq)

// Sign in with email and password
signInReq := supabaseorm.SignInRequest{
    Email:    "test@example.com",
    Password: "password123",
}
authResp, err := auth.SignInWithPassword(context.Background(), signInReq)

// Sign in with OTP (One-Time Password)
otpReq := supabaseorm.SignInRequest{
    Email: "test@example.com",
}
err := auth.SignInWithOTP(context.Background(), otpReq)

// Verify OTP
verifyReq := supabaseorm.VerifyRequest{
    Email: "test@example.com",
    Token: "123456", // OTP from email
    Type:  supabaseorm.MagicLinkType,
}
authResp, err := auth.Verify(context.Background(), verifyReq)

// Reset password
resetReq := supabaseorm.ResetPasswordRequest{
    Email: "test@example.com",
}
err := auth.ResetPassword(context.Background(), resetReq)

// Update password
updateReq := supabaseorm.UpdatePasswordRequest{
    Password: "newpassword123",
}
err := auth.UpdatePassword(context.Background(), updateReq, token)

// Refresh token
refreshReq := supabaseorm.RefreshTokenRequest{
    RefreshToken: authResp.RefreshToken,
}
newAuthResp, err := auth.RefreshToken(context.Background(), refreshReq)

// Get user information
user, err := auth.GetUser(context.Background(), token)

// Sign out
err := auth.SignOut(context.Background(), token)
```

### Transactions

```go
// Begin a transaction
tx := client.Begin()

// Use the transaction
err := tx.Table("users").Insert(&user)

// Commit the transaction
err = tx.Commit()

// Rollback the transaction (not supported in current Supabase REST API)
err = tx.Rollback()
```

## License

MIT