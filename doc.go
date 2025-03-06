// Package supabaseorm provides a lightweight ORM-like library for interacting with Supabase's RESTful API from Go applications.
//
// It offers a fluent query builder interface, support for filtering, ordering, pagination,
// table joins, raw SQL queries via RPC, and complete authentication support.
//
// Basic usage:
//
//	client := supabaseorm.New(
//		"https://your-project.supabase.co",
//		"your-supabase-api-key",
//	)
//
//	var users []User
//	err := client.
//		Table("users").
//		Select("id", "name", "email").
//		Where("email", "like", "%@example.com").
//		Get(&users)
//
// For authentication:
//
//	auth := client.Auth()
//	authResp, err := auth.SignInWithPassword(context.Background(), supabaseorm.SignInRequest{
//		Email:    "test@example.com",
//		Password: "password123",
//	})
package supabaseorm
