package examples

import (
	"context"
	"fmt"
	"log"
	"time"

	supabaseorm "github.com/rinconrj/supabase-orm"
)

// User represents a user in the database
type User struct {
	ID        int       `json:"id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
}

func main() {
	fmt.Println("Supabase ORM Examples")
	fmt.Println("====================")

	// Basic example
	basicExample()

	// Run advanced examples
	fmt.Println("\nRunning advanced examples...")
	fmt.Println("====================")

	// Uncomment these to run the advanced examples
	// examples.JoinsExample()
	// examples.RawSQLExample()
	// examples.AuthExample()
}

func basicExample() {
	// Initialize the client
	client := supabaseorm.New(
		"https://your-project.supabase.co",
		"your-supabase-api-key",
		supabaseorm.WithTimeout(10*time.Second),
	)

	// Example 1: Query all users
	var users []User
	err := client.
		Table("users").
		Get(&users)

	if err != nil {
		log.Fatalf("Error querying users: %v", err)
	}

	fmt.Printf("Found %d users\n", len(users))
	for _, user := range users {
		fmt.Printf("User: %s (%s)\n", user.Name, user.Email)
	}

	// Example 2: Query with filters
	var filteredUsers []User
	err = client.
		Table("users").
		Select("id", "name", "email", "created_at").
		Where("email", "like", "%@example.com").
		Order("created_at", "desc").
		Limit(10).
		Get(&filteredUsers)

	if err != nil {
		log.Fatalf("Error querying filtered users: %v", err)
	}

	fmt.Printf("Found %d filtered users\n", len(filteredUsers))

	// Example 3: Insert a new user
	newUser := User{
		Name:  "John Doe",
		Email: "john@example.com",
	}

	err = client.
		Table("users").
		Insert(&newUser)

	if err != nil {
		log.Fatalf("Error inserting user: %v", err)
	}

	fmt.Printf("Inserted user with ID: %d\n", newUser.ID)

	// Example 4: Update a user
	newUser.Name = "John Smith"
	err = client.
		Table("users").
		Where("id", "=", newUser.ID).
		Update(&newUser)

	if err != nil {
		log.Fatalf("Error updating user: %v", err)
	}

	fmt.Println("User updated successfully")

	// Example 5: Delete a user
	err = client.
		Table("users").
		Where("id", "=", newUser.ID).
		Delete()

	if err != nil {
		log.Fatalf("Error deleting user: %v", err)
	}

	fmt.Println("User deleted successfully")

	// Example 6: Authentication
	fmt.Println("\nExample 6: Authentication (basic)")

	// Get the auth instance
	auth := client.Auth()

	// Sign in with email and password
	signInReq := supabaseorm.SignInRequest{
		Email:    "test@example.com",
		Password: "password123",
	}

	authResp, err := auth.SignInWithPassword(context.Background(), signInReq)
	if err != nil {
		log.Printf("Error signing in: %v", err)
	} else {
		fmt.Printf("User signed in: %s\n", authResp.User.Email)
		fmt.Printf("Access token: %s\n", authResp.AccessToken)
	}
}
