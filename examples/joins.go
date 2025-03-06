package examples

import (
	"fmt"
	"log"
	"time"

	supabaseorm "github.com/rinconrj/go-supabase-orm"
)

// UserModel represents a user in the database
type UserModel struct {
	ID        int         `json:"id"`
	Name      string      `json:"name"`
	Email     string      `json:"email"`
	CreatedAt time.Time   `json:"created_at"`
	Posts     []PostModel `json:"posts"` // Nested relationship
}

// PostModel represents a blog post in the database
type PostModel struct {
	ID        int            `json:"id"`
	Title     string         `json:"title"`
	Content   string         `json:"content"`
	UserID    int            `json:"user_id"`
	CreatedAt time.Time      `json:"created_at"`
	Comments  []CommentModel `json:"comments"` // Nested relationship
}

// CommentModel represents a comment on a blog post
type CommentModel struct {
	ID        int       `json:"id"`
	Content   string    `json:"content"`
	PostID    int       `json:"post_id"`
	UserID    int       `json:"user_id"`
	CreatedAt time.Time `json:"created_at"`
	User      UserModel `json:"user"` // Nested relationship (commenter)
}

// JoinsExample demonstrates how to use joins to query related data
func JoinsExample() {
	// Initialize the client
	client := supabaseorm.New(
		"https://your-project.supabase.co",
		"your-supabase-api-key",
		supabaseorm.WithTimeout(10*time.Second),
	)

	// Example 1: Query users with their posts
	fmt.Println("Example 1: Query users with their posts")
	var users []UserModel
	err := client.
		Table("users").
		Select("id", "name", "email", "created_at").
		InnerJoin("posts", "id", "user_id").
		Where("email", "like", "%@example.com").
		Order("created_at", "desc").
		Limit(5).
		Get(&users)

	if err != nil {
		log.Fatalf("Error querying users with posts: %v", err)
	}

	fmt.Printf("Found %d users\n", len(users))
	for _, user := range users {
		fmt.Printf("User: %s (%s)\n", user.Name, user.Email)
		fmt.Printf("  Posts: %d\n", len(user.Posts))
		for _, post := range user.Posts {
			fmt.Printf("  - %s\n", post.Title)
		}
	}

	// Example 2: Query posts with comments and comment authors (nested joins)
	fmt.Println("\nExample 2: Query posts with comments and comment authors")
	var posts []PostModel
	err = client.
		Table("posts").
		Select("id", "title", "content", "created_at").
		InnerJoin("comments", "id", "post_id").
		Where("created_at", "gt", "2023-01-01").
		Order("created_at", "desc").
		Limit(3).
		Get(&posts)

	if err != nil {
		log.Fatalf("Error querying posts with comments: %v", err)
	}

	fmt.Printf("Found %d posts\n", len(posts))
	for _, post := range posts {
		fmt.Printf("Post: %s\n", post.Title)
		fmt.Printf("  Comments: %d\n", len(post.Comments))
		for _, comment := range post.Comments {
			fmt.Printf("  - %s (by %s)\n", comment.Content, comment.User.Name)
		}
	}
}
