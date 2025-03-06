package examples

import (
	"fmt"
	"log"
	"time"

	supabaseorm "github.com/rinconrj/go-supabase-orm"
)

// RawSQLExample demonstrates how to use raw SQL queries
func RawSQLExample() {
	// Initialize the client
	client := supabaseorm.New(
		"https://your-project.supabase.co",
		"your-supabase-api-key",
		supabaseorm.WithTimeout(10*time.Second),
	)

	// Example 1: Execute a raw SQL query to get post counts by user
	fmt.Println("Example 1: Raw SQL query for post counts by user")

	type PostCount struct {
		UserID    int    `json:"user_id"`
		UserName  string `json:"user_name"`
		PostCount int    `json:"post_count"`
	}

	var postCounts []PostCount
	err := client.
		Table("").
		Raw(`
			SELECT
				u.id as user_id,
				u.name as user_name,
				COUNT(p.id) as post_count
			FROM
				users u
			LEFT JOIN
				posts p ON u.id = p.user_id
			GROUP BY
				u.id, u.name
			ORDER BY
				post_count DESC
			LIMIT 5
		`).
		Get(&postCounts)

	if err != nil {
		log.Fatalf("Error executing raw query: %v", err)
	}

	fmt.Println("Post counts by user:")
	for _, pc := range postCounts {
		fmt.Printf("  %s: %d posts\n", pc.UserName, pc.PostCount)
	}

	// Example 2: Execute a raw SQL query for complex analytics
	fmt.Println("\nExample 2: Raw SQL query for complex analytics")

	type UserActivity struct {
		UserID        int       `json:"user_id"`
		UserName      string    `json:"user_name"`
		PostCount     int       `json:"post_count"`
		CommentCount  int       `json:"comment_count"`
		LastActive    time.Time `json:"last_active"`
		AvgCommentLen float64   `json:"avg_comment_length"`
	}

	var userActivity []UserActivity
	err = client.
		Table("").
		Raw(`
			SELECT
				u.id as user_id,
				u.name as user_name,
				COUNT(DISTINCT p.id) as post_count,
				COUNT(DISTINCT c.id) as comment_count,
				MAX(GREATEST(p.created_at, c.created_at)) as last_active,
				AVG(LENGTH(c.content)) as avg_comment_length
			FROM
				users u
			LEFT JOIN
				posts p ON u.id = p.user_id
			LEFT JOIN
				comments c ON u.id = c.user_id
			GROUP BY
				u.id, u.name
			ORDER BY
				last_active DESC
			LIMIT 5
		`).
		Get(&userActivity)

	if err != nil {
		log.Fatalf("Error executing complex raw query: %v", err)
	}

	fmt.Println("User activity:")
	for _, ua := range userActivity {
		fmt.Printf("  %s: %d posts, %d comments, last active: %s\n",
			ua.UserName, ua.PostCount, ua.CommentCount, ua.LastActive.Format("2006-01-02"))
		fmt.Printf("    Average comment length: %.1f characters\n", ua.AvgCommentLen)
	}
}
