package examples

import (
	"context"
	"fmt"
	"log"
	"time"

	supabaseorm "github.com/zoc/supabase-orm"
)

// AuthExample demonstrates how to use the authentication features
func AuthExample() {
	// Initialize the client
	client := supabaseorm.New(
		"https://your-project.supabase.co",
		"your-supabase-api-key",
		supabaseorm.WithTimeout(10*time.Second),
	)

	// Get the auth instance
	auth := client.Auth()

	// Example 1: Sign up a new user
	fmt.Println("Example 1: Sign up a new user")
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
		log.Printf("Error signing up: %v", err)
	} else {
		fmt.Printf("User signed up: %s\n", authResp.User.Email)
		fmt.Printf("Access token: %s\n", authResp.AccessToken)
		fmt.Printf("Token expires at: %s\n", authResp.ExpiresAt.Format(time.RFC3339))
	}

	// Example 2: Sign in with email and password
	fmt.Println("\nExample 2: Sign in with email and password")
	signInReq := supabaseorm.SignInRequest{
		Email:    "test@example.com",
		Password: "password123",
	}

	authResp, err = auth.SignInWithPassword(context.Background(), signInReq)
	if err != nil {
		log.Printf("Error signing in: %v", err)
	} else {
		fmt.Printf("User signed in: %s\n", authResp.User.Email)
		fmt.Printf("Access token: %s\n", authResp.AccessToken)
		fmt.Printf("Token expires at: %s\n", authResp.ExpiresAt.Format(time.RFC3339))

		// Store the token for later use
		token := authResp.AccessToken

		// Example 3: Get user information
		fmt.Println("\nExample 3: Get user information")
		user, err := auth.GetUser(context.Background(), token)
		if err != nil {
			log.Printf("Error getting user: %v", err)
		} else {
			fmt.Printf("User ID: %s\n", user.ID)
			fmt.Printf("User email: %s\n", user.Email)
			fmt.Printf("User role: %s\n", user.Role)
			fmt.Printf("User metadata: %v\n", user.UserMetadata)
		}

		// Example 4: Update password
		fmt.Println("\nExample 4: Update password")
		updatePasswordReq := supabaseorm.UpdatePasswordRequest{
			Password: "newpassword123",
		}

		err = auth.UpdatePassword(context.Background(), updatePasswordReq, token)
		if err != nil {
			log.Printf("Error updating password: %v", err)
		} else {
			fmt.Println("Password updated successfully")
		}

		// Example 5: Refresh token
		fmt.Println("\nExample 5: Refresh token")
		refreshTokenReq := supabaseorm.RefreshTokenRequest{
			RefreshToken: authResp.RefreshToken,
		}

		refreshedAuthResp, err := auth.RefreshToken(context.Background(), refreshTokenReq)
		if err != nil {
			log.Printf("Error refreshing token: %v", err)
		} else {
			fmt.Printf("Token refreshed: %s\n", refreshedAuthResp.AccessToken)
			fmt.Printf("New token expires at: %s\n", refreshedAuthResp.ExpiresAt.Format(time.RFC3339))

			// Update the token
			token = refreshedAuthResp.AccessToken
		}

		// Example 6: Sign out
		fmt.Println("\nExample 6: Sign out")
		err = auth.SignOut(context.Background(), token)
		if err != nil {
			log.Printf("Error signing out: %v", err)
		} else {
			fmt.Println("User signed out successfully")
		}
	}

	// Example 7: Reset password
	fmt.Println("\nExample 7: Reset password")
	resetPasswordReq := supabaseorm.ResetPasswordRequest{
		Email: "test@example.com",
	}

	err = auth.ResetPassword(context.Background(), resetPasswordReq)
	if err != nil {
		log.Printf("Error resetting password: %v", err)
	} else {
		fmt.Println("Password reset email sent")
	}

	// Example 8: Sign in with OTP (One-Time Password)
	fmt.Println("\nExample 8: Sign in with OTP")
	otpReq := supabaseorm.SignInRequest{
		Email: "test@example.com",
	}

	err = auth.SignInWithOTP(context.Background(), otpReq)
	if err != nil {
		log.Printf("Error sending OTP: %v", err)
	} else {
		fmt.Println("OTP sent to email")

		// In a real application, you would get the OTP from the user
		// and then verify it
		fmt.Println("\nVerifying OTP (this is a simulation)")
		verifyReq := supabaseorm.VerifyRequest{
			Email: "test@example.com",
			Token: "123456", // This would be the OTP from the user
			Type:  supabaseorm.MagicLinkType,
		}

		_, err = auth.Verify(context.Background(), verifyReq)
		if err != nil {
			log.Printf("Error verifying OTP: %v", err)
		} else {
			fmt.Println("OTP verified successfully")
		}
	}
}
