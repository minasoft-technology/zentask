package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/minasoft-technology/zentask"
)

// UserRegistration represents a new user registration
type UserRegistration struct {
	Email     string    `json:"email"`
	Username  string    `json:"username"`
	CreatedAt time.Time `json:"created_at"`
}

// WelcomeEmail represents the welcome email to be sent
type WelcomeEmail struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func main() {
	// 1. Start the task server (typically in a separate service)
	server, err := zentask.NewServer(zentask.ServerConfig{
		NatsURL: "nats://localhost:4222",
		Queues: []zentask.QueueConfig{
			{
				Name:         "registrations",
				WorkerCount:  2,
				MaxRetries:   3,
				RetryBackoff: time.Second,
			},
			{
				Name:         "emails",
				WorkerCount:  2,
				MaxRetries:   3,
				RetryBackoff: time.Second,
			},
		},
		ErrorLogger: log.New(os.Stderr, "[ZENTASK] ", log.LstdFlags),
	})
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Register handler for processing registrations
	err = server.RegisterHandler("registrations", func(ctx context.Context, task *zentask.Task) error {
		var registration UserRegistration
		data, err := json.Marshal(task.Payload)
		if err != nil {
			return fmt.Errorf("failed to marshal registration data: %v", err)
		}

		if err := json.Unmarshal(data, &registration); err != nil {
			return fmt.Errorf("failed to unmarshal registration: %v", err)
		}

		log.Printf("Processing registration for user: %s", registration.Username)

		// Create welcome email task
		client, err := zentask.NewClient(zentask.Config{
			NatsURL: "nats://localhost:4222",
		})
		if err != nil {
			return fmt.Errorf("failed to create client: %v", err)
		}
		defer client.Close()

		// Enqueue welcome email task
		welcomeEmail := WelcomeEmail{
			To:      registration.Email,
			Subject: "Welcome to Our Service!",
			Body:    fmt.Sprintf("Hi %s,\n\nWelcome to our service! We're excited to have you on board.", registration.Username),
		}

		_, err = client.EnqueueContext(ctx, &zentask.Task{
			Queue:   "emails",
			Payload: welcomeEmail,
			Options: &zentask.TaskOptions{
				MaxRetries: 3,
				Priority:  1, // High priority for welcome emails
			},
		})
		if err != nil {
			return fmt.Errorf("failed to enqueue welcome email: %v", err)
		}

		return nil
	})
	if err != nil {
		log.Fatalf("Failed to register registration handler: %v", err)
	}

	// Register handler for sending emails
	err = server.RegisterHandler("emails", func(ctx context.Context, task *zentask.Task) error {
		var email WelcomeEmail
		data, err := json.Marshal(task.Payload)
		if err != nil {
			return fmt.Errorf("failed to marshal email data: %v", err)
		}

		if err := json.Unmarshal(data, &email); err != nil {
			return fmt.Errorf("failed to unmarshal email: %v", err)
		}

		// Here you would integrate with your email service
		log.Printf("Sending email to: %s\nSubject: %s\nBody: %s", 
			email.To, email.Subject, email.Body)

		return nil
	})
	if err != nil {
		log.Fatalf("Failed to register email handler: %v", err)
	}

	// Start the server
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	// 2. Create a client to enqueue tasks (typically in your web service)
	client, err := zentask.NewClient(zentask.Config{
		NatsURL: "nats://localhost:4222",
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Simulate processing some registrations
	registrations := []UserRegistration{
		{
			Email:     "john@example.com",
			Username:  "john_doe",
			CreatedAt: time.Now(),
		},
		{
			Email:     "jane@example.com",
			Username:  "jane_doe",
			CreatedAt: time.Now(),
		},
	}

	// Process each registration
	for _, reg := range registrations {
		// Enqueue registration task
		_, err := client.EnqueueContext(context.Background(), &zentask.Task{
			Queue:   "registrations",
			Payload: reg,
			Metadata: map[string]interface{}{
				"source": "web",
				"ip":     "192.168.1.1",
			},
		})
		if err != nil {
			log.Printf("Failed to enqueue registration task: %v", err)
			continue
		}
		log.Printf("Enqueued registration task for user: %s", reg.Username)
	}

	// Keep the program running
	select {}
}
