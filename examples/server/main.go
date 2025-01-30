package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/minasoft-technology/zentask"
)

func main() {
	// Create a server with two queues: "emails" and "notifications"
	server, err := zentask.NewServer(zentask.ServerConfig{
		NatsURL: "nats://localhost:4222",
		Queues: []zentask.QueueConfig{
			{
				Name:         "emails",
				WorkerCount:  3,
				MaxRetries:   5,
				RetryBackoff: time.Second * 5,
			},
			{
				Name:         "notifications",
				WorkerCount:  2,
				MaxRetries:   3,
				RetryBackoff: time.Second * 2,
			},
		},
		ErrorLogger: log.New(os.Stderr, "[ZENTASK] ", log.LstdFlags),
	})
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}
	defer server.Stop()

	// Register handler for email tasks
	err = server.RegisterHandler("emails", func(ctx context.Context, task *zentask.Task) error {
		var email struct {
			To      string `json:"to"`
			Subject string `json:"subject"`
			Body    string `json:"body"`
		}

		// Parse the task payload
		data, err := json.Marshal(task.Payload)
		if err != nil {
			return fmt.Errorf("failed to marshal payload: %v", err)
		}

		if err := json.Unmarshal(data, &email); err != nil {
			return fmt.Errorf("failed to unmarshal email: %v", err)
		}

		// Simulate sending email
		log.Printf("Sending email to %s: %s", email.To, email.Subject)
		time.Sleep(time.Second) // Simulate work
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to register email handler: %v", err)
	}

	// Register handler for notification tasks
	err = server.RegisterHandler("notifications", func(ctx context.Context, task *zentask.Task) error {
		var notification map[string]interface{}

		// Parse the task payload
		data, err := json.Marshal(task.Payload)
		if err != nil {
			return fmt.Errorf("failed to marshal payload: %v", err)
		}

		if err := json.Unmarshal(data, &notification); err != nil {
			return fmt.Errorf("failed to unmarshal notification: %v", err)
		}

		// Simulate sending notification
		log.Printf("Sending notification: %v", notification)
		
		// Simulate random failure for retry demonstration
		if notification["level"] == "urgent" && task.Metadata["retry_count"] == nil {
			return fmt.Errorf("simulated failure for urgent notification")
		}

		return nil
	})
	if err != nil {
		log.Fatalf("Failed to register notification handler: %v", err)
	}

	// Start the server
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan

	log.Println("Shutting down server...")
}
