package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/minasoft-technology/zentask"
)

// EmailTask represents an email task
type EmailTask struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func main() {
	// Create a server with DLQ enabled
	server, err := zentask.NewServer(zentask.ServerConfig{
		NatsURL: "nats://localhost:4222",
		Queues: []zentask.QueueConfig{
			{
				Name:         "emails",
				WorkerCount:  3,
				MaxRetries:   3,
				RetryBackoff: time.Second * 5,
				DLQSubject:   "ZENTASK.dlq.emails", // Custom DLQ subject
			},
		},
		ErrorLogger:   log.New(os.Stderr, "[ZENTASK] ", log.LstdFlags),
		DLQEnabled:    true,
		DLQRetention:  7 * 24 * time.Hour, // Keep failed tasks for 7 days
	})
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}
	defer server.Stop()

	// Register handler for email tasks
	err = server.RegisterHandler("emails", func(ctx context.Context, task *zentask.Task) error {
		// Simulate a failing task
		return fmt.Errorf("email service unavailable")
	})
	if err != nil {
		log.Fatalf("Failed to register handler: %v", err)
	}

	// Start the server
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// Create a client to enqueue tasks
	client, err := zentask.NewClient(zentask.Config{
		NatsURL: "nats://localhost:4222",
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Enqueue a task that will fail
	task := &zentask.Task{
		Queue: "emails",
		Payload: EmailTask{
			To:      "user@example.com",
			Subject: "Test Email",
			Body:    "This email will fail and go to DLQ",
		},
		Options: &zentask.TaskOptions{
			MaxRetries: 3,
			RetryDelay: time.Second * 2,
		},
	}

	_, err = client.Enqueue(task)
	if err != nil {
		log.Fatalf("Failed to enqueue task: %v", err)
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
}
