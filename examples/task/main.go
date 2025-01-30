package main

import (
	"context"
	"fmt"
	"time"

	"github.com/minasoft-technology/zentask"
)

// EmailData represents sample email task payload
type EmailData struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func main() {
	// Initialize the client
	config := zentask.Config{
		NatsURL:       "nats://localhost:4222",
		RetryAttempts: 3,
		RetryBackoff:  time.Second * 5,
		RateLimit:     100,
		RateBurst:     10,
	}

	client, err := zentask.NewClient(config)
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		return
	}
	defer client.Close()

	// Example 1: Simple immediate task
	simpleTask := &zentask.Task{
		Queue: "emails",
		Payload: EmailData{
			To:      "user@example.com",
			Subject: "Welcome!",
			Body:    "Welcome to our platform!",
		},
	}

	info, err := client.Enqueue(simpleTask)
	if err != nil {
		fmt.Printf("Failed to enqueue simple task: %v\n", err)
	} else {
		fmt.Printf("Enqueued simple task: ID=%s, Queue=%s\n", info.ID, info.Queue)
	}

	// Example 2: Delayed task with options
	delayedTask := &zentask.Task{
		Queue: "emails",
		Payload: EmailData{
			To:      "user@example.com",
			Subject: "Reminder",
			Body:    "Don't forget your appointment tomorrow!",
		},
		Options: &zentask.TaskOptions{
			ProcessAt:  time.Now().Add(time.Hour * 24), // Process after 24 hours
			MaxRetries: 5,
			RetryDelay: time.Minute * 5,
			Priority:   2,
			UniqueKey:  "reminder-email-user123", // Prevent duplicates
		},
		Metadata: map[string]interface{}{
			"customer_id": "user123",
			"type":        "reminder_email",
		},
	}

	info, err = client.Enqueue(delayedTask)
	if err != nil {
		fmt.Printf("Failed to enqueue delayed task: %v\n", err)
	} else {
		fmt.Printf("Enqueued delayed task: ID=%s, Queue=%s, ProcessAt=%v\n",
			info.ID, info.Queue, info.ProcessAt)
	}

	// Example 3: Task with custom context
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	urgentTask := &zentask.Task{
		Queue: "notifications",
		Payload: map[string]interface{}{
			"user_id": "123",
			"message": "Your account requires immediate attention",
			"level":   "urgent",
		},
		Options: &zentask.TaskOptions{
			Priority: 1,           // High priority
			Timeout:  time.Minute, // Task processing timeout
		},
	}

	info, err = client.EnqueueContext(ctx, urgentTask)
	if err != nil {
		fmt.Printf("Failed to enqueue urgent task: %v\n", err)
	} else {
		fmt.Printf("Enqueued urgent task: ID=%s, Queue=%s\n", info.ID, info.Queue)
	}
}
