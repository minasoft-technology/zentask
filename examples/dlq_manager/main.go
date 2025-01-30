package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/minasoft-technology/zentask"
	"github.com/minasoft-technology/zentask/dlq"
)

func main() {
	// Create error logger
	errorLogger := log.New(os.Stderr, "[ZENTASK] ", log.LstdFlags)

	// Create DLQ manager
	manager, err := dlq.NewManager(dlq.ManagerConfig{
		NatsURL:     "nats://localhost:4222",
		StreamName:  "ZENTASK",
		ErrorLogger: errorLogger,
		AlertHandler: func(alert dlq.Alert) {
			// In production, you might want to send this to your monitoring system
			log.Printf("DLQ Alert - Type: %s, Queue: %s, Message: %s, Current: %d, Threshold: %d",
				alert.Type, alert.Queue, alert.Message, alert.CurrentVal, alert.Threshold)
		},
	})
	if err != nil {
		log.Fatalf("Failed to create DLQ manager: %v", err)
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start DLQ monitoring
	if err := manager.StartMonitoring(ctx); err != nil {
		log.Fatalf("Failed to start DLQ monitoring: %v", err)
	}

	// Create server with DLQ enabled
	server, err := zentask.NewServer(zentask.ServerConfig{
		NatsURL: "nats://localhost:4222",
		Queues: []zentask.QueueConfig{
			{
				Name:         "critical_tasks",
				WorkerCount:  5,
				MaxRetries:   3,
				RetryBackoff: time.Second * 5,
			},
		},
		ErrorLogger:   errorLogger,
		DLQEnabled:    true,
		DLQRetention:  7 * 24 * time.Hour,
	})
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}
	defer server.Stop()

	// Register task handler that sometimes fails
	err = server.RegisterHandler("critical_tasks", func(ctx context.Context, task *zentask.Task) error {
		// Simulate random failures
		if time.Now().Unix()%2 == 0 {
			return fmt.Errorf("simulated failure")
		}
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to register handler: %v", err)
	}

	// Start the server
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// Start periodic DLQ processing
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Get DLQ stats
				stats := manager.GetStats()
				log.Printf("DLQ Stats - Total Messages: %d", stats.TotalMessages)
				
				// Print error distribution
				for errType, count := range stats.ErrorTypes {
					log.Printf("Error Type: %s, Count: %d", errType, count)
				}

				// Print queue distribution
				for queue, count := range stats.QueueDistribution {
					log.Printf("Queue: %s, Failed Tasks: %d", queue, count)
				}

				// Retry tasks if needed
				if stats.TotalMessages > 0 {
					retryCtx, retryCancel := context.WithTimeout(ctx, 5*time.Minute)
					err := manager.RetryMessages(retryCtx, "critical_tasks", 100)
					if err != nil {
						log.Printf("Failed to retry messages: %v", err)
					}
					retryCancel()
				}
			}
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
}
