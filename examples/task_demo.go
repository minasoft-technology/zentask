package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/charmbracelet/log"
	"github.com/minasoft-technology/zentask"
	"github.com/minasoft-technology/zentask/task"
	"github.com/nats-io/nats.go"
)

func main() {
	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize logger
	logger := log.NewWithOptions(os.Stderr, log.Options{
		Level:           log.DebugLevel,
		TimeFormat:      time.Kitchen,
		ReportCaller:    false,
		ReportTimestamp: true,
	})

	// Create NATS server configuration
	config := zentask.NATSConfig{
		NATSPort: 4223,        // Changed from DefaultNATSPort to avoid conflict
		NATSHost: "127.0.0.1", // Use localhost instead of 0.0.0.0
		HTTPPort: 8223,
		HTTPHost: "127.0.0.1", // Use localhost instead of 0.0.0.0
		Debug:    true,
		Logger:   logger,
	}

	// Start embedded NATS server
	ns, err := zentask.NewNATSServer(ctx, config)
	if err != nil {
		logger.Error("Detailed error starting NATS", "error", err)
		os.Exit(1)
	}
	defer ns.Stop()

	logger.Info("NATS server started successfully", "url", ns.GetConnectionURL())

	// Connect to NATS using the server's URL
	nc, err := nats.Connect(ns.GetConnectionURL())
	if err != nil {
		logger.Error("Failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	// Create JetStream context
	js, err := nc.JetStream()
	if err != nil {
		logger.Error("Failed to create JetStream context", "error", err)
		os.Exit(1)
	}

	// Create or update the tasks stream
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TASKS",
		Subjects: []string{"TASKS.*"},
	})
	if err != nil {
		logger.Error("Failed to create stream", "error", err)
		os.Exit(1)
	}

	// Create repository
	repo, err := task.NewJetStreamRepository(js)
	if err != nil {
		logger.Error("Failed to create repository", "error", err)
		os.Exit(1)
	}

	// Create processor
	processor, err := task.NewProcessor(js, repo)
	if err != nil {
		logger.Error("Failed to create processor", "error", err)
		os.Exit(1)
	}

	// Register handlers for different task types
	processor.RegisterHandler("immediate_task", func(ctx context.Context, t *task.Task) error {
		logger.Info("Processing immediate task",
			"name", t.Name,
			"id", t.ID,
			"attempt", t.RetryCount)
		time.Sleep(1 * time.Second) // Simulate work
		return nil
	})

	processor.RegisterHandler("delayed_task", func(ctx context.Context, t *task.Task) error {
		logger.Info("Processing delayed task",
			"name", t.Name,
			"id", t.ID,
			"attempt", t.RetryCount)
		time.Sleep(1 * time.Second) // Simulate work
		return nil
	})

	processor.RegisterHandler("failing_task", func(ctx context.Context, t *task.Task) error {
		logger.Warn("Processing failing task",
			"name", t.Name,
			"id", t.ID,
			"attempt", t.RetryCount)
		return fmt.Errorf("simulated failure")
	})

	// Start the processor
	if err := processor.Start(); err != nil {
		logger.Error("Failed to start processor", "error", err)
		os.Exit(1)
	}
	defer processor.Stop()

	// Create and enqueue different types of tasks
	ctx = context.Background()

	// 1. Immediate task
	immediateTask, err := task.NewTask("Immediate Task").
		WithDescription("Task that runs immediately").
		WithPayload("immediate_task", map[string]string{"key": "value"}).
		Build()
	if err != nil {
		logger.Error("Failed to create immediate task", "error", err)
		os.Exit(1)
	}
	if err := repo.Enqueue(ctx, immediateTask); err != nil {
		logger.Error("Failed to enqueue immediate task", "error", err)
		os.Exit(1)
	}
	logger.Info("Enqueued immediate task", "id", immediateTask.ID)

	// 2. Delayed task
	delayedTask, err := task.NewTask("Delayed Task").
		WithDescription("Task that runs after a delay").
		WithPayload("delayed_task", map[string]string{"key": "value"}).
		WithDelay(5 * time.Second).
		Build()
	if err != nil {
		logger.Error("Failed to create delayed task", "error", err)
		os.Exit(1)
	}
	if err := repo.Enqueue(ctx, delayedTask); err != nil {
		logger.Error("Failed to enqueue delayed task", "error", err)
		os.Exit(1)
	}
	logger.Info("Enqueued delayed task", "id", delayedTask.ID)

	// 3. Failing task
	failingTask, err := task.NewTask("Failing Task").
		WithDescription("Task that always fails").
		WithPayload("failing_task", map[string]string{"key": "value"}).
		WithMaxRetries(3). // Add retry configuration
		Build()
	if err != nil {
		logger.Error("Failed to create failing task", "error", err)
		os.Exit(1)
	}
	if err := repo.Enqueue(ctx, failingTask); err != nil {
		logger.Error("Failed to enqueue failing task", "error", err)
		os.Exit(1)
	}
	logger.Info("Enqueued failing task", "id", failingTask.ID)

	// Watch for task updates
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Watch for updates on both tasks
	updates, err := repo.Watch(watchCtx, immediateTask.ID)
	if err != nil {
		logger.Error("Watch error for task", "error", err)
		os.Exit(1)
	}

	failingUpdates, err := repo.Watch(watchCtx, failingTask.ID)
	if err != nil {
		logger.Error("Watch error for failing task", "error", err)
		os.Exit(1)
	}

	// Print task updates
	go func() {
		for {
			select {
			case t := <-updates:
				logger.Info("Task update",
					"id", t.ID,
					"status", t.Status,
					"error", t.Error)
			case t := <-failingUpdates:
				if t.Error != "" {
					logger.Error("Failing task update",
						"id", t.ID,
						"status", t.Status,
						"error", t.Error)
				} else {
					logger.Info("Failing task update",
						"id", t.ID,
						"status", t.Status)
				}
			case <-watchCtx.Done():
				return
			}
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down...")
}
