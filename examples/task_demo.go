package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/minasoft-technology/zentask"
	"github.com/minasoft-technology/zentask/task"
	"github.com/nats-io/nats.go"
)

func main() {
	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Create NATS server configuration
	config := zentask.NATSConfig{
		NATSPort: 4223,        // Changed from DefaultNATSPort to avoid conflict
		NATSHost: "127.0.0.1", // Use localhost instead of 0.0.0.0
		HTTPPort: 8223,
		HTTPHost: "127.0.0.1", // Use localhost instead of 0.0.0.0
		Debug:    false,
		Logger:   logger,
		Username: "zentask_user",     // Add username for authentication
		Password: "zentask_password", // Add password for authentication
	}

	// Start embedded NATS server
	ns, err := zentask.NewNATSServer(ctx, config)
	if err != nil {
		logger.Error("Detailed error starting NATS", "error", err)
		os.Exit(1)
	}
	defer ns.Stop()

	logger.Info("NATS server started successfully", "url", ns.GetConnectionURL())

	// Connect to NATS using the server's URL with authentication
	nc, err := nats.Connect(ns.GetConnectionURL(),
		nats.UserInfo(config.Username, config.Password))
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
		Storage:  nats.MemoryStorage,
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
			"attempt", t.RetryCount,
			"payload", t.Payload)
		time.Sleep(2 * time.Second) // Simulate work
		return nil
	})

	processor.RegisterHandler("failing_task", func(ctx context.Context, t *task.Task) error {
		logger.Info("Processing failing task",
			"name", t.Name,
			"id", t.ID,
			"attempt", t.RetryCount,
			"max_retries", t.MaxRetries)

		// Fail on first two attempts, succeed on third
		if t.RetryCount < 2 {
			return fmt.Errorf("simulated failure (attempt %d of %d)", t.RetryCount+1, t.MaxRetries)
		}

		logger.Info("Task succeeded after retries",
			"name", t.Name,
			"id", t.ID,
			"final_attempt", t.RetryCount)
		return nil
	})

	// Start the processor
	if err := processor.Start(); err != nil {
		logger.Error("Failed to start processor", "error", err)
		os.Exit(1)
	}

	// Create some example tasks
	tasks := []struct {
		name       string
		taskType   string
		maxRetries int
		delay      time.Duration
	}{
		{
			name:       "Simple Task",
			taskType:   "immediate_task",
			maxRetries: 0,
			delay:      0,
		},
		{
			name:       "Retry Task",
			taskType:   "failing_task",
			maxRetries: 3,
			delay:      time.Second,
		},
	}

	// Create and watch tasks
	for _, t := range tasks {
		taskBuilder := task.NewTask(t.name).
			WithRepository(repo).
			WithPayload(t.taskType, map[string]interface{}{
				"name": t.name,
				"type": t.taskType,
			}).
			WithMaxRetries(t.maxRetries)

		if t.delay > 0 {
			taskBuilder.WithDelay(t.delay)
		}

		createdTask, err := taskBuilder.Enqueue(ctx)
		if err != nil {
			logger.Error("Failed to create task",
				"name", t.name,
				"error", err)
			continue
		}

		logger.Info("Task created successfully",
			"name", t.name,
			"id", createdTask.ID,
			"type", t.taskType)

		// Watch task updates
		updates, err := repo.Watch(ctx, createdTask.ID)
		if err != nil {
			logger.Error("Failed to watch task",
				"id", createdTask.ID,
				"error", err)
			continue
		}

		go func(taskName string, taskID string) {
			for {
				select {
				case <-ctx.Done():
					return
				case update := <-updates:
					if update == nil {
						return
					}
					logger.Info("Task status update",
						"name", taskName,
						"id", taskID,
						"status", update.Status,
						"attempt", update.RetryCount,
						"error", update.Error)
				}
			}
		}(t.name, createdTask.ID)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logger.Info("Shutting down...")
	cancel()
	time.Sleep(time.Second) // Give tasks time to clean up
}
