package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/minasoft-technology/zentask"
	"github.com/minasoft-technology/zentask/task"
	"github.com/nats-io/nats.go"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize embedded NATS server with default configuration
	natsConfig := zentask.NATSConfig{
		Logger:   logger,
		Debug:    false,
		Username: "zentask_user",     // Add username for authentication
		Password: "zentask_password", // Add password for authentication
	}

	natsServer, err := zentask.NewNATSServer(ctx, natsConfig)
	if err != nil {
		logger.Error("Failed to create NATS server", "error", err)
	}

	// Connect to the embedded NATS server with authentication
	nc, err := nats.Connect(natsServer.GetConnectionURL(),
		nats.UserInfo(natsConfig.Username, natsConfig.Password))
	if err != nil {
		logger.Error("Failed to connect to NATS", "error", err)
		panic(err) // Exit if we can't connect to NATS
	}
	defer nc.Close()

	// Create JetStream Context
	js, err := nc.JetStream()
	if err != nil {
		logger.Error("Failed to create JetStream context", "error", err)
		panic(err) // Exit if we can't create JetStream context
	}

	// Initialize task repository
	taskRepo, err := task.NewJetStreamRepository(js)
	if err != nil {
		logger.Error("Failed to create task repository", "error", err)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info("Shutting down gracefully...")
		natsServer.Stop() // Stop the NATS server
		cancel()
	}()

	// Example: Create and enqueue a task
	task, err := taskRepo.EnqueueTask(ctx, "example-task",
		task.WithDescription("This is an example task"),
		task.WithPriority(task.PriorityHigh),
		task.WithTags("example", "test"),
	)
	if err != nil {
		logger.Error("Failed to create task", "error", err)
	} else {
		logger.Info("Created task", "id", task.ID)
	}

	// Example: Watch for task updates
	taskChan, err := taskRepo.Watch(ctx, task.ID)
	if err != nil {
		logger.Error("Failed to watch task", "error", err)
	} else {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case updatedTask := <-taskChan:
					if updatedTask != nil {
						logger.Info("Task updated",
							"id", updatedTask.ID,
							"status", updatedTask.Status,
						)
					}
				}
			}
		}()
	}

	// Keep the application running until interrupted
	<-ctx.Done()
	logger.Info("Application stopped")
}
