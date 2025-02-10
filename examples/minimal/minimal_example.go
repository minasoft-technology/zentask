package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/minasoft-technology/zentask/task"
	"github.com/minasoft-technology/zentask/zenhelper"
)

func main() {
	// Initialize basic logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Create minimal configuration
	config := zenhelper.Config{
		Logger: logger,
		NATSOptions: &zenhelper.NATSOptions{
			Host: "127.0.0.1",
			Port: 4222,
		},
	}

	// Create context
	ctx := context.Background()

	// Initialize ZenTask with minimal config
	zen, err := zenhelper.NewZenTask(ctx, config)
	if err != nil {
		logger.Error("Failed to create ZenTask", "error", err)
		os.Exit(1)
	}
	defer zen.Stop()

	// Register a simple task handler
	err = zen.AddHandler("greeting", func(ctx context.Context, t *task.Task) error {
		var message string
		if err := t.GetPayloadData(&message); err != nil {
			return err
		}
		fmt.Printf("Received greeting: %s\n", message)
		return nil
	})
	if err != nil {
		logger.Error("Failed to add handler", "error", err)
		os.Exit(1)
	}

	// Create and enqueue a simple task
	_, err = zen.EnqueueTask("hello", "greeting", "Hello, ZenTask!")
	if err != nil {
		logger.Error("Failed to enqueue task", "error", err)
		os.Exit(1)
	}

	// Wait a bit to see the task being processed
	time.Sleep(2 * time.Second)
}
