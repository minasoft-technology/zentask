package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"testing"
	"time"

	"github.com/minasoft-technology/zentask"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func TestTaskExamples(t *testing.T) {
	// Start a test NATS server
	opts := &server.Options{
		Host:        "127.0.0.1",
		Port:        -1, // Will pick a random available port
		NoLog:       true,
		NoSigs:      true,
		JetStream:   true,
		StoreDir:    t.TempDir(),
		MaxPayload:  server.MAX_PAYLOAD_SIZE,
	}

	ns, err := server.NewServer(opts)
	require.NoError(t, err)
	
	go ns.Start()
	defer ns.Shutdown()
	
	// Wait for server to be ready
	if !ns.ReadyForConnections(4 * time.Second) {
		t.Fatal("NATS server failed to start")
	}

	// Get client URL
	natsURL := ns.ClientURL()

	// Create JetStream context and configure stream
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()

	js, err := nc.JetStream()
	require.NoError(t, err)

	// Create or update the stream
	_, err = js.StreamInfo("ZENTASK")
	if err != nil {
		// Stream doesn't exist, create it
		_, err = js.AddStream(&nats.StreamConfig{
			Name:      "ZENTASK",
			Subjects:  []string{"ZENTASK.tasks.>"},
			Storage:   nats.FileStorage,
			Retention: nats.WorkQueuePolicy,
		})
		require.NoError(t, err)
	} else {
		// Stream exists, update it
		_, err = js.UpdateStream(&nats.StreamConfig{
			Name:      "ZENTASK",
			Subjects:  []string{"ZENTASK.tasks.>"},
			Storage:   nats.FileStorage,
			Retention: nats.WorkQueuePolicy,
		})
		require.NoError(t, err)
	}

	// Create and start the server first
	zentaskServer, err := zentask.NewServer(zentask.ServerConfig{
		NatsURL: natsURL,
		Queues: []zentask.QueueConfig{
			{
				Name:         "emails",
				WorkerCount:  2,
				MaxRetries:   2,
				RetryBackoff: time.Second,
			},
			{
				Name:         "notifications",
				WorkerCount:  2,
				MaxRetries:   2,
				RetryBackoff: time.Second,
			},
		},
		StreamName:   "ZENTASK",
		ErrorLogger:  log.New(os.Stderr, "[TEST] ", log.LstdFlags),
	})
	if err != nil {
		t.Skipf("Skipping test: failed to connect to NATS at %s: %v", natsURL, err)
		return
	}
	require.NotNil(t, zentaskServer)
	defer zentaskServer.Stop()

	// Track processed tasks
	processedTasks := make(map[string]chan *zentask.Task)
	processedTasks["emails"] = make(chan *zentask.Task, 2)
	processedTasks["notifications"] = make(chan *zentask.Task, 1)

	// Register handlers
	err = zentaskServer.RegisterHandler("emails", func(ctx context.Context, task *zentask.Task) error {
		select {
		case processedTasks["emails"] <- task:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	require.NoError(t, err)

	err = zentaskServer.RegisterHandler("notifications", func(ctx context.Context, task *zentask.Task) error {
		select {
		case processedTasks["notifications"] <- task:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	require.NoError(t, err)

	// Start the server
	err = zentaskServer.Start()
	require.NoError(t, err)

	// Wait a bit for the server to be ready
	time.Sleep(time.Second)

	// Initialize the client
	zentaskClient, err := zentask.NewClient(zentask.Config{
		NatsURL:    natsURL,
		StreamName: "ZENTASK",
		RetryAttempts: 3,
		RetryBackoff:  time.Second,
		RateLimit:     100,
		RateBurst:     10,
	})
	require.NoError(t, err)
	require.NotNil(t, zentaskClient)
	defer zentaskClient.Close()

	// Test 1: Simple immediate task
	t.Run("SimpleTask", func(t *testing.T) {
		simpleTask := &zentask.Task{
			Queue: "emails",
			Payload: map[string]interface{}{
				"to":      "user@example.com",
				"subject": "Welcome!",
				"body":    "Welcome to our platform!",
			},
		}

		info, err := zentaskClient.Enqueue(simpleTask)
		require.NoError(t, err)
		require.NotEmpty(t, info.ID)
		require.Equal(t, "emails", info.Queue)

		// Wait for task processing
		select {
		case task := <-processedTasks["emails"]:
			require.Equal(t, simpleTask.Queue, task.Queue)
			
			// Convert task payload to map
			payloadBytes, err := json.Marshal(task.Payload)
			require.NoError(t, err)
			
			var payload map[string]interface{}
			err = json.Unmarshal(payloadBytes, &payload)
			require.NoError(t, err)
			
			require.Equal(t, "user@example.com", payload["to"])
			require.Equal(t, "Welcome!", payload["subject"])
			require.Equal(t, "Welcome to our platform!", payload["body"])
		case <-time.After(15 * time.Second):
			t.Fatal("Timeout waiting for simple task processing")
		}
	})

	// Test 2: Delayed task with options
	t.Run("DelayedTask", func(t *testing.T) {
		processAt := time.Now().Add(3 * time.Second) // Use 3 second delay for better testing
		delayedTask := &zentask.Task{
			Queue: "emails",
			Payload: map[string]interface{}{
				"to":      "user@example.com",
				"subject": "Reminder",
				"body":    "Don't forget your appointment tomorrow!",
			},
			Options: &zentask.TaskOptions{
				ProcessAt:  processAt,
				MaxRetries: 5,
				RetryDelay: time.Second,
				Priority:   2,
				UniqueKey:  "reminder-email-user123",
			},
			Metadata: map[string]interface{}{
				"customer_id": "user123",
				"type":       "reminder",
			},
		}

		info, err := zentaskClient.Enqueue(delayedTask)
		require.NoError(t, err)
		require.NotEmpty(t, info.ID)
		require.Equal(t, "emails", info.Queue)

		// Verify task is not processed before ProcessAt time
		select {
		case <-processedTasks["emails"]:
			t.Fatal("Task was processed before ProcessAt time")
		case <-time.After(2 * time.Second):
			// This is expected - task should not be processed yet
		}

		// Wait for task processing after ProcessAt time
		select {
		case task := <-processedTasks["emails"]:
			require.Equal(t, delayedTask.Queue, task.Queue)
			
			// Verify ProcessAt time was respected
			require.NotNil(t, task.Options)
			require.False(t, task.Options.ProcessAt.IsZero())
			require.Equal(t, processAt.Unix(), task.Options.ProcessAt.Unix())
			
			// Convert task payload to map
			payloadBytes, err := json.Marshal(task.Payload)
			require.NoError(t, err)
			
			var payload map[string]interface{}
			err = json.Unmarshal(payloadBytes, &payload)
			require.NoError(t, err)
			
			require.Equal(t, "user@example.com", payload["to"])
			require.Equal(t, "Reminder", payload["subject"])
			require.Equal(t, "Don't forget your appointment tomorrow!", payload["body"])

			// Verify metadata was preserved
			require.Equal(t, "user123", task.Metadata["customer_id"])
			require.Equal(t, "reminder", task.Metadata["type"])
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for delayed task processing")
		}
	})

	// Test 3: Task with custom context
	t.Run("UrgentTask", func(t *testing.T) {
		// Context for task submission
		submitCtx, submitCancel := context.WithTimeout(context.Background(), time.Second*5)
		defer submitCancel()

		// Context for waiting for task processing
		waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second*15)
		defer waitCancel()

		urgentTask := &zentask.Task{
			Queue: "notifications",
			Payload: map[string]interface{}{
				"user_id": "123",
				"message": "Your account requires immediate attention",
				"level":   "urgent",
			},
			Options: &zentask.TaskOptions{
				Priority: 1,
				Timeout:  time.Second * 10,
			},
		}

		info, err := zentaskClient.EnqueueContext(submitCtx, urgentTask)
		require.NoError(t, err)
		require.NotEmpty(t, info.ID)
		require.Equal(t, "notifications", info.Queue)

		// Wait for task processing
		select {
		case task := <-processedTasks["notifications"]:
			require.Equal(t, urgentTask.Queue, task.Queue)
			
			// Convert task payload to map
			payloadBytes, err := json.Marshal(task.Payload)
			require.NoError(t, err)
			
			var payload map[string]interface{}
			err = json.Unmarshal(payloadBytes, &payload)
			require.NoError(t, err)
			
			require.Equal(t, "123", payload["user_id"])
			require.Equal(t, "Your account requires immediate attention", payload["message"])
			require.Equal(t, "urgent", payload["level"])
		case <-waitCtx.Done():
			t.Fatal("Context timeout while waiting for urgent task processing")
		}
	})
}