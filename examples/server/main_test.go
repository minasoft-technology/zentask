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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

type NotificationPayload struct {
	UserID  string `json:"user_id"`
	Message string `json:"message"`
	Type    string `json:"type"`
}

func TestServerExample(t *testing.T) {
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

	// Create a test server
	server, err := zentask.NewServer(zentask.ServerConfig{
		NatsURL: natsURL,
		Queues: []zentask.QueueConfig{
			{
				Name:         "emails",
				WorkerCount:  1,
				MaxRetries:   2,
				RetryBackoff: time.Second,
			},
			{
				Name:         "notifications",
				WorkerCount:  1,
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
	require.NotNil(t, server)
	defer server.Stop()

	// Test email handler registration and processing
	emailProcessed := make(chan bool)
	err = server.RegisterHandler("emails", func(ctx context.Context, task *zentask.Task) error {
		var email EmailPayload
		data, err := json.Marshal(task.Payload)
		require.NoError(t, err)
		
		err = json.Unmarshal(data, &email)
		require.NoError(t, err)
		
		// Verify email payload
		assert.Equal(t, "test@example.com", email.To)
		assert.Equal(t, "Test Subject", email.Subject)
		assert.Equal(t, "Test Body", email.Body)
		
		emailProcessed <- true
		return nil
	})
	require.NoError(t, err)

	// Test notification handler registration and processing
	notificationProcessed := make(chan bool)
	err = server.RegisterHandler("notifications", func(ctx context.Context, task *zentask.Task) error {
		var notification NotificationPayload
		data, err := json.Marshal(task.Payload)
		require.NoError(t, err)
		
		err = json.Unmarshal(data, &notification)
		require.NoError(t, err)
		
		// Verify notification payload
		assert.Equal(t, "user123", notification.UserID)
		assert.Equal(t, "Test notification", notification.Message)
		assert.Equal(t, "info", notification.Type)
		
		notificationProcessed <- true
		return nil
	})
	require.NoError(t, err)

	// Start the server
	err = server.Start()
	require.NoError(t, err)

	// Wait a bit for the server to be ready
	time.Sleep(time.Second)

	// Create a test client
	client, err := zentask.NewClient(zentask.Config{
		NatsURL:    natsURL,
		StreamName: "ZENTASK",
	})
	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	// Submit test email task
	_, err = client.EnqueueContext(context.Background(), &zentask.Task{
		Queue: "emails",
		Payload: EmailPayload{
			To:      "test@example.com",
			Subject: "Test Subject",
			Body:    "Test Body",
		},
	})
	require.NoError(t, err)

	// Submit test notification task
	_, err = client.EnqueueContext(context.Background(), &zentask.Task{
		Queue: "notifications",
		Payload: NotificationPayload{
			UserID:  "user123",
			Message: "Test notification",
			Type:    "info",
		},
	})
	require.NoError(t, err)

	// Wait for task processing with timeout
	select {
	case <-emailProcessed:
		// Email processed successfully
	case <-time.After(15 * time.Second):
		t.Error("Timeout waiting for email processing")
	}

	select {
	case <-notificationProcessed:
		// Notification processed successfully
	case <-time.After(15 * time.Second):
		t.Error("Timeout waiting for notification processing")
	}
}
