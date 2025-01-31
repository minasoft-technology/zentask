package zentask

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runNATSServer(t *testing.T) (*server.Server, string) {
	opts := &server.Options{
		Host:        "127.0.0.1",
		Port:        -1, // Will pick a random available port
		NoLog:       true,
		NoSigs:      true,
		JetStream:   true,                      // Enable JetStream
		StoreDir:    t.TempDir(),              // Use temp directory for storage
		MaxPayload:  server.MAX_PAYLOAD_SIZE,   // Set max payload size
	}

	ns, err := server.NewServer(opts)
	require.NoError(t, err)
	
	go ns.Start()
	
	// Wait for server to be ready
	if !ns.ReadyForConnections(4 * time.Second) {
		t.Fatal("NATS server failed to start")
	}

	// Get client URL
	url := ns.ClientURL()

	// Create JetStream context and configure stream
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	defer nc.Close()

	js, err := nc.JetStream()
	require.NoError(t, err)

	// Create the stream
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "ZENTASK",
		Subjects: []string{"ZENTASK.>"},
		Storage:  nats.MemoryStorage,
	})
	require.NoError(t, err)

	return ns, url
}

type testPayload struct {
	Name    string `json:"name"`
	Message string `json:"message"`
}

type message struct {
	ID         string          `json:"id"`
	Queue      string          `json:"queue"`
	Payload    json.RawMessage `json:"payload"`
	EnqueuedAt time.Time      `json:"enqueued_at"`
	ProcessAt  time.Time      `json:"process_at,omitempty"`
	MaxRetries int            `json:"max_retries,omitempty"`
	RetryDelay time.Duration  `json:"retry_delay,omitempty"`
	Priority   int            `json:"priority,omitempty"`
	UniqueKey  string         `json:"unique_key,omitempty"`
	GroupID    string         `json:"group_id,omitempty"`
}

func TestNewClient(t *testing.T) {
	ns, url := runNATSServer(t)
	defer ns.Shutdown()

	tests := []struct {
		name    string
		config  Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: Config{
				NatsURL:       url,
				RetryAttempts: 3,
				RetryBackoff:  time.Second,
				StreamName:    "ZENTASK",
			},
			wantErr: false,
		},
		{
			name: "empty NATS URL",
			config: Config{
				NatsURL:       "",
				RetryAttempts: 3,
				RetryBackoff:  time.Second,
			},
			wantErr: true,
			errMsg:  "NATS URL is required",
		},
		{
			name: "invalid NATS URL format",
			config: Config{
				NatsURL:       "invalid://localhost:4222",
				RetryAttempts: 3,
				RetryBackoff:  time.Second,
			},
			wantErr: true,
			errMsg:  "invalid NATS URL format: must start with 'nats://'",
		},
		{
			name: "invalid NATS URL connection",
			config: Config{
				NatsURL:       "nats://nonexistent:4222",
				RetryAttempts: 3,
				RetryBackoff:  time.Second,
			},
			wantErr: true,
			errMsg:  "failed to connect to NATS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewClient(tt.config)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					require.Contains(t, err.Error(), tt.errMsg)
				}
				require.Nil(t, client)
			} else {
				require.NoError(t, err)
				require.NotNil(t, client)
				client.Close()
			}
		})
	}
}

func TestClient_Enqueue(t *testing.T) {
	ns, url := runNATSServer(t)
	defer ns.Shutdown()

	// Create a client
	client, err := NewClient(Config{
		NatsURL:       url,
		RetryAttempts: 3,
		RetryBackoff:  time.Second,
		StreamName:    "ZENTASK",  // Set the stream name to match the one we created
	})
	require.NoError(t, err)
	defer client.Close()

	// Create a subscriber to verify messages
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	defer nc.Close()

	js, err := nc.JetStream()
	require.NoError(t, err)

	// Create a consumer for testing
	_, err = js.AddConsumer("ZENTASK", &nats.ConsumerConfig{
		Durable:   "test-consumer",
		AckPolicy: nats.AckExplicitPolicy,
		FilterSubject: "ZENTASK.>",
	})
	require.NoError(t, err)

	// Subscribe to messages
	sub, err := js.PullSubscribe("ZENTASK.>", "test-consumer")
	require.NoError(t, err)
	defer sub.Unsubscribe()

	// Test cases
	tests := []struct {
		name    string
		task    *Task
		wantErr bool
		verify  func(t *testing.T, msg *nats.Msg, info *TaskInfo)
	}{
		{
			name: "simple task",
			task: &Task{
				Queue: "test-queue",
				Payload: testPayload{
					Name:    "test",
					Message: "hello",
				},
			},
			wantErr: false,
			verify: func(t *testing.T, msg *nats.Msg, info *TaskInfo) {
				var m message
				err := json.Unmarshal(msg.Data, &m)
				assert.NoError(t, err)

				var p testPayload
				err = json.Unmarshal(m.Payload, &p)
				assert.NoError(t, err)
				assert.Equal(t, "test", p.Name)
				assert.Equal(t, "hello", p.Message)

				assert.Equal(t, "test-queue", info.Queue)
				assert.Equal(t, "enqueued", info.Status)
				assert.NotEmpty(t, info.ID)
			},
		},
		{
			name: "task with options",
			task: &Task{
				Queue: "test-queue",
				Payload: testPayload{
					Name:    "test",
					Message: "hello",
				},
				Options: &TaskOptions{
					MaxRetries: 5,
					Priority:  1,
					ProcessAt: time.Now().Add(time.Hour),
				},
			},
			wantErr: false,
			verify: func(t *testing.T, msg *nats.Msg, info *TaskInfo) {
				var m message
				err := json.Unmarshal(msg.Data, &m)
				assert.NoError(t, err)
				assert.Equal(t, 5, m.MaxRetries)
				assert.Equal(t, 1, m.Priority)
				assert.True(t, info.ProcessAt.After(time.Now()))
			},
		},
		{
			name: "empty queue",
			task: &Task{
				Queue: "",
				Payload: testPayload{
					Name:    "test",
					Message: "hello",
				},
			},
			wantErr: true,
			verify:  func(t *testing.T, msg *nats.Msg, info *TaskInfo) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Enqueue the task
			info, err := client.Enqueue(tt.task)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, info)

			// Verify the message if a queue was specified
			if tt.task.Queue != "" {
				msgs, err := sub.Fetch(1, nats.MaxWait(1*time.Second))
				assert.NoError(t, err)
				assert.NotNil(t, msgs)
				assert.Len(t, msgs, 1)
				tt.verify(t, msgs[0], info)
			}
		})
	}
}

func TestClient_EnqueueContext(t *testing.T) {
	ns, url := runNATSServer(t)
	defer ns.Shutdown()

	client, err := NewClient(Config{
		NatsURL:       url,
		RetryAttempts: 3,
		RetryBackoff:  time.Second,
		StreamName:    "ZENTASK",  // Set the stream name to match the one we created
	})
	require.NoError(t, err)
	defer client.Close()

	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		task := &Task{
			Queue: "test-queue",
			Payload: testPayload{
				Name:    "test",
				Message: "hello",
			},
		}

		info, err := client.EnqueueContext(ctx, task)
		assert.Error(t, err)
		assert.Nil(t, info)
	})

	t.Run("context timeout", func(t *testing.T) {
		// Create a context with a very short timeout
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		// Sleep to ensure context is expired
		time.Sleep(10 * time.Millisecond)

		task := &Task{
			Queue: "test-queue",
			Payload: testPayload{
				Name:    "test",
				Message: "hello",
			},
		}

		info, err := client.EnqueueContext(ctx, task)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Nil(t, info)
	})
}

func TestClient_Close(t *testing.T) {
	ns, url := runNATSServer(t)
	defer ns.Shutdown()

	client, err := NewClient(Config{
		NatsURL:       url,
		RetryAttempts: 3,
		RetryBackoff:  time.Second,
		StreamName:    "ZENTASK",  // Set the stream name to match the one we created
	})
	require.NoError(t, err)

	err = client.Close()
	assert.NoError(t, err)

	// Verify that the connection is closed by trying to publish
	task := &Task{
		Queue: "test-queue",
		Payload: testPayload{
			Name:    "test",
			Message: "hello",
		},
	}

	_, err = client.Enqueue(task)
	assert.Error(t, err)
}
