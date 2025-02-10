package zentask

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log/slog"
)

func TestNATSServer(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "nats-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Create config with random ports for parallel testing
	config := NATSConfig{
		NATSPort: -1,          // Random port
		HTTPPort: -1,          // Random port
		NATSHost: "127.0.0.1", // Use localhost for testing
		HTTPHost: "127.0.0.1", // Use localhost for testing
		StoreDir: filepath.Join(tempDir, "nats-data"),
		Debug:    true,
		Logger:   logger,
	}

	// Create and start server
	ctx := context.Background()
	server, err := NewNATSServer(ctx, config)
	require.NoError(t, err)
	defer server.Stop()

	// Get the actual connection URL
	url := server.GetConnectionURL()
	require.NotEmpty(t, url)

	// Test basic connection
	t.Run("Basic Connection", func(t *testing.T) {
		nc, err := nats.Connect(url)
		require.NoError(t, err)
		defer nc.Close()

		// Test pub/sub
		sub, err := nc.SubscribeSync("test")
		require.NoError(t, err)

		err = nc.Publish("test", []byte("hello"))
		require.NoError(t, err)

		msg, err := sub.NextMsg(time.Second)
		require.NoError(t, err)
		assert.Equal(t, "hello", string(msg.Data))
	})

	// Test JetStream
	t.Run("JetStream Operations", func(t *testing.T) {
		nc, err := nats.Connect(url)
		require.NoError(t, err)
		defer nc.Close()

		// Create JetStream context
		js, err := nc.JetStream()
		require.NoError(t, err)

		// Create a stream
		streamName := "TEST_STREAM"
		streamSubject := "test.messages"

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{streamSubject},
		})
		require.NoError(t, err)

		// Publish a message
		_, err = js.Publish(streamSubject, []byte("jetstream test"))
		require.NoError(t, err)

		// Subscribe and receive the message
		sub, err := js.SubscribeSync(streamSubject)
		require.NoError(t, err)

		msg, err := sub.NextMsg(time.Second)
		require.NoError(t, err)
		assert.Equal(t, "jetstream test", string(msg.Data))
	})

	// Test server configuration
	t.Run("Server Configuration", func(t *testing.T) {
		// Verify store directory was created
		_, err := os.Stat(config.StoreDir)
		assert.NoError(t, err)

		// Test invalid configuration
		invalidConfig := NATSConfig{
			NATSPort: -2,
			HTTPPort: -2,
		}
		_, err = NewNATSServer(context.Background(), invalidConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid NATS port")
	})
}

func TestDefaultStoreDir(t *testing.T) {
	dir := getDefaultStoreDir()
	require.NotEmpty(t, dir)

	// Should contain .zentask/nats-data in the path
	assert.Contains(t, dir, ".zentask")
	assert.Contains(t, dir, "nats-data")
}
