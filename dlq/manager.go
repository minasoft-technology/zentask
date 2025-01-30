package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/minasoft-technology/zentask"
	"github.com/nats-io/nats.go"
)

// DLQStats holds statistics about the DLQ
type DLQStats struct {
	TotalMessages     int64
	ErrorTypes        map[string]int64
	QueueDistribution map[string]int64
	OldestMessage     time.Time
	NewestMessage     time.Time
}

// Manager handles DLQ operations and monitoring
type Manager struct {
	js           nats.JetStreamContext
	client       *zentask.Client
	streamName   string
	errorLogger  *log.Logger
	alertHandler AlertHandler
	mu          sync.RWMutex
	stats       DLQStats
	sub         *nats.Subscription
}

// AlertHandler is called when DLQ thresholds are exceeded
type AlertHandler func(alert Alert)

// Alert represents a DLQ alert
type Alert struct {
	Type        string
	Queue       string
	Message     string
	Threshold   int64
	CurrentVal  int64
	TimeStamp   time.Time
}

// ManagerConfig holds configuration for the DLQ manager
type ManagerConfig struct {
	NatsURL            string
	StreamName         string
	ErrorLogger        *log.Logger
	AlertHandler       AlertHandler
	MessageThreshold   int64
	ErrorRateThreshold float64
	ScanInterval      time.Duration
}

// NewManager creates a new DLQ manager
func NewManager(config ManagerConfig) (*Manager, error) {
	nc, err := nats.Connect(config.NatsURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %v", err)
	}

	client, err := zentask.NewClient(zentask.Config{
		NatsURL: config.NatsURL,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create ZenTask client: %v", err)
	}

	return &Manager{
		js:           js,
		client:       client,
		streamName:   config.StreamName,
		errorLogger:  config.ErrorLogger,
		alertHandler: config.AlertHandler,
		stats: DLQStats{
			ErrorTypes:        make(map[string]int64),
			QueueDistribution: make(map[string]int64),
		},
	}, nil
}

// StartMonitoring begins monitoring the DLQ
func (m *Manager) StartMonitoring(ctx context.Context) error {
	// Subscribe to all DLQ messages
	sub, err := m.js.Subscribe(
		fmt.Sprintf("%s.dlq.>", m.streamName),
		m.handleDLQMessage,
		nats.DeliverAll(),
	)
	if err != nil {
		return fmt.Errorf("failed to subscribe to DLQ messages: %w", err)
	}
	m.sub = sub

	// Clean up subscription when context is cancelled
	go func() {
		<-ctx.Done()
		if m.sub != nil {
			m.sub.Unsubscribe()
			m.sub = nil
		}
	}()

	// Start stats analysis
	go m.analyzeStats(ctx)

	return nil
}

// handleDLQMessage processes incoming DLQ messages
func (m *Manager) handleDLQMessage(msg *nats.Msg) {
	var dlqMsg struct {
		Task          *zentask.Task
		OriginalQueue string
		Error         string
		FailedAt      time.Time
	}

	if err := json.Unmarshal(msg.Data, &dlqMsg); err != nil {
		m.logError("Failed to unmarshal DLQ message", err)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Update stats
	m.stats.TotalMessages++
	m.stats.ErrorTypes[dlqMsg.Error]++
	m.stats.QueueDistribution[dlqMsg.OriginalQueue]++

	if m.stats.OldestMessage.IsZero() || dlqMsg.FailedAt.Before(m.stats.OldestMessage) {
		m.stats.OldestMessage = dlqMsg.FailedAt
	}
	if dlqMsg.FailedAt.After(m.stats.NewestMessage) {
		m.stats.NewestMessage = dlqMsg.FailedAt
	}
}

// analyzeStats periodically analyzes DLQ statistics
func (m *Manager) analyzeStats(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.mu.RLock()
			stats := m.stats
			m.mu.RUnlock()

			// Check total message threshold
			if stats.TotalMessages > 1000 {
				m.alertHandler(Alert{
					Type:       "HIGH_MESSAGE_COUNT",
					Message:    "DLQ message count exceeds threshold",
					Threshold:  1000,
					CurrentVal: stats.TotalMessages,
					TimeStamp:  time.Now(),
				})
			}

			// Check error rate for each queue
			for queue, count := range stats.QueueDistribution {
				if count > 100 {
					m.alertHandler(Alert{
						Type:       "HIGH_QUEUE_ERROR_RATE",
						Queue:      queue,
						Message:    fmt.Sprintf("High error rate in queue: %s", queue),
						Threshold:  100,
						CurrentVal: count,
						TimeStamp:  time.Now(),
					})
				}
			}
		}
	}
}

// RetryMessages retries messages from DLQ
func (m *Manager) RetryMessages(ctx context.Context, queue string, batchSize int) error {
	sub, err := m.js.Subscribe(
		fmt.Sprintf("%s.dlq.%s", m.streamName, queue),
		func(msg *nats.Msg) {
			var dlqMsg struct {
				Task *zentask.Task
			}

			if err := json.Unmarshal(msg.Data, &dlqMsg); err != nil {
				m.logError("Failed to unmarshal DLQ message for retry", err)
				return
			}

			// Reset retry count and add retry metadata
			dlqMsg.Task.Options.MaxRetries = 1
			if dlqMsg.Task.Metadata == nil {
				dlqMsg.Task.Metadata = make(map[string]interface{})
			}
			dlqMsg.Task.Metadata["dlq_retry_time"] = time.Now()

			// Retry the task
			_, err := m.client.EnqueueContext(ctx, dlqMsg.Task)
			if err != nil {
				m.logError("Failed to retry task from DLQ", err)
				return
			}

			// Acknowledge the DLQ message
			msg.Ack()
		},
		nats.MaxDeliver(1),
		nats.MaxAckPending(batchSize),
	)
	if err != nil {
		return fmt.Errorf("failed to subscribe to DLQ for retry: %v", err)
	}
	defer sub.Unsubscribe()

	return nil
}

// GetStats returns current DLQ statistics
func (m *Manager) GetStats() DLQStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.stats
}

func (m *Manager) logError(msg string, err error) {
	if m.errorLogger != nil {
		m.errorLogger.Printf("%s: %v", msg, err)
	}
}
