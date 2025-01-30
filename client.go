package zentask

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// Client is the ZenTask client for enqueueing tasks
type Client struct {
	nc         *nats.Conn
	js         nats.JetStreamContext
	config     Config
	streamName string
}

// Config holds the configuration for ZenTask client
type Config struct {
	NatsURL       string
	RetryAttempts int
	RetryBackoff  time.Duration
	RateLimit     float64
	RateBurst     int
	StreamName    string // Name of the JetStream stream
}

// Task represents a background job to be processed
type Task struct {
	Queue    string
	Payload  interface{}
	Options  *TaskOptions
	Metadata map[string]interface{}
}

// TaskOptions contains optional parameters for task processing
type TaskOptions struct {
	MaxRetries int
	RetryDelay time.Duration
	Timeout    time.Duration
	Priority   int
	ProcessAt  time.Time
	UniqueKey  string
	GroupID    string
}

// TaskInfo contains information about an enqueued task
type TaskInfo struct {
	ID         string
	Queue      string
	Status     string
	EnqueuedAt time.Time
	ProcessAt  time.Time
	Sequence   uint64 // JetStream sequence number
}

type taskMessage struct {
	ID         string                 `json:"id"`
	Queue      string                 `json:"queue"`
	Payload    json.RawMessage        `json:"payload"`
	EnqueuedAt time.Time              `json:"enqueued_at"`
	ProcessAt  time.Time              `json:"process_at,omitempty"`
	MaxRetries int                    `json:"max_retries,omitempty"`
	RetryDelay time.Duration          `json:"retry_delay,omitempty"`
	Priority   int                    `json:"priority,omitempty"`
	UniqueKey  string                 `json:"unique_key,omitempty"`
	GroupID    string                 `json:"group_id,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

// NewClient creates a new ZenTask client
func NewClient(config Config) (*Client, error) {
	if config.NatsURL == "" {
		return nil, fmt.Errorf("NATS URL is required")
	}

	// Set default values
	if config.StreamName == "" {
		config.StreamName = "ZENTASK"
	}

	// Connect to NATS
	nc, err := nats.Connect(config.NatsURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %v", err)
	}

	// Create JetStream context
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %v", err)
	}

	return &Client{
		nc:         nc,
		js:         js,
		config:     config,
		streamName: config.StreamName,
	}, nil
}

// Enqueue adds a task to the specified queue
func (c *Client) Enqueue(task *Task) (*TaskInfo, error) {
	return c.EnqueueContext(context.Background(), task)
}

// EnqueueContext adds a task to the specified queue with context
func (c *Client) EnqueueContext(ctx context.Context, task *Task) (*TaskInfo, error) {
	if task.Queue == "" {
		return nil, fmt.Errorf("queue name is required")
	}

	// Create message structure
	msg := taskMessage{
		ID:         generateTaskID(),
		Queue:      task.Queue,
		EnqueuedAt: time.Now(),
		Metadata:   task.Metadata,
	}

	// Set options if provided
	if task.Options != nil {
		msg.MaxRetries = task.Options.MaxRetries
		msg.RetryDelay = task.Options.RetryDelay
		msg.Priority = task.Options.Priority
		msg.ProcessAt = task.Options.ProcessAt
		msg.UniqueKey = task.Options.UniqueKey
		msg.GroupID = task.Options.GroupID
	}

	// Marshal the payload
	if task.Payload != nil {
		payload, err := json.Marshal(task.Payload)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal payload: %v", err)
		}
		msg.Payload = payload
	}

	// Marshal the entire message
	msgData, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %v", err)
	}

	// Create NATS message with task ID in subject for efficient lookups
	subject := fmt.Sprintf("%s.tasks.%s.%s", c.streamName, task.Queue, msg.ID)
	natsMsg := nats.NewMsg(subject)
	natsMsg.Data = msgData

	// Add headers for additional metadata
	headers := nats.Header{}
	if task.Options != nil {
		if task.Options.UniqueKey != "" {
			headers.Set("Nats-Msg-Id", task.Options.UniqueKey)
		}
		if task.Options.Priority > 0 {
			headers.Set("Priority", fmt.Sprintf("%d", task.Options.Priority))
		}
		if !task.Options.ProcessAt.IsZero() {
			headers.Set("Process-At", task.Options.ProcessAt.Format(time.RFC3339))
		}
	}
	natsMsg.Header = headers

	// Publish with context
	pubAck, err := c.js.PublishMsgAsync(natsMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to publish message: %v", err)
	}

	// Wait for publish acknowledgment with context
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case pa := <-pubAck.Ok():
		// Create task info
		processAt := time.Now()
		if task.Options != nil && !task.Options.ProcessAt.IsZero() {
			processAt = task.Options.ProcessAt
		}

		return &TaskInfo{
			ID:         msg.ID,
			Queue:      task.Queue,
			Status:     "enqueued",
			EnqueuedAt: msg.EnqueuedAt,
			ProcessAt:  processAt,
			Sequence:   pa.Sequence,
		}, nil
	case err := <-pubAck.Err():
		return nil, fmt.Errorf("failed to publish message: %v", err)
	}
}

// DeleteTask deletes a task by its ID before it's processed
func (c *Client) DeleteTask(taskID string) error {
	return c.DeleteTaskContext(context.Background(), taskID)
}

// DeleteTaskContext deletes a task by its ID with context before it's processed
func (c *Client) DeleteTaskContext(ctx context.Context, taskID string) error {
	if taskID == "" {
		return fmt.Errorf("task ID is required")
	}

	// Create an ephemeral consumer for the specific task ID
	subject := fmt.Sprintf("%s.tasks.*.%s", c.streamName, taskID)
	sub, err := c.js.PullSubscribe(
		subject,
		"", // Empty name for ephemeral consumer
		nats.AckExplicit(),
	)
	if err != nil {
		return fmt.Errorf("failed to create subscription: %v", err)
	}
	defer sub.Unsubscribe()

	// Try to fetch the specific message
	msgs, err := sub.Fetch(1, nats.Context(ctx))
	if err != nil {
		if err == nats.ErrTimeout || err == context.DeadlineExceeded {
			return fmt.Errorf("task not found: %s", taskID)
		}
		return fmt.Errorf("failed to fetch message: %v", err)
	}

	if len(msgs) == 0 {
		return fmt.Errorf("task not found: %s", taskID)
	}

	// Delete the message
	meta, _ := msgs[0].Metadata()
	if err := c.js.DeleteMsg(c.streamName, meta.Sequence.Stream); err != nil {
		return fmt.Errorf("failed to delete message: %v", err)
	}

	return nil
}

// Close closes the NATS connection
func (c *Client) Close() error {
	if err := c.nc.Drain(); err != nil {
		return fmt.Errorf("failed to drain connection: %v", err)
	}
	return nil
}

func generateTaskID() string {
	return nats.NewInbox()[4:] // Use NATS inbox ID generation without the 'INBOX.' prefix
}

// ListTasksOptions contains options for listing tasks
type ListTasksOptions struct {
	Queue     string    // Optional: filter by queue name
	Status    string    // Optional: filter by status (pending, completed, failed)
	StartTime time.Time // Optional: filter by start time
	EndTime   time.Time // Optional: filter by end time
	PageSize  int       // Number of tasks per page
	PageToken string    // Token for the next page
}

// TaskList represents a paginated list of tasks
type TaskList struct {
	Tasks     []*TaskInfo // List of tasks
	NextToken string      // Token for the next page, empty if no more pages
}

// ListTasks retrieves a paginated list of tasks
func (c *Client) ListTasks(ctx context.Context, opts ListTasksOptions) (*TaskList, error) {
	if opts.PageSize <= 0 {
		opts.PageSize = 100 // Default page size
	}

	subject := fmt.Sprintf("%s.tasks.>", c.streamName)
	if opts.Queue != "" {
		subject = fmt.Sprintf("%s.tasks.%s.>", c.streamName, opts.Queue)
	}

	// Determine start sequence
	startSeq := uint64(1)
	if opts.PageToken != "" {
		var err error
		startSeq, err = strconv.ParseUint(opts.PageToken, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid page token: %w", err)
		}
	}

	// Use direct stream reading for better performance
	tasks := make([]*TaskInfo, 0, opts.PageSize)
	var nextToken string

	for len(tasks) < opts.PageSize {
		msg, err := c.js.GetMsg(c.streamName, startSeq)
		if err != nil {
			if err == nats.ErrMsgNotFound {
				break // No more messages
			}
			return nil, fmt.Errorf("failed to fetch message: %w", err)
		}
		startSeq++ // Move to next sequence

		if !strings.HasPrefix(msg.Subject, subject) {
			continue // Skip messages not matching subject filter
		}

		var tm taskMessage
		if err := json.Unmarshal(msg.Data, &tm); err != nil {
			continue // Skip invalid messages
		}

		// Apply time filters
		if !opts.StartTime.IsZero() && tm.EnqueuedAt.Before(opts.StartTime) {
			continue
		}
		if !opts.EndTime.IsZero() && tm.EnqueuedAt.After(opts.EndTime) {
			continue
		}

		// Get message metadata
		numDelivered := msg.Header.Get("Nats-Num-Deliveries")
		if numDelivered == "" {
			numDelivered = "1"
		}

		// Determine task status
		status := "pending"
		if numDelivered != "1" {
			status = "retrying"
		}
		if opts.Status != "" && status != opts.Status {
			continue
		}

		task := &TaskInfo{
			ID:         tm.ID,
			Queue:      tm.Queue,
			Status:     status,
			EnqueuedAt: tm.EnqueuedAt,
			ProcessAt:  tm.ProcessAt,
			Sequence:   msg.Sequence,
		}

		tasks = append(tasks, task)
		nextToken = fmt.Sprintf("%d", msg.Sequence+1)

		if len(tasks) >= opts.PageSize {
			break
		}
	}

	return &TaskList{
		Tasks:     tasks,
		NextToken: nextToken,
	}, nil
}
