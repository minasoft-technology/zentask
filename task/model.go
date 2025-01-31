package task

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Status represents the current state of a task
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCanceled  Status = "canceled"
)

// Priority represents the importance level of a task
type Priority string

const (
	PriorityLow    Priority = "low"
	PriorityMedium Priority = "medium"
	PriorityHigh   Priority = "high"
)

// Task represents a unit of work in the system
type Task struct {
	ID          string                 `json:"id"`          // Unique identifier
	Name        string                 `json:"name"`        // Task name
	Description string                 `json:"description"` // Task description
	Status      Status                 `json:"status"`      // Current status
	Priority    Priority               `json:"priority"`    // Task priority
	CreatedAt   time.Time             `json:"created_at"`  // Creation timestamp
	UpdatedAt   time.Time             `json:"updated_at"`  // Last update timestamp
	StartedAt   *time.Time            `json:"started_at"`  // When the task started running
	FinishedAt  *time.Time            `json:"finished_at"` // When the task completed/failed
	Error       string                `json:"error"`       // Error message if failed
	Progress    float64               `json:"progress"`    // Progress percentage (0-100)
	Tags        []string              `json:"tags"`        // Task tags for categorization
	Metadata    map[string]interface{} `json:"metadata"`    // Additional task metadata
	Payload     *Payload              `json:"payload"`     // Task payload
	MaxRetries  int                   `json:"max_retries"` // Maximum number of retries
	RetryCount  int                   `json:"retry_count"` // Current retry count
	Schedule    string                `json:"schedule"`    // Cron schedule expression
	DelayDuration time.Duration       `json:"delay_duration"` // Delay duration for delayed tasks
}

// TaskBuilder provides a fluent interface for building tasks
// Required fields:
// - Name (set via NewTask)
//
// Optional fields with default values:
// - Status: StatusPending
// - Priority: PriorityMedium
// - MaxRetries: 3
// - Progress: 0
// - Tags: empty slice
// - Metadata: empty map
// - RetryCount: 0
//
// Optional fields without defaults:
// - Description
// - Payload
// - Schedule (cron expression)
// - DelayDuration
type TaskBuilder struct {
	task *Task
	err  error
	repo Repository
}

// NewTask creates a new TaskBuilder with the given name
// This is the only required field when creating a task.
// The following default values are set:
// - Status: StatusPending
// - Priority: PriorityMedium
// - MaxRetries: 3
// - Progress: 0
// - Tags: empty slice
// - Metadata: empty map
// - RetryCount: 0
func NewTask(name string) *TaskBuilder {
	now := time.Now()
	return &TaskBuilder{
		task: &Task{
			ID:          uuid.New().String(),
			Name:        name,
			Status:      StatusPending,
			Priority:    PriorityMedium,
			CreatedAt:   now,
			UpdatedAt:   now,
			Progress:    0,
			Tags:        make([]string, 0),
			Metadata:    make(map[string]interface{}),
			MaxRetries:  3, // Default to 3 retries
			RetryCount:  0,
		},
	}
}

// WithRepository sets the repository to use for enqueueing tasks
func (b *TaskBuilder) WithRepository(repo Repository) *TaskBuilder {
	if b.err != nil {
		return b
	}
	b.repo = repo
	return b
}

// WithDescription adds a description to the task
func (b *TaskBuilder) WithDescription(description string) *TaskBuilder {
	if b.err != nil {
		return b
	}
	b.task.Description = description
	return b
}

// WithPayload adds a payload to the task
func (b *TaskBuilder) WithPayload(payloadType string, payloadData interface{}) *TaskBuilder {
	if b.err != nil {
		return b
	}
	payload, err := NewPayload(payloadType, payloadData)
	if err != nil {
		b.err = fmt.Errorf("failed to create payload: %w", err)
		return b
	}
	b.task.Payload = payload
	return b
}

// WithPriority sets the task priority
func (b *TaskBuilder) WithPriority(priority Priority) *TaskBuilder {
	if b.err != nil {
		return b
	}
	b.task.Priority = priority
	return b
}

// WithTags adds tags to the task
func (b *TaskBuilder) WithTags(tags ...string) *TaskBuilder {
	if b.err != nil {
		return b
	}
	b.task.Tags = append(b.task.Tags, tags...)
	return b
}

// WithMetadata adds metadata to the task
func (b *TaskBuilder) WithMetadata(key string, value interface{}) *TaskBuilder {
	if b.err != nil {
		return b
	}
	b.task.Metadata[key] = value
	return b
}

// WithMaxRetries sets the maximum number of retries
func (b *TaskBuilder) WithMaxRetries(maxRetries int) *TaskBuilder {
	if b.err != nil {
		return b
	}
	b.task.MaxRetries = maxRetries
	return b
}

// Schedule sets a cron schedule for the task
func (b *TaskBuilder) Schedule(cronExpression string) *TaskBuilder {
	if b.err != nil {
		return b
	}
	b.task.Schedule = cronExpression
	return b
}

// WithDelay sets a delay duration for the task
func (b *TaskBuilder) WithDelay(delay time.Duration) *TaskBuilder {
	if b.err != nil {
		return b
	}
	b.task.DelayDuration = delay
	return b
}

// Build creates and validates the task
func (b *TaskBuilder) Build() (*Task, error) {
	if b.err != nil {
		return nil, b.err
	}
	if err := b.task.Validate(); err != nil {
		return nil, err
	}
	return b.task, nil
}

// Enqueue builds the task and enqueues it immediately
func (b *TaskBuilder) Enqueue(ctx context.Context) (*Task, error) {
	task, err := b.Build()
	if err != nil {
		return nil, err
	}

	if b.repo == nil {
		return nil, fmt.Errorf("repository is required for enqueueing tasks")
	}

	err = b.repo.Enqueue(ctx, task)
	if err != nil {
		return nil, fmt.Errorf("failed to enqueue task: %w", err)
	}

	return task, nil
}

// EnqueueWithDelay builds the task and enqueues it with the specified delay
func (b *TaskBuilder) EnqueueWithDelay(ctx context.Context, delay time.Duration) (*Task, error) {
	b.WithDelay(delay)
	return b.Enqueue(ctx)
}

// ScheduleWithCron builds the task with a cron schedule and enqueues it
func (b *TaskBuilder) ScheduleWithCron(ctx context.Context, cronExpression string) (*Task, error) {
	b.Schedule(cronExpression)
	return b.Enqueue(ctx)
}

// GetPayloadData unmarshals the payload data into the given value
func (t *Task) GetPayloadData(v interface{}) error {
	if t.Payload == nil {
		return fmt.Errorf("task has no payload")
	}
	return t.Payload.UnmarshalData(v)
}

// Validate checks if the task has valid data
func (t *Task) Validate() error {
	if t.Name == "" {
		return fmt.Errorf("task name is required")
	}

	if t.Status == "" {
		return fmt.Errorf("task status is required")
	}

	if t.Priority == "" {
		return fmt.Errorf("task priority is required")
	}

	if t.CreatedAt.IsZero() {
		return fmt.Errorf("created_at is required")
	}

	if t.UpdatedAt.IsZero() {
		return fmt.Errorf("updated_at is required")
	}

	if t.MaxRetries < 0 {
		return fmt.Errorf("max_retries cannot be negative")
	}

	if t.Progress < 0 || t.Progress > 100 {
		return fmt.Errorf("progress must be between 0 and 100")
	}

	if t.Schedule != "" {
		// TODO: Validate cron expression using github.com/robfig/cron/v3
		// For now, we'll just check it's not empty if provided
	}

	if t.DelayDuration < 0 {
		return fmt.Errorf("delay duration cannot be negative")
	}

	return nil
}

// UpdateStatus updates the task status and related timestamps
func (t *Task) UpdateStatus(status Status) {
	now := time.Now()
	t.Status = status
	t.UpdatedAt = now

	switch status {
	case StatusRunning:
		t.StartedAt = &now
	case StatusCompleted, StatusFailed, StatusCanceled:
		t.FinishedAt = &now
	}
}

// UpdateProgress updates the task progress
func (t *Task) UpdateProgress(progress float64) error {
	if progress < 0 || progress > 100 {
		return fmt.Errorf("progress must be between 0 and 100")
	}
	t.Progress = progress
	t.UpdatedAt = time.Now()
	return nil
}
