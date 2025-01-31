package task

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

// TaskRepository provides a high-level interface for task management operations
type TaskRepository struct {
	repo Repository
}

// NewTaskRepository creates a new TaskRepository instance using NATS JetStream
func NewTaskRepository(js nats.JetStreamContext) (*TaskRepository, error) {
	repo, err := NewJetStreamRepository(js)
	if err != nil {
		return nil, err
	}
	return &TaskRepository{repo: repo}, nil
}

// NewTask creates a new TaskBuilder for fluent task creation
func (r *TaskRepository) NewTask(name string) *TaskBuilder {
	return NewTask(name).WithRepository(r.repo)
}

// Get retrieves a task by ID
func (r *TaskRepository) Get(ctx context.Context, id string) (*Task, error) {
	return r.repo.Get(ctx, id)
}

// Update modifies an existing task
func (r *TaskRepository) Update(ctx context.Context, task *Task) error {
	return r.repo.Update(ctx, task)
}

// Delete removes a task
func (r *TaskRepository) Delete(ctx context.Context, id string) error {
	return r.repo.Delete(ctx, id)
}

// List retrieves tasks matching the filter criteria
func (r *TaskRepository) List(ctx context.Context, filter *Filter) ([]*Task, error) {
	return r.repo.List(ctx, filter)
}

// Watch returns a channel that receives task updates
func (r *TaskRepository) Watch(ctx context.Context, taskID string) (<-chan *Task, error) {
	return r.repo.Watch(ctx, taskID)
}

// EnqueueTask is a convenience method to create and enqueue a task in one call
func (r *TaskRepository) EnqueueTask(ctx context.Context, name string, opts ...TaskOption) (*Task, error) {
	builder := r.NewTask(name)
	for _, opt := range opts {
		opt(builder)
	}
	return builder.Enqueue(ctx)
}

// TaskOption defines a function type for configuring tasks
type TaskOption func(*TaskBuilder)

// WithDescription returns a TaskOption that sets the task description
func WithDescription(description string) TaskOption {
	return func(b *TaskBuilder) {
		b.WithDescription(description)
	}
}

// WithPriority returns a TaskOption that sets the task priority
func WithPriority(priority Priority) TaskOption {
	return func(b *TaskBuilder) {
		b.WithPriority(priority)
	}
}

// WithPayload returns a TaskOption that sets the task payload
func WithPayload(payloadType string, payloadData interface{}) TaskOption {
	return func(b *TaskBuilder) {
		b.WithPayload(payloadType, payloadData)
	}
}

// WithTags returns a TaskOption that sets the task tags
func WithTags(tags ...string) TaskOption {
	return func(b *TaskBuilder) {
		b.WithTags(tags...)
	}
}

// WithMetadata returns a TaskOption that adds metadata to the task
func WithMetadata(key string, value interface{}) TaskOption {
	return func(b *TaskBuilder) {
		b.WithMetadata(key, value)
	}
}

// WithMaxRetries returns a TaskOption that sets the maximum number of retries
func WithMaxRetries(maxRetries int) TaskOption {
	return func(b *TaskBuilder) {
		b.WithMaxRetries(maxRetries)
	}
}

// WithDelay returns a TaskOption that sets the task delay duration
func WithDelay(delay time.Duration) TaskOption {
	return func(b *TaskBuilder) {
		b.WithDelay(delay)
	}
}

// WithSchedule returns a TaskOption that sets the task schedule
func WithSchedule(cronExpression string) TaskOption {
	return func(b *TaskBuilder) {
		b.Schedule(cronExpression)
	}
}
