package task

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// Handler is a function that processes a task
type Handler func(ctx context.Context, task *Task) error

// Processor manages task handlers and processes tasks
type Processor struct {
	repo     Repository
	js       nats.JetStreamContext
	handlers map[string]Handler
	mu       sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	// Track in-progress tasks
	inProgress     map[string]context.CancelFunc
	inProgressLock sync.RWMutex
}

// NewProcessor creates a new task processor
func NewProcessor(js nats.JetStreamContext, repo Repository) (*Processor, error) {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Processor{
		repo:       repo,
		js:         js,
		handlers:   make(map[string]Handler),
		ctx:        ctx,
		cancel:     cancel,
		inProgress: make(map[string]context.CancelFunc),
	}

	// Register default handlers
	if err := RegisterDefaultHandlers(p); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to register default handlers: %w", err)
	}

	return p, nil
}

// RegisterHandler registers a handler for a specific task type
func (p *Processor) RegisterHandler(taskType string, handler Handler) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.handlers[taskType]; exists {
		return fmt.Errorf("handler already registered for task type: %s", taskType)
	}

	p.handlers[taskType] = handler
	return nil
}

// getHandler returns the handler for a task type
func (p *Processor) getHandler(taskType string) (Handler, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	handler, exists := p.handlers[taskType]
	if !exists {
		return nil, fmt.Errorf("no handler registered for task type: %s", taskType)
	}

	return handler, nil
}

// Start begins processing tasks
func (p *Processor) Start() error {
	// First, recover any tasks that were left in "Running" state
	if err := p.recoverInProgressTasks(); err != nil {
		return fmt.Errorf("failed to recover in-progress tasks: %w", err)
	}

	// Use unique consumer names for each processor instance
	processorID := fmt.Sprintf("%d", time.Now().UnixNano())

	// Subscribe to immediate tasks
	if err := p.subscribeToTasks(immediateTaskSubject, fmt.Sprintf("immediate_processor_%s", processorID)); err != nil {
		return fmt.Errorf("failed to subscribe to immediate tasks: %w", err)
	}

	// Subscribe to delayed tasks
	if err := p.subscribeToTasks(delayedTaskSubject, fmt.Sprintf("delayed_processor_%s", processorID)); err != nil {
		return fmt.Errorf("failed to subscribe to delayed tasks: %w", err)
	}

	// Subscribe to scheduled tasks
	if err := p.subscribeToTasks(scheduledTaskSubject, fmt.Sprintf("scheduled_processor_%s", processorID)); err != nil {
		return fmt.Errorf("failed to subscribe to scheduled tasks: %w", err)
	}

	return nil
}

// recoverInProgressTasks finds and requeues tasks that were left in "Running" state
func (p *Processor) recoverInProgressTasks() error {
	filter := &Filter{
		Status: []Status{StatusRunning},
	}

	tasks, err := p.repo.List(p.ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to list running tasks: %w", err)
	}

	for _, task := range tasks {
		// Update task status to pending and reset retry count
		task.Status = StatusPending
		task.RetryCount = 0
		task.Error = "Task recovered after server restart"

		if err := p.repo.Update(p.ctx, task); err != nil {
			return fmt.Errorf("failed to update recovered task %s: %w", task.ID, err)
		}

		// Requeue the task
		if err := p.repo.Enqueue(p.ctx, task); err != nil {
			return fmt.Errorf("failed to requeue recovered task %s: %w", task.ID, err)
		}
	}

	return nil
}

// Stop gracefully stops processing tasks
func (p *Processor) Stop() {
	// Cancel all in-progress tasks
	p.inProgressLock.Lock()
	for _, cancel := range p.inProgress {
		cancel()
	}
	p.inProgressLock.Unlock()

	// Cancel the main context
	p.cancel()

	// Wait for all goroutines to finish
	p.wg.Wait()
}

func (p *Processor) subscribeToTasks(subject, consumerName string) error {
	// Create a durable consumer with retry configuration
	_, err := p.js.Subscribe(
		subject,
		func(msg *nats.Msg) {
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				if err := p.processMessage(msg); err != nil {
					fmt.Printf("Error processing message: %v\n", err)
				}
			}()
		},
		nats.ConsumerName(consumerName),
		nats.ManualAck(),
		nats.AckWait(24*time.Hour),    // Support delays up to 24 hours
		nats.MaxDeliver(5),            // Maximum number of delivery attempts
		nats.DeliverNew(),             // Only get new messages
		nats.BackOff([]time.Duration{  // Exponential backoff for retries
			1 * time.Second,
			5 * time.Second,
			15 * time.Second,
			30 * time.Second,
			60 * time.Second,
		}),
	)
	return err
}

func (p *Processor) processMessage(msg *nats.Msg) error {
	var task Task
	if err := json.Unmarshal(msg.Data, &task); err != nil {
		msg.Ack() // Bad message, don't retry
		return fmt.Errorf("failed to unmarshal task: %w", err)
	}

	// Get the task type from the payload
	if task.Payload == nil {
		msg.Ack() // No payload, don't retry
		return fmt.Errorf("task has no payload")
	}

	// Check if the task should be delayed
	if task.DelayDuration > 0 && task.StartedAt == nil {
		now := time.Now()
		if task.CreatedAt.IsZero() {
			task.CreatedAt = now
		}

		// Calculate remaining delay
		remainingDelay := task.DelayDuration - now.Sub(task.CreatedAt)
		if remainingDelay > 0 {
			// If there's still delay remaining, update metadata and Nak the message
			if task.Metadata == nil {
				task.Metadata = make(map[string]interface{})
			}
			// Store the next process time as RFC3339 string for proper serialization
			task.Metadata["next_process_time"] = now.Add(remainingDelay).Format(time.RFC3339)

			// Update the task in storage with the new metadata
			if err := p.repo.Update(p.ctx, &task); err != nil {
				return fmt.Errorf("failed to update task metadata: %w", err)
			}

			// Nak the message to retry after the remaining delay
			msg.Nak()
			return nil
		}
	}

	// Get the handler for this task type
	handler, err := p.getHandler(task.Payload.Type)
	if err != nil {
		msg.Ack() // No handler, don't retry
		return fmt.Errorf("failed to get handler: %w", err)
	}

	// Create a context for this task
	taskCtx, cancel := context.WithCancel(p.ctx)
	defer cancel()

	// Store the cancel function
	p.inProgressLock.Lock()
	p.inProgress[task.ID] = cancel
	p.inProgressLock.Unlock()

	// Clean up when done
	defer func() {
		p.inProgressLock.Lock()
		delete(p.inProgress, task.ID)
		p.inProgressLock.Unlock()
	}()

	// Update task status to running
	task.Status = StatusRunning
	startedAt := time.Now()
	task.StartedAt = &startedAt
	if err := p.repo.Update(taskCtx, &task); err != nil {
		msg.Term() // Permanent failure to update status
		return fmt.Errorf("failed to update task status: %w", err)
	}

	// Process the task
	err = handler(taskCtx, &task)
	if err != nil {
		// Update task status to failed
		task.Status = StatusFailed
		task.Error = err.Error()
		finishedAt := time.Now()
		task.FinishedAt = &finishedAt

		if err := p.repo.Update(taskCtx, &task); err != nil {
			msg.Term() // Permanent failure to update status
			return fmt.Errorf("failed to update task status: %w", err)
		}

		// Permanent failure, don't retry
		msg.Term()
		return fmt.Errorf("task processing failed: %w", err)
	}

	// Update task status to completed
	task.Status = StatusCompleted
	task.Error = ""
	finishedAt := time.Now()
	task.FinishedAt = &finishedAt

	if err := p.repo.Update(taskCtx, &task); err != nil {
		msg.Term() // Permanent failure to update status
		return fmt.Errorf("failed to update task status: %w", err)
	}

	msg.Ack()
	return nil
}
