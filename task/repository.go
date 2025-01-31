package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// Repository defines the interface for task storage operations
type Repository interface {
	// Create stores a new task
	Create(ctx context.Context, task *Task) error
	// Get retrieves a task by ID
	Get(ctx context.Context, id string) (*Task, error)
	// Update modifies an existing task
	Update(ctx context.Context, task *Task) error
	// Delete removes a task
	Delete(ctx context.Context, id string) error
	// List retrieves tasks matching the filter criteria
	List(ctx context.Context, filter *Filter) ([]*Task, error)
	// Watch returns a channel that receives task updates
	Watch(ctx context.Context, taskID string) (<-chan *Task, error)
	// Enqueue adds a task to the queue for processing
	Enqueue(ctx context.Context, task *Task) error
}

// Filter defines criteria for filtering tasks
type Filter struct {
	Status   []Status
	Priority []Priority
	Tags     []string
	Since    time.Time
	Until    time.Time
}

// JetStreamRepository implements Repository using NATS JetStream
type JetStreamRepository struct {
	js nats.JetStreamContext
}

const (
	streamName          = "tasks"
	streamSubjects      = "tasks.>"
	taskSubject         = "tasks.updates.%s"
	immediateTaskSubject = "tasks.queue.immediate.%s"
	scheduledTaskSubject = "tasks.queue.scheduled.%s"
	delayedTaskSubject  = "tasks.queue.delayed.%s"
	queueGroup         = "task_processors"

	// Subjects for different types of tasks
)

// NewJetStreamRepository creates a new JetStream-backed task repository
func NewJetStreamRepository(js nats.JetStreamContext) (*JetStreamRepository, error) {
	// Create a stream for tasks if it doesn't exist
	_, err := js.StreamInfo(streamName)
	if err != nil {
		// Stream doesn't exist, create it
		_, err = js.AddStream(&nats.StreamConfig{
			Name: streamName,
			Subjects: []string{
				"tasks.queue.immediate.>",
				"tasks.queue.scheduled.>",
				"tasks.queue.delayed.>",
				"tasks.updates.>",
			},
			Storage:  nats.FileStorage,
			MaxAge:   30 * 24 * time.Hour, // Keep messages for 30 days
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create stream: %w", err)
		}
	}

	// Create a key-value bucket for tasks if it doesn't exist
	_, err = js.KeyValue(streamName)
	if err != nil {
		// Bucket doesn't exist, create it
		_, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:  streamName,
			Storage: nats.FileStorage,
			History: 1, // Only keep the latest value
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create key-value bucket: %w", err)
		}
	}

	// Create consumers for different task types
	consumers := []struct {
		name    string
		subject string
	}{
		{"immediate_tasks", "tasks.queue.immediate.*"},
		{"scheduled_tasks", "tasks.queue.scheduled.*"},
		{"delayed_tasks", "tasks.queue.delayed.*"},
	}

	for _, c := range consumers {
		_, err = js.AddConsumer(streamName, &nats.ConsumerConfig{
			Durable:       c.name,
			FilterSubject: c.subject,
			AckPolicy:     nats.AckExplicitPolicy,
		})
		if err != nil && !errors.Is(err, nats.ErrConsumerNameAlreadyInUse) {
			return nil, fmt.Errorf("failed to create %s consumer: %w", c.name, err)
		}
	}

	return &JetStreamRepository{js: js}, nil
}

func (r *JetStreamRepository) Create(ctx context.Context, task *Task) error {
	if err := task.Validate(); err != nil {
		return err
	}

	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	_, err = r.js.Publish(fmt.Sprintf(taskSubject, task.ID), data)
	if err != nil {
		return fmt.Errorf("failed to publish task: %w", err)
	}

	return nil
}

func (r *JetStreamRepository) Get(ctx context.Context, id string) (*Task, error) {
	msg, err := r.js.GetLastMsg(streamName, fmt.Sprintf(taskSubject, id))
	if err != nil {
		if err == nats.ErrMsgNotFound {
			return nil, ErrTaskNotFound
		}
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	var task Task
	if err := json.Unmarshal(msg.Data, &task); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task: %w", err)
	}

	return &task, nil
}

func (r *JetStreamRepository) Update(ctx context.Context, task *Task) error {
	if err := task.Validate(); err != nil {
		return fmt.Errorf("invalid task: %w", err)
	}

	// Store the task in the repository
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	// Store the task in the key-value store
	kv, err := r.js.KeyValue(streamName)
	if err != nil {
		return fmt.Errorf("failed to get key-value store: %w", err)
	}

	rev, err := kv.Put(task.ID, data)
	if err != nil {
		return fmt.Errorf("failed to store task: %w", err)
	}
	_ = rev // Ignore revision number

	// Publish the task update to the stream
	_, err = r.js.Publish(fmt.Sprintf(taskSubject, task.ID), data)
	if err != nil {
		return fmt.Errorf("failed to publish task update: %w", err)
	}

	return nil
}

func (r *JetStreamRepository) Delete(ctx context.Context, id string) error {
	// In JetStream, we'll mark deletion by publishing a tombstone message
	task := &Task{
		ID:        id,
		Status:    StatusCanceled,
		UpdatedAt: time.Now(),
	}

	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	_, err = r.js.Publish(fmt.Sprintf(taskSubject, id), data)
	if err != nil {
		return fmt.Errorf("failed to delete task: %w", err)
	}

	return nil
}

func (r *JetStreamRepository) List(ctx context.Context, filter *Filter) ([]*Task, error) {
	var tasks []*Task

	// Create a consumer for batch processing
	consumerName := fmt.Sprintf("task_lister_%d", time.Now().UnixNano())
	_, err := r.js.AddConsumer(streamName, &nats.ConsumerConfig{
		Name:          consumerName,
		Durable:       consumerName,
		FilterSubject: fmt.Sprintf(taskSubject, "*"), // Use wildcard to match all task IDs
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverLastPerSubjectPolicy, // Only get the latest message per subject
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}
	defer r.js.DeleteConsumer(streamName, consumerName)

	// Create a pull-based consumer
	sub, err := r.js.PullSubscribe(fmt.Sprintf(taskSubject, "*"), consumerName)
	if err != nil {
		return nil, fmt.Errorf("failed to create pull subscription: %w", err)
	}
	defer sub.Unsubscribe()

	// Process messages in batches
	seen := make(map[string]bool)
	for {
		msgs, err := sub.Fetch(10, nats.MaxWait(100*time.Millisecond))
		if err != nil {
			if err == nats.ErrTimeout {
				break
			}
			return nil, fmt.Errorf("failed to fetch messages: %w", err)
		}

		for _, msg := range msgs {
			var task Task
			if err := json.Unmarshal(msg.Data, &task); err != nil {
				msg.Ack()
				continue
			}

			// Skip if we've seen this task before
			if seen[task.ID] {
				msg.Ack()
				continue
			}
			seen[task.ID] = true

			// Apply filters
			if filter != nil {
				if !matchesFilter(&task, filter) {
					msg.Ack()
					continue
				}
			}

			tasks = append(tasks, &task)
			msg.Ack()
		}
	}

	return tasks, nil
}

func (r *JetStreamRepository) Watch(ctx context.Context, taskID string) (<-chan *Task, error) {
	// Create a unique durable name for this subscription
	durableName := fmt.Sprintf("watch_%s", taskID)
	
	// Create a pull subscription
	sub, err := r.js.PullSubscribe(
		fmt.Sprintf(taskSubject, taskID),
		durableName,
		nats.PullMaxWaiting(1),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to task updates: %w", err)
	}

	taskChan := make(chan *Task, 1)

	go func() {
		defer close(taskChan)
		defer sub.Unsubscribe()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				msgs, err := sub.Fetch(1, nats.MaxWait(time.Second))
				if err != nil {
					if err == nats.ErrTimeout {
						continue
					}
					return
				}

				for _, msg := range msgs {
					var task Task
					if err := json.Unmarshal(msg.Data, &task); err != nil {
						msg.Nak()
						continue
					}

					msg.Ack()

					select {
					case taskChan <- &task:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return taskChan, nil
}

func (r *JetStreamRepository) Enqueue(ctx context.Context, task *Task) error {
	if err := task.Validate(); err != nil {
		return fmt.Errorf("invalid task: %w", err)
	}

	// First, store the task in the repository
	if err := r.Create(ctx, task); err != nil {
		return fmt.Errorf("failed to store task: %w", err)
	}

	// Determine the appropriate subject based on task scheduling
	var subject string
	if task.Schedule != "" {
		subject = scheduledTaskSubject
	} else if task.DelayDuration > 0 {
		subject = delayedTaskSubject
	} else {
		subject = immediateTaskSubject
	}

	// Serialize the task
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	if task.Schedule != "" {
		// For scheduled tasks, we'll need to implement a scheduler service
		// that uses github.com/robfig/cron/v3 to manage the scheduling
		// For now, we'll just publish it to the scheduled subject
		_, err = r.js.Publish(subject, data)
		if err != nil {
			return fmt.Errorf("failed to publish scheduled task: %w", err)
		}
	} else if task.DelayDuration > 0 {
		// For delayed tasks, use NATS headers for delayed message delivery
		msg := nats.NewMsg(subject)
		msg.Header.Set(nats.MsgIdHdr, task.ID)
		msg.Header.Set("Nats-Expected-Stream", streamName)
		msg.Header.Set("Nats-Expected-Last-Sequence-Per-Subject", "true")
		msg.Header.Set("NATS-Delivery-Delay", task.DelayDuration.String())
		msg.Data = data

		// Publish the message with headers
		_, err = r.js.PublishMsg(msg)
		if err != nil {
			return fmt.Errorf("failed to publish delayed task: %w", err)
		}
	} else {
		// Publish the task without delay
		_, err = r.js.Publish(subject, data)
		if err != nil {
			return fmt.Errorf("failed to publish task: %w", err)
		}
	}

	return nil
}

// matchesFilter checks if a task matches the given filter criteria
func matchesFilter(task *Task, filter *Filter) bool {
	if filter == nil {
		return true
	}

	// Check status
	if len(filter.Status) > 0 {
		statusMatch := false
		for _, s := range filter.Status {
			if task.Status == s {
				statusMatch = true
				break
			}
		}
		if !statusMatch {
			return false
		}
	}

	// Check priority
	if len(filter.Priority) > 0 {
		priorityMatch := false
		for _, p := range filter.Priority {
			if task.Priority == p {
				priorityMatch = true
				break
			}
		}
		if !priorityMatch {
			return false
		}
	}

	// Check tags
	if len(filter.Tags) > 0 {
		for _, tag := range filter.Tags {
			tagFound := false
			for _, taskTag := range task.Tags {
				if taskTag == tag {
					tagFound = true
					break
				}
			}
			if !tagFound {
				return false
			}
		}
	}

	// Check time range
	if !filter.Since.IsZero() && task.CreatedAt.Before(filter.Since) {
		return false
	}
	if !filter.Until.IsZero() && task.CreatedAt.After(filter.Until) {
		return false
	}

	return true
}
