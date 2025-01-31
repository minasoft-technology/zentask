package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/minasoft-technology/zentask"
	"github.com/robfig/cron/v3"
)

// Schedule represents a cron schedule for a task
type Schedule struct {
	ID          string                 `json:"id"`
	CronExpr    string                 `json:"cron_expr"`   // Cron expression (e.g., "0 * * * *" for hourly)
	Queue       string                 `json:"queue"`       // Queue name
	Payload     interface{}            `json:"payload"`     // Task payload
	Options     *zentask.TaskOptions   `json:"options"`     // Task options
	Metadata    map[string]interface{} `json:"metadata"`    // Task metadata
	LastRun     time.Time              `json:"last_run"`    // Last execution time
	NextRun     time.Time              `json:"next_run"`    // Next scheduled execution
	Description string                 `json:"description"` // Human-readable description
}

// Scheduler manages scheduled tasks using cron expressions
type Scheduler struct {
	client    *zentask.Client
	cron      *cron.Cron
	schedules map[string]*Schedule
	mu        sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewScheduler creates a new scheduler
func NewScheduler(client *zentask.Client) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &Scheduler{
		client:    client,
		cron:      cron.New(cron.WithSeconds()),
		schedules: make(map[string]*Schedule),
		ctx:       ctx,
		cancel:    cancel,
	}
}

// AddSchedule adds a new scheduled task
func (s *Scheduler) AddSchedule(schedule *Schedule) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Parse cron expression with support for seconds field
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	cronSchedule, err := parser.Parse(schedule.CronExpr)
	if err != nil {
		return fmt.Errorf("invalid cron expression: %v", err)
	}

	// Calculate next run time
	schedule.NextRun = cronSchedule.Next(time.Now())

	// Create the cron job
	_, err = s.cron.AddFunc(schedule.CronExpr, func() {
		s.mu.Lock()
		schedule.LastRun = time.Now()
		schedule.NextRun = cronSchedule.Next(time.Now())
		s.mu.Unlock()

		// Create and enqueue the task
		task := &zentask.Task{
			Queue:    schedule.Queue,
			Payload:  schedule.Payload,
			Options:  schedule.Options,
			Metadata: schedule.Metadata,
		}

		// Add scheduler metadata
		if task.Metadata == nil {
			task.Metadata = make(map[string]interface{})
		}
		task.Metadata["scheduler_id"] = schedule.ID
		task.Metadata["scheduled_at"] = schedule.LastRun
		task.Metadata["cron_expression"] = schedule.CronExpr

		// Enqueue the task
		_, err := s.client.EnqueueContext(s.ctx, task)
		if err != nil {
			// In a production environment, you might want to log this error
			fmt.Printf("Failed to enqueue scheduled task %s: %v\n", schedule.ID, err)
		}
	})

	if err != nil {
		return fmt.Errorf("failed to add cron job: %v", err)
	}

	s.schedules[schedule.ID] = schedule
	return nil
}

// RemoveSchedule removes a scheduled task
func (s *Scheduler) RemoveSchedule(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.schedules[id]; !exists {
		return fmt.Errorf("schedule not found: %s", id)
	}

	delete(s.schedules, id)
	return nil
}

// GetSchedule returns a schedule by ID
func (s *Scheduler) GetSchedule(id string) (*Schedule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	schedule, exists := s.schedules[id]
	if !exists {
		return nil, fmt.Errorf("schedule not found: %s", id)
	}

	return schedule, nil
}

// ListSchedules returns all schedules
func (s *Scheduler) ListSchedules() []*Schedule {
	s.mu.RLock()
	defer s.mu.RUnlock()

	schedules := make([]*Schedule, 0, len(s.schedules))
	for _, schedule := range s.schedules {
		schedules = append(schedules, schedule)
	}
	return schedules
}

// Start starts the scheduler
func (s *Scheduler) Start() {
	s.cron.Start()
}

// Stop stops the scheduler
func (s *Scheduler) Stop() {
	s.cancel()
	s.cron.Stop()
}
