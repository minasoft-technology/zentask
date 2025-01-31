package scheduler

import (
	"context"
	"fmt"
	"strings"
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
	cronEntryID cron.EntryID          // Internal cron job ID
}

// SchedulerOption configures a Scheduler
type SchedulerOption func(*Scheduler)

// Scheduler manages scheduled tasks using cron expressions
type Scheduler struct {
	client     *zentask.Client
	cron       *cron.Cron
	schedules  map[string]*Schedule
	mu         sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc
	useSeconds bool
	location   *time.Location
	logger     cron.Logger
}

// WithLocation sets the time location for the scheduler
func WithLocation(loc *time.Location) SchedulerOption {
	return func(s *Scheduler) {
		s.location = loc
		s.recreateCron()
	}
}

// WithLogger sets a logger for the scheduler
func WithLogger(logger cron.Logger) SchedulerOption {
	return func(s *Scheduler) {
		s.logger = logger
		s.recreateCron()
	}
}

// WithSeconds enables the seconds field in cron expressions
func WithSeconds(enabled bool) SchedulerOption {
	return func(s *Scheduler) {
		s.useSeconds = enabled
		s.recreateCron()
	}
}

// recreateCron recreates the cron instance with current settings
func (s *Scheduler) recreateCron() {
	opts := []cron.Option{}
	if s.useSeconds {
		opts = append(opts, cron.WithSeconds())
	}
	if s.location != nil {
		opts = append(opts, cron.WithLocation(s.location))
	}
	if s.logger != nil {
		opts = append(opts, cron.WithLogger(s.logger))
	}
	s.cron = cron.New(opts...)
}

// NewScheduler creates a new scheduler with the given options
func NewScheduler(client *zentask.Client, opts ...SchedulerOption) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Scheduler{
		client:     client,
		schedules:  make(map[string]*Schedule),
		ctx:        ctx,
		cancel:     cancel,
		useSeconds: false,
		location:   time.Local,
	}

	// Initialize cron with default settings
	s.recreateCron()

	// Apply options
	for _, opt := range opts {
		opt(s)
	}

	return s
}

// AddSchedule adds a new scheduled task
func (s *Scheduler) AddSchedule(schedule *Schedule) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.schedules[schedule.ID]; exists {
		return fmt.Errorf("schedule with ID %s already exists", schedule.ID)
	}

	// Handle both 5 and 6 field expressions
	cronExpr := schedule.CronExpr
	fields := strings.Fields(cronExpr)
	if len(fields) == 5 && !s.useSeconds {
		// Standard 5-field expression with standard cron
		// No modification needed
	} else if len(fields) == 5 && s.useSeconds {
		// Add "0" as the seconds field
		cronExpr = "0 " + cronExpr
	} else if len(fields) == 6 && !s.useSeconds {
		return fmt.Errorf("6-field cron expression provided but seconds are not enabled. Use WithSeconds(true) option when creating the scheduler")
	}

	// Create the cron job
	entryID, err := s.cron.AddFunc(cronExpr, func() {
		s.mu.Lock()
		schedule.LastRun = time.Now()
		entry := s.cron.Entry(schedule.cronEntryID)
		schedule.NextRun = entry.Next
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

	schedule.cronEntryID = entryID
	entry := s.cron.Entry(entryID)
	schedule.NextRun = entry.Next
	s.schedules[schedule.ID] = schedule
	return nil
}

// RemoveSchedule removes a schedule by ID
func (s *Scheduler) RemoveSchedule(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	schedule, exists := s.schedules[id]
	if !exists {
		return fmt.Errorf("schedule with ID %s not found", id)
	}

	s.cron.Remove(schedule.cronEntryID)
	delete(s.schedules, id)
	return nil
}

// GetSchedule returns a schedule by ID
func (s *Scheduler) GetSchedule(id string) (*Schedule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	schedule, exists := s.schedules[id]
	if !exists {
		return nil, fmt.Errorf("schedule with ID %s not found", id)
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

// Stop stops the scheduler and waits for all running jobs to complete
func (s *Scheduler) Stop() {
	s.cancel()
	<-s.cron.Stop().Done()
}
