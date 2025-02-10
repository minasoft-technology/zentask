package service

import (
	"context"

	"github.com/minasoft-technology/zentask/task"
)

// TaskService provides high-level business operations for tasks
type TaskService struct {
	repo task.Repository
}

// NewTaskService creates a new TaskService instance
func NewTaskService(repo task.Repository) *TaskService {
	return &TaskService{repo: repo}
}

// CreateTask creates a new task with the given parameters
func (s *TaskService) CreateTask(ctx context.Context, name, description string, priority task.Priority, tags []string) (*task.Task, error) {
	return s.repo.EnqueueTask(ctx, name,
		task.WithDescription(description),
		task.WithPriority(priority),
		task.WithTags(tags...),
	)
}

// GetTask retrieves a task by its ID
func (s *TaskService) GetTask(ctx context.Context, id string) (*task.Task, error) {
	return s.repo.Get(ctx, id)
}

// UpdateTaskStatus updates the status of a task
func (s *TaskService) UpdateTaskStatus(ctx context.Context, id string, status task.Status) error {
	t, err := s.repo.Get(ctx, id)
	if err != nil {
		return err
	}
	t.Status = status
	return s.repo.Update(ctx, t)
}

// ListTasks retrieves tasks based on the provided filter
func (s *TaskService) ListTasks(ctx context.Context, filter *task.Filter) ([]*task.Task, error) {
	return s.repo.List(ctx, filter)
}

// WatchTask sets up a watch on a specific task
func (s *TaskService) WatchTask(ctx context.Context, id string) (<-chan *task.Task, error) {
	return s.repo.Watch(ctx, id)
}

// DeleteTask removes a task by its ID
func (s *TaskService) DeleteTask(ctx context.Context, id string) error {
	return s.repo.Delete(ctx, id)
}
