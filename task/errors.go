package task

import "errors"

var (
	// ErrEmptyTaskName is returned when a task name is empty
	ErrEmptyTaskName = errors.New("task name cannot be empty")

	// ErrInvalidStatus is returned when a task status is invalid
	ErrInvalidStatus = errors.New("invalid task status")

	// ErrInvalidPriority is returned when a task priority is invalid
	ErrInvalidPriority = errors.New("invalid task priority")

	// ErrInvalidProgress is returned when progress is not between 0 and 100
	ErrInvalidProgress = errors.New("progress must be between 0 and 100")

	// ErrTaskNotFound is returned when a task is not found
	ErrTaskNotFound = errors.New("task not found")

	// ErrTaskAlreadyExists is returned when a task with the same ID already exists
	ErrTaskAlreadyExists = errors.New("task already exists")

	// ErrInvalidTaskID is returned when a task ID is invalid
	ErrInvalidTaskID = errors.New("invalid task ID")

	// ErrTaskNameRequired is returned when a task name is empty
	ErrTaskNameRequired = errors.New("task name is required")

	// ErrTaskStatusRequired is returned when a task status is empty
	ErrTaskStatusRequired = errors.New("task status is required")

	// ErrTaskPriorityRequired is returned when a task priority is empty
	ErrTaskPriorityRequired = errors.New("task priority is required")
)
