package tasq

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// TaskStatus is an enum type describing the status a task is currently in.
type TaskStatus string

// The collection of possible task statuses.
const (
	StatusNew        TaskStatus = "NEW"
	StatusEnqueued   TaskStatus = "ENQUEUED"
	StatusInProgress TaskStatus = "IN_PROGRESS"
	StatusSuccessful TaskStatus = "SUCCESSFUL"
	StatusFailed     TaskStatus = "FAILED"
)

// TaskStatusGroup is an enum type describing the key used in the
// map of TaskStatuses which groups them for different purposes.
type TaskStatusGroup int

// The collection of possible task status groupings.
const (
	AllTasks TaskStatusGroup = iota
	OpenTasks
	FinishedTasks
)

// GetTaskStatuses returns a slice of TaskStatuses based on the TaskStatusGroup
// passed as an argument.
func GetTaskStatuses(taskStatusGroup TaskStatusGroup) []TaskStatus {
	if selected, ok := map[TaskStatusGroup][]TaskStatus{
		AllTasks: {
			StatusNew,
			StatusEnqueued,
			StatusInProgress,
			StatusSuccessful,
			StatusFailed,
		},
		OpenTasks: {
			StatusNew,
			StatusEnqueued,
			StatusInProgress,
		},
		FinishedTasks: {
			StatusSuccessful,
			StatusFailed,
		},
	}[taskStatusGroup]; ok {
		return selected
	}

	return nil
}

// Task is the struct used to represent an atomic task managed by tasq.
type Task struct {
	ID           uuid.UUID
	Type         string
	Args         []byte
	Queue        string
	Priority     int16
	Status       TaskStatus
	ReceiveCount int32
	MaxReceives  int32
	LastError    *string
	CreatedAt    time.Time
	StartedAt    *time.Time
	FinishedAt   *time.Time
	VisibleAt    time.Time
}

// NewTask creates a new Task struct based on the supplied arguments required to define it.
func NewTask(taskType string, taskArgs any, queue string, priority int16, maxReceives int32) (*Task, error) {
	taskID, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("failed to generate new task ID: %w", err)
	}

	encodedArgs, err := encodeTaskArgs(taskArgs)
	if err != nil {
		return nil, err
	}

	return &Task{
		ID:           taskID,
		Type:         taskType,
		Args:         encodedArgs,
		Queue:        queue,
		Priority:     priority,
		Status:       StatusNew,
		ReceiveCount: 0,
		MaxReceives:  maxReceives,
		LastError:    nil,
		CreatedAt:    time.Now(),
		StartedAt:    nil,
		FinishedAt:   nil,
		VisibleAt:    time.Time{},
	}, nil
}

// UnmarshalArgs decodes the task arguments into the passed target interface.
func (t *Task) UnmarshalArgs(target any) error {
	var (
		buffer  = bytes.NewBuffer(t.Args)
		decoder = gob.NewDecoder(buffer)
	)

	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("failed to decode task arguments: %w", err)
	}

	return nil
}

func encodeTaskArgs(taskArgs any) ([]byte, error) {
	var (
		buffer  bytes.Buffer
		encoder = gob.NewEncoder(&buffer)
	)

	err := encoder.Encode(taskArgs)
	if err != nil {
		return []byte{}, fmt.Errorf("failed to encode task arguments: %w", err)
	}

	return buffer.Bytes(), nil
}
