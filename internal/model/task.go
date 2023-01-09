package model

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type TaskStatus string

const (
	StatusNew        TaskStatus = "NEW"
	StatusEnqueued   TaskStatus = "ENQUEUED"
	StatusInProgress TaskStatus = "IN_PROGRESS"
	StatusSuccessful TaskStatus = "SUCCESSFUL"
	StatusFailed     TaskStatus = "FAILED"
)

var (
	AllTaskStatuses = []TaskStatus{
		StatusNew,
		StatusEnqueued,
		StatusInProgress,
		StatusSuccessful,
		StatusFailed,
	}
	OpenTaskStatuses = []TaskStatus{
		StatusNew,
		StatusEnqueued,
		StatusInProgress,
	}
	FinishedTaskStatuses = []TaskStatus{
		StatusSuccessful,
		StatusFailed,
	}
)

type Task struct {
	ID           uuid.UUID
	Type         string
	Args         []byte
	Queue        string
	Priority     int16
	Status       TaskStatus
	ReceiveCount int32
	MaxReceives  int32
	LastError    sql.NullString
	CreatedAt    time.Time
	StartedAt    *time.Time
	FinishedAt   *time.Time
	VisibleAt    time.Time
}

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
		LastError: sql.NullString{
			Valid:  false,
			String: "",
		},
		CreatedAt:  time.Now(),
		StartedAt:  nil,
		FinishedAt: nil,
		VisibleAt:  time.Time{},
	}, nil
}

func (t *Task) GetDetails() *Task {
	return t
}

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
