package model

import (
	"bytes"
	"database/sql"
	"encoding/gob"
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

type TaskPriority int16

const (
	MinTaskPriority TaskPriority = -32767
	MaxTaskPriority TaskPriority = 32767
)

type Task struct {
	ID           uuid.UUID      `db:"id"`
	Type         string         `db:"type"`
	Args         []byte         `db:"args"`
	Queue        string         `db:"queue"`
	Priority     TaskPriority   `db:"priority"`
	Status       TaskStatus     `db:"status"`
	ReceiveCount int32          `db:"receive_count"`
	MaxReceives  int32          `db:"max_receives"`
	LastError    sql.NullString `db:"last_error"`
	CreatedAt    time.Time      `db:"created_at"`
	StartedAt    *time.Time     `db:"started_at"`
	FinishedAt   *time.Time     `db:"finished_at"`
	VisibleAt    time.Time      `db:"visible_at"`
}

func NewTask(taskType string, taskArgs any, queue string, priority TaskPriority, maxReceives int32) *Task {
	taskID, err := uuid.NewRandom()
	if err != nil {
		return nil
	}

	encodedArgs, err := encodeTaskArgs(taskArgs)
	if err != nil {
		return nil
	}

	return &Task{
		ID:          taskID,
		Type:        taskType,
		Args:        encodedArgs,
		Queue:       queue,
		Priority:    priority,
		MaxReceives: maxReceives,
		Status:      StatusNew,
		CreatedAt:   time.Now(),
	}
}

func (t *Task) GetDetails() *Task {
	return t
}

func (t *Task) UnmarshalArgs(v any) error {
	var (
		buffer  = bytes.NewBuffer(t.Args)
		decoder = gob.NewDecoder(buffer)
	)

	if err := decoder.Decode(v); err != nil {
		return err
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
		return []byte{}, err
	}

	return buffer.Bytes(), nil
}
