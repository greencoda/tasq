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

const (
	MinTaskPriority int16 = -32767
	MaxTaskPriority int16 = 32767
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

func NewTask(taskType string, taskArgs any, queue string, priority int16, maxReceives int32) *Task {
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
