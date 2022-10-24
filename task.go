package tasq

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"time"

	"github.com/google/uuid"
)

type taskStatus string

const (
	StatusNew        taskStatus = "NEW"
	StatusEnqueued   taskStatus = "ENQUEUED"
	StatusInProgress taskStatus = "IN_PROGRESS"
	StatusSuccessful taskStatus = "SUCCESSFUL"
	StatusFailed     taskStatus = "FAILED"
)

var (
	allTaskStatuses = []taskStatus{
		StatusNew,
		StatusEnqueued,
		StatusInProgress,
		StatusSuccessful,
		StatusFailed,
	}
	openTaskStatuses = []taskStatus{
		StatusNew,
		StatusEnqueued,
		StatusInProgress,
	}
	closedTaskStatuses = []taskStatus{
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
	Status       taskStatus     `db:"status"`
	ReceiveCount int32          `db:"receive_count"`
	MaxReceives  int32          `db:"max_receives"`
	LastError    sql.NullString `db:"last_error"`
	CreatedAt    time.Time      `db:"created_at"`
	StartedAt    *time.Time     `db:"started_at"`
	FinishedAt   *time.Time     `db:"finished_at"`
	VisibleAt    time.Time      `db:"visible_at"`

	consumer *Consumer
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

func (t *Task) setConsumer(consumer *Consumer) {
	t.consumer = consumer
}

func (t *Task) start(ctx context.Context) {
	_, err := t.consumer.dao.registerStart(ctx, t)
	if err != nil {
		panic(err)
	}
}

func (t *Task) error(ctx context.Context, taskError error) {
	_, err := t.consumer.dao.registerError(ctx, t, taskError)
	if err != nil {
		panic(err)
	}

	if t.MaxReceives > 0 && (t.ReceiveCount) >= t.MaxReceives {
		t.fail(ctx)
	} else {
		t.requeue(ctx)
	}
}

func (t *Task) success(ctx context.Context) {
	if t.consumer.autoDeleteOnSuccess {
		err := t.consumer.dao.deleteTask(ctx, t)
		if err != nil {
			panic(err)
		}
	} else {
		_, err := t.consumer.dao.registerSuccess(ctx, t)
		if err != nil {
			panic(err)
		}
	}

	t.removeAsActive()
}

func (t *Task) fail(ctx context.Context) {
	_, err := t.consumer.dao.registerFailure(ctx, t)
	if err != nil {
		panic(err)
	}

	t.removeAsActive()
}

func (t *Task) requeue(ctx context.Context) {
	_, err := t.consumer.dao.requeueTask(ctx, t)
	if err != nil {
		panic(err)
	}

	t.removeAsActive()
}

func (t *Task) removeAsActive() {
	delete(t.consumer.activeTasks, t.ID)
}
