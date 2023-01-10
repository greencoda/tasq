package repository

import (
	"bytes"
	"context"
	"text/template"
	"time"

	"github.com/google/uuid"
	"github.com/greencoda/tasq/pkg/model"
)

// Ordering is an enum type describing the polling strategy utitlized during
// the polling process
type Ordering int

// The collection of orderings
const (
	OrderingCreatedAtFirst Ordering = iota
	OrderingPriorityFirst
)

// IRepository describes the mandatory methods a repository must implement
// in order for tasq to be able to use it
type IRepository interface {
	Migrate(ctx context.Context) error

	PingTasks(ctx context.Context, taskIDs []uuid.UUID, visibilityTimeout time.Duration) ([]*model.Task, error)
	PollTasks(ctx context.Context, types, queues []string, visibilityTimeout time.Duration, ordering Ordering, limit int) ([]*model.Task, error)
	CleanTasks(ctx context.Context, minimumAge time.Duration) (int64, error)

	RegisterStart(ctx context.Context, task *model.Task) (*model.Task, error)
	RegisterError(ctx context.Context, task *model.Task, errTask error) (*model.Task, error)
	RegisterFinish(ctx context.Context, task *model.Task, finishStatus model.TaskStatus) (*model.Task, error)

	SubmitTask(ctx context.Context, task *model.Task) (*model.Task, error)
	DeleteTask(ctx context.Context, task *model.Task) error
	RequeueTask(ctx context.Context, task *model.Task) (*model.Task, error)
}

func InterpolateSQL(sql string, params map[string]any) string {
	template, err := template.New("sql").Parse(sql)
	if err != nil {
		panic(err)
	}

	var outputBuffer bytes.Buffer

	err = template.Execute(&outputBuffer, params)
	if err != nil {
		panic(err)
	}

	return outputBuffer.String()
}
