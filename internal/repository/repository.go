package repository

import (
	"bytes"
	"context"
	"database/sql"
	"text/template"
	"time"

	"github.com/google/uuid"
	"github.com/greencoda/tasq/internal/model"
)

var (
	OrderingCreatedAtFirst = []string{"created_at ASC", "priority DESC"}
	OrderingPriorityFirst  = []string{"priority DESC", "created_at ASC"}
)

type IRepository interface {
	DB() *sql.DB

	Migrate(ctx context.Context) error

	PingTasks(ctx context.Context, taskIDs []uuid.UUID, visibilityTimeout time.Duration) ([]*model.Task, error)
	PollTasks(ctx context.Context, types, queues []string, visibilityTimeout time.Duration, ordering []string, limit int) ([]*model.Task, error)
	CleanTasks(ctx context.Context, minimumAge time.Duration) (int64, error)

	RegisterStart(ctx context.Context, task *model.Task) (*model.Task, error)
	RegisterError(ctx context.Context, task *model.Task, taskError error) (*model.Task, error)

	RegisterSuccess(ctx context.Context, task *model.Task) (*model.Task, error)
	RegisterFailure(ctx context.Context, task *model.Task) (*model.Task, error)

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
