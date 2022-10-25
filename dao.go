package tasq

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/template"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

var (
	OrderingCreatedAtFirst = []string{"created_at ASC", "priority DESC"}
	OrderingPriorityFirst  = []string{"priority DESC", "created_at ASC"}
)

type iDAO interface {
	migrate(ctx context.Context) error

	pingTasks(ctx context.Context, taskIDs []uuid.UUID, visibilityTimeout time.Duration) ([]*Task, error)
	pollTasks(ctx context.Context, types, queues []string, visibilityTimeout time.Duration, ordering []string, limit int) ([]*Task, error)
	cleanTasks(ctx context.Context, minimumAge time.Duration) error

	registerStart(ctx context.Context, task *Task) (*Task, error)
	registerError(ctx context.Context, task *Task, taskError error) (*Task, error)

	registerSuccess(ctx context.Context, task *Task) (*Task, error)
	registerFailure(ctx context.Context, task *Task) (*Task, error)

	submitTask(ctx context.Context, task *Task) (*Task, error)
	deleteTask(ctx context.Context, task *Task) error
	requeueTask(ctx context.Context, task *Task) (*Task, error)
}

func interpolateSQL(sql string, params map[string]any) (string, error) {
	template, err := template.New("sql").Parse(sql)
	if err != nil {
		return "", err
	}

	var outputBuffer bytes.Buffer

	err = template.Execute(&outputBuffer, params)
	if err != nil {
		return "", err
	}

	return outputBuffer.String(), nil
}

func sliceToSQLValueList[T any](slice []T) string {
	var stringSlice = make([]string, 0, len(slice))

	for _, s := range slice {
		stringSlice = append(stringSlice, fmt.Sprint(s))
	}

	return fmt.Sprintf("'%s'", strings.Join(stringSlice, "','"))
}
