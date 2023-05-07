package tasq

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Ordering is an enum type describing the polling strategy utitlized during
// the polling process.
type Ordering int

// The collection of orderings.
const (
	OrderingCreatedAtFirst Ordering = iota
	OrderingPriorityFirst
)

// IRepository describes the mandatory methods a repository must implement
// in order for tasq to be able to use it.
type IRepository interface {
	Migrate(ctx context.Context) error

	PingTasks(ctx context.Context, taskIDs []uuid.UUID, visibilityTimeout time.Duration) ([]*Task, error)
	PollTasks(ctx context.Context, types, queues []string, visibilityTimeout time.Duration, ordering Ordering, limit int) ([]*Task, error)
	CleanTasks(ctx context.Context, minimumAge time.Duration) (int64, error)

	RegisterStart(ctx context.Context, task *Task) (*Task, error)
	RegisterError(ctx context.Context, task *Task, errTask error) (*Task, error)
	RegisterFinish(ctx context.Context, task *Task, finishStatus TaskStatus) (*Task, error)

	SubmitTask(ctx context.Context, task *Task) (*Task, error)
	DeleteTask(ctx context.Context, task *Task, visibleOnly bool) error
	RequeueTask(ctx context.Context, task *Task) (*Task, error)

	ScanTasks(ctx context.Context, taskStatuses []TaskStatus, taskTypes, queues []string, ordering Ordering, limit int) ([]*Task, error)
	CountTasks(ctx context.Context, taskStatuses []TaskStatus, taskTypes, queues []string) (int, error)
}
