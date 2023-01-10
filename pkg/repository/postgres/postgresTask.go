package postgres

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/greencoda/tasq/pkg/model"
)

type postgresTask struct {
	ID           uuid.UUID        `db:"id"`
	Type         string           `db:"type"`
	Args         []byte           `db:"args"`
	Queue        string           `db:"queue"`
	Priority     int16            `db:"priority"`
	Status       model.TaskStatus `db:"status"`
	ReceiveCount int32            `db:"receive_count"`
	MaxReceives  int32            `db:"max_receives"`
	LastError    sql.NullString   `db:"last_error"`
	CreatedAt    time.Time        `db:"created_at"`
	StartedAt    *time.Time       `db:"started_at"`
	FinishedAt   *time.Time       `db:"finished_at"`
	VisibleAt    time.Time        `db:"visible_at"`
}

func newFromTask(task *model.Task) *postgresTask {
	return &postgresTask{
		ID:           task.ID,
		Type:         task.Type,
		Args:         task.Args,
		Queue:        task.Queue,
		Priority:     task.Priority,
		Status:       task.Status,
		ReceiveCount: task.ReceiveCount,
		MaxReceives:  task.MaxReceives,
		LastError:    task.LastError,
		CreatedAt:    task.CreatedAt,
		StartedAt:    task.StartedAt,
		FinishedAt:   task.FinishedAt,
		VisibleAt:    task.VisibleAt,
	}
}

func (t *postgresTask) toTask() *model.Task {
	return &model.Task{
		ID:           t.ID,
		Type:         t.Type,
		Args:         t.Args,
		Queue:        t.Queue,
		Priority:     t.Priority,
		Status:       t.Status,
		ReceiveCount: t.ReceiveCount,
		MaxReceives:  t.MaxReceives,
		LastError:    t.LastError,
		CreatedAt:    t.CreatedAt,
		StartedAt:    t.StartedAt,
		FinishedAt:   t.FinishedAt,
		VisibleAt:    t.VisibleAt,
	}
}

func postgresTasksToTasks(postgresTasks []*postgresTask) []*model.Task {
	tasks := make([]*model.Task, len(postgresTasks))

	for i, postgresTask := range postgresTasks {
		tasks[i] = postgresTask.toTask()
	}

	return tasks
}
