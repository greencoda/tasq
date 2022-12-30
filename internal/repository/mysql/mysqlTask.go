package mysql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/greencoda/tasq/internal/model"
)

const timeFormat = "2006-01-02 15:04:05.999999"

type MySQLTaskID [16]byte

func (i *MySQLTaskID) Scan(src any) error {
	switch src := src.(type) {
	case nil:
		return nil

	// case string:
	// 	if len(src) == 0 {
	// 		return nil
	// 	}

	// 	// see Parse for required string format
	// 	u, err := uuid.Parse(src)
	// 	if err != nil {
	// 		return fmt.Errorf("Scan: %v", err)
	// 	}

	// 	*i = [16]byte(u)
	case []byte:
		if len(src) == 0 {
			return nil
		}

		if len(src) != 16 {
			return fmt.Errorf("Scan: taskID is of incorrect length %d", len(src))
		}

		copy((*i)[:], src)

	default:
		return fmt.Errorf("Scan: unable to scan type %T into MySQLTaskID", src)
	}

	return nil
}

func (i MySQLTaskID) Value() (driver.Value, error) {
	return i[:], nil
}

type MySQLTask struct {
	ID           MySQLTaskID      `db:"id"`
	Type         string           `db:"type"`
	Args         []byte           `db:"args"`
	Queue        string           `db:"queue"`
	Priority     int16            `db:"priority"`
	Status       model.TaskStatus `db:"status"`
	ReceiveCount int32            `db:"receive_count"`
	MaxReceives  int32            `db:"max_receives"`
	LastError    sql.NullString   `db:"last_error"`
	CreatedAt    string           `db:"created_at"`
	StartedAt    sql.NullString   `db:"started_at"`
	FinishedAt   sql.NullString   `db:"finished_at"`
	VisibleAt    string           `db:"visible_at"`
}

func (t *MySQLTask) toTask() *model.Task {
	return &model.Task{
		ID:           uuid.UUID(t.ID),
		Type:         t.Type,
		Args:         t.Args,
		Queue:        t.Queue,
		Priority:     t.Priority,
		Status:       t.Status,
		ReceiveCount: t.ReceiveCount,
		MaxReceives:  t.MaxReceives,
		LastError:    t.LastError,
		CreatedAt:    parseTime(t.CreatedAt),
		StartedAt:    parseNullableTime(t.StartedAt),
		FinishedAt:   parseNullableTime(t.FinishedAt),
		VisibleAt:    parseTime(t.VisibleAt),
	}
}

func mySQLTasksToTasks(mySQLTasks []*MySQLTask) []*model.Task {
	tasks := make([]*model.Task, len(mySQLTasks))

	for i, mySQLTask := range mySQLTasks {
		tasks[i] = mySQLTask.toTask()
	}

	return tasks
}

func parseTime(input string) time.Time {
	parsedTime, err := time.Parse(timeFormat, input)
	if err != nil {
		return time.Time{}
	}

	return parsedTime
}

func parseNullableTime(input sql.NullString) *time.Time {
	if !input.Valid {
		return nil
	}

	parsedTime := parseTime(input.String)

	return &parsedTime
}
