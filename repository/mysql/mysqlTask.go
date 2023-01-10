package mysql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/greencoda/tasq"
)

const (
	idLength   = 16
	timeFormat = "2006-01-02 15:04:05.999999"
)

var (
	errIncorrectLength = errors.New("Scan: MySQLTaskID is of incorrect length")
	errUnableToScan    = errors.New("Scan: unable to scan type into MySQLTaskID")
)

// TaskID represents the types used to manage conversion of UUID
// to MySQL's binary(16) format.
type TaskID [idLength]byte

// Scan implements sql.Scanner so TaskIDs can be read from MySQL transparently.
func (i *TaskID) Scan(src any) error {
	switch src := src.(type) {
	case nil:
		return nil
	case []byte:
		if len(src) == 0 {
			return nil
		}

		if len(src) != idLength {
			return fmt.Errorf("%w: %v", errIncorrectLength, len(src))
		}

		copy((*i)[:], src)
	default:
		return fmt.Errorf("%w: %T", errUnableToScan, src)
	}

	return nil
}

// Value implements sql.Valuer so that TaskIDs can be written to MySQL
// transparently.
func (i TaskID) Value() (driver.Value, error) {
	return i[:], nil
}

type mySQLTask struct {
	ID           TaskID          `db:"id"`
	Type         string          `db:"type"`
	Args         []byte          `db:"args"`
	Queue        string          `db:"queue"`
	Priority     int16           `db:"priority"`
	Status       tasq.TaskStatus `db:"status"`
	ReceiveCount int32           `db:"receive_count"`
	MaxReceives  int32           `db:"max_receives"`
	LastError    sql.NullString  `db:"last_error"`
	CreatedAt    string          `db:"created_at"`
	StartedAt    sql.NullString  `db:"started_at"`
	FinishedAt   sql.NullString  `db:"finished_at"`
	VisibleAt    string          `db:"visible_at"`
}

func newFromTask(task *tasq.Task) *mySQLTask {
	return &mySQLTask{
		ID:           TaskID(task.ID),
		Type:         task.Type,
		Args:         task.Args,
		Queue:        task.Queue,
		Priority:     task.Priority,
		Status:       task.Status,
		ReceiveCount: task.ReceiveCount,
		MaxReceives:  task.MaxReceives,
		LastError:    stringToSQLNullString(task.LastError),
		CreatedAt:    timeToString(task.CreatedAt),
		StartedAt:    timeToSQLNullString(task.StartedAt),
		FinishedAt:   timeToSQLNullString(task.FinishedAt),
		VisibleAt:    timeToString(task.VisibleAt),
	}
}

func (t *mySQLTask) toTask() *tasq.Task {
	return &tasq.Task{
		ID:           uuid.UUID(t.ID),
		Type:         t.Type,
		Args:         t.Args,
		Queue:        t.Queue,
		Priority:     t.Priority,
		Status:       t.Status,
		ReceiveCount: t.ReceiveCount,
		MaxReceives:  t.MaxReceives,
		LastError:    parseNullableString(t.LastError),
		CreatedAt:    parseTime(t.CreatedAt),
		StartedAt:    parseNullableTime(t.StartedAt),
		FinishedAt:   parseNullableTime(t.FinishedAt),
		VisibleAt:    parseTime(t.VisibleAt),
	}
}

func mySQLTasksToTasks(mySQLTasks []*mySQLTask) []*tasq.Task {
	tasks := make([]*tasq.Task, len(mySQLTasks))

	for i, mySQLTask := range mySQLTasks {
		tasks[i] = mySQLTask.toTask()
	}

	return tasks
}

func stringToSQLNullString(input *string) sql.NullString {
	if input == nil {
		return sql.NullString{
			String: "",
			Valid:  false,
		}
	}

	return sql.NullString{
		String: *input,
		Valid:  true,
	}
}

func timeToString(input time.Time) string {
	return input.Format(timeFormat)
}

func timeToSQLNullString(input *time.Time) sql.NullString {
	if input == nil {
		return sql.NullString{
			String: "",
			Valid:  false,
		}
	}

	return sql.NullString{
		String: input.Format(timeFormat),
		Valid:  true,
	}
}

func parseNullableString(input sql.NullString) *string {
	if !input.Valid {
		return nil
	}

	return &input.String
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
