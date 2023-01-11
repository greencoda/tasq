package mysql

import (
	"database/sql"
	"database/sql/driver"

	"github.com/greencoda/tasq"
)

func GetTestTaskValues(task *tasq.Task) []driver.Value {
	testMySQLTask := newFromTask(task)

	return []driver.Value{
		testMySQLTask.ID,
		testMySQLTask.Type,
		testMySQLTask.Args,
		testMySQLTask.Queue,
		testMySQLTask.Priority,
		testMySQLTask.Status,
		testMySQLTask.ReceiveCount,
		testMySQLTask.MaxReceives,
		testMySQLTask.LastError,
		testMySQLTask.CreatedAt,
		testMySQLTask.StartedAt,
		testMySQLTask.FinishedAt,
		testMySQLTask.VisibleAt,
	}
}

func InterpolateSQL(sql string, params map[string]any) string {
	return interpolateSQL(sql, params)
}

func (d *Repository) GetQueryWithTableName(sqlTemplate string, args ...any) (string, []any) {
	return d.getQueryWithTableName(sqlTemplate, args...)
}

func StringToSQLNullString(input *string) sql.NullString {
	return stringToSQLNullString(input)
}

func ParseNullableString(input sql.NullString) *string {
	return parseNullableString(input)
}
