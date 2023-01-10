package postgres

import (
	"database/sql/driver"

	"github.com/greencoda/tasq"
	"github.com/jmoiron/sqlx"
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

func (d *Repository) PrepareWithTableName(sqlTemplate string) *sqlx.NamedStmt {
	return d.prepareWithTableName(sqlTemplate)
}
