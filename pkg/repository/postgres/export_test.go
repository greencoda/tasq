package postgres

import (
	"database/sql/driver"

	"github.com/greencoda/tasq/pkg/model"
	"github.com/jmoiron/sqlx"
)

func GetTestTaskValues(task *model.Task) []driver.Value {
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

func (d *Repository) PrepareWithTableName(sqlTemplate string) *sqlx.NamedStmt {
	return d.prepareWithTableName(sqlTemplate)
}
