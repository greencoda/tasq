package tasq

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type postgresDAO struct {
	db     *sqlx.DB
	prefix string
}

func newPostgresDAO(ctx context.Context, db *sql.DB, prefix string) (d iDAO, err error) {
	d = &postgresDAO{
		db:     sqlx.NewDb(db, "postgres"),
		prefix: prefix,
	}

	err = d.migrate(ctx)
	if err != nil {
		return nil, err
	}

	return d, nil
}

func (d *postgresDAO) tableName() string {
	return d.prefix + "_tasks"
}

func (d *postgresDAO) statusTypeName() string {
	return d.prefix + "_task_status"
}

func (d *postgresDAO) migrate(ctx context.Context) (err error) {
	err = d.migrateStatus(ctx)
	if err != nil {
		return err
	}

	err = d.migrateTable(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (d *postgresDAO) pingTasks(ctx context.Context, taskIDs []uuid.UUID, visibilityTimeout time.Duration) ([]*Task, error) {
	if len(taskIDs) == 0 {
		return []*Task{}, nil
	}

	var (
		pingedTime  = time.Now()
		sqlTemplate = `UPDATE
				{{.tableName}}
			SET
				"visible_at" = :visible_at
			WHERE
				"id" = ANY(:pinged_ids)
			RETURNING id;`
	)

	stmt, err := d.interpolateNamedStmt(ctx, sqlTemplate)
	if err != nil {
		return []*Task{}, err
	}

	var pingedTasks []*Task

	err = stmt.SelectContext(ctx, &pingedTasks, map[string]any{
		"visible_at": pingedTime.Add(visibilityTimeout),
		"pinged_ids": pq.Array(taskIDs),
	})
	if err != nil {
		return []*Task{}, err
	}

	return pingedTasks, nil
}

func (d *postgresDAO) pollTasks(ctx context.Context, queues []string, visibilityTimeout time.Duration, ordering []string, pollLimit int) ([]*Task, error) {
	if pollLimit == 0 {
		return []*Task{}, nil
	}

	var (
		pollTime    = time.Now()
		sqlTemplate = `UPDATE {{.tableName}} SET
				"status" = :status,
				"receive_count" = "receive_count" + 1,
				"visible_at" = :visible_at
			WHERE
				"id" IN(
					SELECT
						"id" FROM {{.tableName}}
					WHERE
						"queue" = ANY(:poll_queues) AND 
						"status" = ANY(:poll_statuses) AND 
						"visible_at" <= :poll_time
					ORDER BY
						:poll_ordering
					LIMIT :poll_limit
				FOR UPDATE)
			RETURNING *;`
	)

	stmt, err := d.interpolateNamedStmt(ctx, sqlTemplate)
	if err != nil {
		return []*Task{}, err
	}

	var polledTasks []*Task

	err = stmt.SelectContext(ctx, &polledTasks, map[string]any{
		"status":        StatusEnqueued,
		"visible_at":    pollTime.Add(visibilityTimeout),
		"poll_queues":   pq.Array(queues),
		"poll_statuses": pq.Array(openTaskStatuses),
		"poll_time":     pollTime,
		"poll_ordering": pq.Array(ordering),
		"poll_limit":    pollLimit,
	})
	if err != nil && err != sql.ErrNoRows {
		return []*Task{}, err
	}

	return polledTasks, nil
}

func (d *postgresDAO) cleanTasks(ctx context.Context, cleanAge time.Duration) error {
	var (
		cleanTime   = time.Now()
		sqlTemplate = `DELETE FROM
			{{.tableName}}
		WHERE
			"status" = ANY(:statuses)
			"created_at" <= :cleanAt;`
	)

	query, err := interpolateSQL(sqlTemplate, map[string]any{
		"statuses": pq.Array(closedTaskStatuses),
		"cleanAt":  cleanTime.Add(-cleanAge),
	})
	if err != nil {
		return err
	}

	_, err = d.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

func (d *postgresDAO) registerStart(ctx context.Context, task *Task) (*Task, error) {
	var (
		startTime   = time.Now()
		sqlTemplate = `UPDATE
			{{.tableName}}
		SET
			"status" = :status,
			"started_at" = :startTime
		WHERE
			"id" = :taskID
		RETURNING *`
	)

	stmt, err := d.interpolateNamedStmt(ctx, sqlTemplate)
	if err != nil {
		return &Task{}, fmt.Errorf("a: %v", err)
	}

	var updatedTask = new(Task)

	row := stmt.QueryRowContext(ctx, map[string]any{
		"status":    StatusInProgress,
		"startTime": startTime,
		"taskID":    task.ID,
	})

	err = row.StructScan(updatedTask)
	if err != nil && err != sql.ErrNoRows {
		return &Task{}, fmt.Errorf("b: %v", err)
	}

	return updatedTask, nil
}

func (d *postgresDAO) registerError(ctx context.Context, task *Task, taskError error) (*Task, error) {
	var (
		sqlTemplate = `UPDATE
			{{.tableName}}
		SET
			"last_error" = :errorMessage
		WHERE
			"id" = :taskID
		RETURNING *`
	)

	stmt, err := d.interpolateNamedStmt(ctx, sqlTemplate)
	if err != nil {
		return &Task{}, err
	}

	var updatedTask = new(Task)

	row := stmt.QueryRowContext(ctx, map[string]any{
		"errorMessage": taskError.Error(),
		"taskID":       task.ID,
	})

	err = row.StructScan(updatedTask)
	if err != nil && err != sql.ErrNoRows {
		return &Task{}, err
	}

	return updatedTask, nil
}

func (d *postgresDAO) registerSuccess(ctx context.Context, task *Task) (*Task, error) {
	return d.registerFinish(ctx, task, StatusSuccessful)
}

func (d *postgresDAO) registerFailure(ctx context.Context, task *Task) (*Task, error) {
	return d.registerFinish(ctx, task, StatusFailed)
}

func (d *postgresDAO) registerFinish(ctx context.Context, task *Task, finishStatus taskStatus) (*Task, error) {
	var (
		finishTime  = time.Now()
		sqlTemplate = `UPDATE
			{{.tableName}}
		SET
			"status" = :status,
			"finished_at" = :finishTime
		WHERE
			"id" = :taskID
		RETURNING *`
	)

	stmt, err := d.interpolateNamedStmt(ctx, sqlTemplate)
	if err != nil {
		return &Task{}, err
	}

	var updatedTask = new(Task)

	row := stmt.QueryRowContext(ctx, map[string]any{
		"status":     finishStatus,
		"finishTime": finishTime,
		"taskID":     task.ID,
	})

	err = row.StructScan(updatedTask)
	if err != nil && err != sql.ErrNoRows {
		return &Task{}, err
	}

	return updatedTask, nil
}

func (d *postgresDAO) submitTask(ctx context.Context, task *Task) (*Task, error) {
	var sqlTemplate = `INSERT INTO {{.tableName}} (id, type, args, queue, priority, status, max_receives, created_at) 
		VALUES 
			(:id, :type, :args, :queue, :priority, :status, :maxReceives, :createdAt)
		RETURNING *`

	stmt, err := d.interpolateNamedStmt(ctx, sqlTemplate)
	if err != nil {
		return &Task{}, err
	}

	var submittedTask = new(Task)

	row := stmt.QueryRowContext(ctx, map[string]any{
		"id":          task.ID,
		"type":        task.Type,
		"args":        task.Args,
		"queue":       task.Queue,
		"priority":    task.Priority,
		"status":      task.Status,
		"maxReceives": task.MaxReceives,
		"createdAt":   task.CreatedAt,
	})

	err = row.StructScan(submittedTask)
	if err != nil {
		return &Task{}, err
	}

	return submittedTask, nil
}

func (d *postgresDAO) deleteTask(ctx context.Context, task *Task) error {
	var sqlTemplate = `DELETE FROM {{.tableName}}
		WHERE
			"id" = :taskID
		RETURNING *`

	stmt, err := d.interpolateNamedStmt(ctx, sqlTemplate)
	if err != nil {
		return err
	}

	_, err = stmt.ExecContext(ctx, map[string]any{
		"taskID": task.ID,
	})
	if err != nil {
		return err
	}

	return nil
}

func (d *postgresDAO) requeueTask(ctx context.Context, task *Task) (*Task, error) {
	var sqlTemplate = `UPDATE {{.tableName}}
		SET
			"status" = :status
		WHERE
			"id" = :taskID
		RETURNING *`

	stmt, err := d.interpolateNamedStmt(ctx, sqlTemplate)
	if err != nil {
		return &Task{}, err
	}

	var updatedTask = new(Task)

	row := stmt.QueryRowContext(ctx, map[string]any{
		"status": StatusNew,
		"taskID": task.ID,
	})

	err = row.StructScan(updatedTask)
	if err != nil && err != sql.ErrNoRows {
		return &Task{}, err
	}

	return updatedTask, err
}

func (d *postgresDAO) interpolateNamedStmt(ctx context.Context, sqlTemplate string) (*sqlx.NamedStmt, error) {
	query, err := interpolateSQL(sqlTemplate, map[string]any{
		"tableName": d.tableName(),
	})
	if err != nil {
		return nil, err
	}

	return d.db.PrepareNamedContext(ctx, query)
}

func (d *postgresDAO) migrateStatus(ctx context.Context) error {
	var sqlTemplate = `DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '{{.statusTypeName}}') THEN
				CREATE TYPE {{.statusTypeName}} AS ENUM ({{.enumValues}});
			END IF;
		END$$;`

	query, err := interpolateSQL(sqlTemplate, map[string]any{
		"statusTypeName": d.statusTypeName(),
		"enumValues":     sliceToSQLValueList(allTaskStatuses),
	})
	if err != nil {
		return err
	}

	_, err = d.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

func (d *postgresDAO) migrateTable(ctx context.Context) error {
	var sqlTemplate = `CREATE TABLE IF NOT EXISTS {{.tableName}} (
			"id" UUID NOT NULL PRIMARY KEY,
			"type" TEXT NOT NULL,
			"args" BYTEA NOT NULL,
			"queue" TEXT NOT NULL,
			"priority" SMALLINT NOT NULL,
			"status" {{.statusTypeName}} NOT NULL,
			"receive_count" INTEGER NOT NULL DEFAULT 0,
			"max_receives" INTEGER NOT NULL DEFAULT 0,
			"last_error" TEXT,
			"created_at" TIMESTAMPTZ NOT NULL DEFAULT '0001-01-01 00:00:00.000000',
			"started_at" TIMESTAMPTZ,
			"finished_at" TIMESTAMPTZ,
			"visible_at" TIMESTAMPTZ NOT NULL DEFAULT '0001-01-01 00:00:00.000000'
		);`

	query, err := interpolateSQL(sqlTemplate, map[string]any{
		"tableName":      d.tableName(),
		"statusTypeName": d.statusTypeName(),
	})
	if err != nil {
		return err
	}

	_, err = d.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	return nil
}
