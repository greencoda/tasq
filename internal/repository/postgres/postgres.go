package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/greencoda/tasq/internal/model"
	"github.com/greencoda/tasq/internal/repository"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type postgresRepository struct {
	db     *sqlx.DB
	prefix string
}

func NewRepository(dataSource any, driver, prefix string) (repository.IRepository, error) {
	switch d := dataSource.(type) {
	case string:
		return newRepositoryFromDSN(d, driver, prefix)
	case *sql.DB:
		return newRepositoryFromDB(d, driver, prefix)
	}

	return nil, fmt.Errorf("unexpected dataSource type: %T", dataSource)
}

func newRepositoryFromDSN(dsn string, driver, prefix string) (repository.IRepository, error) {
	dbx, err := sqlx.Open(driver, dsn)
	if err != nil {
		return nil, err
	}

	return &postgresRepository{
		db:     dbx,
		prefix: prefix,
	}, nil
}

func newRepositoryFromDB(db *sql.DB, driver, prefix string) (repository.IRepository, error) {
	dbx := sqlx.NewDb(db, driver)

	return &postgresRepository{
		db:     dbx,
		prefix: prefix,
	}, nil
}

func (d *postgresRepository) DB() *sql.DB {
	return d.db.DB
}

func (d *postgresRepository) tableName() string {
	return d.prefix + "_tasks"
}

func (d *postgresRepository) statusTypeName() string {
	return d.prefix + "_task_status"
}

func (d *postgresRepository) Migrate(ctx context.Context) (err error) {
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

func (d *postgresRepository) PingTasks(ctx context.Context, taskIDs []uuid.UUID, visibilityTimeout time.Duration) (pingedTasks []*model.Task, err error) {
	if len(taskIDs) == 0 {
		return []*model.Task{}, nil
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
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	err = stmt.SelectContext(ctx, &pingedTasks, map[string]any{
		"visible_at": pingedTime.Add(visibilityTimeout),
		"pinged_ids": pq.Array(taskIDs),
	})
	if err != nil {
		return []*model.Task{}, err
	}

	return pingedTasks, nil
}

func (d *postgresRepository) PollTasks(ctx context.Context, types, queues []string, visibilityTimeout time.Duration, ordering []string, pollLimit int) (polledTasks []*model.Task, err error) {
	if pollLimit == 0 {
		return []*model.Task{}, nil
	}

	var (
		pollTime    = time.Now()
		sqlTemplate = `UPDATE {{.tableName}} SET
				"status" = :status,
				"receive_count" = "receive_count" + 1,
				"visible_at" = :visible_at
			WHERE
				"id" IN (
					SELECT
						"id" FROM {{.tableName}}
					WHERE
						"type" = ANY(:poll_types) AND
						"queue" = ANY(:poll_queues) AND 
						"status" = ANY(:poll_statuses) AND 
						"visible_at" <= :poll_time
					ORDER BY
						:poll_ordering
					LIMIT :poll_limit
				FOR UPDATE )
			RETURNING *;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	err = stmt.SelectContext(ctx, &polledTasks, map[string]any{
		"status":        model.StatusEnqueued,
		"visible_at":    pollTime.Add(visibilityTimeout),
		"poll_types":    pq.Array(types),
		"poll_queues":   pq.Array(queues),
		"poll_statuses": pq.Array(model.OpenTaskStatuses),
		"poll_time":     pollTime,
		"poll_ordering": pq.Array(ordering),
		"poll_limit":    pollLimit,
	})
	if err != nil && err != sql.ErrNoRows {
		return []*model.Task{}, err
	}

	return polledTasks, nil
}

func (d *postgresRepository) CleanTasks(ctx context.Context, cleanAge time.Duration) (rowsAffected int64, err error) {
	var (
		cleanTime   = time.Now()
		sqlTemplate = `DELETE FROM {{.tableName}}
			WHERE
				"status" = ANY(:statuses) AND
				"created_at" <= :cleanAt;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	result, err := stmt.ExecContext(ctx, map[string]any{
		"statuses": pq.Array(model.FinishedTaskStatuses),
		"cleanAt":  cleanTime.Add(-cleanAge),
	})
	if err != nil {
		return 0, err
	}

	rowsAffected, err = result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return rowsAffected, nil
}

func (d *postgresRepository) RegisterStart(ctx context.Context, task *model.Task) (updatedTask *model.Task, err error) {
	var (
		startTime   = time.Now()
		sqlTemplate = `UPDATE {{.tableName}} SET
				"status" = :status,
				"started_at" = :startTime
			WHERE
				"id" = :taskID
			RETURNING *;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	row := stmt.QueryRowContext(ctx, map[string]any{
		"status":    model.StatusInProgress,
		"startTime": startTime,
		"taskID":    task.ID,
	})

	updatedTask = new(model.Task)

	err = row.StructScan(updatedTask)
	if err != nil && err != sql.ErrNoRows {
		return &model.Task{}, err
	}

	return updatedTask, nil
}

func (d *postgresRepository) RegisterError(ctx context.Context, task *model.Task, taskError error) (updatedTask *model.Task, err error) {
	var (
		sqlTemplate = `UPDATE {{.tableName}} SET
				"last_error" = :errorMessage
			WHERE
				"id" = :taskID
			RETURNING *;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	row := stmt.QueryRowContext(ctx, map[string]any{
		"errorMessage": taskError.Error(),
		"taskID":       task.ID,
	})

	updatedTask = new(model.Task)

	err = row.StructScan(updatedTask)
	if err != nil && err != sql.ErrNoRows {
		return &model.Task{}, err
	}

	return updatedTask, nil
}

func (d *postgresRepository) RegisterSuccess(ctx context.Context, task *model.Task) (*model.Task, error) {
	return d.registerFinish(ctx, task, model.StatusSuccessful)
}

func (d *postgresRepository) RegisterFailure(ctx context.Context, task *model.Task) (*model.Task, error) {
	return d.registerFinish(ctx, task, model.StatusFailed)
}

func (d *postgresRepository) registerFinish(ctx context.Context, task *model.Task, finishStatus model.TaskStatus) (updatedTask *model.Task, err error) {
	var (
		finishTime  = time.Now()
		sqlTemplate = `UPDATE {{.tableName}} SET
				"status" = :status,
				"finished_at" = :finishTime
			WHERE
				"id" = :taskID
			RETURNING *;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	row := stmt.QueryRowContext(ctx, map[string]any{
		"status":     finishStatus,
		"finishTime": finishTime,
		"taskID":     task.ID,
	})

	updatedTask = new(model.Task)

	err = row.StructScan(updatedTask)
	if err != nil && err != sql.ErrNoRows {
		return &model.Task{}, err
	}

	return updatedTask, nil
}

func (d *postgresRepository) SubmitTask(ctx context.Context, task *model.Task) (submittedTask *model.Task, err error) {
	var (
		sqlTemplate = `INSERT INTO {{.tableName}} (id, type, args, queue, priority, status, max_receives, created_at) 
			VALUES 
				(:id, :type, :args, :queue, :priority, :status, :maxReceives, :createdAt)
			RETURNING *;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

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

	submittedTask = new(model.Task)

	err = row.StructScan(submittedTask)
	if err != nil {
		return &model.Task{}, err
	}

	return submittedTask, nil
}

func (d *postgresRepository) DeleteTask(ctx context.Context, task *model.Task) (err error) {
	var (
		sqlTemplate = `DELETE FROM {{.tableName}}
			WHERE
				"id" = :taskID
			RETURNING *;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	_, err = stmt.ExecContext(ctx, map[string]any{
		"taskID": task.ID,
	})

	return err
}

func (d *postgresRepository) RequeueTask(ctx context.Context, task *model.Task) (updatedTask *model.Task, err error) {
	var (
		sqlTemplate = `UPDATE {{.tableName}} SET
				"status" = :status
			WHERE
				"id" = :taskID
			RETURNING *;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	row := stmt.QueryRowContext(ctx, map[string]any{
		"status": model.StatusNew,
		"taskID": task.ID,
	})

	updatedTask = new(model.Task)

	err = row.StructScan(updatedTask)
	if err != nil && err != sql.ErrNoRows {
		return &model.Task{}, err
	}

	return updatedTask, err
}

func (d *postgresRepository) prepareWithTableName(sqlTemplate string) (namedStmt *sqlx.NamedStmt) {
	var (
		query = repository.InterpolateSQL(sqlTemplate, map[string]any{
			"tableName": d.tableName(),
		})
		err error
	)

	namedStmt, err = d.db.PrepareNamed(query)
	if err != nil {
		panic(err)
	}

	return namedStmt
}

func (d *postgresRepository) migrateStatus(ctx context.Context) (err error) {
	var (
		sqlTemplate = `DO $$
			BEGIN
				IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '{{.statusTypeName}}') THEN
					CREATE TYPE {{.statusTypeName}} AS ENUM ({{.enumValues}});
				END IF;
			END$$;`
		query = repository.InterpolateSQL(sqlTemplate, map[string]any{
			"statusTypeName": d.statusTypeName(),
			"enumValues":     sliceToPostgreSQLValueList(model.AllTaskStatuses),
		})
	)

	_, err = d.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

func (d *postgresRepository) migrateTable(ctx context.Context) error {
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

	query := repository.InterpolateSQL(sqlTemplate, map[string]any{
		"tableName":      d.tableName(),
		"statusTypeName": d.statusTypeName(),
	})

	_, err := d.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

func sliceToPostgreSQLValueList[T any](slice []T) string {
	var stringSlice = make([]string, 0, len(slice))

	for _, s := range slice {
		stringSlice = append(stringSlice, fmt.Sprint(s))
	}

	return fmt.Sprintf("'%s'", strings.Join(stringSlice, "','"))
}
