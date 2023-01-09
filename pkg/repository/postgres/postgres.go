package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/greencoda/tasq/internal/model"
	"github.com/greencoda/tasq/pkg/repository"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

const driverName = "postgres"

type postgresRepository struct {
	db     *sqlx.DB
	prefix string
}

func NewRepository(dataSource any, prefix string) (repository.IRepository, error) {
	switch d := dataSource.(type) {
	case string:
		return newRepositoryFromDSN(d, prefix)
	case *sql.DB:
		return newRepositoryFromDB(d, prefix)
	}

	return nil, fmt.Errorf("unexpected dataSource type: %T", dataSource)
}

func newRepositoryFromDSN(dsn string, prefix string) (repository.IRepository, error) {
	dbx, _ := sqlx.Open(driverName, dsn)

	return &postgresRepository{
		db:     dbx,
		prefix: prefix,
	}, nil
}

func newRepositoryFromDB(db *sql.DB, prefix string) (repository.IRepository, error) {
	dbx := sqlx.NewDb(db, driverName)

	return &postgresRepository{
		db:     dbx,
		prefix: prefix,
	}, nil
}

func (d *postgresRepository) DB() *sql.DB {
	return d.db.DB
}

func (d *postgresRepository) tableName() string {
	const tableName = "tasks"

	if len(d.prefix) > 0 {
		return d.prefix + "_" + tableName
	}

	return tableName
}

func (d *postgresRepository) statusTypeName() string {
	typeNameSegments := []string{"task_status"}
	if len(d.prefix) > 0 {
		typeNameSegments = append([]string{d.prefix}, typeNameSegments...)
	}

	return strings.Join(typeNameSegments, "_")
}

func (d *postgresRepository) Migrate(ctx context.Context) error {
	err := d.migrateStatus(ctx)
	if err != nil {
		return err
	}

	err = d.migrateTable(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (d *postgresRepository) PingTasks(ctx context.Context, taskIDs []uuid.UUID, visibilityTimeout time.Duration) ([]*model.Task, error) {
	if len(taskIDs) == 0 {
		return []*model.Task{}, nil
	}

	var (
		pingedTasks []*postgresTask
		pingTime    = time.Now()
		sqlTemplate = `UPDATE
				{{.tableName}}
			SET
				"visible_at" = :visible_at
			WHERE
				"id" = ANY(:pinged_ids)
			RETURNING id;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	err := stmt.SelectContext(ctx, &pingedTasks, map[string]any{
		"visible_at": pingTime.Add(visibilityTimeout),
		"pinged_ids": pq.Array(taskIDs),
	})
	if err != nil && err != sql.ErrNoRows {
		return []*model.Task{}, err
	}

	return postgresTasksToTasks(pingedTasks), nil
}

func (d *postgresRepository) PollTasks(ctx context.Context, types, queues []string, visibilityTimeout time.Duration, ordering []string, pollLimit int) ([]*model.Task, error) {
	if pollLimit == 0 {
		return []*model.Task{}, nil
	}

	var (
		polledTasks []*postgresTask
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

	err := stmt.SelectContext(ctx, &polledTasks, map[string]any{
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

	return postgresTasksToTasks(polledTasks), nil
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

func (d *postgresRepository) RegisterStart(ctx context.Context, task *model.Task) (*model.Task, error) {
	var (
		updatedTask = new(postgresTask)
		startTime   = time.Now()
		sqlTemplate = `UPDATE {{.tableName}} SET
				"status" = :status,
				"started_at" = :startTime
			WHERE
				"id" = :taskID
			RETURNING *;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	err := stmt.
		QueryRowContext(ctx, map[string]any{
			"status":    model.StatusInProgress,
			"startTime": startTime,
			"taskID":    task.ID,
		}).
		StructScan(updatedTask)
	if err != nil && err != sql.ErrNoRows {
		return &model.Task{}, err
	}

	return updatedTask.toTask(), nil
}

func (d *postgresRepository) RegisterError(ctx context.Context, task *model.Task, errTask error) (*model.Task, error) {
	var (
		updatedTask = new(postgresTask)
		sqlTemplate = `UPDATE {{.tableName}} SET
				"last_error" = :errorMessage
			WHERE
				"id" = :taskID
			RETURNING *;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	err := stmt.
		QueryRowContext(ctx, map[string]any{
			"errorMessage": errTask.Error(),
			"taskID":       task.ID,
		}).
		StructScan(updatedTask)
	if err != nil && err != sql.ErrNoRows {
		return &model.Task{}, err
	}

	return updatedTask.toTask(), nil
}

func (d *postgresRepository) RegisterSuccess(ctx context.Context, task *model.Task) (*model.Task, error) {
	return d.registerFinish(ctx, task, model.StatusSuccessful)
}

func (d *postgresRepository) RegisterFailure(ctx context.Context, task *model.Task) (*model.Task, error) {
	return d.registerFinish(ctx, task, model.StatusFailed)
}

func (d *postgresRepository) registerFinish(ctx context.Context, task *model.Task, finishStatus model.TaskStatus) (*model.Task, error) {
	var (
		updatedTask = new(postgresTask)
		finishTime  = time.Now()
		sqlTemplate = `UPDATE {{.tableName}} SET
				"status" = :status,
				"finished_at" = :finishTime
			WHERE
				"id" = :taskID
			RETURNING *;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	err := stmt.
		QueryRowContext(ctx, map[string]any{
			"status":     finishStatus,
			"finishTime": finishTime,
			"taskID":     task.ID,
		}).
		StructScan(updatedTask)
	if err != nil && err != sql.ErrNoRows {
		return &model.Task{}, err
	}

	return updatedTask.toTask(), nil
}

func (d *postgresRepository) SubmitTask(ctx context.Context, task *model.Task) (*model.Task, error) {
	var (
		postgresTask = newFromTask(task)
		sqlTemplate  = `INSERT INTO {{.tableName}} 
				(id, type, args, queue, priority, status, max_receives, created_at) 
			VALUES 
				(:id, :type, :args, :queue, :priority, :status, :maxReceives, :createdAt)
			RETURNING *;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	err := stmt.
		QueryRowContext(ctx, map[string]any{
			"id":          postgresTask.ID,
			"type":        postgresTask.Type,
			"args":        postgresTask.Args,
			"queue":       postgresTask.Queue,
			"priority":    postgresTask.Priority,
			"status":      postgresTask.Status,
			"maxReceives": postgresTask.MaxReceives,
			"createdAt":   postgresTask.CreatedAt,
		}).
		StructScan(postgresTask)
	if err != nil {
		return &model.Task{}, err
	}

	return postgresTask.toTask(), nil
}

func (d *postgresRepository) DeleteTask(ctx context.Context, task *model.Task) (err error) {
	var (
		sqlTemplate = `DELETE FROM {{.tableName}}
			WHERE
				"id" = :taskID;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	_, err = stmt.ExecContext(ctx, map[string]any{
		"taskID": task.ID,
	})

	return err
}

func (d *postgresRepository) RequeueTask(ctx context.Context, task *model.Task) (*model.Task, error) {
	var (
		updatedTask = new(postgresTask)
		sqlTemplate = `UPDATE {{.tableName}} SET
				"status" = :status
			WHERE
				"id" = :taskID
			RETURNING *;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	err := stmt.
		QueryRowContext(ctx, map[string]any{
			"status": model.StatusNew,
			"taskID": task.ID,
		}).
		StructScan(updatedTask)
	if err != nil && err != sql.ErrNoRows {
		return &model.Task{}, err
	}

	return updatedTask.toTask(), err
}

func (d *postgresRepository) prepareWithTableName(sqlTemplate string) *sqlx.NamedStmt {
	query := repository.InterpolateSQL(sqlTemplate, map[string]any{
		"tableName": d.tableName(),
	})

	namedStmt, err := d.db.PrepareNamed(query)
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
	const sqlTemplate = `CREATE TABLE IF NOT EXISTS {{.tableName}} (
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
	stringSlice := make([]string, 0, len(slice))

	for _, s := range slice {
		stringSlice = append(stringSlice, fmt.Sprint(s))
	}

	return fmt.Sprintf("'%s'", strings.Join(stringSlice, "','"))
}
