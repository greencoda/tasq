package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/greencoda/tasq/internal/model"
	"github.com/greencoda/tasq/internal/repository"
	"github.com/jmoiron/sqlx"
)

type mysqlRepository struct {
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

	return &mysqlRepository{
		db:     dbx,
		prefix: prefix,
	}, nil
}

func newRepositoryFromDB(db *sql.DB, driver, prefix string) (repository.IRepository, error) {
	dbx := sqlx.NewDb(db, driver)

	return &mysqlRepository{
		db:     dbx,
		prefix: prefix,
	}, nil
}

func (d *mysqlRepository) DB() *sql.DB {
	return d.db.DB
}

func (d *mysqlRepository) tableName() string {
	var tableNameSegments = []string{"tasks"}
	if len(d.prefix) > 0 {
		tableNameSegments = append([]string{d.prefix}, tableNameSegments...)
	}

	return strings.Join(tableNameSegments, "_")
}

func (d *mysqlRepository) Migrate(ctx context.Context) (err error) {
	err = d.migrateTable(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (d *mysqlRepository) PingTasks(ctx context.Context, taskIDs []uuid.UUID, visibilityTimeout time.Duration) (pingedTasks []*model.Task, err error) {
	if len(taskIDs) == 0 {
		return []*model.Task{}, nil
	}

	var (
		pingedTime  = time.Now()
		sqlTemplate = `UPDATE
				{{.tableName}}
			SET
				visible_at = :visible_at
			WHERE
				id IN (:pinged_ids)
			RETURNING id;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	err = stmt.SelectContext(ctx, &pingedTasks, map[string]any{
		"visible_at": pingedTime.Add(visibilityTimeout),
		"pinged_ids": taskIDs,
	})
	if err != nil {
		return []*model.Task{}, err
	}

	return pingedTasks, nil
}

func (d *mysqlRepository) PollTasks(ctx context.Context, types, queues []string, visibilityTimeout time.Duration, ordering []string, pollLimit int) (polledTasks []*model.Task, err error) {
	if pollLimit == 0 {
		return []*model.Task{}, nil
	}

	var (
		pollTime    = time.Now()
		sqlTemplate = `UPDATE {{.tableName}} SET
				status = :status,
				receive_count = "receive_count" + 1,
				visible_at = :visible_at
			WHERE
				id IN (
					SELECT
						id FROM {{.tableName}}
					WHERE
						type IN (:poll_types) AND
						queue IN (:poll_queues) AND 
						status IN (:poll_statuses) AND 
						visible_at <= :poll_time
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
		"poll_types":    types,
		"poll_queues":   queues,
		"poll_statuses": model.OpenTaskStatuses,
		"poll_time":     pollTime,
		"poll_ordering": ordering,
		"poll_limit":    pollLimit,
	})
	if err != nil && err != sql.ErrNoRows {
		return []*model.Task{}, err
	}

	return polledTasks, nil
}

func (d *mysqlRepository) CleanTasks(ctx context.Context, cleanAge time.Duration) (rowsAffected int64, err error) {
	var (
		cleanTime   = time.Now()
		sqlTemplate = `DELETE FROM {{.tableName}}
			WHERE
				status IN (:statuses) AND
				created_at <= :cleanAt;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	result, err := stmt.ExecContext(ctx, map[string]any{
		"statuses": sliceToMySQLValueList(model.FinishedTaskStatuses),
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

func (d *mysqlRepository) RegisterStart(ctx context.Context, task *model.Task) (updatedTask *model.Task, err error) {
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

func (d *mysqlRepository) RegisterError(ctx context.Context, task *model.Task, taskError error) (updatedTask *model.Task, err error) {
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

func (d *mysqlRepository) RegisterSuccess(ctx context.Context, task *model.Task) (*model.Task, error) {
	return d.registerFinish(ctx, task, model.StatusSuccessful)
}

func (d *mysqlRepository) RegisterFailure(ctx context.Context, task *model.Task) (*model.Task, error) {
	return d.registerFinish(ctx, task, model.StatusFailed)
}

func (d *mysqlRepository) registerFinish(ctx context.Context, task *model.Task, finishStatus model.TaskStatus) (updatedTask *model.Task, err error) {
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

func (d *mysqlRepository) SubmitTask(ctx context.Context, task *model.Task) (submittedTask *model.Task, err error) {
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

func (d *mysqlRepository) DeleteTask(ctx context.Context, task *model.Task) (err error) {
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

func (d *mysqlRepository) RequeueTask(ctx context.Context, task *model.Task) (updatedTask *model.Task, err error) {
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

func (d *mysqlRepository) prepareWithTableName(sqlTemplate string) (namedStmt *sqlx.NamedStmt) {
	var (
		query = repository.InterpolateSQL(sqlTemplate, map[string]any{
			"tableName": d.tableName(),
		})
		err error
	)

	log.Printf("%s", query)

	namedStmt, err = d.db.PrepareNamed(query)
	if err != nil {
		panic(err)
	}

	return namedStmt
}

func (d *mysqlRepository) migrateTable(ctx context.Context) error {
	var sqlTemplate = `CREATE TABLE IF NOT EXISTS {{.tableName}} (
            id binary(16) NOT NULL,
            type text NOT NULL,
            args longblob NOT NULL,
            queue text NOT NULL,
            priority smallint NOT NULL,
            status enum({{.enumValues}}) NOT NULL,
            receive_count int NOT NULL DEFAULT '0',
            max_receives int NOT NULL DEFAULT '0',
            last_error text,
            created_at datetime(6) NOT NULL DEFAULT '0001-01-01 00:00:00.000000',
            started_at datetime(6),
            finished_at datetime(6),
            visible_at datetime(6) NOT NULL DEFAULT '0001-01-01 00:00:00.000000',
            PRIMARY KEY (id)
		);`

	query := repository.InterpolateSQL(sqlTemplate, map[string]any{
		"tableName":  d.tableName(),
		"enumValues": sliceToMySQLValueList(model.AllTaskStatuses),
	})

	_, err := d.db.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	return nil
}

func sliceToMySQLValueList[T any](slice []T) string {
	var stringSlice = make([]string, 0, len(slice))

	for _, s := range slice {
		stringSlice = append(stringSlice, fmt.Sprint(s))
	}

	return fmt.Sprintf("'%s'", strings.Join(stringSlice, "','"))
}
