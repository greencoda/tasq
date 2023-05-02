// Package postgres provides the implementation of a tasq repository in PostgreSQL
package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"text/template"
	"time"

	"github.com/google/uuid"
	"github.com/greencoda/tasq"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

const driverName = "postgres"

var (
	errUnexpectedDataSourceType   = errors.New("unexpected dataSource type")
	errFailedToExecuteUpdate      = errors.New("failed to execute update query")
	errFailedToExecuteDelete      = errors.New("failed to execute delete query")
	errFailedToExecuteInsert      = errors.New("failed to execute insert query")
	errFailedToExecuteCreateTable = errors.New("failed to execute create table query")
	errFailedToExecuteCreateType  = errors.New("failed to execute create type query")
)

// Repository implements the menthods necessary for tasq to work in PostgreSQL.
type Repository struct {
	db             *sqlx.DB
	statusTypeName string
	tableName      string
}

// NewRepository creates a new PostgreSQL Repository instance.
func NewRepository(dataSource any, prefix string) (*Repository, error) {
	switch d := dataSource.(type) {
	case string:
		return newRepositoryFromDSN(d, prefix)
	case *sql.DB:
		return newRepositoryFromDB(d, prefix)
	}

	return nil, fmt.Errorf("%w: %T", errUnexpectedDataSourceType, dataSource)
}

func newRepositoryFromDSN(dsn string, prefix string) (*Repository, error) {
	dbx, _ := sqlx.Open(driverName, dsn)

	return &Repository{
		db:             dbx,
		statusTypeName: statusTypeName(prefix),
		tableName:      tableName(prefix),
	}, nil
}

func newRepositoryFromDB(db *sql.DB, prefix string) (*Repository, error) {
	dbx := sqlx.NewDb(db, driverName)

	return &Repository{
		db:             dbx,
		statusTypeName: statusTypeName(prefix),
		tableName:      tableName(prefix),
	}, nil
}

// Migrate prepares the database with the task status type
// and by adding the tasks table.
func (d *Repository) Migrate(ctx context.Context) error {
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

// PingTasks pings a list of tasks by their ID
// and extends their invisibility timestamp with the supplied timeout parameter.
func (d *Repository) PingTasks(ctx context.Context, taskIDs []uuid.UUID, visibilityTimeout time.Duration) ([]*tasq.Task, error) {
	if len(taskIDs) == 0 {
		return []*tasq.Task{}, nil
	}

	var (
		pingedTasks []*postgresTask
		pingTime    = time.Now()
		sqlTemplate = `UPDATE
				{{.tableName}}
			SET
				"visible_at" = :visibleAt
			WHERE
				"id" = ANY(:pingedTaskIDs)
			RETURNING id;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	err := stmt.SelectContext(ctx, &pingedTasks, map[string]any{
		"visibleAt":     pingTime.Add(visibilityTimeout),
		"pingedTaskIDs": pq.Array(taskIDs),
	})
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return []*tasq.Task{}, fmt.Errorf("failed to update tasks: %w", err)
	}

	return postgresTasksToTasks(pingedTasks), nil
}

// PollTasks polls for available tasks matching supplied the parameters
// and sets their invisibility the supplied timeout parameter to the future.
func (d *Repository) PollTasks(ctx context.Context, types, queues []string, visibilityTimeout time.Duration, ordering tasq.Ordering, pollLimit int) ([]*tasq.Task, error) {
	if pollLimit == 0 {
		return []*tasq.Task{}, nil
	}

	var (
		polledTasks []*postgresTask
		pollTime    = time.Now()
		sqlTemplate = `UPDATE {{.tableName}} SET
				"status" = :status,
				"receive_count" = "receive_count" + 1,
				"visible_at" = :visibleAt
			WHERE
				"id" IN (
					SELECT
						"id" FROM {{.tableName}}
					WHERE
						"type" = ANY(:pollTypes) AND
						"queue" = ANY(:pollQueues) AND 
						"status" = ANY(:pollStatuses) AND 
						"visible_at" <= :pollTime
					ORDER BY
						:pollOrdering
					LIMIT :pollLimit
				FOR UPDATE )
			RETURNING *;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	err := stmt.SelectContext(ctx, &polledTasks, map[string]any{
		"status":       tasq.StatusEnqueued,
		"visibleAt":    pollTime.Add(visibilityTimeout),
		"pollTypes":    pq.Array(types),
		"pollQueues":   pq.Array(queues),
		"pollStatuses": pq.Array(tasq.GetTaskStatuses(tasq.OpenTasks)),
		"pollTime":     pollTime,
		"pollOrdering": pq.Array(getOrderingDirectives(ordering)),
		"pollLimit":    pollLimit,
	})
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return []*tasq.Task{}, fmt.Errorf("failed to update tasks: %w", err)
	}

	return postgresTasksToTasks(polledTasks), nil
}

// CleanTasks removes finished tasks from the queue
// if their creation date is past the supplied duration.
func (d *Repository) CleanTasks(ctx context.Context, cleanAge time.Duration) (int64, error) {
	var (
		cleanTime   = time.Now()
		sqlTemplate = `DELETE FROM {{.tableName}}
			WHERE
				"status" = ANY(:statuses) AND
				"created_at" <= :cleanAt;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	result, err := stmt.ExecContext(ctx, map[string]any{
		"statuses": pq.Array(tasq.GetTaskStatuses(tasq.FinishedTasks)),
		"cleanAt":  cleanTime.Add(-cleanAge),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to delete tasks: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get number of affected rows: %w", err)
	}

	return rowsAffected, nil
}

// RegisterStart marks a task as started with the 'in progress' status
// and records the time of start.
func (d *Repository) RegisterStart(ctx context.Context, task *tasq.Task) (*tasq.Task, error) {
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
			"status":    tasq.StatusInProgress,
			"startTime": startTime,
			"taskID":    task.ID,
		}).
		StructScan(updatedTask)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToExecuteUpdate, err)
	}

	return updatedTask.toTask(), nil
}

// RegisterError records an error message on the task as last error.
func (d *Repository) RegisterError(ctx context.Context, task *tasq.Task, errTask error) (*tasq.Task, error) {
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
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToExecuteUpdate, err)
	}

	return updatedTask.toTask(), nil
}

// RegisterFinish marks a task as finished with the supplied status
// and records the time of finish.
func (d *Repository) RegisterFinish(ctx context.Context, task *tasq.Task, finishStatus tasq.TaskStatus) (*tasq.Task, error) {
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
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToExecuteUpdate, err)
	}

	return updatedTask.toTask(), nil
}

// SubmitTask adds the supplied task to the queue.
func (d *Repository) SubmitTask(ctx context.Context, task *tasq.Task) (*tasq.Task, error) {
	var (
		postgresTask = newFromTask(task)
		sqlTemplate  = `INSERT INTO {{.tableName}} 
				(id, type, args, queue, priority, status, max_receives, created_at, visible_at) 
			VALUES 
				(:id, :type, :args, :queue, :priority, :status, :maxReceives, :createdAt, :visibleAt)
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
			"visibleAt":   postgresTask.VisibleAt,
		}).
		StructScan(postgresTask)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToExecuteInsert, err)
	}

	return postgresTask.toTask(), nil
}

// DeleteTask removes the supplied task from the queue.
func (d *Repository) DeleteTask(ctx context.Context, task *tasq.Task) error {
	var (
		sqlTemplate = `DELETE FROM {{.tableName}}
			WHERE
				"id" = :taskID;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	_, err := stmt.ExecContext(ctx, map[string]any{
		"taskID": task.ID,
	})
	if err != nil {
		return fmt.Errorf("%s: %w", errFailedToExecuteDelete, err)
	}

	return nil
}

// RequeueTask marks a task as new, so it can be picked up again.
func (d *Repository) RequeueTask(ctx context.Context, task *tasq.Task) (*tasq.Task, error) {
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
			"status": tasq.StatusNew,
			"taskID": task.ID,
		}).
		StructScan(updatedTask)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToExecuteUpdate, err)
	}

	return updatedTask.toTask(), err
}

// Count returns the number of tasks in the queue based on the supplied filters.
func (d *Repository) Count(ctx context.Context, taskStatuses []tasq.TaskStatus, taskTypes, queues []string) (int, error) {
	var (
		count                   int
		sqlTemplate, parameters = d.buildCountSQLTemplate(taskStatuses, taskTypes, queues)
		stmt                    = d.prepareWithTableName(sqlTemplate)
	)

	err := stmt.GetContext(ctx, &count, parameters)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("failed to count tasks: %w", err)
	}

	return count, nil
}

// Scan returns a list of tasks in the queue based on the supplied filters.
func (d *Repository) Scan(ctx context.Context, taskStatuses []tasq.TaskStatus, taskTypes, queues []string, ordering tasq.Ordering, scanLimit int) ([]*tasq.Task, error) {
	var (
		scannedTasks            []*postgresTask
		sqlTemplate, parameters = d.buildScanSQLTemplate(taskStatuses, taskTypes, queues, ordering, scanLimit)
		stmt                    = d.prepareWithTableName(sqlTemplate)
	)

	err := stmt.SelectContext(ctx, &scannedTasks, parameters)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return []*tasq.Task{}, fmt.Errorf("failed to scan tasks: %w", err)
	}

	return postgresTasksToTasks(scannedTasks), nil
}

func (d *Repository) buildCountSQLTemplate(taskStatuses []tasq.TaskStatus, taskTypes, queues []string) (string, map[string]any) {
	var (
		conditions, parameters = d.buildFilterConditions(taskStatuses, taskTypes, queues)
		sqlTemplate            = `SELECT COUNT(*) FROM {{.tableName}}`
	)

	if len(conditions) > 0 {
		sqlTemplate += ` WHERE ` + strings.Join(conditions, " AND ")
	}

	return sqlTemplate, parameters
}

func (d *Repository) buildScanSQLTemplate(taskStatuses []tasq.TaskStatus, taskTypes, queues []string, ordering tasq.Ordering, scanLimit int) (string, map[string]any) {
	var (
		conditions, parameters = d.buildFilterConditions(taskStatuses, taskTypes, queues)
		sqlTemplate            = `SELECT * FROM {{.tableName}}`
	)

	if len(conditions) > 0 {
		sqlTemplate += ` WHERE ` + strings.Join(conditions, " AND ")
	}

	sqlTemplate += ` ORDER BY :scanOrdering LIMIT :limit;`

	parameters["scanOrdering"] = pq.Array(getOrderingDirectives(ordering))
	parameters["limit"] = scanLimit

	return sqlTemplate, parameters
}

func (d *Repository) buildFilterConditions(taskStatuses []tasq.TaskStatus, taskTypes, queues []string) ([]string, map[string]any) {
	var (
		conditions []string
		parameters = make(map[string]any)
	)

	if len(taskStatuses) > 0 {
		conditions = append(conditions, `"status" = ANY(:statuses)`)
		parameters["statuses"] = pq.Array(taskStatuses)
	}

	if len(taskTypes) > 0 {
		conditions = append(conditions, `"type" = ANY(:types)`)
		parameters["types"] = pq.Array(taskTypes)
	}

	if len(queues) > 0 {
		conditions = append(conditions, `"queue" = ANY(:queues)`)
		parameters["queues"] = pq.Array(queues)
	}

	return conditions, parameters
}

func (d *Repository) migrateStatus(ctx context.Context) error {
	var (
		sqlTemplate = `DO $$
			BEGIN
				IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '{{.statusTypeName}}') THEN
					CREATE TYPE {{.statusTypeName}} AS ENUM ({{.enumValues}});
				END IF;
			END$$;`
		query = interpolateSQL(sqlTemplate, map[string]any{
			"statusTypeName": d.statusTypeName,
			"enumValues":     sliceToPostgreSQLValueList(tasq.GetTaskStatuses(tasq.AllTasks)),
		})
	)

	_, err := d.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("%s: %w", errFailedToExecuteCreateType, err)
	}

	return nil
}

func (d *Repository) migrateTable(ctx context.Context) error {
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

	query := interpolateSQL(sqlTemplate, map[string]any{
		"tableName":      d.tableName,
		"statusTypeName": d.statusTypeName,
	})

	_, err := d.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("%s: %w", errFailedToExecuteCreateTable, err)
	}

	return nil
}

func (d *Repository) prepareWithTableName(sqlTemplate string) *sqlx.NamedStmt {
	query := interpolateSQL(sqlTemplate, map[string]any{
		"tableName": d.tableName,
	})

	namedStmt, err := d.db.PrepareNamed(query)
	if err != nil {
		panic(err)
	}

	return namedStmt
}

func getOrderingDirectives(ordering tasq.Ordering) []string {
	var (
		OrderingCreatedAtFirst = []string{"created_at ASC", "priority DESC"}
		OrderingPriorityFirst  = []string{"priority DESC", "created_at ASC"}
	)

	if orderingDirectives, ok := map[tasq.Ordering][]string{
		tasq.OrderingCreatedAtFirst: OrderingCreatedAtFirst,
		tasq.OrderingPriorityFirst:  OrderingPriorityFirst,
	}[ordering]; ok {
		return orderingDirectives
	}

	return OrderingCreatedAtFirst
}

func sliceToPostgreSQLValueList[T any](slice []T) string {
	stringSlice := make([]string, 0, len(slice))

	for _, s := range slice {
		stringSlice = append(stringSlice, fmt.Sprint(s))
	}

	return fmt.Sprintf("'%s'", strings.Join(stringSlice, "','"))
}

func statusTypeName(prefix string) string {
	const statusTypeName = "task_status"

	if len(prefix) > 0 {
		return prefix + "_" + statusTypeName
	}

	return statusTypeName
}

func tableName(prefix string) string {
	const tableName = "tasks"

	if len(prefix) > 0 {
		return prefix + "_" + tableName
	}

	return tableName
}

func interpolateSQL(sql string, params map[string]any) string {
	template, err := template.New("sql").Parse(sql)
	if err != nil {
		panic(err)
	}

	var outputBuffer bytes.Buffer

	err = template.Execute(&outputBuffer, params)
	if err != nil {
		panic(err)
	}

	return outputBuffer.String()
}
