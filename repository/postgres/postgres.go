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

// pgUniqueViolation is the PostgreSQL error code for unique_violation.
const pgUniqueViolation = "23505"

// migrationLockKey is the advisory lock key serializing concurrent migrations.
const migrationLockKey = "tasq_migrate"

const (
	defaultTableName            = "tasks"
	defaultStatusTypeNamePrefix = "tasq"
)

var (
	errUnexpectedDataSourceType   = errors.New("unexpected dataSource type")
	errFailedToExecuteUpdate      = errors.New("failed to execute update query")
	errFailedToExecuteDelete      = errors.New("failed to execute delete query")
	errFailedToExecuteInsert      = errors.New("failed to execute insert query")
	errFailedToExecuteCreateTable = errors.New("failed to execute create table query")
	errFailedToExecuteCreateType  = errors.New("failed to execute create type query")
	errFailedToExecuteAlterTable  = errors.New("failed to execute alter table query")
	errFailedToExecuteCreateIndex = errors.New("failed to execute create index query")
	errFailedToExecuteSelect      = errors.New("failed to execute select query")
	errFailedToAcquireLock        = errors.New("failed to acquire migration lock")
	errDedupSubmitExhausted       = errors.New("failed to submit task with dedup key: conflicting task kept disappearing")
)

// Repository implements the menthods necessary for tasq to work in PostgreSQL.
type Repository struct {
	db             *sqlx.DB
	schemaName     *string
	tableName      string
	statusTypeName string
}

// NewRepository creates a new PostgreSQL Repository instance.
func NewRepository(dataSource any, options ...Option) (*Repository, error) {
	switch d := dataSource.(type) {
	case string:
		return newRepositoryFromDSN(d, options)
	case *sql.DB:
		return newRepositoryFromDB(d, options)
	}

	return nil, fmt.Errorf("%w: %T", errUnexpectedDataSourceType, dataSource)
}

func newRepositoryFromDSN(dsn string, options []Option) (*Repository, error) {
	dbx, _ := sqlx.Open(driverName, dsn)

	repository := &Repository{
		db:             dbx,
		tableName:      defaultTableName,
		statusTypeName: statusTypeName(defaultStatusTypeNamePrefix),
	}

	for _, option := range options {
		repository = option(repository)(repository)
	}

	return repository, nil
}

func newRepositoryFromDB(db *sql.DB, options []Option) (*Repository, error) {
	dbx := sqlx.NewDb(db, driverName)

	repository := &Repository{
		db:             dbx,
		statusTypeName: statusTypeName(defaultStatusTypeNamePrefix),
		tableName:      defaultTableName,
	}

	for _, option := range options {
		repository = option(repository)(repository)
	}

	return repository, nil
}

// Migrate prepares the database with the task status type
// and by adding the tasks table.
func (d *Repository) Migrate(ctx context.Context) (err error) {
	tx, err := d.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			return
		}

		if rbErr := tx.Rollback(); rbErr != nil {
			err = fmt.Errorf("tx error: %v, rollback error: %v", err, rbErr)
		}
	}()

	if err = d.lockMigration(ctx, tx); err != nil {
		return err
	}
	if err = d.migrateStatus(ctx, tx); err != nil {
		return err
	}
	if err = d.migrateSchema(ctx, tx); err != nil {
		return err
	}
	if err = d.migrateTable(ctx, tx); err != nil {
		return err
	}
	if err = d.migrateDedupKey(ctx, tx); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
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

	defer d.closeStmt(stmt)

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

	defer d.closeStmt(stmt)

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

	defer d.closeStmt(stmt)

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

	defer d.closeStmt(stmt)

	err := stmt.
		QueryRowContext(ctx, map[string]any{
			"status":    tasq.StatusInProgress,
			"startTime": startTime,
			"taskID":    task.ID,
		}).
		StructScan(updatedTask)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errFailedToExecuteUpdate, err)
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

	defer d.closeStmt(stmt)

	err := stmt.
		QueryRowContext(ctx, map[string]any{
			"errorMessage": errTask.Error(),
			"taskID":       task.ID,
		}).
		StructScan(updatedTask)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errFailedToExecuteUpdate, err)
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

	defer d.closeStmt(stmt)

	err := stmt.
		QueryRowContext(ctx, map[string]any{
			"status":     finishStatus,
			"finishTime": finishTime,
			"taskID":     task.ID,
		}).
		StructScan(updatedTask)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errFailedToExecuteUpdate, err)
	}

	return updatedTask.toTask(), nil
}

// SubmitTask adds the supplied task to the queue.
//
// If the task has a dedup key and an open task (NEW, ENQUEUED or IN_PROGRESS) with the
// same type and key already exists, nothing is inserted and the existing task is returned
// together with an error wrapping tasq.ErrDuplicateTask. Tasks without a dedup key are
// inserted unconditionally.
func (d *Repository) SubmitTask(ctx context.Context, task *tasq.Task) (*tasq.Task, error) {
	if task.DedupKey == nil {
		return d.insertTask(ctx, task)
	}

	// The conflicting task may finish between a skipped insert and the select
	// that looks for it, in which case the insert is attempted once more.
	const maxAttempts = 2

	for range maxAttempts {
		insertedTask, err := d.insertUniqueTask(ctx, task)
		if err != nil {
			return nil, err
		}

		if insertedTask != nil {
			return insertedTask, nil
		}

		existingTask, err := d.selectOpenTaskByDedupKey(ctx, task.Type, *task.DedupKey)
		if err != nil {
			return nil, err
		}

		if existingTask != nil {
			return existingTask, fmt.Errorf("%w: type %q, dedup key %q", tasq.ErrDuplicateTask, task.Type, *task.DedupKey)
		}
	}

	return nil, errDedupSubmitExhausted
}

func (d *Repository) insertTask(ctx context.Context, task *tasq.Task) (*tasq.Task, error) {
	var (
		postgresTask = newFromTask(task)
		sqlTemplate  = `INSERT INTO {{.tableName}} 
				(id, type, args, queue, priority, status, max_receives, created_at, visible_at) 
			VALUES 
				(:id, :type, :args, :queue, :priority, :status, :maxReceives, :createdAt, :visibleAt)
			RETURNING *;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	defer d.closeStmt(stmt)

	err := stmt.
		QueryRowContext(ctx, insertParameters(postgresTask)).
		StructScan(postgresTask)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errFailedToExecuteInsert, err)
	}

	return postgresTask.toTask(), nil
}

// insertUniqueTask inserts a task carrying a dedup key. It returns a nil task
// and a nil error if the insert was skipped because of a conflicting open task.
func (d *Repository) insertUniqueTask(ctx context.Context, task *tasq.Task) (*tasq.Task, error) {
	var (
		postgresTask = newFromTask(task)
		sqlTemplate  = `INSERT INTO {{.tableName}} 
				(id, type, args, queue, priority, status, max_receives, created_at, visible_at, dedup_key) 
			VALUES 
				(:id, :type, :args, :queue, :priority, :status, :maxReceives, :createdAt, :visibleAt, :dedupKey)
			ON CONFLICT ("type", "dedup_key") WHERE {{.predicate}} DO NOTHING
			RETURNING *;`
		stmt = d.prepareWithTableName(interpolateSQL(sqlTemplate, map[string]any{
			"tableName": "{{.tableName}}",
			"predicate": dedupIndexPredicate(),
		}))
	)

	defer d.closeStmt(stmt)

	parameters := insertParameters(postgresTask)
	parameters["dedupKey"] = postgresTask.DedupKey

	err := stmt.
		QueryRowContext(ctx, parameters).
		StructScan(postgresTask)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil //nolint:nilnil // nothing was inserted, signalled to the caller by the nil task
	}

	if err != nil {
		return nil, fmt.Errorf("%w: %w", errFailedToExecuteInsert, err)
	}

	return postgresTask.toTask(), nil
}

// selectOpenTaskByDedupKey returns the open task holding the supplied type and dedup key,
// or nil without an error if there is none.
func (d *Repository) selectOpenTaskByDedupKey(ctx context.Context, taskType, dedupKey string) (*tasq.Task, error) {
	var (
		existingTask = new(postgresTask)
		sqlTemplate  = `SELECT * FROM {{.tableName}}
			WHERE
				"type" = :type AND
				"dedup_key" = :dedupKey AND
				"status" = ANY(:statuses)
			LIMIT 1;`
		stmt = d.prepareWithTableName(sqlTemplate)
	)

	defer d.closeStmt(stmt)

	err := stmt.
		QueryRowContext(ctx, map[string]any{
			"type":     taskType,
			"dedupKey": dedupKey,
			"statuses": pq.Array(tasq.GetTaskStatuses(tasq.OpenTasks)),
		}).
		StructScan(existingTask)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil //nolint:nilnil // no open task found, signalled to the caller by the nil task
	}

	if err != nil {
		return nil, fmt.Errorf("%w: %w", errFailedToExecuteSelect, err)
	}

	return existingTask.toTask(), nil
}

func insertParameters(task *postgresTask) map[string]any {
	return map[string]any{
		"id":          task.ID,
		"type":        task.Type,
		"args":        task.Args,
		"queue":       task.Queue,
		"priority":    task.Priority,
		"status":      task.Status,
		"maxReceives": task.MaxReceives,
		"createdAt":   task.CreatedAt,
		"visibleAt":   task.VisibleAt,
	}
}

// DeleteTask removes the supplied task from the queue.
func (d *Repository) DeleteTask(ctx context.Context, task *tasq.Task, safeDelete bool) error {
	var (
		conditions = []string{
			`"id" = :taskID`,
		}
		parameters = map[string]any{
			"taskID": task.ID,
		}
	)

	if safeDelete {
		d.applySafeDeleteConditions(&conditions, &parameters)
	}

	sqlTemplate := `DELETE FROM {{.tableName}} WHERE ` + strings.Join(conditions, ` AND `) + `;`

	_, err := d.prepareWithTableName(sqlTemplate).ExecContext(ctx, parameters)
	if err != nil {
		return fmt.Errorf("%w: %w", errFailedToExecuteDelete, err)
	}

	return nil
}

// RequeueTask marks a task as new, so it can be picked up again.
//
// Requeueing a finished task that carries a dedup key fails with an error wrapping
// tasq.ErrDuplicateTask if another open task has since claimed the same type and key.
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
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == pgUniqueViolation {
			return nil, fmt.Errorf("%w: %w: %w", errFailedToExecuteUpdate, tasq.ErrDuplicateTask, err)
		}

		return nil, fmt.Errorf("%w: %w", errFailedToExecuteUpdate, err)
	}

	return updatedTask.toTask(), err
}

// CountTasks returns the number of tasks in the queue based on the supplied filters.
func (d *Repository) CountTasks(ctx context.Context, taskStatuses []tasq.TaskStatus, taskTypes, queues []string) (int64, error) {
	var (
		count                   int64
		sqlTemplate, parameters = d.buildCountSQLTemplate(taskStatuses, taskTypes, queues)
		stmt                    = d.prepareWithTableName(sqlTemplate)
	)

	err := stmt.GetContext(ctx, &count, parameters)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("failed to count tasks: %w", err)
	}

	return count, nil
}

// ScanTasks returns a list of tasks in the queue based on the supplied filters.
func (d *Repository) ScanTasks(ctx context.Context, taskStatuses []tasq.TaskStatus, taskTypes, queues []string, ordering tasq.Ordering, scanLimit int) ([]*tasq.Task, error) {
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

// PurgeTasks removes all tasks from the queue based on the supplied filters.
func (d *Repository) PurgeTasks(ctx context.Context, taskStatuses []tasq.TaskStatus, taskTypes, queues []string, safeDelete bool) (int64, error) {
	var (
		sqlTemplate, parameters = d.buildPurgeSQLTemplate(taskStatuses, taskTypes, queues, safeDelete)
		stmt                    = d.prepareWithTableName(sqlTemplate)
	)

	result, err := stmt.ExecContext(ctx, parameters)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("failed to purge tasks: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get number of affected rows: %w", err)
	}

	return rowsAffected, nil
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

func (d *Repository) buildPurgeSQLTemplate(taskStatuses []tasq.TaskStatus, taskTypes, queues []string, safeDelete bool) (string, map[string]any) {
	var (
		conditions, parameters = d.buildFilterConditions(taskStatuses, taskTypes, queues)
		sqlTemplate            = `DELETE FROM {{.tableName}}`
	)

	if safeDelete {
		d.applySafeDeleteConditions(&conditions, &parameters)
	}

	if len(conditions) > 0 {
		sqlTemplate += ` WHERE ` + strings.Join(conditions, " AND ")
	}

	return sqlTemplate + `;`, parameters
}

func (d *Repository) applySafeDeleteConditions(conditions *[]string, parameters *map[string]any) {
	*conditions = append(*conditions, `(
			(
				"visible_at" <= :visibleAt
			) OR (
				"status" = ANY(:statuses) AND 
				"visible_at" > :visibleAt
			)
		)`)
	(*parameters)["statuses"] = pq.Array([]tasq.TaskStatus{tasq.StatusNew})
	(*parameters)["visibleAt"] = time.Now()
}

func (d *Repository) buildFilterConditions(taskStatuses []tasq.TaskStatus, taskTypes, queues []string) ([]string, map[string]any) {
	var (
		conditions []string
		parameters = make(map[string]any)
	)

	if len(taskStatuses) > 0 {
		conditions = append(conditions, `"status" = ANY(:filterStatuses)`)
		parameters["filterStatuses"] = pq.Array(taskStatuses)
	}

	if len(taskTypes) > 0 {
		conditions = append(conditions, `"type" = ANY(:filterTypes)`)
		parameters["filterTypes"] = pq.Array(taskTypes)
	}

	if len(queues) > 0 {
		conditions = append(conditions, `"queue" = ANY(:filterQueues)`)
		parameters["filterQueues"] = pq.Array(queues)
	}

	return conditions, parameters
}

func (d *Repository) tableNameWithSchema() string {
	if d.schemaName != nil {
		return fmt.Sprintf("%s.%s", *d.schemaName, d.tableName)
	}

	return d.tableName
}

func (d *Repository) migrateStatus(ctx context.Context, tx *sqlx.Tx) error {
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

	_, err := tx.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("%w: %w", errFailedToExecuteCreateType, err)
	}

	return nil
}

func (d *Repository) migrateSchema(ctx context.Context, tx *sqlx.Tx) error {
	if d.schemaName == nil {
		return nil
	}

	const sqlSchemaTemplate = `CREATE SCHEMA IF NOT EXISTS {{.schemaName}};`

	query := interpolateSQL(sqlSchemaTemplate, map[string]any{
		"schemaName": *d.schemaName,
	})

	_, err := tx.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("%w: %w", errFailedToExecuteCreateTable, err)
	}

	return nil
}

func (d *Repository) migrateTable(ctx context.Context, tx *sqlx.Tx) error {
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
			"visible_at" TIMESTAMPTZ NOT NULL DEFAULT '0001-01-01 00:00:00.000000',
			"dedup_key" TEXT
		);`

	query := interpolateSQL(sqlTemplate, map[string]any{
		"tableName":      d.tableNameWithSchema(),
		"statusTypeName": d.statusTypeName,
	})

	_, err := tx.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("%w: %w", errFailedToExecuteCreateTable, err)
	}

	return nil
}

// lockMigration takes a transaction-scoped advisory lock so that several instances
// migrating at the same time are serialized instead of racing on the DDL statements.
func (d *Repository) lockMigration(ctx context.Context, tx *sqlx.Tx) error {
	_, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1));`, migrationLockKey)
	if err != nil {
		return fmt.Errorf("%w: %w", errFailedToAcquireLock, err)
	}

	return nil
}

// dedupIndexPredicate is the WHERE clause shared by the partial unique index
// and the ON CONFLICT arbiter inference, which must match it exactly.
func dedupIndexPredicate() string {
	return interpolateSQL(`"dedup_key" IS NOT NULL AND "status" IN ({{.openStatuses}})`, map[string]any{
		"openStatuses": sliceToPostgreSQLValueList(tasq.GetTaskStatuses(tasq.OpenTasks)),
	})
}

// migrateDedupKey adds the dedup_key column to tables created by earlier versions
// and creates the partial unique index enforcing uniqueness among open tasks.
func (d *Repository) migrateDedupKey(ctx context.Context, tx *sqlx.Tx) error {
	const (
		alterTemplate = `ALTER TABLE {{.tableName}} ADD COLUMN IF NOT EXISTS "dedup_key" TEXT;`
		indexTemplate = `CREATE UNIQUE INDEX IF NOT EXISTS {{.indexName}} ON {{.tableName}} ("type", "dedup_key") WHERE {{.predicate}};`
	)

	params := map[string]any{
		"tableName": d.tableNameWithSchema(),
		"indexName": d.tableName + "_open_dedup_key_idx",
		"predicate": dedupIndexPredicate(),
	}

	if _, err := tx.ExecContext(ctx, interpolateSQL(alterTemplate, params)); err != nil {
		return fmt.Errorf("%w: %w", errFailedToExecuteAlterTable, err)
	}

	if _, err := tx.ExecContext(ctx, interpolateSQL(indexTemplate, params)); err != nil {
		return fmt.Errorf("%w: %w", errFailedToExecuteCreateIndex, err)
	}

	return nil
}

func (d *Repository) prepareWithTableName(sqlTemplate string) *sqlx.NamedStmt {
	query := interpolateSQL(sqlTemplate, map[string]any{
		"tableName": d.tableNameWithSchema(),
	})

	namedStmt, err := d.db.PrepareNamed(query)
	if err != nil {
		panic(err)
	}

	return namedStmt
}

type closeableStmt interface {
	Close() error
}

func (d *Repository) closeStmt(stmt closeableStmt) {
	if err := stmt.Close(); err != nil {
		panic(err)
	}
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
