// Package mysql provides the implementation of a tasq repository in MySQL
package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"text/template"
	"time"

	_ "github.com/go-sql-driver/mysql" // import mysql driver
	"github.com/google/uuid"
	"github.com/greencoda/tasq"
	"github.com/jmoiron/sqlx"
)

const driverName = "mysql"

var (
	errUnexpectedDataSourceType   = errors.New("unexpected dataSource type")
	errFailedToBeginTx            = errors.New("failed to begin transaction")
	errFailedToCommitTx           = errors.New("failed to commit transaction")
	errFailedToExecuteSelect      = errors.New("failed to execute select query")
	errFailedToExecuteUpdate      = errors.New("failed to execute update query")
	errFailedToExecuteDelete      = errors.New("failed to execute delete query")
	errFailedToExecuteInsert      = errors.New("failed to execute insert query")
	errFailedToExecuteCreateTable = errors.New("failed to execute create table query")
	errFailedGetRowsAffected      = errors.New("failed to get rows affected by query")
)

// Repository implements the menthods necessary for tasq to work in MySQL.
type Repository struct {
	db        *sqlx.DB
	tableName string
}

// NewRepository creates a new MySQL Repository instance.
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
	dbx, err := sqlx.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open DB from dsn: %w", err)
	}

	return &Repository{
		db:        dbx,
		tableName: tableName(prefix),
	}, nil
}

func newRepositoryFromDB(db *sql.DB, prefix string) (*Repository, error) {
	dbx := sqlx.NewDb(db, driverName)

	return &Repository{
		db:        dbx,
		tableName: tableName(prefix),
	}, nil
}

// Migrate prepares the database by adding the tasks table.
func (d *Repository) Migrate(ctx context.Context) error {
	if err := d.migrateTable(ctx); err != nil {
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

	const (
		updatePingedTasksSQLTemplate = `UPDATE
			{{.tableName}}
		SET
			visible_at = :visibleAt
		WHERE
			id IN (:pingedTaskIDs);`
		selectPingedTasksSQLTemplate = `SELECT 
			* 
		FROM 
			{{.tableName}}
		WHERE
			id IN (:pingedTaskIDs);`
	)

	tx, err := d.db.Beginx()
	if err != nil {
		return []*tasq.Task{}, fmt.Errorf("%s: %w", errFailedToBeginTx, err)
	}

	defer rollback(tx)

	var (
		pingTime                                      = time.Now()
		updatePingedTasksQuery, updatePingedTasksArgs = d.getQueryWithTableName(updatePingedTasksSQLTemplate, map[string]any{
			"visibleAt":     timeToString(pingTime.Add(visibilityTimeout)),
			"pingedTaskIDs": taskIDs,
		})
	)

	_, err = tx.ExecContext(ctx, updatePingedTasksQuery, updatePingedTasksArgs...)
	if err != nil {
		return []*tasq.Task{}, fmt.Errorf("%s: %w", errFailedToExecuteUpdate, err)
	}

	var (
		pingedMySQLTasks                              []*mySQLTask
		selectPingedTasksQuery, selectPingedTasksArgs = d.getQueryWithTableName(selectPingedTasksSQLTemplate, map[string]any{
			"pingedTaskIDs": taskIDs,
		})
	)

	err = tx.SelectContext(ctx, &pingedMySQLTasks, selectPingedTasksQuery, selectPingedTasksArgs...)
	if err != nil {
		return []*tasq.Task{}, fmt.Errorf("%s: %w", errFailedToExecuteSelect, err)
	}

	err = tx.Commit()
	if err != nil {
		return []*tasq.Task{}, fmt.Errorf("%s: %w", errFailedToCommitTx, err)
	}

	return mySQLTasksToTasks(pingedMySQLTasks), nil
}

// PollTasks polls for available tasks matching supplied the parameters
// and sets their invisibility the supplied timeout parameter to the future.
func (d *Repository) PollTasks(ctx context.Context, types, queues []string, visibilityTimeout time.Duration, ordering tasq.Ordering, pollLimit int) ([]*tasq.Task, error) {
	if pollLimit == 0 {
		return []*tasq.Task{}, nil
	}

	const (
		selectPolledTasksSQLTemplate = `SELECT 
				id 
			FROM 
				{{.tableName}}
			WHERE
				type IN (:pollTypes) AND
				queue IN (:pollQueues) AND 
				status IN (:pollStatuses) AND 
				visible_at <= :pollTime
			ORDER BY
				:pollOrdering
			LIMIT :pollLimit
			FOR UPDATE SKIP LOCKED;`
		updatePolledTasksSQLTemplate = `UPDATE 
				{{.tableName}} 
			SET
				status = :status,
				receive_count = receive_count + 1,
				visible_at = :visibleAt
			WHERE
				id IN (:polledTaskIDs);`
		selectUpdatedPolledTasksSQLTemplate = `SELECT 
				* 
			FROM 
				{{.tableName}}
			WHERE
				id IN (:polledTaskIDs);`
	)

	tx, err := d.db.Beginx()
	if err != nil {
		return []*tasq.Task{}, fmt.Errorf("%s: %w", errFailedToBeginTx, err)
	}

	defer rollback(tx)

	var (
		polledTaskIDs                                 []TaskID
		pollTime                                      = time.Now()
		selectPolledTasksQuery, selectPolledTasksArgs = d.getQueryWithTableName(selectPolledTasksSQLTemplate, map[string]any{
			"pollTypes":    types,
			"pollQueues":   queues,
			"pollStatuses": tasq.GetTaskStatuses(tasq.OpenTasks),
			"pollTime":     timeToString(pollTime),
			"pollOrdering": getOrderingDirectives(ordering),
			"pollLimit":    pollLimit,
		})
	)

	err = tx.SelectContext(ctx, &polledTaskIDs, selectPolledTasksQuery, selectPolledTasksArgs...)
	if err != nil {
		return []*tasq.Task{}, fmt.Errorf("%s: %w", errFailedToExecuteSelect, err)
	}

	if len(polledTaskIDs) == 0 {
		return []*tasq.Task{}, nil
	}

	updatePolledTasksQuery, updatePolledTasksArgs := d.getQueryWithTableName(updatePolledTasksSQLTemplate, map[string]any{
		"status":        tasq.StatusEnqueued,
		"visibleAt":     timeToString(pollTime.Add(visibilityTimeout)),
		"polledTaskIDs": polledTaskIDs,
	})

	_, err = tx.ExecContext(ctx, updatePolledTasksQuery, updatePolledTasksArgs...)
	if err != nil {
		return []*tasq.Task{}, fmt.Errorf("%s: %w", errFailedToExecuteUpdate, err)
	}

	var (
		polledMySQLTasks                                []*mySQLTask
		selectUpdatedTasksQuery, selectUpdatedTasksArgs = d.getQueryWithTableName(selectUpdatedPolledTasksSQLTemplate, map[string]any{
			"polledTaskIDs": polledTaskIDs,
		})
	)

	err = tx.SelectContext(ctx, &polledMySQLTasks, selectUpdatedTasksQuery, selectUpdatedTasksArgs...)
	if err != nil {
		return []*tasq.Task{}, fmt.Errorf("%s: %w", errFailedToExecuteSelect, err)
	}

	err = tx.Commit()
	if err != nil {
		return []*tasq.Task{}, fmt.Errorf("%s: %w", errFailedToCommitTx, err)
	}

	return mySQLTasksToTasks(polledMySQLTasks), nil
}

// CleanTasks removes finished tasks from the queue
// if their creation date is past the supplied duration.
func (d *Repository) CleanTasks(ctx context.Context, cleanAge time.Duration) (int64, error) {
	const cleanTasksSQLTemplate = `DELETE FROM
			{{.tableName}}
		WHERE
			status IN (:statuses) AND
			created_at <= :cleanAt;`

	var (
		cleanTime                       = time.Now()
		cleanTasksQuery, cleanTasksArgs = d.getQueryWithTableName(cleanTasksSQLTemplate, map[string]any{
			"statuses": tasq.GetTaskStatuses(tasq.FinishedTasks),
			"cleanAt":  timeToString(cleanTime.Add(-cleanAge)),
		})
	)

	result, err := d.db.ExecContext(ctx, cleanTasksQuery, cleanTasksArgs...)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", errFailedToExecuteDelete, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("%s: %w", errFailedGetRowsAffected, err)
	}

	return rowsAffected, nil
}

// RegisterStart marks a task as started with the 'in progress' status
// and records the time of start.
func (d *Repository) RegisterStart(ctx context.Context, task *tasq.Task) (*tasq.Task, error) {
	const (
		updateTaskSQLTemplate = `UPDATE 
				{{.tableName}} 
			SET
				status = :status,
				started_at = :startTime
			WHERE
				id = :taskID;`
		selectUpdatedTaskSQLTemplate = `SELECT * 
			FROM 
				{{.tableName}}
			WHERE
				id = :taskID;`
	)

	tx, err := d.db.Beginx()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToBeginTx, err)
	}

	defer rollback(tx)

	var (
		mySQLTask                       = newFromTask(task)
		startTime                       = time.Now()
		updateTaskQuery, updateTaskArgs = d.getQueryWithTableName(updateTaskSQLTemplate, map[string]any{
			"status":    tasq.StatusInProgress,
			"startTime": timeToString(startTime),
			"taskID":    mySQLTask.ID,
		})
	)

	_, err = tx.ExecContext(ctx, updateTaskQuery, updateTaskArgs...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToExecuteUpdate, err)
	}

	selectUpdatedTaskQuery, selectUpdatedTaskArgs := d.getQueryWithTableName(selectUpdatedTaskSQLTemplate, map[string]any{
		"taskID": mySQLTask.ID,
	})

	err = tx.QueryRowxContext(ctx, selectUpdatedTaskQuery, selectUpdatedTaskArgs...).
		StructScan(mySQLTask)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToExecuteSelect, err)
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToCommitTx, err)
	}

	return mySQLTask.toTask(), nil
}

// RegisterError records an error message on the task as last error.
func (d *Repository) RegisterError(ctx context.Context, task *tasq.Task, errTask error) (*tasq.Task, error) {
	const (
		updateTaskSQLTemplate = `UPDATE 
				{{.tableName}} 
			SET
				last_error = :errorMessage
			WHERE
				id = :taskID;`
		selectUpdatedTaskSQLTemplate = `SELECT * 
			FROM 
				{{.tableName}}
			WHERE
				id = :taskID;`
	)

	tx, err := d.db.Beginx()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToBeginTx, err)
	}

	defer rollback(tx)

	var (
		mySQLTask                       = newFromTask(task)
		updateTaskQuery, updateTaskArgs = d.getQueryWithTableName(updateTaskSQLTemplate, map[string]any{
			"errorMessage": errTask.Error(),
			"taskID":       mySQLTask.ID,
		})
	)

	_, err = tx.ExecContext(ctx, updateTaskQuery, updateTaskArgs...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToExecuteUpdate, err)
	}

	selectUpdatedTaskQuery, selectUpdatedTaskArgs := d.getQueryWithTableName(selectUpdatedTaskSQLTemplate, map[string]any{
		"taskID": mySQLTask.ID,
	})

	err = tx.QueryRowxContext(ctx, selectUpdatedTaskQuery, selectUpdatedTaskArgs...).
		StructScan(mySQLTask)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToExecuteSelect, err)
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToCommitTx, err)
	}

	return mySQLTask.toTask(), nil
}

// RegisterFinish marks a task as finished with the supplied status
// and records the time of finish.
func (d *Repository) RegisterFinish(ctx context.Context, task *tasq.Task, finishStatus tasq.TaskStatus) (*tasq.Task, error) {
	const (
		updateTaskSQLTemplate = `UPDATE 
				{{.tableName}} 
			SET
				status = :status,
				finished_at = :finishTime
			WHERE
				id = :taskID;`
		selectUpdatedTaskSQLTemplate = `SELECT * 
			FROM 
				{{.tableName}}
			WHERE
				id = :taskID;`
	)

	tx, err := d.db.Beginx()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToBeginTx, err)
	}

	defer rollback(tx)

	var (
		mySQLTask                         = newFromTask(task)
		finishTime                        = time.Now()
		updateTasksQuery, updateTasksArgs = d.getQueryWithTableName(updateTaskSQLTemplate, map[string]any{
			"status":     finishStatus,
			"finishTime": timeToString(finishTime),
			"taskID":     mySQLTask.ID,
		})
	)

	_, err = tx.ExecContext(ctx, updateTasksQuery, updateTasksArgs...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToExecuteUpdate, err)
	}

	selectUpdatedTasksQuery, selectUpdatedTasksArgs := d.getQueryWithTableName(selectUpdatedTaskSQLTemplate, map[string]any{
		"taskID": mySQLTask.ID,
	})

	err = tx.QueryRowxContext(ctx, selectUpdatedTasksQuery, selectUpdatedTasksArgs...).
		StructScan(mySQLTask)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToExecuteSelect, err)
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToCommitTx, err)
	}

	return mySQLTask.toTask(), nil
}

// SubmitTask adds the supplied task to the queue.
func (d *Repository) SubmitTask(ctx context.Context, task *tasq.Task) (*tasq.Task, error) {
	const (
		insertTaskSQLTemplate = `INSERT INTO 
				{{.tableName}} 
				(id, type, args, queue, priority, status, max_receives, created_at) 
			VALUES
				(:id, :type, :args, :queue, :priority, :status, :maxReceives, :createdAt);`
		selectInsertedTaskSQLTemplate = `SELECT * 
			FROM 
				{{.tableName}}
			WHERE
				id = :taskID;`
	)

	tx, err := d.db.Beginx()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToBeginTx, err)
	}

	defer rollback(tx)

	var (
		mySQLTask                       = newFromTask(task)
		insertTaskQuery, insertTaskArgs = d.getQueryWithTableName(insertTaskSQLTemplate, map[string]any{
			"id":          mySQLTask.ID,
			"type":        mySQLTask.Type,
			"args":        mySQLTask.Args,
			"queue":       mySQLTask.Queue,
			"priority":    mySQLTask.Priority,
			"status":      mySQLTask.Status,
			"maxReceives": mySQLTask.MaxReceives,
			"createdAt":   mySQLTask.CreatedAt,
		})
	)

	_, err = tx.ExecContext(ctx, insertTaskQuery, insertTaskArgs...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToExecuteInsert, err)
	}

	selectInsertedTaskQuery, selectInsertedTaskArgs := d.getQueryWithTableName(selectInsertedTaskSQLTemplate, map[string]any{
		"taskID": mySQLTask.ID,
	})

	err = tx.QueryRowxContext(ctx, selectInsertedTaskQuery, selectInsertedTaskArgs...).
		StructScan(mySQLTask)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToExecuteSelect, err)
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToCommitTx, err)
	}

	return mySQLTask.toTask(), nil
}

// DeleteTask removes the supplied task from the queue.
func (d *Repository) DeleteTask(ctx context.Context, task *tasq.Task) error {
	const deleteTaskSQLTemplate = `DELETE 
		FROM 
			{{.tableName}}
		WHERE
			id = :taskID;`

	var (
		mySQLTask                       = newFromTask(task)
		deleteTaskQuery, deleteTaskArgs = d.getQueryWithTableName(deleteTaskSQLTemplate, map[string]any{
			"taskID": mySQLTask.ID,
		})
	)

	_, err := d.db.ExecContext(ctx, deleteTaskQuery, deleteTaskArgs...)
	if err != nil {
		return fmt.Errorf("%s: %w", errFailedToExecuteDelete, err)
	}

	return nil
}

// RequeueTask marks a task as new, so it can be picked up again.
func (d *Repository) RequeueTask(ctx context.Context, task *tasq.Task) (*tasq.Task, error) {
	const (
		updateTaskSQLTemplate = `UPDATE 
				{{.tableName}} 
			SET
				status = :status
			WHERE
				id = :taskID;`
		selectUpdatedTaskSQLTemplate = `SELECT * 
			FROM 
				{{.tableName}}
			WHERE
				id = :taskID;`
	)

	tx, err := d.db.Beginx()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToBeginTx, err)
	}

	defer rollback(tx)

	var (
		mySQLTask                       = newFromTask(task)
		updateTaskQuery, updateTaskArgs = d.getQueryWithTableName(updateTaskSQLTemplate, map[string]any{
			"status": tasq.StatusNew,
			"taskID": mySQLTask.ID,
		})
	)

	_, err = tx.ExecContext(ctx, updateTaskQuery, updateTaskArgs...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToExecuteUpdate, err)
	}

	selectUpdatedTaskQuery, selectUpdatedTaskArgs := d.getQueryWithTableName(selectUpdatedTaskSQLTemplate, map[string]any{
		"taskID": mySQLTask.ID,
	})

	err = tx.QueryRowxContext(ctx, selectUpdatedTaskQuery, selectUpdatedTaskArgs...).
		StructScan(mySQLTask)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToExecuteSelect, err)
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", errFailedToCommitTx, err)
	}

	return mySQLTask.toTask(), err
}

func (d *Repository) getQueryWithTableName(sqlTemplate string, args ...any) (string, []any) {
	query := interpolateSQL(sqlTemplate, map[string]any{
		"tableName": d.tableName,
	})

	query, args, err := sqlx.Named(query, args)
	if err != nil {
		panic(err)
	}

	query, args, err = sqlx.In(query, args...)
	if err != nil {
		panic(err)
	}

	return d.db.Rebind(query), args
}

func (d *Repository) migrateTable(ctx context.Context) error {
	const sqlTemplate = `CREATE TABLE IF NOT EXISTS {{.tableName}} (
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

	query := interpolateSQL(sqlTemplate, map[string]any{
		"tableName":  d.tableName,
		"enumValues": sliceToMySQLValueList(tasq.GetTaskStatuses(tasq.AllTasks)),
	})

	_, err := d.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("%s: %w", errFailedToExecuteCreateTable, err)
	}

	return nil
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

func rollback(tx *sqlx.Tx) {
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		panic(err)
	}
}

func sliceToMySQLValueList[T any](slice []T) string {
	stringSlice := make([]string, 0, len(slice))

	for _, s := range slice {
		stringSlice = append(stringSlice, fmt.Sprint(s))
	}

	return fmt.Sprintf(`"%s"`, strings.Join(stringSlice, `", "`))
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
