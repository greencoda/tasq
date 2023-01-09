package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql" // import mysql driver
	"github.com/google/uuid"
	"github.com/greencoda/tasq/internal/model"
	"github.com/greencoda/tasq/pkg/repository"
	"github.com/jmoiron/sqlx"
)

const driverName = "mysql"

var errUnexpectedDataSourceType = errors.New("unexpected dataSource type")

type Repository struct {
	db     *sqlx.DB
	prefix string
}

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
		db:     dbx,
		prefix: prefix,
	}, nil
}

func newRepositoryFromDB(db *sql.DB, prefix string) (*Repository, error) {
	dbx := sqlx.NewDb(db, driverName)

	return &Repository{
		db:     dbx,
		prefix: prefix,
	}, nil
}

func (d *Repository) DB() *sql.DB {
	return d.db.DB
}

func (d *Repository) tableName() string {
	const tableName = "tasks"

	if len(d.prefix) > 0 {
		return d.prefix + "_" + tableName
	}

	return tableName
}

func (d *Repository) Migrate(ctx context.Context) error {
	if err := d.migrateTable(ctx); err != nil {
		return err
	}

	return nil
}

func (d *Repository) PingTasks(ctx context.Context, taskIDs []uuid.UUID, visibilityTimeout time.Duration) ([]*model.Task, error) {
	if len(taskIDs) == 0 {
		return []*model.Task{}, nil
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
		return []*model.Task{}, fmt.Errorf("failed to begin transaction for PingTasks: %w", err)
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
		return []*model.Task{}, fmt.Errorf("failed to execute query for PingTasks: %w", err)
	}

	var (
		pingedMySQLTasks                              []*mySQLTask
		selectPingedTasksQuery, selectPingedTasksArgs = d.getQueryWithTableName(selectPingedTasksSQLTemplate, map[string]any{
			"pingedTaskIDs": taskIDs,
		})
	)

	err = tx.SelectContext(ctx, &pingedMySQLTasks, selectPingedTasksQuery, selectPingedTasksArgs...)
	if err != nil {
		return []*model.Task{}, err
	}

	err = tx.Commit()
	if err != nil {
		return []*model.Task{}, err
	}

	return mySQLTasksToTasks(pingedMySQLTasks), nil
}

func (d *Repository) PollTasks(ctx context.Context, types, queues []string, visibilityTimeout time.Duration, ordering []string, pollLimit int) ([]*model.Task, error) {
	if pollLimit == 0 {
		return []*model.Task{}, nil
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
		selectUpdatedTasksSQLTemplate = `SELECT 
				* 
			FROM 
				{{.tableName}}
			WHERE
				id IN (:polledTaskIDs);`
	)

	tx, err := d.db.Beginx()
	if err != nil {
		return []*model.Task{}, err
	}

	defer rollback(tx)

	var (
		polledTaskIDs                                 []TaskID
		pollTime                                      = time.Now()
		selectPolledTasksQuery, selectPolledTasksArgs = d.getQueryWithTableName(selectPolledTasksSQLTemplate, map[string]any{
			"pollTypes":    types,
			"pollQueues":   queues,
			"pollStatuses": model.OpenTaskStatuses,
			"pollTime":     timeToString(pollTime),
			"pollOrdering": ordering,
			"pollLimit":    pollLimit,
		})
	)

	err = tx.SelectContext(ctx, &polledTaskIDs, selectPolledTasksQuery, selectPolledTasksArgs...)
	if err != nil {
		return []*model.Task{}, err
	}

	if len(polledTaskIDs) == 0 {
		return []*model.Task{}, nil
	}

	updatePolledTasksQuery, updatePolledTasksArgs := d.getQueryWithTableName(updatePolledTasksSQLTemplate, map[string]any{
		"status":        model.StatusEnqueued,
		"visibleAt":     timeToString(pollTime.Add(visibilityTimeout)),
		"polledTaskIDs": polledTaskIDs,
	})

	_, err = tx.ExecContext(ctx, updatePolledTasksQuery, updatePolledTasksArgs...)
	if err != nil {
		return []*model.Task{}, err
	}

	var (
		polledMySQLTasks                                []*mySQLTask
		selectUpdatedTasksQuery, selectUpdatedTasksArgs = d.getQueryWithTableName(selectUpdatedTasksSQLTemplate, map[string]any{
			"polledTaskIDs": polledTaskIDs,
		})
	)

	err = tx.SelectContext(ctx, &polledMySQLTasks, selectUpdatedTasksQuery, selectUpdatedTasksArgs...)
	if err != nil {
		return []*model.Task{}, err
	}

	err = tx.Commit()
	if err != nil {
		return []*model.Task{}, err
	}

	return mySQLTasksToTasks(polledMySQLTasks), nil
}

func (d *Repository) CleanTasks(ctx context.Context, cleanAge time.Duration) (int64, error) {
	const cleanTasksSQLTemplate = `DELETE FROM
			{{.tableName}}
		WHERE
			status IN (:statuses) AND
			created_at <= :cleanAt;`

	var (
		cleanTime                       = time.Now()
		cleanTasksQuery, cleanTasksArgs = d.getQueryWithTableName(cleanTasksSQLTemplate, map[string]any{
			"statuses": model.FinishedTaskStatuses,
			"cleanAt":  timeToString(cleanTime.Add(-cleanAge)),
		})
	)

	result, err := d.db.ExecContext(ctx, cleanTasksQuery, cleanTasksArgs...)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return rowsAffected, nil
}

func (d *Repository) RegisterStart(ctx context.Context, task *model.Task) (*model.Task, error) {
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
		return nil, err
	}

	defer rollback(tx)

	var (
		mySQLTask                       = newFromTask(task)
		startTime                       = time.Now()
		updateTaskQuery, updateTaskArgs = d.getQueryWithTableName(updateTaskSQLTemplate, map[string]any{
			"status":    model.StatusInProgress,
			"startTime": timeToString(startTime),
			"taskID":    mySQLTask.ID,
		})
	)

	_, err = tx.ExecContext(ctx, updateTaskQuery, updateTaskArgs...)
	if err != nil {
		return nil, err
	}

	selectUpdatedTaskQuery, selectUpdatedTaskArgs := d.getQueryWithTableName(selectUpdatedTaskSQLTemplate, map[string]any{
		"taskID": mySQLTask.ID,
	})

	err = tx.QueryRowxContext(ctx, selectUpdatedTaskQuery, selectUpdatedTaskArgs...).
		StructScan(mySQLTask)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return mySQLTask.toTask(), nil
}

func (d *Repository) RegisterError(ctx context.Context, task *model.Task, errTask error) (*model.Task, error) {
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
		return nil, err
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
		return nil, err
	}

	selectUpdatedTaskQuery, selectUpdatedTaskArgs := d.getQueryWithTableName(selectUpdatedTaskSQLTemplate, map[string]any{
		"taskID": mySQLTask.ID,
	})

	err = tx.QueryRowxContext(ctx, selectUpdatedTaskQuery, selectUpdatedTaskArgs...).
		StructScan(mySQLTask)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return mySQLTask.toTask(), nil
}

func (d *Repository) RegisterSuccess(ctx context.Context, task *model.Task) (*model.Task, error) {
	return d.registerFinish(ctx, task, model.StatusSuccessful)
}

func (d *Repository) RegisterFailure(ctx context.Context, task *model.Task) (*model.Task, error) {
	return d.registerFinish(ctx, task, model.StatusFailed)
}

func (d *Repository) registerFinish(ctx context.Context, task *model.Task, finishStatus model.TaskStatus) (*model.Task, error) {
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
		return nil, err
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
		return nil, err
	}

	selectUpdatedTasksQuery, selectUpdatedTasksArgs := d.getQueryWithTableName(selectUpdatedTaskSQLTemplate, map[string]any{
		"taskID": mySQLTask.ID,
	})

	err = tx.QueryRowxContext(ctx, selectUpdatedTasksQuery, selectUpdatedTasksArgs...).
		StructScan(mySQLTask)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return mySQLTask.toTask(), nil
}

func (d *Repository) SubmitTask(ctx context.Context, task *model.Task) (*model.Task, error) {
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
		return nil, err
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
		return nil, err
	}

	selectInsertedTaskQuery, selectInsertedTaskArgs := d.getQueryWithTableName(selectInsertedTaskSQLTemplate, map[string]any{
		"taskID": mySQLTask.ID,
	})

	err = tx.QueryRowxContext(ctx, selectInsertedTaskQuery, selectInsertedTaskArgs...).
		StructScan(mySQLTask)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return mySQLTask.toTask(), nil
}

func (d *Repository) DeleteTask(ctx context.Context, task *model.Task) error {
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

	return err
}

func (d *Repository) RequeueTask(ctx context.Context, task *model.Task) (*model.Task, error) {
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
		return nil, err
	}

	defer rollback(tx)

	var (
		mySQLTask                       = newFromTask(task)
		updateTaskQuery, updateTaskArgs = d.getQueryWithTableName(updateTaskSQLTemplate, map[string]any{
			"status": model.StatusNew,
			"taskID": mySQLTask.ID,
		})
	)

	_, err = tx.ExecContext(ctx, updateTaskQuery, updateTaskArgs...)
	if err != nil {
		return nil, err
	}

	selectUpdatedTaskQuery, selectUpdatedTaskArgs := d.getQueryWithTableName(selectUpdatedTaskSQLTemplate, map[string]any{
		"taskID": mySQLTask.ID,
	})

	err = tx.QueryRowxContext(ctx, selectUpdatedTaskQuery, selectUpdatedTaskArgs...).
		StructScan(mySQLTask)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return mySQLTask.toTask(), err
}

func (d *Repository) getQueryWithTableName(sqlTemplate string, args ...any) (string, []any) {
	query := repository.InterpolateSQL(sqlTemplate, map[string]any{
		"tableName": d.tableName(),
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
	stringSlice := make([]string, 0, len(slice))

	for _, s := range slice {
		stringSlice = append(stringSlice, fmt.Sprint(s))
	}

	return fmt.Sprintf(`"%s"`, strings.Join(stringSlice, `", "`))
}

func rollback(tx *sqlx.Tx) {
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		panic(err)
	}
}
