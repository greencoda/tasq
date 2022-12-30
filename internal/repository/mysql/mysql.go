package mysql

import (
	"context"
	"database/sql"
	"errors"
	"log"

	"fmt"
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

	const sqlTemplate = `UPDATE
			{{.tableName}}
		SET
			visible_at = :visible_at
		WHERE
			id IN (:pinged_ids)
		RETURNING id;`

	var (
		pingedMySQLTasks []*MySQLTask
		pingedTime       = time.Now()
		stmt             = d.prepareWithTableName(sqlTemplate)
	)

	err = stmt.SelectContext(ctx, &pingedMySQLTasks, map[string]any{
		"visible_at": pingedTime.Add(visibilityTimeout),
		"pinged_ids": taskIDs,
	})
	if err != nil {
		return []*model.Task{}, err
	}

	return mySQLTasksToTasks(pingedMySQLTasks), nil
}

func (d *mysqlRepository) PollTasks(ctx context.Context, types, queues []string, visibilityTimeout time.Duration, ordering []string, pollLimit int) (polledTasks []*model.Task, err error) {
	const (
		selectPolledTasksSQLTemplate = `SELECT 
				id 
			FROM 
				{{.tableName}}
			WHERE
				type IN (:poll_types) AND
				queue IN (:poll_queues) AND 
				status IN (:poll_statuses) AND 
				visible_at <= :poll_time
			ORDER BY
				:poll_ordering
			LIMIT :poll_limit
			FOR UPDATE SKIP LOCKED;`
		updatePolledTasksSQLTemplate = `UPDATE {{.tableName}} 
			SET
				status = :status,
				receive_count = receive_count + 1,
				visible_at = :visible_at
			WHERE
				id IN (:polledTaskIDs);`
		selectUpdatedTasksSQLTemplate = `SELECT * 
			FROM 
				{{.tableName}}
			WHERE
				id IN (:polledTaskIDs);`
	)

	var (
		polledTaskIDs                                 []MySQLTaskID
		pollTime                                      = time.Now()
		selectPolledTasksQuery, selectPolledTasksArgs = d.getQueryWithTableName(selectPolledTasksSQLTemplate, map[string]any{
			"poll_types":    types,
			"poll_queues":   queues,
			"poll_statuses": model.OpenTaskStatuses,
			"poll_time":     pollTime,
			"poll_ordering": sliceToMySQLEntityList(ordering),
			"poll_limit":    pollLimit,
		})
	)

	tx, err := d.db.Beginx()
	if err != nil {
		return []*model.Task{}, err
	}
	defer rollback(tx)

	err = tx.SelectContext(ctx, &polledTaskIDs, selectPolledTasksQuery, selectPolledTasksArgs...)
	if err == sql.ErrNoRows {
		return []*model.Task{}, nil
	}
	if err != nil {
		return []*model.Task{}, err
	}

	var (
		updatePolledTasksQuery, updatePolledTasksArgs = d.getQueryWithTableName(updatePolledTasksSQLTemplate, map[string]any{
			"status":        model.StatusEnqueued,
			"visible_at":    pollTime.Add(visibilityTimeout),
			"polledTaskIDs": polledTaskIDs,
		})
	)

	_, err = tx.ExecContext(ctx, updatePolledTasksQuery, updatePolledTasksArgs...)
	if err != nil {
		return []*model.Task{}, err
	}

	var (
		polledMySQLTasks                                []*MySQLTask
		selectUpdatedTasksQuery, selectUpdatedTasksArgs = d.getQueryWithTableName(selectUpdatedTasksSQLTemplate, map[string]any{
			"polledTaskIDs": polledTaskIDs,
		})
	)

	err = tx.SelectContext(ctx, &polledMySQLTasks, selectUpdatedTasksQuery, selectUpdatedTasksArgs...)
	if err == sql.ErrNoRows {
		return []*model.Task{}, nil
	}
	if err != nil {
		return []*model.Task{}, err
	}

	err = tx.Commit()
	if err != nil {
		return []*model.Task{}, err
	}

	return mySQLTasksToTasks(polledMySQLTasks), nil
}

func (d *mysqlRepository) CleanTasks(ctx context.Context, cleanAge time.Duration) (rowsAffected int64, err error) {
	const cleanTasksSQLTemplate = `DELETE FROM {{.tableName}}
		WHERE
			status IN (:statuses) AND
			created_at <= :cleanAt;`

	var (
		cleanTime                       = time.Now()
		cleanTasksQuery, cleanTasksArgs = d.getQueryWithTableName(cleanTasksSQLTemplate, map[string]any{
			"statuses": model.FinishedTaskStatuses,
			"cleanAt":  cleanTime.Add(-cleanAge),
		})
	)

	result, err := d.db.ExecContext(ctx, cleanTasksQuery, cleanTasksArgs...)
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
		return &model.Task{}, err
	}
	defer rollback(tx)

	var (
		startTime             = time.Now()
		updateTaskSTMT        = d.prepareTxWithTableName(updateTaskSQLTemplate, tx)
		selectUpdatedTaskSTMT = d.prepareTxWithTableName(selectUpdatedTaskSQLTemplate, tx)
	)

	_, err = updateTaskSTMT.ExecContext(ctx, map[string]any{
		"status":    model.StatusInProgress,
		"startTime": startTime,
		"taskID":    task.ID,
	})
	if err != nil {
		return &model.Task{}, err
	}

	updatedTask = new(model.Task) // TODO: test if this is needed

	row := selectUpdatedTaskSTMT.QueryRowContext(ctx, map[string]any{
		"taskID": task.ID,
	})

	err = row.StructScan(updatedTask)
	if err != nil && err != sql.ErrNoRows {
		return &model.Task{}, err
	}

	return updatedTask, nil
}

func (d *mysqlRepository) RegisterError(ctx context.Context, task *model.Task, taskError error) (updatedTask *model.Task, err error) {
	const (
		updateTaskSQLTemplate = `UPDATE 
				{{.tableName}} 
			SET
				"last_error" = :errorMessage
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
		return &model.Task{}, err
	}
	defer rollback(tx)

	var (
		updateTaskSTMT        = d.prepareTxWithTableName(updateTaskSQLTemplate, tx)
		selectUpdatedTaskSTMT = d.prepareTxWithTableName(selectUpdatedTaskSQLTemplate, tx)
	)

	_, err = updateTaskSTMT.ExecContext(ctx, map[string]any{
		"errorMessage": taskError.Error(),
		"taskID":       task.ID,
	})
	if err != nil {
		return &model.Task{}, err
	}

	updatedTask = new(model.Task) // TODO: test if this is needed

	row := selectUpdatedTaskSTMT.QueryRowContext(ctx, map[string]any{
		"taskID": task.ID,
	})

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
		return &model.Task{}, err
	}
	defer rollback(tx)

	var (
		finishTime                        = time.Now()
		updateTasksQuery, updateTasksArgs = d.getQueryWithTableName(updateTaskSQLTemplate, map[string]any{
			"status":     finishStatus,
			"finishTime": finishTime,
			"taskID":     task.ID,
		})
		selectUpdatedTasksQuery, selectUpdatedTasksArgs = d.getQueryWithTableName(selectUpdatedTaskSQLTemplate, map[string]any{
			"taskID": task.ID,
		})
	)

	_, err = tx.ExecContext(ctx, updateTasksQuery, updateTasksArgs...)
	if err != nil {
		return &model.Task{}, err
	}

	var updatedMySQLTask = new(MySQLTask)

	err = tx.GetContext(ctx, updatedMySQLTask, selectUpdatedTasksQuery, selectUpdatedTasksArgs...)
	if err == sql.ErrNoRows {
		return &model.Task{}, nil
	}
	if err != nil {
		log.Printf("keke")
		return &model.Task{}, err
	}

	err = tx.Commit()
	if err != nil {
		return &model.Task{}, err
	}

	return updatedTask, nil
}

func (d *mysqlRepository) SubmitTask(ctx context.Context, task *model.Task) (submittedTask *model.Task, err error) {
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
		return &model.Task{}, err
	}
	defer rollback(tx)

	var (
		insertTaskSTMT         = d.prepareTxWithTableName(insertTaskSQLTemplate, tx)
		selectInsertedTaskSTMT = d.prepareTxWithTableName(selectInsertedTaskSQLTemplate, tx)
	)

	taskID, err := task.ID.MarshalBinary()
	if err != nil {
		return &model.Task{}, err
	}

	_, err = insertTaskSTMT.ExecContext(ctx, map[string]any{
		"id":          taskID,
		"type":        task.Type,
		"args":        task.Args,
		"queue":       task.Queue,
		"priority":    task.Priority,
		"status":      task.Status,
		"maxReceives": task.MaxReceives,
		"createdAt":   task.CreatedAt,
	})
	if err != nil {
		return &model.Task{}, err
	}

	row := selectInsertedTaskSTMT.QueryRowContext(ctx, map[string]any{
		"taskID": taskID,
	})

	var submittedMySQLTask MySQLTask

	err = row.StructScan(&submittedMySQLTask)
	if err != nil && err != sql.ErrNoRows {

		return &model.Task{}, err
	}

	err = tx.Commit()
	if err != nil {
		return &model.Task{}, err
	}

	return submittedMySQLTask.toTask(), nil
}

func (d *mysqlRepository) DeleteTask(ctx context.Context, task *model.Task) (err error) {
	const deleteTaskSQLTemplate = `DELETE FROM 
			{{.tableName}}
		WHERE
			"id" = :taskID;`

	var deleteTaskQuery, deleteTaskArgs = d.getQueryWithTableName(deleteTaskSQLTemplate, map[string]any{
		"taskID": task.ID,
	})

	_, err = d.db.ExecContext(ctx, deleteTaskQuery, deleteTaskArgs...)

	return err
}

func (d *mysqlRepository) RequeueTask(ctx context.Context, task *model.Task) (updatedTask *model.Task, err error) {
	const (
		updateTaskSQLTemplate = `UPDATE 
				{{.tableName}} 
			SET
				"status" = :status
			WHERE
				"id" = :taskID;`
		selectUpdatedTaskSQLTemplate = `SELECT * 
			FROM 
				{{.tableName}}
			WHERE
				id = :taskID;`
	)

	tx, err := d.db.Beginx()
	if err != nil {
		return &model.Task{}, err
	}
	defer rollback(tx)

	var (
		updateTaskSTMT        = d.prepareTxWithTableName(updateTaskSQLTemplate, tx)
		selectUpdatedTaskSTMT = d.prepareTxWithTableName(selectUpdatedTaskSQLTemplate, tx)
	)

	_, err = updateTaskSTMT.ExecContext(ctx, map[string]any{
		"status": model.StatusNew,
		"taskID": task.ID,
	})
	if err != nil {
		return &model.Task{}, err
	}

	updatedTask = new(model.Task) // TODO: test if this is needed

	row := selectUpdatedTaskSTMT.QueryRowContext(ctx, map[string]any{
		"taskID": task.ID,
	})

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

	namedStmt, err = d.db.PrepareNamed(query)
	if err != nil {
		panic(err)
	}

	return namedStmt
}

func (d *mysqlRepository) prepareTxWithTableName(sqlTemplate string, tx *sqlx.Tx) (namedStmt *sqlx.NamedStmt) {
	var (
		query = repository.InterpolateSQL(sqlTemplate, map[string]any{
			"tableName": d.tableName(),
		})
		err error
	)

	namedStmt, err = tx.PrepareNamed(query)
	if err != nil {
		panic(err)
	}

	return namedStmt
}

func (d *mysqlRepository) getQueryWithTableName(sqlTemplate string, args ...any) (string, []any) {
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

func (d *mysqlRepository) migrateTable(ctx context.Context) error {
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

func sliceToMySQLEntityList[T any](slice []T) string {
	var stringSlice = make([]string, 0, len(slice))

	for _, s := range slice {
		stringSlice = append(stringSlice, fmt.Sprint(s))
	}

	return strings.Join(stringSlice, ` `)
}

func sliceToMySQLValueList[T any](slice []T) string {
	var stringSlice = make([]string, 0, len(slice))

	for _, s := range slice {
		stringSlice = append(stringSlice, fmt.Sprint(s))
	}

	return fmt.Sprintf(`"%s"`, strings.Join(stringSlice, `", "`))
}

func rollback(tx *sqlx.Tx) {
	err := tx.Rollback()
	if err != nil && !errors.Is(err, sql.ErrTxDone) {
		panic(err)
	}
}
