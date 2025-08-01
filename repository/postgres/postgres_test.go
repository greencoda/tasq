package postgres_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/greencoda/tasq"
	"github.com/greencoda/tasq/repository/postgres"
	"github.com/stretchr/testify/suite"
)

var (
	ctx         = context.Background()
	testTask    = getStartedTestTask()
	taskColumns = []string{
		"id",
		"type",
		"args",
		"queue",
		"priority",
		"status",
		"receive_count",
		"max_receives",
		"last_error",
		"created_at",
		"started_at",
		"finished_at",
		"visible_at",
	}
	testTaskType  = "testTask"
	testTaskQueue = "testQueue"
	taskValues    = postgres.GetTestTaskValues(testTask)
	errSQL        = errors.New("sql error")
	errTask       = errors.New("task error")
)

func getStartedTestTask() *tasq.Task {
	var (
		testTask, _ = tasq.NewTask(testTaskType, true, testTaskQueue, 100, 5)
		startTime   = testTask.CreatedAt.Add(time.Second)
	)

	testTask.StartedAt = &startTime

	return testTask
}

type PostgresTestSuite struct {
	suite.Suite

	db               *sql.DB
	sqlMock          sqlmock.Sqlmock
	mockedRepository tasq.IRepository
}

func TestTaskTestSuite(t *testing.T) {
	t.Parallel()

	suite.Run(t, new(PostgresTestSuite))
}

func (s *PostgresTestSuite) SetupTest() {
	var err error

	s.db, s.sqlMock, err = sqlmock.New()
	s.Require().NoError(err)

	s.mockedRepository, err = postgres.NewRepository(s.db, "test")
	s.Require().NotNil(s.mockedRepository)
	s.Require().NoError(err)
}

func (s *PostgresTestSuite) TestNewRepository() {
	// providing the datasource as *sql.DB
	repository, err := postgres.NewRepository(s.db, "test")
	s.NotNil(repository)
	s.NoError(err)

	// providing the datasource as *sql.DB with no prefix
	repository, err = postgres.NewRepository(s.db, "")
	s.NotNil(repository)
	s.NoError(err)

	// providing the datasource as dsn string
	repository, err = postgres.NewRepository("testDSN", "test")
	s.NotNil(repository)
	s.NoError(err)

	// providing the datasource as unknown datasource type
	repository, err = postgres.NewRepository(false, "test")
	s.Nil(repository)
	s.Error(err)
}

func (s *PostgresTestSuite) TestMigrate() {
	// First try - creating the task_status type fails
	s.sqlMock.ExpectExec(`CREATE TYPE test_task_status AS ENUM`).WillReturnError(errSQL)

	err := s.mockedRepository.Migrate(ctx)
	s.Error(err)

	// Second try - creating the tasks table fails
	s.sqlMock.ExpectExec(`CREATE TYPE test_task_status AS ENUM`).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectExec(`CREATE TABLE IF NOT EXISTS test_tasks`).WillReturnError(errSQL)

	err = s.mockedRepository.Migrate(ctx)
	s.Error(err)

	// Third try - migration succeeds
	s.sqlMock.ExpectExec(`CREATE TYPE test_task_status AS ENUM`).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectExec(`CREATE TABLE IF NOT EXISTS test_tasks`).WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.mockedRepository.Migrate(ctx)
	s.NoError(err)
}

func (s *PostgresTestSuite) TestPingTasks() {
	var (
		taskUUID       = uuid.New()
		stmtMockRegexp = regexp.QuoteMeta(`UPDATE test_tasks SET "visible_at" = $1 WHERE "id" = ANY($2) RETURNING id;`)
	)
	// pinging empty tasklist
	tasks, err := s.mockedRepository.PingTasks(ctx, []uuid.UUID{}, 15*time.Second)
	s.Empty(tasks)
	s.NoError(err)

	// pinging when stmt preparation returns an error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).WillReturnError(errSQL)

	s.PanicsWithError(errSQL.Error(), func() {
		_, _ = s.mockedRepository.PingTasks(ctx, []uuid.UUID{taskUUID}, 15*time.Second)
	})

	// pinging when DB returns no rows
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnError(errSQL)

	tasks, err = s.mockedRepository.PingTasks(ctx, []uuid.UUID{taskUUID}, 15*time.Second)
	s.Empty(tasks)
	s.Error(err)

	// pinging existing task
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUID))

	tasks, err = s.mockedRepository.PingTasks(ctx, []uuid.UUID{taskUUID}, 15*time.Second)
	s.Len(tasks, 1)
	s.NoError(err)
}

func (s *PostgresTestSuite) TestPollTasks() {
	stmtMockRegexp := regexp.QuoteMeta(`UPDATE test_tasks SET 
			"status" = $1, 
			"receive_count" = "receive_count" + 1, 
			"visible_at" = $2 
		WHERE "id" IN ( 
			SELECT 
				"id" 
			FROM test_tasks 
			WHERE "type" = ANY($3) 
				AND "queue" = ANY($4) 
				AND "status" = ANY($5) 
				AND "visible_at" <= $6 
			ORDER BY $7 
			LIMIT $8 
			FOR UPDATE 
		) RETURNING *;`)

	// polling with 0 limit
	tasks, err := s.mockedRepository.PollTasks(ctx, []string{testTaskType}, []string{"testQueue"}, 15*time.Second, tasq.OrderingCreatedAtFirst, 0)
	s.Empty(tasks)
	s.NoError(err)

	// polling when DB returns no rows
	s.sqlMock.ExpectPrepare(stmtMockRegexp).
		ExpectQuery().
		WillReturnError(sql.ErrNoRows)

	tasks, err = s.mockedRepository.PollTasks(ctx, []string{testTaskType}, []string{"testQueue"}, 15*time.Second, tasq.OrderingCreatedAtFirst, 1)
	s.Empty(tasks)
	s.NoError(err)

	// polling when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).
		ExpectQuery().
		WillReturnError(errSQL)

	tasks, err = s.mockedRepository.PollTasks(ctx, []string{testTaskType}, []string{"testQueue"}, 15*time.Second, tasq.OrderingCreatedAtFirst, 1)
	s.Empty(tasks)
	s.Error(err)

	// polling for existing tasks
	s.sqlMock.ExpectPrepare(stmtMockRegexp).
		ExpectQuery().
		WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))

	tasks, err = s.mockedRepository.PollTasks(ctx, []string{testTaskType}, []string{"testQueue"}, 15*time.Second, tasq.OrderingCreatedAtFirst, 1)
	s.Len(tasks, 1)
	s.NoError(err)

	// polling for existing tasks with unknown ordering
	s.sqlMock.ExpectPrepare(stmtMockRegexp).
		ExpectQuery().
		WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))

	tasks, err = s.mockedRepository.PollTasks(ctx, []string{testTaskType}, []string{"testQueue"}, 15*time.Second, -1, 1)
	s.Len(tasks, 1)
	s.NoError(err)
}

func (s *PostgresTestSuite) TestCleanTasks() {
	stmtMockRegexp := regexp.QuoteMeta(`DELETE FROM test_tasks WHERE "status" = ANY($1) AND "created_at" <= $2;`)

	// cleaning when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectExec().WillReturnError(errSQL)

	rowsAffected, err := s.mockedRepository.CleanTasks(ctx, time.Hour)
	s.Zero(rowsAffected)
	s.Error(err)

	// cleaning when no rows are found
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectExec().WillReturnResult(driver.ResultNoRows)

	rowsAffected, err = s.mockedRepository.CleanTasks(ctx, time.Hour)
	s.Equal(int64(0), rowsAffected)
	s.Error(err)

	// cleaning successful
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectExec().WillReturnResult(sqlmock.NewResult(1, 1))

	rowsAffected, err = s.mockedRepository.CleanTasks(ctx, time.Hour)
	s.Equal(int64(1), rowsAffected)
	s.NoError(err)
}

func (s *PostgresTestSuite) TestRegisterStart() {
	stmtMockRegexp := regexp.QuoteMeta(`UPDATE test_tasks SET "status" = $1, "started_at" = $2 WHERE "id" = $3 RETURNING *;`)

	// registering start when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnError(errSQL)

	task, err := s.mockedRepository.RegisterStart(ctx, testTask)
	s.Empty(task)
	s.Error(err)

	// registering start successful
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))

	task, err = s.mockedRepository.RegisterStart(ctx, testTask)
	s.NotEmpty(task)
	s.NoError(err)
}

func (s *PostgresTestSuite) TestRegisterError() {
	stmtMockRegexp := regexp.QuoteMeta(`UPDATE test_tasks SET "last_error" = $1 WHERE "id" = $2 RETURNING *;`)

	// registering error when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnError(errSQL)

	task, err := s.mockedRepository.RegisterError(ctx, testTask, errTask)
	s.Empty(task)
	s.Error(err)

	// registering error successful
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))

	task, err = s.mockedRepository.RegisterError(ctx, testTask, errTask)
	s.NotEmpty(task)
	s.NoError(err)
}

func (s *PostgresTestSuite) TestRegisterFinish() {
	stmtMockRegexp := regexp.QuoteMeta(`UPDATE test_tasks SET "status" = $1, "finished_at" = $2 WHERE "id" = $3 RETURNING *;`)

	// registering failure when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnError(errSQL)

	task, err := s.mockedRepository.RegisterFinish(ctx, testTask, tasq.StatusSuccessful)
	s.Empty(task)
	s.Error(err)

	// registering failure successful
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))

	task, err = s.mockedRepository.RegisterFinish(ctx, testTask, tasq.StatusSuccessful)
	s.NotEmpty(task)
	s.NoError(err)
}

func (s *PostgresTestSuite) TestSubmitTask() {
	stmtMockRegexp := regexp.QuoteMeta(`INSERT INTO test_tasks  (id, type, args, queue, priority, status, max_receives, created_at, visible_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *;`)

	// submitting task when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnError(errSQL)

	task, err := s.mockedRepository.SubmitTask(ctx, testTask)
	s.Empty(task)
	s.Error(err)

	// submitting task successful
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))

	task, err = s.mockedRepository.SubmitTask(ctx, testTask)
	s.NotEmpty(task)
	s.NoError(err)
}

func (s *PostgresTestSuite) TestDeleteTask() {
	var (
		stmtMockRegexp = regexp.QuoteMeta(`DELETE 
		FROM 
			test_tasks 
		WHERE 
			"id" = $1;`)
		stmtInvisibleMockRegexp = regexp.QuoteMeta(`DELETE 
		FROM 
			test_tasks 
		WHERE 
			"id" = $1 AND
			(
				(
					"visible_at" <= $2
				) OR 
				( 
					"status" = ANY($3) AND 
					"visible_at" > $4 
				) 
			);`)
	)

	// deleting task when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectExec().WillReturnError(errSQL)

	err := s.mockedRepository.DeleteTask(ctx, testTask, false)
	s.Error(err)

	// deleting task successful
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectExec().WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.mockedRepository.DeleteTask(ctx, testTask, false)
	s.NoError(err)

	// deleting invisible task successful
	s.sqlMock.ExpectPrepare(stmtInvisibleMockRegexp).ExpectExec().WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.mockedRepository.DeleteTask(ctx, testTask, true)
	s.NoError(err)
}

func (s *PostgresTestSuite) TestRequeueTask() {
	stmtMockRegexp := regexp.QuoteMeta(`UPDATE test_tasks SET "status" = $1 WHERE "id" = $2 RETURNING *;`)

	// requeuing task when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnError(errSQL)

	task, err := s.mockedRepository.RequeueTask(ctx, testTask)
	s.Empty(task)
	s.Error(err)

	// requeuing task successful
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))

	task, err = s.mockedRepository.RequeueTask(ctx, testTask)
	s.NotEmpty(task)
	s.NoError(err)
}

func (s *PostgresTestSuite) TestCountTasks() {
	stmtMockRegexp := regexp.QuoteMeta(`SELECT COUNT(*) FROM test_tasks WHERE "status" = ANY($1) AND "type" = ANY($2) AND "queue" = ANY($3)`)

	// counting tasks when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnError(errSQL)

	count, err := s.mockedRepository.CountTasks(ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"test"}, []string{"test"})
	s.Equal(int64(0), count)
	s.Error(err)

	// counting tasks successful
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(10))

	count, err = s.mockedRepository.CountTasks(ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"test"}, []string{"test"})
	s.Equal(int64(10), count)
	s.NoError(err)
}

func (s *PostgresTestSuite) TestScanTasks() {
	stmtMockRegexp := regexp.QuoteMeta(`SELECT * FROM test_tasks WHERE "status" = ANY($1) AND "type" = ANY($2) AND "queue" = ANY($3) ORDER BY $4 LIMIT $5;`)

	// scanning tasks when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnError(errSQL)

	tasks, err := s.mockedRepository.ScanTasks(ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"test"}, []string{"test"}, 0, 10)
	s.Empty(tasks)
	s.Error(err)

	// scanning tasks successful
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))

	tasks, err = s.mockedRepository.ScanTasks(ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"test"}, []string{"test"}, 0, 10)
	s.NotEmpty(tasks)
	s.NoError(err)
}

func (s *PostgresTestSuite) TestPurgeTasks() {
	var (
		stmtMockRegexp = regexp.QuoteMeta(`DELETE 
			FROM 
				test_tasks 
			WHERE 
				"status" = ANY($1) AND 
				"queue" = ANY($2);`)
		stmtSafeDeleteMockRegexp = regexp.QuoteMeta(`DELETE 
			FROM 
				test_tasks 
			WHERE 
				"status" = ANY($1) AND 
				"queue" = ANY($2) AND 
				( 
					( "visible_at" <= $3 ) OR 
					( "status" = ANY($4) AND "visible_at" > $5 ) 
				);`)
	)

	// purging tasks when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectExec().WillReturnError(errSQL)

	count, err := s.mockedRepository.PurgeTasks(ctx, []tasq.TaskStatus{tasq.StatusFailed}, []string{}, []string{testTaskQueue}, false)
	s.Equal(int64(0), count)
	s.Error(err)

	// purging when no rows are found
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectExec().WillReturnResult(driver.ResultNoRows)

	count, err = s.mockedRepository.PurgeTasks(ctx, []tasq.TaskStatus{tasq.StatusFailed}, []string{}, []string{testTaskQueue}, false)
	s.Equal(int64(0), count)
	s.Error(err)

	// purging tasks successful
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectExec().WillReturnResult(sqlmock.NewResult(1, 1))

	count, err = s.mockedRepository.PurgeTasks(ctx, []tasq.TaskStatus{tasq.StatusFailed}, []string{}, []string{testTaskQueue}, false)
	s.Equal(int64(1), count)
	s.NoError(err)

	// purging tasks with safeDelete successful
	s.sqlMock.ExpectPrepare(stmtSafeDeleteMockRegexp).ExpectExec().WillReturnResult(sqlmock.NewResult(1, 1))

	count, err = s.mockedRepository.PurgeTasks(ctx, []tasq.TaskStatus{tasq.StatusFailed}, []string{}, []string{testTaskQueue}, true)
	s.Equal(int64(1), count)
	s.NoError(err)
}

func (s *PostgresTestSuite) TestPrepareWithTableName() {
	stmtMockRegexp := regexp.QuoteMeta(`SELECT * FROM test_tasks`)

	postgresRepository, ok := s.mockedRepository.(*postgres.Repository)
	s.Require().True(ok)

	// preparing stmt with table name when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).WillReturnError(errSQL)

	s.PanicsWithError("sql error", func() {
		_ = postgresRepository.PrepareWithTableName("SELECT * FROM {{.tableName}}")
	})
}

func (s *PostgresTestSuite) TestCloseNamedStmt() {
	stmtMockRegexp := regexp.QuoteMeta(`SELECT * FROM test_tasks`)

	postgresRepository, ok := s.mockedRepository.(*postgres.Repository)
	s.Require().True(ok)

	s.sqlMock.ExpectPrepare(stmtMockRegexp)

	stmt, err := s.db.PrepareContext(ctx, "SELECT * FROM test_tasks")
	s.Require().NoError(err)

	// an alternative DB to test the panic
	altDB, altSQLMock, err := sqlmock.New()
	s.Require().NoError(err)

	altSQLMock.ExpectBegin()

	tx, err := altDB.BeginTx(ctx, nil)
	s.Require().NoError(err)

	s.PanicsWithError("sql: Tx.Stmt: statement from different database used", func() {
		postgresRepository.CloseNamedStmt(tx.StmtContext(ctx, stmt))
	})
}

func (s *PostgresTestSuite) TestInterpolateSQL() {
	params := map[string]any{"tableName": "test_table"}

	// Interpolate SQL successfully
	interpolatedSQL := postgres.InterpolateSQL("SELECT * FROM {{.tableName}}", params)
	s.Equal("SELECT * FROM test_table", interpolatedSQL)

	// Fail interpolaing unparseable SQL template
	s.Panics(func() {
		unparseableTemplateSQL := postgres.InterpolateSQL("SELECT * FROM {{.tableName", params)
		s.Empty(unparseableTemplateSQL)
	})

	// Fail interpolaing unexecutable SQL template
	s.Panics(func() {
		unexecutableTemplateSQL := postgres.InterpolateSQL(`SELECT * FROM {{if .tableName eq 1}} {{end}} {{.tableName}}`, params)
		s.Empty(unexecutableTemplateSQL)
	})
}
