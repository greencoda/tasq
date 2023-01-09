package postgres

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
	"github.com/greencoda/tasq/internal/model"
	"github.com/greencoda/tasq/pkg/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var (
	ctx              context.Context
	testTask, _      = model.NewTask("testTask", true, "testQueue", 100, 5)
	testPostgresTask = newFromTask(testTask)
	taskColumns      = []string{
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
	taskValues = []driver.Value{
		testPostgresTask.ID,
		testPostgresTask.Type,
		testPostgresTask.Args,
		testPostgresTask.Queue,
		testPostgresTask.Priority,
		testPostgresTask.Status,
		testPostgresTask.ReceiveCount,
		testPostgresTask.MaxReceives,
		testPostgresTask.LastError,
		testPostgresTask.CreatedAt,
		testPostgresTask.StartedAt,
		testPostgresTask.FinishedAt,
		testPostgresTask.VisibleAt,
	}
	errSQL  = errors.New("sql error")
	errTask = errors.New("task error")
)

type PostgresTestSuite struct {
	suite.Suite
	db               *sql.DB
	sqlMock          sqlmock.Sqlmock
	mockedRepository repository.IRepository
}

func TestTaskTestSuite(t *testing.T) {
	t.Parallel()

	suite.Run(t, new(PostgresTestSuite))
}

func (s *PostgresTestSuite) SetupTest() {
	var err error

	ctx = context.Background()

	s.db, s.sqlMock, err = sqlmock.New()
	require.Nil(s.T(), err)

	s.mockedRepository, err = NewRepository(s.db, "test")
	require.NotNil(s.T(), s.mockedRepository)
	require.Nil(s.T(), err)
}

func (s *PostgresTestSuite) TestNewRepository() {
	// providing the datasource as *sql.DB
	dbPostgresRepository, err := NewRepository(s.db, "test")
	assert.Equal(s.T(), "test_tasks", dbPostgresRepository.tableName())
	assert.Equal(s.T(), "test_task_status", dbPostgresRepository.statusTypeName())
	assert.NotNil(s.T(), dbPostgresRepository)
	assert.Nil(s.T(), err)

	// providing the datasource as *sql.DB with no prefix
	noPrefixDBPostgresRepository, err := NewRepository(s.db, "")
	assert.Equal(s.T(), "tasks", noPrefixDBPostgresRepository.tableName())
	assert.Equal(s.T(), "task_status", noPrefixDBPostgresRepository.statusTypeName())
	assert.NotNil(s.T(), noPrefixDBPostgresRepository)
	assert.Nil(s.T(), err)

	// providing the datasource as dsn string
	dsnRepository, err := NewRepository("testDSN", "test")
	assert.NotNil(s.T(), dsnRepository)
	assert.Nil(s.T(), err)

	// providing the datasource as unknown datasource type
	unknownDatasourceRepository, err := NewRepository(false, "test")
	assert.Nil(s.T(), unknownDatasourceRepository)
	assert.NotNil(s.T(), err)
}

func (s *PostgresTestSuite) TestDB() {
	dbRef := s.mockedRepository.DB()
	assert.NotNil(s.T(), dbRef)

	dbRef.SetMaxOpenConns(5)

	dbRef = s.mockedRepository.DB()
	assert.NotNil(s.T(), dbRef)
	assert.Equal(s.T(), 5, dbRef.Stats().MaxOpenConnections)
}

func (s *PostgresTestSuite) TestMigrate() {
	// First try - creating the task_status type fails
	s.sqlMock.ExpectExec(`CREATE TYPE test_task_status AS ENUM`).WillReturnError(errSQL)

	err := s.mockedRepository.Migrate(ctx)
	assert.NotNil(s.T(), err)

	// Second try - creating the tasks table fails
	s.sqlMock.ExpectExec(`CREATE TYPE test_task_status AS ENUM`).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectExec(`CREATE TABLE IF NOT EXISTS test_tasks`).WillReturnError(errSQL)

	err = s.mockedRepository.Migrate(ctx)
	assert.NotNil(s.T(), err)

	// Third try - migration succeeds
	s.sqlMock.ExpectExec(`CREATE TYPE test_task_status AS ENUM`).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectExec(`CREATE TABLE IF NOT EXISTS test_tasks`).WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.mockedRepository.Migrate(ctx)
	assert.Nil(s.T(), err)
}

func (s *PostgresTestSuite) TestPingTasks() {
	var (
		taskUUID       = uuid.New()
		stmtMockRegexp = regexp.QuoteMeta(`UPDATE test_tasks SET "visible_at" = $1 WHERE "id" = ANY($2) RETURNING id;`)
	)
	// pinging empty tasklist
	noTasks, err := s.mockedRepository.PingTasks(ctx, []uuid.UUID{}, 15*time.Second)
	assert.Len(s.T(), noTasks, 0)
	assert.Nil(s.T(), err)

	// pinging when DB returns no rows
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnError(errSQL)

	errTasks, err := s.mockedRepository.PingTasks(ctx, []uuid.UUID{taskUUID}, 15*time.Second)
	assert.Len(s.T(), errTasks, 0)
	assert.NotNil(s.T(), err)

	// pinging existing task
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUID))

	tasks, err := s.mockedRepository.PingTasks(ctx, []uuid.UUID{taskUUID}, 15*time.Second)
	assert.Len(s.T(), tasks, 1)
	assert.Nil(s.T(), err)
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
	zeroLimitTasks, err := s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, []string{"created_at ASC", "priority DESC"}, 0)
	assert.Len(s.T(), zeroLimitTasks, 0)
	assert.Nil(s.T(), err)

	// polling when DB returns no rows
	s.sqlMock.ExpectPrepare(stmtMockRegexp).
		ExpectQuery().
		WillReturnError(sql.ErrNoRows)

	noTasks, err := s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, []string{"created_at ASC", "priority DESC"}, 1)
	assert.Len(s.T(), noTasks, 0)
	assert.Nil(s.T(), err)

	// polling when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).
		ExpectQuery().
		WillReturnError(errSQL)

	errTasks, err := s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, []string{"created_at ASC", "priority DESC"}, 1)
	assert.Len(s.T(), errTasks, 0)
	assert.NotNil(s.T(), err)

	// polling for existing tasks
	s.sqlMock.ExpectPrepare(stmtMockRegexp).
		ExpectQuery().
		WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))

	tasks, err := s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, []string{"created_at ASC", "priority DESC"}, 1)
	assert.Len(s.T(), tasks, 1)
	assert.Nil(s.T(), err)
}

func (s *PostgresTestSuite) TestCleanTasks() {
	stmtMockRegexp := regexp.QuoteMeta(`DELETE FROM test_tasks WHERE "status" = ANY($1) AND "created_at" <= $2;`)

	// cleaning when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectExec().WillReturnError(errSQL)

	rowsAffected, err := s.mockedRepository.CleanTasks(ctx, time.Hour)
	assert.Zero(s.T(), rowsAffected)
	assert.NotNil(s.T(), err)

	// cleaning when no rows are found
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectExec().WillReturnResult(driver.ResultNoRows)

	rowsAffected, err = s.mockedRepository.CleanTasks(ctx, time.Hour)
	assert.Equal(s.T(), int64(0), rowsAffected)
	assert.NotNil(s.T(), err)

	// cleaning successful
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectExec().WillReturnResult(sqlmock.NewResult(1, 1))

	rowsAffected, err = s.mockedRepository.CleanTasks(ctx, time.Hour)
	assert.Equal(s.T(), int64(1), rowsAffected)
	assert.Nil(s.T(), err)
}

func (s *PostgresTestSuite) TestRegisterStart() {
	stmtMockRegexp := regexp.QuoteMeta(`UPDATE test_tasks SET "status" = $1, "started_at" = $2 WHERE "id" = $3 RETURNING *;`)

	// registering start when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnError(errSQL)

	errStartedTask, err := s.mockedRepository.RegisterStart(ctx, testTask)
	assert.Empty(s.T(), errStartedTask)
	assert.NotNil(s.T(), err)

	// registering start successful
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))

	startedTask, err := s.mockedRepository.RegisterStart(ctx, testTask)
	assert.NotEmpty(s.T(), startedTask)
	assert.Nil(s.T(), err)
}

func (s *PostgresTestSuite) TestRegisterError() {
	stmtMockRegexp := regexp.QuoteMeta(`UPDATE test_tasks SET "last_error" = $1 WHERE "id" = $2 RETURNING *;`)

	// registering error when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnError(errSQL)

	errErroredTask, err := s.mockedRepository.RegisterError(ctx, testTask, errTask)
	assert.Empty(s.T(), errErroredTask)
	assert.NotNil(s.T(), err)

	// registering error successful
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))

	erroredTask, err := s.mockedRepository.RegisterError(ctx, testTask, errTask)
	assert.NotEmpty(s.T(), erroredTask)
	assert.Nil(s.T(), err)
}

func (s *PostgresTestSuite) TestRegisterSuccess() {
	stmtMockRegexp := regexp.QuoteMeta(`UPDATE test_tasks SET "status" = $1, "finished_at" = $2 WHERE "id" = $3 RETURNING *;`)

	// registering success when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnError(errSQL)

	errSuccessfulTask, err := s.mockedRepository.RegisterSuccess(ctx, testTask)
	assert.Empty(s.T(), errSuccessfulTask)
	assert.NotNil(s.T(), err)

	// registering success successful
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))

	successfulTask, err := s.mockedRepository.RegisterSuccess(ctx, testTask)
	assert.NotEmpty(s.T(), successfulTask)
	assert.Nil(s.T(), err)
}

func (s *PostgresTestSuite) TestRegisterFailure() {
	stmtMockRegexp := regexp.QuoteMeta(`UPDATE test_tasks SET "status" = $1, "finished_at" = $2 WHERE "id" = $3 RETURNING *;`)

	// registering failure when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnError(errSQL)

	errFailedTask, err := s.mockedRepository.RegisterFailure(ctx, testTask)
	assert.Empty(s.T(), errFailedTask)
	assert.NotNil(s.T(), err)

	// registering failure successful
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))

	failedTask, err := s.mockedRepository.RegisterFailure(ctx, testTask)
	assert.NotEmpty(s.T(), failedTask)
	assert.Nil(s.T(), err)
}

func (s *PostgresTestSuite) TestSubmitTask() {
	stmtMockRegexp := regexp.QuoteMeta(`INSERT INTO test_tasks  (id, type, args, queue, priority, status, max_receives, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *;`)

	// submitting task when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnError(errSQL)

	errSubmittedTask, err := s.mockedRepository.SubmitTask(ctx, testTask)
	assert.Empty(s.T(), errSubmittedTask)
	assert.NotNil(s.T(), err)

	// submitting task successful
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))

	submittedTask, err := s.mockedRepository.SubmitTask(ctx, testTask)
	assert.NotEmpty(s.T(), submittedTask)
	assert.Nil(s.T(), err)
}

func (s *PostgresTestSuite) TestDeleteTask() {
	stmtMockRegexp := regexp.QuoteMeta(`DELETE FROM test_tasks WHERE "id" = $1;`)

	// deleting task when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectExec().WillReturnError(errSQL)

	err := s.mockedRepository.DeleteTask(ctx, testTask)
	assert.NotNil(s.T(), err)

	// deleting task successful
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectExec().WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.mockedRepository.DeleteTask(ctx, testTask)
	assert.Nil(s.T(), err)
}

func (s *PostgresTestSuite) TestRequeueTask() {
	stmtMockRegexp := regexp.QuoteMeta(`UPDATE test_tasks SET "status" = $1 WHERE "id" = $2 RETURNING *;`)

	// requeuing task when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnError(errSQL)

	errRequeuedTask, err := s.mockedRepository.RequeueTask(ctx, testTask)
	assert.Empty(s.T(), errRequeuedTask)
	assert.NotNil(s.T(), err)

	// requeuing task successful
	s.sqlMock.ExpectPrepare(stmtMockRegexp).ExpectQuery().WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))

	requeuedTask, err := s.mockedRepository.RequeueTask(ctx, testTask)
	assert.NotEmpty(s.T(), requeuedTask)
	assert.Nil(s.T(), err)
}

func (s *PostgresTestSuite) TestPrepareWithTableName() {
	stmtMockRegexp := regexp.QuoteMeta(`SELECT * FROM test_tasks`)

	postgresRepository, ok := s.mockedRepository.(*Repository)
	require.True(s.T(), ok)

	// preparing stmt with table name when DB returns error
	s.sqlMock.ExpectPrepare(stmtMockRegexp).WillReturnError(errSQL)

	assert.PanicsWithError(s.T(), "sql error", func() {
		_ = postgresRepository.prepareWithTableName("SELECT * FROM {{.tableName}}")
	})
}
