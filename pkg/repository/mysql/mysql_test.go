package mysql

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
	ctx           context.Context
	testTask      = getStartedTestTask()
	testMySQLTask = newFromTask(testTask)
	taskColumns   = []string{
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
		testMySQLTask.ID,
		testMySQLTask.Type,
		testMySQLTask.Args,
		testMySQLTask.Queue,
		testMySQLTask.Priority,
		testMySQLTask.Status,
		testMySQLTask.ReceiveCount,
		testMySQLTask.MaxReceives,
		testMySQLTask.LastError,
		testMySQLTask.CreatedAt,
		testMySQLTask.StartedAt,
		testMySQLTask.FinishedAt,
		testMySQLTask.VisibleAt,
	}
	errSQL  = errors.New("sql error")
	errTask = errors.New("task error")
)

func getStartedTestTask() *model.Task {
	var (
		testTask, _ = model.NewTask("testTask", true, "testQueue", 100, 5)
		startTime   = testTask.CreatedAt.Add(time.Second)
	)

	testTask.StartedAt = &startTime

	return testTask
}

type MySQLTestSuite struct {
	suite.Suite
	db               *sql.DB
	sqlMock          sqlmock.Sqlmock
	mockedRepository repository.IRepository
}

func TestTaskTestSuite(t *testing.T) {
	t.Parallel()

	suite.Run(t, new(MySQLTestSuite))
}

func (s *MySQLTestSuite) SetupTest() {
	var err error

	ctx = context.Background()

	s.db, s.sqlMock, err = sqlmock.New()
	require.Nil(s.T(), err)

	s.mockedRepository, err = NewRepository(s.db, "test")
	require.NotNil(s.T(), s.mockedRepository)
	require.Nil(s.T(), err)
}

func (s *MySQLTestSuite) TestNewRepository() {
	// providing the datasource as *sql.DB
	dbMySQLRepository, err := NewRepository(s.db, "test")
	assert.NotNil(s.T(), dbMySQLRepository)
	assert.Equal(s.T(), "test_tasks", dbMySQLRepository.tableName())
	assert.Nil(s.T(), err)

	// providing the datasource as *sql.DB with no prefix
	noPrefixDBMySQLRepository, err := NewRepository(s.db, "")
	assert.NotNil(s.T(), noPrefixDBMySQLRepository)
	assert.Equal(s.T(), "tasks", noPrefixDBMySQLRepository.tableName())
	assert.Nil(s.T(), err)

	// providing the datasource as dsn string
	dsnRepository, err := NewRepository("root:root@/test", "test")
	assert.NotNil(s.T(), dsnRepository)
	assert.Nil(s.T(), err)

	// providing an invalid drivdsner as dsn string
	invalidDSNRepository, err := NewRepository("invalidDSN", "test")
	assert.Nil(s.T(), invalidDSNRepository)
	assert.NotNil(s.T(), err)

	// providing the datasource as unknown datasource type
	unknownDatasourceRepository, err := NewRepository(false, "test")
	assert.Nil(s.T(), unknownDatasourceRepository)
	assert.NotNil(s.T(), err)
}

func (s *MySQLTestSuite) TestDB() {
	dbRef := s.mockedRepository.DB()
	assert.NotNil(s.T(), dbRef)

	dbRef.SetMaxOpenConns(5)

	dbRef = s.mockedRepository.DB()
	assert.NotNil(s.T(), dbRef)
	assert.Equal(s.T(), 5, dbRef.Stats().MaxOpenConnections)
}

func (s *MySQLTestSuite) TestMigrate() {
	// First try - creating the tasks table fails
	s.sqlMock.ExpectExec(`CREATE TABLE IF NOT EXISTS test_tasks`).WillReturnError(errSQL)

	err := s.mockedRepository.Migrate(ctx)
	assert.NotNil(s.T(), err)

	// Second try - migration succeeds
	s.sqlMock.ExpectExec(`CREATE TABLE IF NOT EXISTS test_tasks`).WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.mockedRepository.Migrate(ctx)
	assert.Nil(s.T(), err)
}

func (s *MySQLTestSuite) TestPingTasks() {
	var (
		taskUUID         = uuid.New()
		taskUUIDBytes, _ = taskUUID.MarshalBinary()
		updateMockRegexp = regexp.QuoteMeta(`UPDATE test_tasks SET visible_at = ? WHERE id IN (?);`)
		selectMockRegexp = regexp.QuoteMeta(`SELECT * FROM test_tasks WHERE id IN (?);`)
	)

	// pinging empty tasklist
	noTasks, err := s.mockedRepository.PingTasks(ctx, []uuid.UUID{}, 15*time.Second)
	assert.Len(s.T(), noTasks, 0)
	assert.Nil(s.T(), err)

	// beginning the transaction fails
	s.sqlMock.ExpectBegin().WillReturnError(errSQL)
	tasks, err := s.mockedRepository.PingTasks(ctx, []uuid.UUID{taskUUID}, 15*time.Second)
	assert.Len(s.T(), tasks, 0)
	assert.NotNil(s.T(), err)

	// pinging when DB returns no rows
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	tasks, err = s.mockedRepository.PingTasks(ctx, []uuid.UUID{taskUUID}, 15*time.Second)
	assert.Len(s.T(), tasks, 0)
	assert.NotNil(s.T(), err)

	// pinging when DB returns no rows, rollback fails
	assert.PanicsWithError(s.T(), errSQL.Error(), func() {
		s.sqlMock.ExpectBegin()
		s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(errSQL)
		s.sqlMock.ExpectRollback().WillReturnError(errSQL)

		_, _ = s.mockedRepository.PingTasks(ctx, []uuid.UUID{taskUUID}, 15*time.Second)
	})

	// pinging existing task fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	tasks, err = s.mockedRepository.PingTasks(ctx, []uuid.UUID{taskUUID}, 15*time.Second)
	assert.Len(s.T(), tasks, 0)
	assert.NotNil(s.T(), err)

	// pinging existing task succeeds, commit fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUIDBytes))
	s.sqlMock.ExpectCommit().WillReturnError(errSQL)

	tasks, err = s.mockedRepository.PingTasks(ctx, []uuid.UUID{taskUUID}, 15*time.Second)
	assert.Len(s.T(), tasks, 0)
	assert.NotNil(s.T(), err)

	// pinging existing task succeeds, commit succeeds
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUIDBytes))
	s.sqlMock.ExpectCommit()

	tasks, err = s.mockedRepository.PingTasks(ctx, []uuid.UUID{taskUUID}, 15*time.Second)
	assert.Len(s.T(), tasks, 1)
	assert.Nil(s.T(), err)
}

func (s *MySQLTestSuite) TestPollTasks() {
	var (
		taskUUID         = uuid.New()
		taskUUIDBytes, _ = taskUUID.MarshalBinary()
		selectMockRegexp = regexp.QuoteMeta(`SELECT
				id
			FROM
				test_tasks
			WHERE
				type IN (?) AND
				queue IN (?) AND
				status IN (?, ?, ?) AND
				visible_at <= ?
			ORDER BY
				?, ?
			LIMIT ?
			FOR UPDATE SKIP LOCKED;`)
		updateMockRegexp = regexp.QuoteMeta(`UPDATE 
				test_tasks
			SET
				status = ?,
				receive_count = receive_count + 1,
				visible_at = ?
			WHERE
				id IN (?);`)
		selectUpdatedMockRegexp = regexp.QuoteMeta(`SELECT 
				* 
			FROM 
				test_tasks
			WHERE
				id IN (?);`)
	)

	// polling with 0 limit
	tasks, err := s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, []string{"created_at ASC", "priority DESC"}, 0)
	assert.Len(s.T(), tasks, 0)
	assert.Nil(s.T(), err)

	// beginning the transaction fails
	s.sqlMock.ExpectBegin().WillReturnError(errSQL)

	tasks, err = s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, []string{"created_at ASC", "priority DESC"}, 1)
	assert.Len(s.T(), tasks, 0)
	assert.NotNil(s.T(), err)

	// polling when DB returns an error
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	tasks, err = s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, []string{"created_at ASC", "priority DESC"}, 1)
	assert.Len(s.T(), tasks, 0)
	assert.NotNil(s.T(), err)

	// polling when DB returns no task IDs
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}))
	s.sqlMock.ExpectRollback()

	tasks, err = s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, []string{"created_at ASC", "priority DESC"}, 1)
	assert.Len(s.T(), tasks, 0)
	assert.Nil(s.T(), err)

	// polling when DB fails to update task
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUIDBytes))
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	tasks, err = s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, []string{"created_at ASC", "priority DESC"}, 1)
	assert.Len(s.T(), tasks, 0)
	assert.NotNil(s.T(), err)

	// polling when DB succeeds to update task but fails to select the updated tasks
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUIDBytes))
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectUpdatedMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	tasks, err = s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, []string{"created_at ASC", "priority DESC"}, 1)
	assert.Len(s.T(), tasks, 0)
	assert.NotNil(s.T(), err)

	// polling when DB succeeds to update task but fails to select the updated tasks
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUIDBytes))
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectUpdatedMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit().WillReturnError(errSQL)

	tasks, err = s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, []string{"created_at ASC", "priority DESC"}, 1)
	assert.Len(s.T(), tasks, 0)
	assert.NotNil(s.T(), err)

	// polling successfully
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUIDBytes))
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectUpdatedMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	tasks, err = s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, []string{"created_at ASC", "priority DESC"}, 1)
	assert.Len(s.T(), tasks, 1)
	assert.Nil(s.T(), err)
}

func (s *MySQLTestSuite) TestCleanTasks() {
	deleteMockRegexp := regexp.QuoteMeta(`DELETE 
		FROM 
			test_tasks 
		WHERE 
			status IN (?, ?) AND
			created_at <= ?;`)

	// cleaning when DB returns error
	s.sqlMock.ExpectExec(deleteMockRegexp).WillReturnError(errSQL)

	rowsAffected, err := s.mockedRepository.CleanTasks(ctx, time.Hour)
	assert.Zero(s.T(), rowsAffected)
	assert.NotNil(s.T(), err)

	// cleaning when no rows are found
	s.sqlMock.ExpectExec(deleteMockRegexp).WillReturnResult(driver.ResultNoRows)

	rowsAffected, err = s.mockedRepository.CleanTasks(ctx, time.Hour)
	assert.Equal(s.T(), int64(0), rowsAffected)
	assert.NotNil(s.T(), err)

	// cleaning successful
	s.sqlMock.ExpectExec(deleteMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))

	rowsAffected, err = s.mockedRepository.CleanTasks(ctx, time.Hour)
	assert.Equal(s.T(), int64(1), rowsAffected)
	assert.Nil(s.T(), err)
}

func (s *MySQLTestSuite) TestRegisterStart() {
	var (
		updateMockRegexp = regexp.QuoteMeta(`UPDATE 
				test_tasks
			SET
				status = ?,
				started_at = ?
			WHERE
				id = ?;`)
		selectMockRegexp = regexp.QuoteMeta(`SELECT * 
			FROM 
				test_tasks
			WHERE
				id = ?;`)
	)

	// beginning the transaction fails
	s.sqlMock.ExpectBegin().WillReturnError(errSQL)

	task, err := s.mockedRepository.RegisterStart(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering start when update fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterStart(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering start when update is successful but select fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterStart(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering start when commit fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit().WillReturnError(errSQL)

	task, err = s.mockedRepository.RegisterStart(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering error when update is successful but select returns no rows
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns))
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterStart(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering start successful
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	task, err = s.mockedRepository.RegisterStart(ctx, testTask)
	assert.NotEmpty(s.T(), task)
	assert.Nil(s.T(), err)
}

func (s *MySQLTestSuite) TestRegisterError() {
	var (
		updateMockRegexp = regexp.QuoteMeta(`UPDATE 
				test_tasks
			SET
				last_error = ?
			WHERE
				id = ?;`)
		selectMockRegexp = regexp.QuoteMeta(`SELECT * 
			FROM 
				test_tasks
			WHERE
				id = ?;`)
	)

	// beginning the transaction fails
	s.sqlMock.ExpectBegin().WillReturnError(errSQL)

	task, err := s.mockedRepository.RegisterError(ctx, testTask, errTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering error when update fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterError(ctx, testTask, errTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering error when update is successful but select fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterError(ctx, testTask, errTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering error when update is successful but select returns no rows
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns))
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterError(ctx, testTask, errTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering error when commit fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit().WillReturnError(errSQL)

	task, err = s.mockedRepository.RegisterError(ctx, testTask, errTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering error successful
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	task, err = s.mockedRepository.RegisterError(ctx, testTask, errTask)
	assert.NotEmpty(s.T(), task)
	assert.Nil(s.T(), err)
}

func (s *MySQLTestSuite) TestRegisterSuccess() {
	var (
		updateMockRegexp = regexp.QuoteMeta(`UPDATE 
				test_tasks
			SET
				status = ?,
				finished_at = ?
			WHERE
				id = ?;`)
		selectMockRegexp = regexp.QuoteMeta(`SELECT * 
			FROM 
				test_tasks
			WHERE
				id = ?;`)
	)

	// beginning the transaction fails
	s.sqlMock.ExpectBegin().WillReturnError(errSQL)

	task, err := s.mockedRepository.RegisterSuccess(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering success when update fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterSuccess(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering success when update is successful but select fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterSuccess(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering success when update is successful but select returns no rows
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns))
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterSuccess(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering success when commit fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit().WillReturnError(errSQL)

	task, err = s.mockedRepository.RegisterSuccess(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering success successful
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	task, err = s.mockedRepository.RegisterSuccess(ctx, testTask)
	assert.NotEmpty(s.T(), task)
	assert.Nil(s.T(), err)
}

func (s *MySQLTestSuite) TestRegisterFailure() {
	var (
		updateMockRegexp = regexp.QuoteMeta(`UPDATE 
				test_tasks
			SET
				status = ?,
				finished_at = ?
			WHERE
				id = ?;`)
		selectMockRegexp = regexp.QuoteMeta(`SELECT * 
			FROM 
				test_tasks
			WHERE
				id = ?;`)
	)

	// beginning the transaction fails
	s.sqlMock.ExpectBegin().WillReturnError(errSQL)

	task, err := s.mockedRepository.RegisterFailure(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering failure when update fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterFailure(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering failure when update is successful but select fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterFailure(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering failure when update is successful but select returns no rows
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns))
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterFailure(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering failure when commit fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit().WillReturnError(errSQL)

	task, err = s.mockedRepository.RegisterFailure(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering failure successful
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	task, err = s.mockedRepository.RegisterFailure(ctx, testTask)
	assert.NotEmpty(s.T(), task)
	assert.Nil(s.T(), err)
}

func (s *MySQLTestSuite) TestSubmitTask() {
	var (
		insertMockRegexp = regexp.QuoteMeta(`INSERT INTO 
				test_tasks
				(id, type, args, queue, priority, status, max_receives, created_at) 
			VALUES
				(?, ?, ?, ?, ?, ?, ?, ?);`)
		selectMockRegexp = regexp.QuoteMeta(`SELECT * 
			FROM 
				test_tasks
			WHERE
				id = ?;`)
	)

	// beginning the transaction fails
	s.sqlMock.ExpectBegin().WillReturnError(errSQL)

	task, err := s.mockedRepository.SubmitTask(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering failure when update fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(insertMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.SubmitTask(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering failure when update is successful but select fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(insertMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.SubmitTask(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering failure when update is successful but select returns no rows
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(insertMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns))
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.SubmitTask(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering failure when commit fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(insertMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit().WillReturnError(errSQL)

	task, err = s.mockedRepository.SubmitTask(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering failure successful
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(insertMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	task, err = s.mockedRepository.SubmitTask(ctx, testTask)
	assert.NotEmpty(s.T(), task)
	assert.Nil(s.T(), err)
}

func (s *MySQLTestSuite) TestDeleteTask() {
	deleteMockRegexp := regexp.QuoteMeta(`DELETE 
		FROM 
			test_tasks
		WHERE
			id = ?;`)

	// deleting when DB returns error
	s.sqlMock.ExpectExec(deleteMockRegexp).WillReturnError(errSQL)

	err := s.mockedRepository.DeleteTask(ctx, testTask)
	assert.NotNil(s.T(), err)

	// deleting successful
	s.sqlMock.ExpectExec(deleteMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.mockedRepository.DeleteTask(ctx, testTask)
	assert.Nil(s.T(), err)
}

func (s *MySQLTestSuite) TestRequeueTask() {
	var (
		updateMockRegexp = regexp.QuoteMeta(`UPDATE 
				test_tasks
			SET
				status = ?
			WHERE
				id = ?;`)
		selectMockRegexp = regexp.QuoteMeta(`SELECT * 
			FROM 
				test_tasks
			WHERE
				id = ?;`)
	)

	// beginning the transaction fails
	s.sqlMock.ExpectBegin().WillReturnError(errSQL)

	task, err := s.mockedRepository.RequeueTask(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// requeuing when update fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RequeueTask(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// requeuing when update is successful but select fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RequeueTask(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// requeuing when update is successful but select returns no rows
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns))
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RequeueTask(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// requeuing when commit fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit().WillReturnError(errSQL)

	task, err = s.mockedRepository.RequeueTask(ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// requeuing successful
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	task, err = s.mockedRepository.RequeueTask(ctx, testTask)
	assert.NotEmpty(s.T(), task)
	assert.Nil(s.T(), err)
}

func (s *MySQLTestSuite) TestGetQueryWithTableName() {
	var (
		taskUUID         = uuid.New()
		taskUUIDBytes, _ = taskUUID.MarshalBinary()
	)

	mysqlRepository, ok := s.mockedRepository.(*Repository)
	require.True(s.T(), ok)

	assert.Panics(s.T(), func() {
		_, _ = mysqlRepository.getQueryWithTableName("SELECT * FROM {{.tableName}} WHERE id = :taskID:;", map[string]any{
			"taskID": taskUUIDBytes,
		})
	})

	assert.Panics(s.T(), func() {
		_, _ = mysqlRepository.getQueryWithTableName("SELECT * FROM {{.tableName}} WHERE id IN (:taskIDs);", map[string]any{
			"taskIDs": [][]byte{},
		})
	})

	assert.NotPanics(s.T(), func() {
		query, args := mysqlRepository.getQueryWithTableName("SELECT * FROM {{.tableName}} WHERE id = :taskID", map[string]any{
			"taskID": taskUUIDBytes,
		})
		assert.Equal(s.T(), "SELECT * FROM test_tasks WHERE id = ?", query)
		assert.Contains(s.T(), args, taskUUIDBytes)
	})
}
