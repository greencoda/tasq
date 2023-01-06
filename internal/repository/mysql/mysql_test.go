package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"log"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/greencoda/tasq/internal/model"
	"github.com/greencoda/tasq/internal/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var (
	testTask      = model.NewTask("testTask", true, "testQueue", 100, 5)
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
	sqlError  = errors.New("sql error")
	taskError = errors.New("task error")
)

type MySQLTestSuite struct {
	suite.Suite
	ctx              context.Context
	db               *sql.DB
	sqlMock          sqlmock.Sqlmock
	mockedRepository repository.IRepository
}

func TestTaskTestSuite(t *testing.T) {
	suite.Run(t, new(MySQLTestSuite))
}

func (s *MySQLTestSuite) SetupTest() {
	var err error

	s.ctx = context.Background()

	s.db, s.sqlMock, err = sqlmock.New()
	require.Nil(s.T(), err)

	s.mockedRepository, err = NewRepository(s.db, "mysql", "test")
	require.NotNil(s.T(), s.mockedRepository)
	require.Nil(s.T(), err)
}

func (s *MySQLTestSuite) TestNewRepository() {
	// providing the datasource as *sql.DB
	dbRepository, err := NewRepository(s.db, "mysql", "test")
	assert.NotNil(s.T(), dbRepository)
	assert.Nil(s.T(), err)

	dbMySQLRepository, ok := dbRepository.(*mysqlRepository)
	require.True(s.T(), ok)
	assert.Equal(s.T(), "test_tasks", dbMySQLRepository.tableName())

	// providing the datasource as *sql.DB with no prefix
	noPrefixDBRepository, err := NewRepository(s.db, "mysql", "")
	assert.NotNil(s.T(), noPrefixDBRepository)
	assert.Nil(s.T(), err)

	noPrefixDBMySQLRepository, ok := noPrefixDBRepository.(*mysqlRepository)
	require.True(s.T(), ok)
	assert.Equal(s.T(), "tasks", noPrefixDBMySQLRepository.tableName())

	// providing the datasource as dsn string
	dsnRepository, err := NewRepository("root:root@/test", "mysql", "test")
	assert.NotNil(s.T(), dsnRepository)
	log.Printf("%v", err)
	assert.Nil(s.T(), err)

	// providing an invalid drivdsner as dsn string
	invalidDSNRepository, err := NewRepository("invalidDSN", "mysql", "test")
	assert.Nil(s.T(), invalidDSNRepository)
	assert.NotNil(s.T(), err)

	// providing an invalid driver as dsn string
	unknownDriverRepository, err := NewRepository("root:root@/test", "unknown", "test")
	assert.Nil(s.T(), unknownDriverRepository)
	assert.NotNil(s.T(), err)

	// providing the datasource as unknown datasource type
	unknownDatasourceRepository, err := NewRepository(false, "mysql", "test")
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
	s.sqlMock.ExpectExec(`CREATE TABLE IF NOT EXISTS test_tasks`).WillReturnError(sqlError)

	err := s.mockedRepository.Migrate(s.ctx)
	assert.NotNil(s.T(), err)

	// Second try - migration succeeds
	s.sqlMock.ExpectExec(`CREATE TABLE IF NOT EXISTS test_tasks`).WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.mockedRepository.Migrate(s.ctx)
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
	noTasks, err := s.mockedRepository.PingTasks(s.ctx, []uuid.UUID{}, 15*time.Second)
	assert.Len(s.T(), noTasks, 0)
	assert.Nil(s.T(), err)

	// beginning the transaction fails
	s.sqlMock.ExpectBegin().WillReturnError(sqlError)
	tasks, err := s.mockedRepository.PingTasks(s.ctx, []uuid.UUID{taskUUID}, 15*time.Second)
	assert.Len(s.T(), tasks, 0)
	assert.NotNil(s.T(), err)

	// pinging when DB returns no rows
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(sqlError)
	s.sqlMock.ExpectRollback()

	tasks, err = s.mockedRepository.PingTasks(s.ctx, []uuid.UUID{taskUUID}, 15*time.Second)
	assert.Len(s.T(), tasks, 0)
	assert.NotNil(s.T(), err)

	// pinging when DB returns no rows, rollback fails
	assert.PanicsWithError(s.T(), sqlError.Error(), func() {
		s.sqlMock.ExpectBegin()
		s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(sqlError)
		s.sqlMock.ExpectRollback().WillReturnError(sqlError)

		_, _ = s.mockedRepository.PingTasks(s.ctx, []uuid.UUID{taskUUID}, 15*time.Second)
	})

	// pinging existing task fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(sqlError)
	s.sqlMock.ExpectRollback()

	tasks, err = s.mockedRepository.PingTasks(s.ctx, []uuid.UUID{taskUUID}, 15*time.Second)
	assert.Len(s.T(), tasks, 0)
	assert.NotNil(s.T(), err)

	// pinging existing task succeeds, commit fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUIDBytes))
	s.sqlMock.ExpectCommit().WillReturnError(sqlError)

	tasks, err = s.mockedRepository.PingTasks(s.ctx, []uuid.UUID{taskUUID}, 15*time.Second)
	assert.Len(s.T(), tasks, 0)
	assert.NotNil(s.T(), err)

	// pinging existing task succeeds, commit succeeds
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUIDBytes))
	s.sqlMock.ExpectCommit()

	tasks, err = s.mockedRepository.PingTasks(s.ctx, []uuid.UUID{taskUUID}, 15*time.Second)
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
	tasks, err := s.mockedRepository.PollTasks(s.ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, []string{"created_at ASC", "priority DESC"}, 0)
	assert.Len(s.T(), tasks, 0)
	assert.Nil(s.T(), err)

	// beginning the transaction fails
	s.sqlMock.ExpectBegin().WillReturnError(sqlError)

	tasks, err = s.mockedRepository.PollTasks(s.ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, []string{"created_at ASC", "priority DESC"}, 1)
	assert.Len(s.T(), tasks, 0)
	assert.NotNil(s.T(), err)

	// polling when DB returns an error
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(sqlError)
	s.sqlMock.ExpectRollback()

	tasks, err = s.mockedRepository.PollTasks(s.ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, []string{"created_at ASC", "priority DESC"}, 1)
	assert.Len(s.T(), tasks, 0)
	assert.NotNil(s.T(), err)

	// polling when DB returns no task IDs
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}))
	s.sqlMock.ExpectRollback()

	tasks, err = s.mockedRepository.PollTasks(s.ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, []string{"created_at ASC", "priority DESC"}, 1)
	assert.Len(s.T(), tasks, 0)
	assert.Nil(s.T(), err)

	// polling when DB fails to update task
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUIDBytes))
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(sqlError)
	s.sqlMock.ExpectRollback()

	tasks, err = s.mockedRepository.PollTasks(s.ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, []string{"created_at ASC", "priority DESC"}, 1)
	assert.Len(s.T(), tasks, 0)
	assert.NotNil(s.T(), err)

	// polling when DB succeeds to update task but fails to select the updated tasks
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUIDBytes))
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectUpdatedMockRegexp).WillReturnError(sqlError)
	s.sqlMock.ExpectRollback()

	tasks, err = s.mockedRepository.PollTasks(s.ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, []string{"created_at ASC", "priority DESC"}, 1)
	assert.Len(s.T(), tasks, 0)
	assert.NotNil(s.T(), err)

	// polling when DB succeeds to update task but fails to select the updated tasks
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUIDBytes))
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectUpdatedMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit().WillReturnError(sqlError)

	tasks, err = s.mockedRepository.PollTasks(s.ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, []string{"created_at ASC", "priority DESC"}, 1)
	assert.Len(s.T(), tasks, 0)
	assert.NotNil(s.T(), err)

	// polling successfully
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUIDBytes))
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectUpdatedMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	tasks, err = s.mockedRepository.PollTasks(s.ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, []string{"created_at ASC", "priority DESC"}, 1)
	assert.Len(s.T(), tasks, 1)
	assert.Nil(s.T(), err)
}

func (s *MySQLTestSuite) TestCleanTasks() {
	var deleteMockRegexp = regexp.QuoteMeta(`DELETE 
		FROM 
			test_tasks 
		WHERE 
			status IN (?, ?) AND
			created_at <= ?;`)

	// cleaning when DB returns error
	s.sqlMock.ExpectExec(deleteMockRegexp).WillReturnError(sqlError)

	rowsAffected, err := s.mockedRepository.CleanTasks(s.ctx, time.Hour)
	assert.Zero(s.T(), rowsAffected)
	assert.NotNil(s.T(), err)

	// cleaning when no rows are found
	s.sqlMock.ExpectExec(deleteMockRegexp).WillReturnResult(driver.ResultNoRows)

	rowsAffected, err = s.mockedRepository.CleanTasks(s.ctx, time.Hour)
	assert.Equal(s.T(), int64(0), rowsAffected)
	assert.NotNil(s.T(), err)

	// cleaning successful
	s.sqlMock.ExpectExec(deleteMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))

	rowsAffected, err = s.mockedRepository.CleanTasks(s.ctx, time.Hour)
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
	s.sqlMock.ExpectBegin().WillReturnError(sqlError)

	task, err := s.mockedRepository.RegisterStart(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering start when update fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(sqlError)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterStart(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering start when update is successful but select fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(sqlError)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterStart(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering start when commit fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit().WillReturnError(sqlError)

	task, err = s.mockedRepository.RegisterStart(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering error when update is successful but select returns no rows
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns))
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterStart(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.Nil(s.T(), err)

	// registering start successful
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	task, err = s.mockedRepository.RegisterStart(s.ctx, testTask)
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
	s.sqlMock.ExpectBegin().WillReturnError(sqlError)

	task, err := s.mockedRepository.RegisterError(s.ctx, testTask, taskError)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering error when update fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(sqlError)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterError(s.ctx, testTask, taskError)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering error when update is successful but select fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(sqlError)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterError(s.ctx, testTask, taskError)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering error when update is successful but select returns no rows
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns))
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterError(s.ctx, testTask, taskError)
	assert.Empty(s.T(), task)
	assert.Nil(s.T(), err)

	// registering error when commit fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit().WillReturnError(sqlError)

	task, err = s.mockedRepository.RegisterError(s.ctx, testTask, taskError)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering error successful
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	task, err = s.mockedRepository.RegisterError(s.ctx, testTask, taskError)
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
	s.sqlMock.ExpectBegin().WillReturnError(sqlError)

	task, err := s.mockedRepository.RegisterSuccess(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering success when update fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(sqlError)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterSuccess(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering success when update is successful but select fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(sqlError)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterSuccess(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering success when update is successful but select returns no rows
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns))
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterSuccess(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.Nil(s.T(), err)

	// registering success when commit fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit().WillReturnError(sqlError)

	task, err = s.mockedRepository.RegisterSuccess(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering success successful
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	task, err = s.mockedRepository.RegisterSuccess(s.ctx, testTask)
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
	s.sqlMock.ExpectBegin().WillReturnError(sqlError)

	task, err := s.mockedRepository.RegisterFailure(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering failure when update fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(sqlError)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterFailure(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering failure when update is successful but select fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(sqlError)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterFailure(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering failure when update is successful but select returns no rows
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns))
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterFailure(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.Nil(s.T(), err)

	// registering failure when commit fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit().WillReturnError(sqlError)

	task, err = s.mockedRepository.RegisterFailure(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering failure successful
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	task, err = s.mockedRepository.RegisterFailure(s.ctx, testTask)
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
	s.sqlMock.ExpectBegin().WillReturnError(sqlError)

	task, err := s.mockedRepository.SubmitTask(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering failure when update fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(insertMockRegexp).WillReturnError(sqlError)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.SubmitTask(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering failure when update is successful but select fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(insertMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(sqlError)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.SubmitTask(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering failure when update is successful but select returns no rows
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(insertMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns))
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.SubmitTask(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.Nil(s.T(), err)

	// registering failure when commit fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(insertMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit().WillReturnError(sqlError)

	task, err = s.mockedRepository.SubmitTask(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// registering failure successful
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(insertMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	task, err = s.mockedRepository.SubmitTask(s.ctx, testTask)
	assert.NotEmpty(s.T(), task)
	assert.Nil(s.T(), err)
}

func (s *MySQLTestSuite) TestDeleteTask() {
	var deleteMockRegexp = regexp.QuoteMeta(`DELETE 
		FROM 
			test_tasks
		WHERE
			id = ?;`)

	// deleting when DB returns error
	s.sqlMock.ExpectExec(deleteMockRegexp).WillReturnError(sqlError)

	err := s.mockedRepository.DeleteTask(s.ctx, testTask)
	assert.NotNil(s.T(), err)

	// deleting successful
	s.sqlMock.ExpectExec(deleteMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.mockedRepository.DeleteTask(s.ctx, testTask)
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
	s.sqlMock.ExpectBegin().WillReturnError(sqlError)

	task, err := s.mockedRepository.RequeueTask(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// requeuing when update fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(sqlError)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RequeueTask(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// requeuing when update is successful but select fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(sqlError)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RequeueTask(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// requeuing when update is successful but select returns no rows
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns))
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RequeueTask(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.Nil(s.T(), err)

	// requeuing when commit fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit().WillReturnError(sqlError)

	task, err = s.mockedRepository.RequeueTask(s.ctx, testTask)
	assert.Empty(s.T(), task)
	assert.NotNil(s.T(), err)

	// requeuing successful
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	task, err = s.mockedRepository.RequeueTask(s.ctx, testTask)
	assert.NotEmpty(s.T(), task)
	assert.Nil(s.T(), err)
}

func (s *MySQLTestSuite) TestGetQueryWithTableName() {
	var (
		taskUUID         = uuid.New()
		taskUUIDBytes, _ = taskUUID.MarshalBinary()
	)

	mysqlRepository, ok := s.mockedRepository.(*mysqlRepository)
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
