package mysql_test

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
	"github.com/greencoda/tasq/repository/mysql"
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
	taskValues    = mysql.GetTestTaskValues(testTask)
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

type MySQLTestSuite struct {
	suite.Suite

	db               *sql.DB
	sqlMock          sqlmock.Sqlmock
	mockedRepository tasq.IRepository
}

func TestTaskTestSuite(t *testing.T) {
	t.Parallel()

	suite.Run(t, new(MySQLTestSuite))
}

func (s *MySQLTestSuite) SetupTest() {
	var err error

	s.db, s.sqlMock, err = sqlmock.New()
	s.Require().NoError(err)

	s.mockedRepository, err = mysql.NewRepository(s.db, "test")
	s.Require().NotNil(s.mockedRepository)
	s.Require().NoError(err)
}

func (s *MySQLTestSuite) TestNewRepository() {
	// providing the datasource as *sql.DB
	repository, err := mysql.NewRepository(s.db, "test")
	s.NotNil(repository)
	s.NoError(err)

	// providing the datasource as *sql.DB with no prefix
	repository, err = mysql.NewRepository(s.db, "")
	s.NotNil(repository)
	s.NoError(err)

	// providing the datasource as dsn string
	repository, err = mysql.NewRepository("root:root@/test", "test")
	s.NotNil(repository)
	s.NoError(err)

	// providing an invalid drivdsner as dsn string
	repository, err = mysql.NewRepository("invalidDSN", "test")
	s.Nil(repository)
	s.Error(err)

	// providing the datasource as unknown datasource type
	repository, err = mysql.NewRepository(false, "test")
	s.Nil(repository)
	s.Error(err)
}

func (s *MySQLTestSuite) TestMigrate() {
	// First try - creating the tasks table fails
	s.sqlMock.ExpectExec(`CREATE TABLE IF NOT EXISTS test_tasks`).WillReturnError(errSQL)

	err := s.mockedRepository.Migrate(ctx)
	s.Error(err)

	// Second try - migration succeeds
	s.sqlMock.ExpectExec(`CREATE TABLE IF NOT EXISTS test_tasks`).WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.mockedRepository.Migrate(ctx)
	s.NoError(err)
}

func (s *MySQLTestSuite) TestPingTasks() {
	var (
		taskUUID         = uuid.New()
		taskUUIDBytes, _ = taskUUID.MarshalBinary()
		updateMockRegexp = regexp.QuoteMeta(`UPDATE test_tasks SET visible_at = ? WHERE id IN (?);`)
		selectMockRegexp = regexp.QuoteMeta(`SELECT * FROM test_tasks WHERE id IN (?);`)
	)

	// pinging empty tasklist
	tasks, err := s.mockedRepository.PingTasks(ctx, []uuid.UUID{}, 15*time.Second)
	s.Empty(tasks)
	s.NoError(err)

	// beginning the transaction fails
	s.sqlMock.ExpectBegin().WillReturnError(errSQL)
	tasks, err = s.mockedRepository.PingTasks(ctx, []uuid.UUID{taskUUID}, 15*time.Second)
	s.Empty(tasks)
	s.Error(err)

	// pinging when DB returns no rows
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	tasks, err = s.mockedRepository.PingTasks(ctx, []uuid.UUID{taskUUID}, 15*time.Second)
	s.Empty(tasks)
	s.Error(err)

	// pinging when DB returns no rows, rollback fails
	s.PanicsWithError(errSQL.Error(), func() {
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
	s.Empty(tasks)
	s.Error(err)

	// pinging existing task succeeds, commit fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUIDBytes))
	s.sqlMock.ExpectCommit().WillReturnError(errSQL)

	tasks, err = s.mockedRepository.PingTasks(ctx, []uuid.UUID{taskUUID}, 15*time.Second)
	s.Empty(tasks)
	s.Error(err)

	// pinging existing task succeeds, commit succeeds
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUIDBytes))
	s.sqlMock.ExpectCommit()

	tasks, err = s.mockedRepository.PingTasks(ctx, []uuid.UUID{taskUUID}, 15*time.Second)
	s.Len(tasks, 1)
	s.NoError(err)
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
	tasks, err := s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, tasq.OrderingCreatedAtFirst, 0)
	s.Empty(tasks)
	s.NoError(err)

	// beginning the transaction fails
	s.sqlMock.ExpectBegin().WillReturnError(errSQL)

	tasks, err = s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, tasq.OrderingCreatedAtFirst, 1)
	s.Empty(tasks)
	s.Error(err)

	// polling when DB returns an error
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	tasks, err = s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, tasq.OrderingCreatedAtFirst, 1)
	s.Empty(tasks)
	s.Error(err)

	// polling when DB returns no task IDs
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}))
	s.sqlMock.ExpectRollback()

	tasks, err = s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, tasq.OrderingCreatedAtFirst, 1)
	s.Empty(tasks)
	s.NoError(err)

	// polling when DB fails to update task
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUIDBytes))
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	tasks, err = s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, tasq.OrderingCreatedAtFirst, 1)
	s.Empty(tasks)
	s.Error(err)

	// polling when DB succeeds to update task but fails to select the updated tasks
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUIDBytes))
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectUpdatedMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	tasks, err = s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, tasq.OrderingCreatedAtFirst, 1)
	s.Empty(tasks)
	s.Error(err)

	// polling when DB succeeds to update task but fails to select the updated tasks
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUIDBytes))
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectUpdatedMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit().WillReturnError(errSQL)

	tasks, err = s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, tasq.OrderingCreatedAtFirst, 1)
	s.Empty(tasks)
	s.Error(err)

	// polling successfully
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUIDBytes))
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectUpdatedMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	tasks, err = s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, tasq.OrderingCreatedAtFirst, 1)
	s.Len(tasks, 1)
	s.NoError(err)

	// polling successfully with unknown ordering
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskUUIDBytes))
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectUpdatedMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	tasks, err = s.mockedRepository.PollTasks(ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, -1, 1)
	s.Len(tasks, 1)
	s.NoError(err)
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
	s.Zero(rowsAffected)
	s.Error(err)

	// cleaning when no rows are found
	s.sqlMock.ExpectExec(deleteMockRegexp).WillReturnResult(driver.ResultNoRows)

	rowsAffected, err = s.mockedRepository.CleanTasks(ctx, time.Hour)
	s.Equal(int64(0), rowsAffected)
	s.Error(err)

	// cleaning successful
	s.sqlMock.ExpectExec(deleteMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))

	rowsAffected, err = s.mockedRepository.CleanTasks(ctx, time.Hour)
	s.Equal(int64(1), rowsAffected)
	s.NoError(err)
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
	s.Empty(task)
	s.Error(err)

	// registering start when update fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterStart(ctx, testTask)
	s.Empty(task)
	s.Error(err)

	// registering start when update is successful but select fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterStart(ctx, testTask)
	s.Empty(task)
	s.Error(err)

	// registering start when commit fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit().WillReturnError(errSQL)

	task, err = s.mockedRepository.RegisterStart(ctx, testTask)
	s.Empty(task)
	s.Error(err)

	// registering error when update is successful but select returns no rows
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns))
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterStart(ctx, testTask)
	s.Empty(task)
	s.Error(err)

	// registering start successful
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	task, err = s.mockedRepository.RegisterStart(ctx, testTask)
	s.NotEmpty(task)
	s.NoError(err)
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
	s.Empty(task)
	s.Error(err)

	// registering error when update fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterError(ctx, testTask, errTask)
	s.Empty(task)
	s.Error(err)

	// registering error when update is successful but select fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterError(ctx, testTask, errTask)
	s.Empty(task)
	s.Error(err)

	// registering error when update is successful but select returns no rows
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns))
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterError(ctx, testTask, errTask)
	s.Empty(task)
	s.Error(err)

	// registering error when commit fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit().WillReturnError(errSQL)

	task, err = s.mockedRepository.RegisterError(ctx, testTask, errTask)
	s.Empty(task)
	s.Error(err)

	// registering error successful
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	task, err = s.mockedRepository.RegisterError(ctx, testTask, errTask)
	s.NotEmpty(task)
	s.NoError(err)
}

func (s *MySQLTestSuite) TestRegisterFinish() {
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

	task, err := s.mockedRepository.RegisterFinish(ctx, testTask, tasq.StatusSuccessful)
	s.Empty(task)
	s.Error(err)

	// registering success when update fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterFinish(ctx, testTask, tasq.StatusSuccessful)
	s.Empty(task)
	s.Error(err)

	// registering success when update is successful but select fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterFinish(ctx, testTask, tasq.StatusSuccessful)
	s.Empty(task)
	s.Error(err)

	// registering success when update is successful but select returns no rows
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns))
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RegisterFinish(ctx, testTask, tasq.StatusSuccessful)
	s.Empty(task)
	s.Error(err)

	// registering success when commit fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit().WillReturnError(errSQL)

	task, err = s.mockedRepository.RegisterFinish(ctx, testTask, tasq.StatusSuccessful)
	s.Empty(task)
	s.Error(err)

	// registering success successful
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	task, err = s.mockedRepository.RegisterFinish(ctx, testTask, tasq.StatusSuccessful)
	s.NotEmpty(task)
	s.NoError(err)
}

func (s *MySQLTestSuite) TestSubmitTask() {
	var (
		insertMockRegexp = regexp.QuoteMeta(`INSERT INTO 
				test_tasks
				(id, type, args, queue, priority, status, max_receives, created_at, visible_at) 
			VALUES
				(?, ?, ?, ?, ?, ?, ?, ?, ?);`)
		selectMockRegexp = regexp.QuoteMeta(`SELECT * 
			FROM 
				test_tasks
			WHERE
				id = ?;`)
	)

	// beginning the transaction fails
	s.sqlMock.ExpectBegin().WillReturnError(errSQL)

	task, err := s.mockedRepository.SubmitTask(ctx, testTask)
	s.Empty(task)
	s.Error(err)

	// registering failure when update fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(insertMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.SubmitTask(ctx, testTask)
	s.Empty(task)
	s.Error(err)

	// registering failure when update is successful but select fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(insertMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.SubmitTask(ctx, testTask)
	s.Empty(task)
	s.Error(err)

	// registering failure when update is successful but select returns no rows
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(insertMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns))
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.SubmitTask(ctx, testTask)
	s.Empty(task)
	s.Error(err)

	// registering failure when commit fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(insertMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit().WillReturnError(errSQL)

	task, err = s.mockedRepository.SubmitTask(ctx, testTask)
	s.Empty(task)
	s.Error(err)

	// registering failure successful
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(insertMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	task, err = s.mockedRepository.SubmitTask(ctx, testTask)
	s.NotEmpty(task)
	s.NoError(err)
}

func (s *MySQLTestSuite) TestDeleteTask() {
	var (
		deleteMockRegexp = regexp.QuoteMeta(`DELETE 
			FROM 
				test_tasks
			WHERE
				id = ?;`)
		deleteSafeDeleteMockRegexp = regexp.QuoteMeta(`DELETE 
			FROM 
				test_tasks 
			WHERE 
				id = ? AND 
				(
					(
						visible_at <= ? 
					) OR 
					( 
						status IN (?) AND 
						visible_at > ? 
					) 
				);`)
	)

	// deleting task when DB returns error
	s.sqlMock.ExpectExec(deleteMockRegexp).WillReturnError(errSQL)

	err := s.mockedRepository.DeleteTask(ctx, testTask, false)
	s.Error(err)

	// deleting task successful
	s.sqlMock.ExpectExec(deleteMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.mockedRepository.DeleteTask(ctx, testTask, false)
	s.NoError(err)

	// deleting invisible task successful
	s.sqlMock.ExpectExec(deleteSafeDeleteMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.mockedRepository.DeleteTask(ctx, testTask, true)
	s.NoError(err)
}

func (s *MySQLTestSuite) TestCountTasks() {
	selectMockRegexp := regexp.QuoteMeta(`SELECT
			COUNT(*)
		FROM
			test_tasks
		WHERE 
			status IN (?) AND 
			type IN (?) AND
			queue IN (?);`)

	// counting when DB returns error
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(errSQL)

	count, err := s.mockedRepository.CountTasks(ctx, []tasq.TaskStatus{testTask.Status}, []string{testTask.Type}, []string{testTask.Queue})
	s.Equal(int64(0), count)
	s.Error(err)

	// counting successful
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(10))

	count, err = s.mockedRepository.CountTasks(ctx, []tasq.TaskStatus{testTask.Status}, []string{testTask.Type}, []string{testTask.Queue})
	s.Equal(int64(10), count)
	s.NoError(err)
}

func (s *MySQLTestSuite) TestScanTasks() {
	selectMockRegexp := regexp.QuoteMeta(`SELECT
			*
		FROM
			test_tasks
		WHERE
			status IN (?) AND
			type IN (?) AND
			queue IN (?) 
		ORDER BY ? 
		LIMIT ?;`)

	// scanning when DB returns error
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(errSQL)

	tasks, err := s.mockedRepository.ScanTasks(ctx, []tasq.TaskStatus{testTask.Status}, []string{testTask.Type}, []string{testTask.Queue}, tasq.OrderingCreatedAtFirst, 10)
	s.Empty(tasks)
	s.Error(err)

	// scanning successful
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))

	tasks, err = s.mockedRepository.ScanTasks(ctx, []tasq.TaskStatus{testTask.Status}, []string{testTask.Type}, []string{testTask.Queue}, tasq.OrderingCreatedAtFirst, 10)
	s.NotEmpty(tasks)
	s.NoError(err)
}

func (s *MySQLTestSuite) TestPurgeTasks() {
	var (
		purgeMockRegexp = regexp.QuoteMeta(`DELETE 
			FROM 
				test_tasks 
			WHERE 
				status IN (?) AND 
				queue IN (?);`)
		purgeSafeDeleteMockRegexp = regexp.QuoteMeta(`DELETE 
			FROM
				test_tasks
			WHERE
				status IN (?) AND
				queue IN (?) AND 
				( 
					( visible_at <= ? ) OR 
					( 
						status IN (?) AND 
						visible_at > ? 
					) 
				);`)
	)

	// purging tasks when DB returns error
	s.sqlMock.ExpectExec(purgeMockRegexp).WillReturnError(errSQL)

	count, err := s.mockedRepository.PurgeTasks(ctx, []tasq.TaskStatus{tasq.StatusFailed}, []string{}, []string{testTaskQueue}, false)
	s.Equal(int64(0), count)
	s.Error(err)

	// purging when no rows are found
	s.sqlMock.ExpectExec(purgeMockRegexp).WillReturnResult(driver.ResultNoRows)

	count, err = s.mockedRepository.PurgeTasks(ctx, []tasq.TaskStatus{tasq.StatusFailed}, []string{}, []string{testTaskQueue}, false)
	s.Equal(int64(0), count)
	s.Error(err)

	// purging tasks successful
	s.sqlMock.ExpectExec(purgeMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))

	count, err = s.mockedRepository.PurgeTasks(ctx, []tasq.TaskStatus{tasq.StatusFailed}, []string{}, []string{testTaskQueue}, false)
	s.Equal(int64(1), count)
	s.NoError(err)

	// purging tasks with safeDelete successful
	s.sqlMock.ExpectExec(purgeSafeDeleteMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))

	count, err = s.mockedRepository.PurgeTasks(ctx, []tasq.TaskStatus{tasq.StatusFailed}, []string{}, []string{testTaskQueue}, true)
	s.Equal(int64(1), count)
	s.NoError(err)
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
	s.Empty(task)
	s.Error(err)

	// requeuing when update fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RequeueTask(ctx, testTask)
	s.Empty(task)
	s.Error(err)

	// requeuing when update is successful but select fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnError(errSQL)
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RequeueTask(ctx, testTask)
	s.Empty(task)
	s.Error(err)

	// requeuing when update is successful but select returns no rows
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns))
	s.sqlMock.ExpectRollback()

	task, err = s.mockedRepository.RequeueTask(ctx, testTask)
	s.Empty(task)
	s.Error(err)

	// requeuing when commit fails
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit().WillReturnError(errSQL)

	task, err = s.mockedRepository.RequeueTask(ctx, testTask)
	s.Empty(task)
	s.Error(err)

	// requeuing successful
	s.sqlMock.ExpectBegin()
	s.sqlMock.ExpectExec(updateMockRegexp).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectQuery(selectMockRegexp).WillReturnRows(sqlmock.NewRows(taskColumns).AddRow(taskValues...))
	s.sqlMock.ExpectCommit()

	task, err = s.mockedRepository.RequeueTask(ctx, testTask)
	s.NotEmpty(task)
	s.NoError(err)
}

func (s *MySQLTestSuite) TestGetQueryWithTableName() {
	var (
		taskUUID         = uuid.New()
		taskUUIDBytes, _ = taskUUID.MarshalBinary()
	)

	mysqlRepository, ok := s.mockedRepository.(*mysql.Repository)
	s.Require().True(ok)

	s.Panics(func() {
		_, _ = mysqlRepository.GetQueryWithTableName("SELECT * FROM {{.tableName}} WHERE id = :taskID:;", map[string]any{
			"taskID": taskUUIDBytes,
		})
	})

	s.Panics(func() {
		_, _ = mysqlRepository.GetQueryWithTableName("SELECT * FROM {{.tableName}} WHERE id IN (:taskIDs);", map[string]any{
			"taskIDs": [][]byte{},
		})
	})

	s.NotPanics(func() {
		query, args := mysqlRepository.GetQueryWithTableName("SELECT * FROM {{.tableName}} WHERE id = :taskID", map[string]any{
			"taskID": taskUUIDBytes,
		})
		s.Equal("SELECT * FROM test_tasks WHERE id = ?", query)
		s.Contains(args, taskUUIDBytes)
	})
}

func (s *MySQLTestSuite) TestInterpolateSQL() {
	params := map[string]any{"tableName": "test_table"}

	// Interpolate SQL successfully
	interpolatedSQL := mysql.InterpolateSQL("SELECT * FROM {{.tableName}}", params)
	s.Equal("SELECT * FROM test_table", interpolatedSQL)

	// Fail interpolaing unparseable SQL template
	s.Panics(func() {
		unparseableTemplateSQL := mysql.InterpolateSQL("SELECT * FROM {{.tableName", params)
		s.Empty(unparseableTemplateSQL)
	})

	// Fail interpolaing unexecutable SQL template
	s.Panics(func() {
		unexecutableTemplateSQL := mysql.InterpolateSQL(`SELECT * FROM {{if .tableName eq 1}} {{end}} {{.tableName}}`, params)
		s.Empty(unexecutableTemplateSQL)
	})
}
