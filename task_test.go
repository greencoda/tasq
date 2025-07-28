package tasq_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/greencoda/tasq"
	"github.com/stretchr/testify/suite"
)

var errTest = errors.New("test error")

type errorReader int

func (errorReader) Read(p []byte) (int, error) {
	return 0, errTest
}

type TaskTestSuite struct {
	suite.Suite
}

func TestTaskTestSuite(t *testing.T) {
	suite.Run(t, new(TaskTestSuite))
}

func (s *TaskTestSuite) SetupTest() {
	uuid.SetRand(nil)
}

func (s *TaskTestSuite) TestGetTaskStatuses() {
	allTasks := tasq.GetTaskStatuses(tasq.AllTasks)
	s.ElementsMatch(allTasks, []tasq.TaskStatus{
		tasq.StatusNew,
		tasq.StatusEnqueued,
		tasq.StatusInProgress,
		tasq.StatusSuccessful,
		tasq.StatusFailed,
	})

	openTasks := tasq.GetTaskStatuses(tasq.OpenTasks)
	s.ElementsMatch(openTasks, []tasq.TaskStatus{
		tasq.StatusNew,
		tasq.StatusEnqueued,
		tasq.StatusInProgress,
	})

	finishedTasks := tasq.GetTaskStatuses(tasq.FinishedTasks)
	s.ElementsMatch(finishedTasks, []tasq.TaskStatus{
		tasq.StatusSuccessful,
		tasq.StatusFailed,
	})

	unknownTasks := tasq.GetTaskStatuses(-1)
	s.Empty(unknownTasks)
}

func (s *TaskTestSuite) TestNewTask() {
	// Create task successfully
	task, _ := tasq.NewTask("testTask", true, "testQueue", 0, 5)
	s.NotNil(task)

	// Fail by creating task with nil args
	nilTask, err := tasq.NewTask("testTask", nil, "testQueue", 0, 5)
	s.Nil(nilTask)
	s.Require().Error(err)

	// Fail by causing uuid generation to return error
	uuid.SetRand(new(errorReader))

	invalidUUIDTask, err := tasq.NewTask("testTask", false, "testQueue", 0, 5)
	s.Nil(invalidUUIDTask)
	s.Error(err)
}

func (s *TaskTestSuite) TestTaskUnmarshalArgs() {
	// Create task successfully
	task, _ := tasq.NewTask("testTask", true, "testQueue", 0, 5)
	s.NotNil(task)

	// Unmarshal task args successfully
	var args bool

	err := task.UnmarshalArgs(&args)
	s.Require().NoError(err)
	s.True(args)

	// Fail by unmarshaling args to incorrect type
	var incorrectTypeArgs string

	err = task.UnmarshalArgs(&incorrectTypeArgs)
	s.Require().Error(err)
	s.Empty(incorrectTypeArgs)
}

func (s *TaskTestSuite) TestTaskIsLastReceive() {
	// Create singleReceiveTask successfully
	singleReceiveTask, _ := tasq.NewTask("testTask", true, "testQueue", 0, 1)
	singleReceiveTask.ReceiveCount = 1
	s.NotNil(singleReceiveTask)

	// Check if task is in its last receive before reaching the maximum amount of receives
	s.True(singleReceiveTask.IsLastReceive())

	// Create multiReceiveTask successfully
	multiReceiveTask, _ := tasq.NewTask("testTask", true, "testQueue", 0, 5)
	multiReceiveTask.ReceiveCount = 1
	s.NotNil(multiReceiveTask)

	// Check if task is in its last receive before reaching the maximum amount of receives
	s.False(multiReceiveTask.IsLastReceive())
}

func (s *TaskTestSuite) TestTaskSetVisibility() {
	// Create task successfully
	task, _ := tasq.NewTask("testTask", true, "testQueue", 0, 5)
	s.NotNil(task)

	// Set Visibility successfully
	visibilityTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	task.SetVisibility(visibilityTime)
	s.Equal(task.VisibleAt, visibilityTime)
}
