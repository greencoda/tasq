package model_test

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/greencoda/tasq/internal/model"
	"github.com/stretchr/testify/assert"
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
	t.Parallel()

	suite.Run(t, new(TaskTestSuite))
}

func (s *TaskTestSuite) SetupTest() {
	uuid.SetRand(nil)
}

func (s *TaskTestSuite) TestNewTask() {
	// Create task successfully
	task, _ := model.NewTask("testTask", true, "testQueue", 0, 5)
	assert.NotNil(s.T(), task)

	// Fail by creating task with nil args
	nilTask, err := model.NewTask("testTask", nil, "testQueue", 0, 5)
	assert.Nil(s.T(), nilTask)
	assert.NotNil(s.T(), err)

	// Fail by causing uuid generation to return error
	uuid.SetRand(new(errorReader))

	invalidUUIDTask, err := model.NewTask("testTask", false, "testQueue", 0, 5)
	assert.Nil(s.T(), invalidUUIDTask)
	assert.NotNil(s.T(), err)
}

func (s *TaskTestSuite) TestTaskGetDetails() {
	// Create task successfully
	task, _ := model.NewTask("testTask", true, "testQueue", 0, 5)
	assert.NotNil(s.T(), task)

	// Get task details
	taskModel := task.GetDetails()
	assert.IsType(s.T(), task, taskModel)
}

func (s *TaskTestSuite) TestTaskUnmarshalArgs() {
	// Create task successfully
	task, _ := model.NewTask("testTask", true, "testQueue", 0, 5)
	assert.NotNil(s.T(), task)

	// Unmarshal task args successfully
	var args bool
	err := task.UnmarshalArgs(&args)
	assert.Nil(s.T(), err)
	assert.True(s.T(), args)

	// Fail by unmarshaling args to incorrect type
	var incorrectTypeArgs string
	err = task.UnmarshalArgs(&incorrectTypeArgs)
	assert.NotNil(s.T(), err)
	assert.Empty(s.T(), incorrectTypeArgs)
}
