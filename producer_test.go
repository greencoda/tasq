package tasq_test

import (
	"context"
	"testing"

	"github.com/greencoda/tasq"
	"github.com/greencoda/tasq/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type ProducterTestSuite struct {
	suite.Suite

	mockRepository *mocks.IRepository
	tasqClient     *tasq.Client
	tasqProducer   *tasq.Producer
	testTask       *tasq.Task
}

func TestProducterTestSuite(t *testing.T) {
	t.Parallel()

	suite.Run(t, new(ProducterTestSuite))
}

func (s *ProducterTestSuite) SetupTest() {
	s.mockRepository = mocks.NewIRepository(s.T())

	s.tasqClient = tasq.NewClient(s.mockRepository)
	s.Require().NotNil(s.tasqClient)

	s.tasqProducer = s.tasqClient.NewProducer()
	s.Require().NotNil(s.tasqProducer)

	testArgs := "testData"
	testTask, err := tasq.NewTask("testTask", testArgs, "testQueue", 100, 5)
	s.Require().NoError(err)

	s.testTask = testTask
}

func (s *ProducterTestSuite) TestNewProducer() {
	s.NotNil(s.tasqProducer)
}

func (s *ProducterTestSuite) TestSubmitSuccessful() {
	ctx := context.Background()

	s.mockRepository.On("SubmitTask", ctx, mock.AnythingOfType("*tasq.Task")).Return(s.testTask, nil)

	task, err := s.tasqProducer.Submit(ctx, s.testTask.Type, s.testTask.Args, s.testTask.Queue, s.testTask.Priority, s.testTask.MaxReceives)

	s.NotNil(task)
	s.True(s.mockRepository.AssertCalled(s.T(), "SubmitTask", ctx, mock.AnythingOfType("*tasq.Task")))
	s.NoError(err)
}

func (s *ProducterTestSuite) TestSubmitUnsuccessful() {
	var (
		ctx         = context.Background()
		testArgs    = "testData"
		testTask, _ = tasq.NewTask("testTask", testArgs, "testQueue", 100, 5)
	)

	s.mockRepository.On("SubmitTask", ctx, mock.AnythingOfType("*tasq.Task")).Return(nil, errRepository)

	task, err := s.tasqProducer.Submit(ctx, testTask.Type, testArgs, testTask.Queue, testTask.Priority, testTask.MaxReceives)

	s.Nil(task)
	s.True(s.mockRepository.AssertCalled(s.T(), "SubmitTask", ctx, mock.AnythingOfType("*tasq.Task")))
	s.Error(err)
}

func (s *ProducterTestSuite) TestSubmitInvalidpriority() {
	ctx := context.Background()

	task, err := s.tasqProducer.Submit(ctx, "testData", nil, "testQueue", 100, 5)

	s.Nil(task)
	s.Error(err)
}

func (s *ProducterTestSuite) TestSubmitTask() {
	ctx := context.Background()

	s.mockRepository.On("SubmitTask", ctx, mock.AnythingOfType("*tasq.Task")).Return(s.testTask, nil)

	task, err := tasq.NewTask(s.testTask.Type, s.testTask.Args, s.testTask.Queue, s.testTask.Priority, s.testTask.MaxReceives)
	s.Require().NotNil(task)
	s.Require().NoError(err)

	submittedTask, err := s.tasqProducer.SubmitTask(ctx, task)

	s.NotNil(submittedTask)
	s.True(s.mockRepository.AssertCalled(s.T(), "SubmitTask", ctx, mock.AnythingOfType("*tasq.Task")))
	s.NoError(err)
}
