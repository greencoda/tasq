package tasq_test

import (
	"context"
	"testing"

	"github.com/greencoda/tasq"
	"github.com/greencoda/tasq/internal/model"
	mockrepository "github.com/greencoda/tasq/pkg/mocks/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var ctx = context.Background()

type ProducterTestSuite struct {
	suite.Suite
	mockRepository *mockrepository.IRepository
	tasqClient     *tasq.Client
	tasqProducer   *tasq.Producer
}

func TestProducterTestSuite(t *testing.T) {
	t.Parallel()

	suite.Run(t, new(ProducterTestSuite))
}

func (s *ProducterTestSuite) SetupTest() {
	s.mockRepository = mockrepository.NewIRepository(s.T())

	s.tasqClient = tasq.NewClient(s.mockRepository)
	require.NotNil(s.T(), s.tasqClient)

	s.tasqProducer = s.tasqClient.NewProducer()
}

func (s *ProducterTestSuite) TestNewProducer() {
	assert.NotNil(s.T(), s.tasqProducer)
}

func (s *ProducterTestSuite) TestSubmitSuccessful() {
	var (
		testArgs    = "testData"
		testTask, _ = model.NewTask("testTask", testArgs, "testQueue", 100, 5)
	)

	s.mockRepository.On("SubmitTask", ctx, mock.AnythingOfType("*model.Task")).Return(testTask, nil)

	task, err := s.tasqProducer.Submit(ctx, testTask.Type, testArgs, testTask.Queue, testTask.Priority, testTask.MaxReceives)

	assert.NotNil(s.T(), task)
	assert.True(s.T(), s.mockRepository.AssertCalled(s.T(), "SubmitTask", ctx, mock.AnythingOfType("*model.Task")))
	assert.Nil(s.T(), err)
}

func (s *ProducterTestSuite) TestSubmitUnsuccessful() {
	var (
		testArgs    = "testData"
		testTask, _ = model.NewTask("testTask", testArgs, "testQueue", 100, 5)
	)

	s.mockRepository.On("SubmitTask", ctx, mock.AnythingOfType("*model.Task")).Return(nil, errRepository)

	task, err := s.tasqProducer.Submit(ctx, testTask.Type, testArgs, testTask.Queue, testTask.Priority, testTask.MaxReceives)

	assert.Nil(s.T(), task)
	assert.True(s.T(), s.mockRepository.AssertCalled(s.T(), "SubmitTask", ctx, mock.AnythingOfType("*model.Task")))
	assert.NotNil(s.T(), err)
}

func (s *ProducterTestSuite) TestSubmitInvalidpriority() {
	task, err := s.tasqProducer.Submit(ctx, "testData", nil, "testQueue", 100, 5)

	assert.Nil(s.T(), task)
	assert.NotNil(s.T(), err)
}
