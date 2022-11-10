package tasq

import (
	"context"
	"errors"
	"testing"

	mock_repository "github.com/greencoda/tasq/internal/mocks/repository"
	"github.com/greencoda/tasq/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ProducterTestSuite struct {
	suite.Suite
	ctx            context.Context
	mockRepository *mock_repository.IRepository
	tasqClient     *Client
	tasqProducer   *Producer
}

func (s *ProducterTestSuite) SetupTest() {
	s.ctx = context.Background()
	s.mockRepository = mock_repository.NewIRepository(s.T())

	s.tasqClient = NewClient(s.ctx, s.mockRepository)
	require.NotNil(s.T(), s.tasqClient)

	s.tasqProducer = s.tasqClient.NewProducer()
}

func (s *ProducterTestSuite) TestNewProducer() {
	assert.NotNil(s.T(), s.tasqProducer)
}

func (s *ProducterTestSuite) TestSubmitSuccessful() {
	var (
		testArgs = "testData"
		testTask = model.NewTask("testTask", testArgs, "testQueue", 100, 5)
	)

	s.mockRepository.On("SubmitTask", s.ctx, mock.AnythingOfType("*model.Task")).Return(testTask, nil)

	task, err := s.tasqProducer.Submit(s.ctx, testTask.Type, testArgs, testTask.Queue, testTask.Priority, testTask.MaxReceives)

	assert.NotNil(s.T(), task)
	assert.True(s.T(), s.mockRepository.AssertCalled(s.T(), "SubmitTask", s.ctx, mock.AnythingOfType("*model.Task")))
	assert.Nil(s.T(), err)
}

func (s *ProducterTestSuite) TestSubmitUnsuccessful() {
	var (
		testArgs = "testData"
		testTask = model.NewTask("testTask", testArgs, "testQueue", 100, 5)
	)

	s.mockRepository.On("SubmitTask", s.ctx, mock.AnythingOfType("*model.Task")).Return(nil, errors.New("some repository error"))

	task, err := s.tasqProducer.Submit(s.ctx, testTask.Type, testArgs, testTask.Queue, testTask.Priority, testTask.MaxReceives)

	assert.Nil(s.T(), task)
	assert.True(s.T(), s.mockRepository.AssertCalled(s.T(), "SubmitTask", s.ctx, mock.AnythingOfType("*model.Task")))
	assert.NotNil(s.T(), err)
}

func (s *ProducterTestSuite) TestSubmitInvalidpriority() {
	task, err := s.tasqProducer.Submit(s.ctx, "testData", nil, "testQueue", 100, 5)

	assert.Nil(s.T(), task)
	assert.NotNil(s.T(), err)
}

func TestProducterTestSuite(t *testing.T) {
	suite.Run(t, new(ProducterTestSuite))
}
