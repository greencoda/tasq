package tasq_test

import (
	"context"
	"testing"

	"github.com/greencoda/tasq"
	"github.com/greencoda/tasq/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type InspectorTestSuite struct {
	suite.Suite
	mockRepository *mocks.IRepository
	tasqClient     *tasq.Client
	tasqInspector  *tasq.Inspector
}

func TestInspectorTestSuite(t *testing.T) {
	t.Parallel()

	suite.Run(t, new(InspectorTestSuite))
}

func (s *InspectorTestSuite) SetupTest() {
	s.mockRepository = mocks.NewIRepository(s.T())

	s.tasqClient = tasq.NewClient(s.mockRepository)
	require.NotNil(s.T(), s.tasqClient)

	s.tasqInspector = s.tasqClient.NewInspector()
}

func (s *InspectorTestSuite) TestNewCleaner() {
	assert.NotNil(s.T(), s.tasqInspector)
}

func (s *InspectorTestSuite) TestCount() {
	ctx := context.Background()

	s.mockRepository.On("CountTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}).Return(int64(1), nil).Once()

	taskCount, err := s.tasqInspector.Count(ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"})
	assert.Equal(s.T(), int64(1), taskCount)
	assert.True(s.T(), s.mockRepository.AssertCalled(s.T(), "CountTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}))
	assert.Nil(s.T(), err)

	s.mockRepository.On("CountTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}).Return(int64(0), errRepository).Once()

	taskCount, err = s.tasqInspector.Count(ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"})
	assert.Equal(s.T(), int64(0), taskCount)
	assert.True(s.T(), s.mockRepository.AssertCalled(s.T(), "CountTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}))
	assert.NotNil(s.T(), err)
}

func (s *InspectorTestSuite) TestScan() {
	ctx := context.Background()

	testTask, err := tasq.NewTask("testTask", true, "testQueue", 100, 5)
	require.NotNil(s.T(), testTask)
	require.Nil(s.T(), err)

	s.mockRepository.On("ScanTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}, tasq.OrderingCreatedAtFirst, 100).Return([]*tasq.Task{testTask}, nil).Once()

	tasks, err := s.tasqInspector.Scan(ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}, tasq.OrderingCreatedAtFirst, 100)
	assert.Equal(s.T(), []*tasq.Task{testTask}, tasks)
	assert.True(s.T(), s.mockRepository.AssertCalled(s.T(), "ScanTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}, tasq.OrderingCreatedAtFirst, 100))
	assert.Nil(s.T(), err)

	s.mockRepository.On("ScanTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}, tasq.OrderingCreatedAtFirst, 100).Return([]*tasq.Task{}, errRepository).Once()

	tasks, err = s.tasqInspector.Scan(ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}, tasq.OrderingCreatedAtFirst, 100)
	assert.Len(s.T(), tasks, 0)
	assert.True(s.T(), s.mockRepository.AssertCalled(s.T(), "ScanTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}, tasq.OrderingCreatedAtFirst, 100))
	assert.NotNil(s.T(), err)
}

func (s *InspectorTestSuite) TestPurge() {
	ctx := context.Background()

	s.mockRepository.On("PurgeTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}, true).Return(int64(10), nil).Once()

	count, err := s.tasqInspector.Purge(ctx, true, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"})
	assert.True(s.T(), s.mockRepository.AssertCalled(s.T(), "PurgeTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}, true))
	assert.Equal(s.T(), int64(10), count)
	assert.Nil(s.T(), err)

	s.mockRepository.On("PurgeTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}, true).Return(int64(0), errRepository).Once()

	count, err = s.tasqInspector.Purge(ctx, true, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"})
	assert.True(s.T(), s.mockRepository.AssertCalled(s.T(), "PurgeTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}, true))
	assert.Equal(s.T(), int64(0), count)
	assert.NotNil(s.T(), err)
}

func (s *InspectorTestSuite) TestDelete() {
	ctx := context.Background()

	testTask, err := tasq.NewTask("testTask", true, "testQueue", 100, 5)
	require.NotNil(s.T(), testTask)
	require.Nil(s.T(), err)

	s.mockRepository.On("DeleteTask", ctx, testTask, true).Once().Return(errRepository)

	err = s.tasqInspector.Delete(ctx, true, testTask)
	assert.NotNil(s.T(), err)
	assert.True(s.T(), s.mockRepository.AssertCalled(s.T(), "DeleteTask", ctx, testTask, true))

	s.mockRepository.On("DeleteTask", ctx, testTask, true).Once().Return(nil)

	err = s.tasqInspector.Delete(ctx, true, testTask)
	assert.Nil(s.T(), err)
	assert.True(s.T(), s.mockRepository.AssertCalled(s.T(), "DeleteTask", ctx, testTask, true))
}
