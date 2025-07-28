package tasq_test

import (
	"context"
	"testing"

	"github.com/greencoda/tasq"
	"github.com/greencoda/tasq/mocks"
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
	s.Require().NotNil(s.tasqClient)

	s.tasqInspector = s.tasqClient.NewInspector()
}

func (s *InspectorTestSuite) TestNewCleaner() {
	s.NotNil(s.tasqInspector)
}

func (s *InspectorTestSuite) TestCount() {
	ctx := context.Background()

	s.mockRepository.On("CountTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}).Return(int64(1), nil).Once()

	taskCount, err := s.tasqInspector.Count(ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"})
	s.Equal(int64(1), taskCount)
	s.True(s.mockRepository.AssertCalled(s.T(), "CountTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}))
	s.NoError(err)

	s.mockRepository.On("CountTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}).Return(int64(0), errRepository).Once()

	taskCount, err = s.tasqInspector.Count(ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"})
	s.Equal(int64(0), taskCount)
	s.True(s.mockRepository.AssertCalled(s.T(), "CountTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}))
	s.Error(err)
}

func (s *InspectorTestSuite) TestScan() {
	ctx := context.Background()

	testTask, err := tasq.NewTask("testTask", true, "testQueue", 100, 5)
	s.Require().NotNil(testTask)
	s.Require().NoError(err)

	s.mockRepository.On("ScanTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}, tasq.OrderingCreatedAtFirst, 100).Return([]*tasq.Task{testTask}, nil).Once()

	tasks, err := s.tasqInspector.Scan(ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}, tasq.OrderingCreatedAtFirst, 100)
	s.Equal([]*tasq.Task{testTask}, tasks)
	s.True(s.mockRepository.AssertCalled(s.T(), "ScanTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}, tasq.OrderingCreatedAtFirst, 100))
	s.NoError(err)

	s.mockRepository.On("ScanTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}, tasq.OrderingCreatedAtFirst, 100).Return([]*tasq.Task{}, errRepository).Once()

	tasks, err = s.tasqInspector.Scan(ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}, tasq.OrderingCreatedAtFirst, 100)
	s.Empty(tasks)
	s.True(s.mockRepository.AssertCalled(s.T(), "ScanTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}, tasq.OrderingCreatedAtFirst, 100))
	s.Error(err)
}

func (s *InspectorTestSuite) TestPurge() {
	ctx := context.Background()

	s.mockRepository.On("PurgeTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}, true).Return(int64(10), nil).Once()

	count, err := s.tasqInspector.Purge(ctx, true, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"})
	s.True(s.mockRepository.AssertCalled(s.T(), "PurgeTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}, true))
	s.Equal(int64(10), count)
	s.NoError(err)

	s.mockRepository.On("PurgeTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}, true).Return(int64(0), errRepository).Once()

	count, err = s.tasqInspector.Purge(ctx, true, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"})
	s.True(s.mockRepository.AssertCalled(s.T(), "PurgeTasks", ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{"testType"}, []string{"testQueue"}, true))
	s.Equal(int64(0), count)
	s.Error(err)
}

func (s *InspectorTestSuite) TestDelete() {
	ctx := context.Background()

	testTask, err := tasq.NewTask("testTask", true, "testQueue", 100, 5)
	s.Require().NotNil(testTask)
	s.Require().NoError(err)

	s.mockRepository.On("DeleteTask", ctx, testTask, true).Once().Return(errRepository)

	err = s.tasqInspector.Delete(ctx, true, testTask)
	s.Error(err)
	s.True(s.mockRepository.AssertCalled(s.T(), "DeleteTask", ctx, testTask, true))

	s.mockRepository.On("DeleteTask", ctx, testTask, true).Once().Return(nil)

	err = s.tasqInspector.Delete(ctx, true, testTask)
	s.NoError(err)
	s.True(s.mockRepository.AssertCalled(s.T(), "DeleteTask", ctx, testTask, true))
}
