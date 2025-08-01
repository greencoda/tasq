package tasq_test

import (
	"context"
	"testing"
	"time"

	"github.com/greencoda/tasq"
	"github.com/greencoda/tasq/mocks"
	"github.com/stretchr/testify/suite"
)

type CleanerTestSuite struct {
	suite.Suite

	mockRepository *mocks.IRepository
	tasqClient     *tasq.Client
	tasqCleaner    *tasq.Cleaner
}

func TestCleanerTestSuite(t *testing.T) {
	t.Parallel()

	suite.Run(t, new(CleanerTestSuite))
}

func (s *CleanerTestSuite) SetupTest() {
	s.mockRepository = mocks.NewIRepository(s.T())

	s.tasqClient = tasq.NewClient(s.mockRepository)
	s.Require().NotNil(s.tasqClient)

	s.tasqCleaner = s.tasqClient.NewCleaner().WithTaskAge(time.Hour)
}

func (s *CleanerTestSuite) TestNewCleaner() {
	s.NotNil(s.tasqCleaner)
}

func (s *CleanerTestSuite) TestClean() {
	ctx := context.Background()

	s.mockRepository.On("CleanTasks", ctx, time.Hour).Return(int64(1), nil).Once()

	rowsAffected, err := s.tasqCleaner.Clean(ctx)

	s.Equal(int64(1), rowsAffected)
	s.True(s.mockRepository.AssertCalled(s.T(), "CleanTasks", ctx, time.Hour))
	s.NoError(err)

	s.mockRepository.On("CleanTasks", ctx, time.Hour).Return(int64(0), errRepository).Once()
	rowsAffected, err = s.tasqCleaner.Clean(ctx)

	s.Equal(int64(0), rowsAffected)
	s.True(s.mockRepository.AssertCalled(s.T(), "CleanTasks", ctx, time.Hour))
	s.Error(err)
}
