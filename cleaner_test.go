package tasq_test

import (
	"context"
	"testing"
	"time"

	"github.com/greencoda/tasq"
	mockrepository "github.com/greencoda/tasq/pkg/mocks/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type CleanerTestSuite struct {
	suite.Suite
	mockRepository *mockrepository.IRepository
	tasqClient     *tasq.Client
	tasqCleaner    *tasq.Cleaner
}

func TestCleanerTestSuite(t *testing.T) {
	t.Parallel()

	suite.Run(t, new(CleanerTestSuite))
}

func (s *CleanerTestSuite) SetupTest() {
	s.mockRepository = mockrepository.NewIRepository(s.T())

	s.tasqClient = tasq.NewClient(s.mockRepository)
	require.NotNil(s.T(), s.tasqClient)

	s.tasqCleaner = s.tasqClient.NewCleaner()
}

func (s *CleanerTestSuite) TestNewCleaner() {
	assert.NotNil(s.T(), s.tasqCleaner)
}

func (s *CleanerTestSuite) TestClean() {
	ctx := context.Background()

	s.tasqCleaner.WithTaskAge(time.Hour)

	s.mockRepository.On("CleanTasks", ctx, time.Hour).Return(int64(1), nil)

	rowsAffected, err := s.tasqCleaner.Clean(ctx)

	assert.Equal(s.T(), int64(1), rowsAffected)
	assert.True(s.T(), s.mockRepository.AssertCalled(s.T(), "CleanTasks", ctx, time.Hour))
	assert.Nil(s.T(), err)
}
