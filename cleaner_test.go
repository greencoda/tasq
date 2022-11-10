package tasq

import (
	"context"
	"testing"
	"time"

	mock_repository "github.com/greencoda/tasq/internal/mocks/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type CleanerTestSuite struct {
	suite.Suite
	ctx            context.Context
	mockRepository *mock_repository.IRepository
	tasqClient     *Client
	tasqCleaner    *Cleaner
}

func (s *CleanerTestSuite) SetupTest() {
	s.ctx = context.Background()
	s.mockRepository = mock_repository.NewIRepository(s.T())

	s.tasqClient = NewClient(s.ctx, s.mockRepository)
	require.NotNil(s.T(), s.tasqClient)

	s.tasqCleaner = s.tasqClient.NewCleaner()
}

func (s *CleanerTestSuite) TestNewCleaner() {
	assert.NotNil(s.T(), s.tasqCleaner)
}

func (s *CleanerTestSuite) TestClean() {
	s.tasqCleaner.WithTaskAge(time.Hour)

	s.mockRepository.On("CleanTasks", s.ctx, time.Hour).Return(int64(1), nil)

	rowsAffected, err := s.tasqCleaner.Clean(s.ctx)

	assert.Equal(s.T(), int64(1), rowsAffected)
	assert.True(s.T(), s.mockRepository.AssertCalled(s.T(), "CleanTasks", s.ctx, time.Hour))
	assert.Nil(s.T(), err)
}

func TestCleanerTestSuite(t *testing.T) {
	suite.Run(t, new(CleanerTestSuite))
}
