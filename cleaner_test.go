package tasq

import (
	"context"
	"testing"
	"time"

	mockrepository "github.com/greencoda/tasq/pkg/mocks/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type CleanerTestSuite struct {
	suite.Suite
	mockRepository *mockrepository.IRepository
	tasqClient     *Client
	tasqCleaner    *Cleaner
}

func TestCleanerTestSuite(t *testing.T) {
	t.Parallel()

	suite.Run(t, new(CleanerTestSuite))
}

func (s *CleanerTestSuite) SetupTest() {
	s.mockRepository = mockrepository.NewIRepository(s.T())

	s.tasqClient = NewClient(context.Background(), s.mockRepository)
	require.NotNil(s.T(), s.tasqClient)

	s.tasqCleaner = s.tasqClient.NewCleaner()
}

func (s *CleanerTestSuite) TestNewCleaner() {
	assert.NotNil(s.T(), s.tasqCleaner)
}

func (s *CleanerTestSuite) TestClean() {
	s.tasqCleaner.WithTaskAge(time.Hour)

	s.mockRepository.On("CleanTasks", s.tasqClient.getContext(), time.Hour).Return(int64(1), nil)

	rowsAffected, err := s.tasqCleaner.Clean()

	assert.Equal(s.T(), int64(1), rowsAffected)
	assert.True(s.T(), s.mockRepository.AssertCalled(s.T(), "CleanTasks", s.tasqClient.getContext(), time.Hour))
	assert.Nil(s.T(), err)
}
