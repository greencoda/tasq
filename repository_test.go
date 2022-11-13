package tasq

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/DATA-DOG/go-sqlmock"
)

type RepositoryTestSuite struct {
	suite.Suite
	db      *sql.DB
	sqlMock sqlmock.Sqlmock
}

func (s *RepositoryTestSuite) SetupTest() {
	var err error

	s.db, s.sqlMock, err = sqlmock.New()
	require.Nil(s.T(), err)
}

func (s *RepositoryTestSuite) TestNewPostgresRepositoryFromDBWithMigration() {
	s.sqlMock.ExpectExec(`CREATE TYPE test_task_status AS ENUM`).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectExec(`CREATE TABLE IF NOT EXISTS test_tasks`).WillReturnResult(sqlmock.NewResult(1, 1))

	repository, err := NewRepository(s.db, "postgres", "test", true, 5*time.Second)

	assert.NotNil(s.T(), repository)
	assert.Nil(s.T(), err)
}

func (s *RepositoryTestSuite) TestNewPostgresRepositoryFromDBWithMigrationError() {
	s.sqlMock.ExpectExec(`CREATE TYPE test_task_status AS ENUM`).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectExec(`CREATE TABLE IF NOT EXISTS test_tasks`).WillReturnError(sql.ErrNoRows)

	repository, err := NewRepository(s.db, "postgres", "test", true, 5*time.Second)

	assert.Nil(s.T(), repository)
	assert.NotNil(s.T(), err)
}

func (s *RepositoryTestSuite) TestNewPostgresRepositoryFromDB() {
	s.sqlMock.ExpectExec(`CREATE TYPE test_task_status AS ENUM`).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectExec(`CREATE TABLE IF NOT EXISTS test_tasks`).WillReturnResult(sqlmock.NewResult(1, 1))

	repository, err := NewRepository(s.db, "postgres", "test", false, 5*time.Second)

	assert.NotNil(s.T(), repository)
	assert.Nil(s.T(), err)
}

func (s *RepositoryTestSuite) TestNewPostgresRepositoryFromInvalidDSN() {
	s.sqlMock.ExpectExec(`CREATE TYPE test_task_status AS ENUM`).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectExec(`CREATE TABLE IF NOT EXISTS test_tasks`).WillReturnResult(sqlmock.NewResult(1, 1))

	repository, err := NewRepository("abc", "postgres", "test", false, 5*time.Second)

	assert.NotNil(s.T(), repository)
	assert.Nil(s.T(), err)
}

func (s *RepositoryTestSuite) TestNewPostgresRepositoryFromInvalidDataSource() {
	s.sqlMock.ExpectExec(`CREATE TYPE test_task_status AS ENUM`).WillReturnResult(sqlmock.NewResult(1, 1))
	s.sqlMock.ExpectExec(`CREATE TABLE IF NOT EXISTS test_tasks`).WillReturnResult(sqlmock.NewResult(1, 1))

	repository, err := NewRepository(true, "postgres", "test", false, 5*time.Second)

	assert.Nil(s.T(), repository)
	assert.NotNil(s.T(), err)
}

func (suite *RepositoryTestSuite) TestNewUnknownRepository() {
	suite.sqlMock.ExpectExec(`CREATE TYPE test_task_status AS ENUM`).WillReturnResult(sqlmock.NewResult(1, 1))
	suite.sqlMock.ExpectExec(`CREATE TABLE IF NOT EXISTS test_tasks`).WillReturnResult(sqlmock.NewResult(1, 1))

	repository, err := NewRepository(suite.db, "unknown", "test", false, 5*time.Second)

	assert.Nil(suite.T(), repository)
	assert.NotNil(suite.T(), err)
}

func TestRepositoryTestSuite(t *testing.T) {
	suite.Run(t, new(RepositoryTestSuite))
}
