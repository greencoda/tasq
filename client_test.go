package tasq_test

import (
	"testing"

	"github.com/greencoda/tasq"
	mockrepository "github.com/greencoda/tasq/pkg/mocks/repository"
	"github.com/stretchr/testify/assert"
)

func TestNewClient(t *testing.T) {
	t.Parallel()

	repository := mockrepository.NewIRepository(t)

	tasqClient := tasq.NewClient(repository)
	assert.NotNil(t, tasqClient)
}
