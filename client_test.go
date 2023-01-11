package tasq_test

import (
	"testing"

	"github.com/greencoda/tasq"
	"github.com/greencoda/tasq/mocks"
	"github.com/stretchr/testify/assert"
)

func TestNewClient(t *testing.T) {
	t.Parallel()

	repository := mocks.NewIRepository(t)

	tasqClient := tasq.NewClient(repository)
	assert.NotNil(t, tasqClient)
}
