package tasq

import (
	"context"
	"testing"

	mockrepository "github.com/greencoda/tasq/pkg/mocks/repository"
	"github.com/stretchr/testify/assert"
)

func TestNewClient(t *testing.T) {
	t.Parallel()

	var (
		ctx        = context.Background()
		repository = mockrepository.NewIRepository(t)
	)

	tasqClient := NewClient(ctx, repository)
	assert.NotNil(t, tasqClient)

	clientRepository := tasqClient.repository
	assert.Equal(t, repository, clientRepository)
}
