package tasq

import (
	"context"
	"testing"

	mock_repository "github.com/greencoda/tasq/internal/mocks/repository"
	"github.com/stretchr/testify/assert"
)

func TestNewClient(t *testing.T) {
	var (
		ctx        = context.Background()
		repository = mock_repository.NewIRepository(t)
	)

	tasqClient := NewClient(ctx, repository)

	assert.NotNil(t, tasqClient)

	clientRepository := tasqClient.getRepository()

	assert.Equal(t, repository, clientRepository)
}
