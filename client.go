package tasq

import (
	"context"

	"github.com/greencoda/tasq/internal/repository"
)

// Client wraps the tasq repository interface which is used
// by the different services to access the database
type Client struct {
	ctx        context.Context
	repository repository.IRepository
}

// NewClient creates a new tasq client instance with the provided tasq repository
func NewClient(ctx context.Context, repository repository.IRepository) *Client {
	return &Client{
		ctx:        ctx,
		repository: repository,
	}
}

// getRepository returns a reference to the the wrapped tasq repository
func (c *Client) getContext() context.Context {
	return c.ctx
}

// getRepository returns a reference to the the wrapped tasq repository
func (c *Client) getRepository() repository.IRepository {
	return c.repository
}
