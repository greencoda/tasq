package tasq

import (
	"context"

	"github.com/greencoda/tasq/internal/repository"
)

type Client struct {
	repository repository.IRepository
}

func NewClient(ctx context.Context, repository repository.IRepository) *Client {
	return &Client{
		repository: repository,
	}
}

func (c *Client) Repository() repository.IRepository {
	return c.repository
}
