package tasq

import (
	"github.com/greencoda/tasq/pkg/repository"
)

// Client wraps the tasq repository interface which is used
// by the different services to access the database.
type Client struct {
	repository repository.IRepository
}

// NewClient creates a new tasq client instance with the provided tasq repository.
func NewClient(repository repository.IRepository) *Client {
	return &Client{
		repository: repository,
	}
}
