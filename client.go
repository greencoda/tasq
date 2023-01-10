// Package tasq provides a task queue implementation compapible with multiple repositories
package tasq

// Client wraps the tasq repository interface which is used
// by the different services to access the database.
type Client struct {
	repository IRepository
}

// NewClient creates a new tasq client instance with the provided tasq.
func NewClient(repository IRepository) *Client {
	return &Client{
		repository: repository,
	}
}
