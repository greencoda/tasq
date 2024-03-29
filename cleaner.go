package tasq

import (
	"context"
	"fmt"
	"time"
)

const defaultTaskAgeLimit = 15 * time.Minute

// Cleaner is a service instance created by a Client with reference to that client
// and the task age limit parameter.
type Cleaner struct {
	client *Client

	taskAgeLimit time.Duration
}

// NewCleaner creates a new cleaner with a reference to the original tasq client.
func (c *Client) NewCleaner() *Cleaner {
	return &Cleaner{
		client: c,

		taskAgeLimit: defaultTaskAgeLimit,
	}
}

// WithTaskAge defines the minimum time duration that must have passed since the creation of a finished task
// in order for it to be eligible for cleanup when the Cleaner's Clean() method is called.
//
// Default value: 15 minutes.
func (c *Cleaner) WithTaskAge(taskAge time.Duration) *Cleaner {
	c.taskAgeLimit = taskAge

	return c
}

// Clean will initiate the removal of finished (either succeeded or failed) tasks from the tasks table
// if they have been created long enough ago for them to be eligible.
func (c *Cleaner) Clean(ctx context.Context) (int64, error) {
	cleanedTaskCount, err := c.client.repository.CleanTasks(ctx, c.taskAgeLimit)
	if err != nil {
		return 0, fmt.Errorf("failed to clean tasks: %w", err)
	}

	return cleanedTaskCount, nil
}
