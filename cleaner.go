package tasq

import (
	"time"
)

var defaultTaskAgeLimit = 15 * time.Minute

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

// Clean will initiate the removal of finished (either succeeded or failed) tasks from the queue table
// if they have been created long enough ago for them to be eligible.
func (c *Cleaner) Clean() (int64, error) {
	return c.client.repository.CleanTasks(c.client.getContext(), c.taskAgeLimit)
}

// WithTaskAge defines the minimum time duration that must have passed since the creation of a finished task
// in order for it to be eligible for cleanup when the Cleaner's Clean() method is called.
//
// Default value: 15 minutes.
func (c *Cleaner) WithTaskAge(taskAge time.Duration) *Cleaner {
	c.taskAgeLimit = taskAge

	return c
}
