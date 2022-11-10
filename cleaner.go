package tasq

import (
	"context"
	"time"
)

type Cleaner struct {
	client *Client

	taskAgeLimit time.Duration
}

func (c *Client) NewCleaner() *Cleaner {
	return &Cleaner{
		client: c,
	}
}

func (c *Cleaner) WithTaskAge(taskAge time.Duration) *Cleaner {
	c.taskAgeLimit = taskAge
	return c
}

func (c *Cleaner) Clean(ctx context.Context) (int64, error) {
	return c.client.Repository().CleanTasks(ctx, c.taskAgeLimit)
}
