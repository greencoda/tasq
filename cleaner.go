package tasq

import (
	"context"
	"time"
)

type ICleaner interface {
	WithTaskAge(taskAge time.Duration) ICleaner

	Clean(ctx context.Context) error
}

type Cleaner struct {
	dao iDAO

	taskAge time.Duration
}

func (t *Client) NewCleaner() ICleaner {
	return &Cleaner{
		dao: t.dao,
	}
}

func (c *Cleaner) WithTaskAge(taskAge time.Duration) ICleaner {
	c.taskAge = taskAge

	return c
}

func (c *Cleaner) Clean(ctx context.Context) error {
	return c.dao.cleanTasks(ctx, c.taskAge)
}
