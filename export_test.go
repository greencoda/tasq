package tasq

import (
	"sync"

	"github.com/benbjohnson/clock"
)

// SetClock needs to be exported so we can adjust the clock during tests
// to avoid failures due to different results of time.Now().
func (c *Consumer) SetClock(clock clock.Clock) *Consumer {
	c.clock = clock

	return c
}

// GetWaitGroup needs to be exported so we can get the waitgroup of the consumer
// in order to allow the tests for the consumption to finish.
func (c *Consumer) GetWaitGroup() *sync.WaitGroup {
	return &c.wg
}
