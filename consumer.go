package tasq

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
)

type pollStrategy string

const (
	PollStrategyByCreatedAt pollStrategy = "pollByCreatedAt"
	PollStrategyByPriority  pollStrategy = "pollByPriority"
)

var (
	defaultChannelSize         = 10
	defaultPollInterval        = 5 * time.Second
	defaultPollStrategy        = PollStrategyByCreatedAt
	defaultPollLimit           = 10
	defaultAutoDeleteOnSuccess = false
	defaultMaxActiveTasks      = 10
	defaultQueues              = []string{""}
	defaultVisibilityTimeout   = 15 * time.Second
)

type TaskFunc func(ctx context.Context, task *Task) error

type TaskFuncMap map[string]TaskFunc

type IConsumer interface {
	WithChannelSize(channelSize int) IConsumer
	WithPollInterval(pollInterval time.Duration) IConsumer
	WithPollStrategy(pollStrategy pollStrategy) IConsumer
	WithPollLimit(pollLimit int) IConsumer
	WithAutoDeleteOnSuccess(autoDeleteOnSuccess bool) IConsumer
	WithMaxActiveTasks(maxActiveTasks int) IConsumer
	WithQueues(queues ...string) IConsumer
	WithVisibilityTimeout(visibilityTimeout time.Duration) IConsumer

	Learn(name string, taskFunc TaskFunc, override bool) error
	Forget(name string) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error

	Channel() chan *job
}

type Consumer struct {
	c   chan *job
	dao iDAO

	taskFuncMap TaskFuncMap

	activeTasks map[uuid.UUID]struct{}

	pollInterval        time.Duration
	pollLimit           int
	pollStrategy        pollStrategy
	autoDeleteOnSuccess bool
	maxActiveTasks      int
	visibilityTimeout   time.Duration
	queues              []string

	stop chan struct{}
}

func (t *Client) NewConsumer() IConsumer {
	return &Consumer{
		c:   nil,
		dao: t.dao,

		taskFuncMap: make(TaskFuncMap),

		activeTasks: make(map[uuid.UUID]struct{}),

		pollInterval:        defaultPollInterval,
		pollLimit:           defaultPollLimit,
		pollStrategy:        defaultPollStrategy,
		autoDeleteOnSuccess: defaultAutoDeleteOnSuccess,
		maxActiveTasks:      defaultMaxActiveTasks,
		visibilityTimeout:   defaultVisibilityTimeout,
		queues:              defaultQueues,

		stop: make(chan struct{}, 1),
	}
}

func (c *Consumer) WithChannelSize(channelSize int) IConsumer {
	if c.c != nil {
		panic("channel size already set")
	}

	c.c = make(chan *job, channelSize)

	return c
}

func (c *Consumer) WithPollInterval(pollInterval time.Duration) IConsumer {
	c.pollInterval = pollInterval
	return c
}

func (c *Consumer) WithPollLimit(pollLimit int) IConsumer {
	c.pollLimit = pollLimit
	return c
}

func (c *Consumer) WithPollStrategy(pollStrategy pollStrategy) IConsumer {
	c.pollStrategy = pollStrategy
	return c
}

func (c *Consumer) WithAutoDeleteOnSuccess(autoDeleteOnSuccess bool) IConsumer {
	c.autoDeleteOnSuccess = autoDeleteOnSuccess
	return c
}

func (c *Consumer) WithMaxActiveTasks(maxActiveTasks int) IConsumer {
	c.maxActiveTasks = maxActiveTasks
	return c
}

func (c *Consumer) WithVisibilityTimeout(visibilityTimeout time.Duration) IConsumer {
	c.visibilityTimeout = visibilityTimeout
	return c
}

func (c *Consumer) WithQueues(queues ...string) IConsumer {
	c.queues = queues
	return c
}

func (c *Consumer) Learn(name string, taskFunc TaskFunc, override bool) error {
	if _, exists := c.taskFuncMap[name]; exists && !override {
		return fmt.Errorf("task with the name '%s' already learned", name)
	}

	c.taskFuncMap[name] = taskFunc

	return nil
}

func (c *Consumer) Forget(name string) error {
	if _, exists := c.taskFuncMap[name]; !exists {
		return fmt.Errorf("task with the name '%s' not found", name)
	}

	delete(c.taskFuncMap, name)

	return nil
}

func (c *Consumer) Start(ctx context.Context) error {
	if c.c == nil {
		c.c = make(chan *job, defaultChannelSize)
	}

	if c.visibilityTimeout <= c.pollInterval {
		return fmt.Errorf("visibility timeout '%v' must be longer than poll interval '%v'", c.visibilityTimeout, c.pollInterval)
	}

	go c.processLoop(ctx)

	return nil
}

func (c *Consumer) Stop(ctx context.Context) error {
	c.stop <- struct{}{}

	return nil
}

func (c *Consumer) Channel() chan *job {
	return c.c
}

// ---

func (c *Consumer) getActiveTaskIDs() []uuid.UUID {
	activeTaskIDs := make([]uuid.UUID, 0, len(c.activeTasks))

	for taskID := range c.activeTasks {
		activeTaskIDs = append(activeTaskIDs, taskID)
	}

	return activeTaskIDs
}

func (c *Consumer) getKnownTaskTypes() []string {
	taskTypes := make([]string, 0, len(c.taskFuncMap))

	for taskType := range c.taskFuncMap {
		taskTypes = append(taskTypes, taskType)
	}

	return taskTypes
}

func (c *Consumer) getPollOrdering() ([]string, error) {
	switch c.pollStrategy {
	case PollStrategyByCreatedAt:
		return OrderingCreatedAtFirst, nil
	case PollStrategyByPriority:
		return OrderingPriorityFirst, nil
	default:
		return nil, fmt.Errorf("unknown poll strategy '%s", c.pollStrategy)
	}
}

func (c *Consumer) getPollQuantity() int {
	taskCapacity := c.maxActiveTasks - len(c.activeTasks)

	if c.pollLimit < taskCapacity {
		return c.pollLimit
	}

	return taskCapacity
}

func (c *Consumer) processLoop(ctx context.Context) {
	defer log.Print("processing stopped")

	timer := time.NewTicker(c.pollInterval)
	defer timer.Stop()

	for {
		err := c.pingActiveTasks(ctx)
		if err != nil {
			log.Printf("error pinging active tasks: %s", err)
		}

		tasks, err := c.pollForTasks(ctx)
		if err != nil {
			log.Printf("error polling for tasks: %s", err)
		}

		c.activateTasks(ctx, tasks)

		select {
		case <-c.stop:
			close(c.stop)
			close(c.c)
			return
		case <-timer.C:
			continue
		}
	}
}

func (c *Consumer) pollForTasks(ctx context.Context) ([]*Task, error) {
	pollOrdering, err := c.getPollOrdering()
	if err != nil {
		return nil, err
	}

	return c.dao.pollTasks(ctx, c.getKnownTaskTypes(), c.queues, c.visibilityTimeout, pollOrdering, c.getPollQuantity())
}

func (c *Consumer) pingActiveTasks(ctx context.Context) (err error) {
	_, err = c.dao.pingTasks(ctx, c.getActiveTaskIDs(), c.visibilityTimeout)

	return err
}

func (c *Consumer) mapTaskToJob(ctx context.Context, task *Task) (*job, error) {
	if taskFunc, ok := c.taskFuncMap[task.Type]; ok {
		return newJob(ctx, taskFunc, task), nil
	}

	return nil, fmt.Errorf("task type '%s' is not known by this consumer", task.Type)
}

func (c *Consumer) activateTasks(ctx context.Context, tasks []*Task) {
	for _, task := range tasks {
		err := c.activateTask(ctx, task)
		if err != nil {
			task.fail(ctx)
			continue
		}
	}
}

func (c *Consumer) activateTask(ctx context.Context, task *Task) error {
	task.setConsumer(c)

	job, err := c.mapTaskToJob(ctx, task)
	if err != nil {
		return err
	}

	c.activeTasks[task.ID] = struct{}{}
	c.c <- job

	return nil
}
