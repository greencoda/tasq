package tasq

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/google/uuid"
	"github.com/greencoda/tasq/internal/model"
	"github.com/greencoda/tasq/internal/repository"
)

type Logger interface {
	Print(v ...any)
	Printf(format string, v ...any)
}

type Job func()

type taskFunc func(ctx context.Context, task Task) error

type taskFuncMap map[string]taskFunc

type PollStrategy string

const (
	PollStrategyByCreatedAt PollStrategy = "pollByCreatedAt"
	PollStrategyByPriority  PollStrategy = "pollByPriority"
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
	defaultLogger              = log.New(io.Discard, "", log.LstdFlags)
)

type Consumer struct {
	c      chan *Job
	client *Client
	clock  clock.Clock
	logger Logger

	taskFuncMap taskFuncMap

	activeTasks map[uuid.UUID]struct{}

	pollInterval        time.Duration
	pollLimit           int
	pollStrategy        PollStrategy
	autoDeleteOnSuccess bool
	maxActiveTasks      int
	visibilityTimeout   time.Duration
	queues              []string

	stop chan struct{}
}

func (c *Client) NewConsumer() *Consumer {

	return &Consumer{
		c:      nil,
		client: c,
		clock:  clock.New(),
		logger: defaultLogger,

		taskFuncMap: make(taskFuncMap),

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

func (c *Consumer) WithChannelSize(channelSize int) *Consumer {
	if c.c != nil {
		panic("channel size already set")
	}

	c.c = make(chan *Job, channelSize)

	return c
}

func (c *Consumer) WithLogger(logger Logger) *Consumer {
	c.logger = logger
	return c
}

func (c *Consumer) WithPollInterval(pollInterval time.Duration) *Consumer {
	c.pollInterval = pollInterval
	return c
}

func (c *Consumer) WithPollLimit(pollLimit int) *Consumer {
	c.pollLimit = pollLimit
	return c
}

func (c *Consumer) WithPollStrategy(pollStrategy PollStrategy) *Consumer {
	c.pollStrategy = pollStrategy
	return c
}

func (c *Consumer) WithAutoDeleteOnSuccess(autoDeleteOnSuccess bool) *Consumer {
	c.autoDeleteOnSuccess = autoDeleteOnSuccess
	return c
}

func (c *Consumer) WithMaxActiveTasks(maxActiveTasks int) *Consumer {
	c.maxActiveTasks = maxActiveTasks
	return c
}

func (c *Consumer) WithVisibilityTimeout(visibilityTimeout time.Duration) *Consumer {
	c.visibilityTimeout = visibilityTimeout
	return c
}

func (c *Consumer) WithQueues(queues ...string) *Consumer {
	c.queues = queues
	return c
}

func (c *Consumer) Learn(name string, tF taskFunc, override bool) error {
	if _, exists := c.taskFuncMap[name]; exists && !override {
		return fmt.Errorf("task with the name '%s' already learned", name)
	}

	c.taskFuncMap[name] = tF

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
		c.c = make(chan *Job, defaultChannelSize)
	}

	if c.visibilityTimeout <= c.pollInterval {
		return fmt.Errorf("visibility timeout '%v' must be longer than poll interval '%v'", c.visibilityTimeout, c.pollInterval)
	}

	ticker := c.clock.Ticker(c.pollInterval)

	go c.processLoop(ctx, ticker)

	return nil
}

func (c *Consumer) Stop(ctx context.Context) error {
	c.stop <- struct{}{}

	return nil
}

func (c *Consumer) Channel() <-chan *Job {
	return c.c
}

func (c *Consumer) registerTaskStart(ctx context.Context, task *model.Task) {
	_, err := c.client.Repository().RegisterStart(ctx, task)
	if err != nil {
		panic(err)
	}
}

func (c *Consumer) registerTaskError(ctx context.Context, task *model.Task, taskError error) {
	_, err := c.client.Repository().RegisterError(ctx, task, taskError)
	if err != nil {
		panic(err)
	}

	if task.MaxReceives > 0 && (task.ReceiveCount) >= task.MaxReceives {
		c.registerTaskFail(ctx, task)
	} else {
		c.requeueTask(ctx, task)
	}
}

func (c *Consumer) registerTaskSuccess(ctx context.Context, task *model.Task) {
	if c.autoDeleteOnSuccess {
		err := c.client.Repository().DeleteTask(ctx, task)
		if err != nil {
			panic(err)
		}
	} else {
		_, err := c.client.Repository().RegisterSuccess(ctx, task)
		if err != nil {
			panic(err)
		}
	}

	c.removeAsActive(task)
}

func (c *Consumer) registerTaskFail(ctx context.Context, task *model.Task) {
	_, err := c.client.Repository().RegisterFailure(ctx, task)
	if err != nil {
		panic(err)
	}

	c.removeAsActive(task)
}

func (c *Consumer) requeueTask(ctx context.Context, task *model.Task) {
	_, err := c.client.Repository().RequeueTask(ctx, task)
	if err != nil {
		panic(err)
	}

	c.removeAsActive(task)
}

func (c *Consumer) removeAsActive(task *model.Task) {
	delete(c.activeTasks, task.ID)
}

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
		return repository.OrderingCreatedAtFirst, nil
	case PollStrategyByPriority:
		return repository.OrderingPriorityFirst, nil
	default:
		return nil, fmt.Errorf("unknown poll strategy '%s'", c.pollStrategy)
	}
}

func (c *Consumer) getPollQuantity() int {
	taskCapacity := c.maxActiveTasks - len(c.activeTasks)

	if c.pollLimit < taskCapacity {
		return c.pollLimit
	}

	return taskCapacity
}

func (c *Consumer) processLoop(ctx context.Context, ticker *clock.Ticker) {
	defer c.logger.Print("processing stopped")

	defer ticker.Stop()

	for {
		err := c.pingActiveTasks(ctx)
		if err != nil {
			c.logger.Printf("error pinging active tasks: %s", err)
		}

		tasks, err := c.pollForTasks(ctx)
		if err != nil {
			c.logger.Printf("error polling for tasks: %s", err)
		}

		err = c.activateTasks(ctx, tasks)
		if err != nil {
			c.logger.Printf("error activating tasks: %s", err)
		}

		select {
		case <-c.stop:
			close(c.stop)
			close(c.c)
			return
		case <-ticker.C:
			continue
		}
	}
}

func (c *Consumer) pollForTasks(ctx context.Context) ([]*model.Task, error) {
	pollOrdering, err := c.getPollOrdering()
	if err != nil {
		return nil, err
	}

	return c.client.Repository().PollTasks(ctx, c.getKnownTaskTypes(), c.queues, c.visibilityTimeout, pollOrdering, c.getPollQuantity())
}

func (c *Consumer) pingActiveTasks(ctx context.Context) (err error) {
	_, err = c.client.Repository().PingTasks(ctx, c.getActiveTaskIDs(), c.visibilityTimeout)

	return err
}

func (c *Consumer) activateTasks(ctx context.Context, tasks []*model.Task) error {
	var errors []error

	for _, task := range tasks {
		err := c.activateTask(ctx, task)
		if err != nil {
			errors = append(errors, err)
			c.registerTaskFail(ctx, task)
			continue
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("%v tasks could not be activated", len(errors))
	}

	return nil
}

func (c *Consumer) activateTask(ctx context.Context, task *model.Task) error {
	job, err := c.createJobFromTask(ctx, task)
	if err != nil {
		return err
	}

	c.activeTasks[task.ID] = struct{}{}
	c.c <- job

	return nil
}

func (c *Consumer) createJobFromTask(ctx context.Context, task *model.Task) (*Job, error) {
	if taskFunc, ok := c.taskFuncMap[task.Type]; ok {
		return c.newJob(ctx, c, taskFunc, task), nil
	}

	return nil, fmt.Errorf("task type '%s' is not known by this consumer", task.Type)
}

func (c *Consumer) newJob(ctx context.Context, consumer *Consumer, tF taskFunc, task *model.Task) *Job {
	j := Job(func() {
		consumer.registerTaskStart(ctx, task)

		if err := tF(ctx, task); err == nil {
			consumer.registerTaskSuccess(ctx, task)
		} else {
			consumer.registerTaskError(ctx, task, err)
		}
	})

	return &j
}

func (c *Consumer) setClock(clock clock.Clock) *Consumer {
	c.clock = clock
	return c
}
