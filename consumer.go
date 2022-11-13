package tasq

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/google/uuid"
	"github.com/greencoda/tasq/internal/model"
	"github.com/greencoda/tasq/internal/repository"
)

// Logger is the interface used for event logging during task consumption
type Logger interface {
	Print(v ...any)
	Printf(format string, v ...any)
}

type handlerFunc func(task Task) error

type handlerFuncMap map[string]handlerFunc

// PollStrategy is the label assigned to the ordering by which tasks are polled for consumption
type PollStrategy string

const (
	PollStrategyByCreatedAt PollStrategy = "pollByCreatedAt" // Poll by oldest tasks first
	PollStrategyByPriority  PollStrategy = "pollByPriority"  // Poll by highest priority task first
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
	NoopLogger                 = log.New(io.Discard, "", 0) // discards the log messages written to it
)

// Consumer is a service instance created by a Client with reference to that client
// and the various parameters that define the task consumption behaviour
type Consumer struct {
	mu sync.Mutex
	wg sync.WaitGroup

	c      chan *func()
	client *Client
	clock  clock.Clock
	logger Logger

	handlerFuncMap handlerFuncMap

	running     bool
	activeTasks map[uuid.UUID]struct{}

	channelSize         int
	pollInterval        time.Duration
	pollLimit           int
	pollStrategy        PollStrategy
	autoDeleteOnSuccess bool
	maxActiveTasks      int
	visibilityTimeout   time.Duration
	queues              []string

	stop chan struct{}
}

// NewCleaner creates a new consumer with a reference to the original tasq client
// and default consumer parameters
func (c *Client) NewConsumer() *Consumer {
	return &Consumer{
		c:      nil,
		client: c,
		clock:  clock.New(),
		logger: NoopLogger,

		handlerFuncMap: make(handlerFuncMap),

		activeTasks: make(map[uuid.UUID]struct{}),

		channelSize:         defaultChannelSize,
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

// WithChannelSize sets the size of the buffered channel used for outputting the polled messages to
//
// default value: 10
func (c *Consumer) WithChannelSize(channelSize int) *Consumer {
	c.channelSize = channelSize

	return c
}

// WithLogger sets the Logger interface that is used for event logging during task consumption
//
// default value: NoopLogger
func (c *Consumer) WithLogger(logger Logger) *Consumer {
	c.logger = logger
	return c
}

// WithPollInterval sets the interval at which the consumer will try and poll for new tasks to be executed
// must not be greater than or equal to visibility timeout
//
// default value: 5 seconds
func (c *Consumer) WithPollInterval(pollInterval time.Duration) *Consumer {
	c.pollInterval = pollInterval
	return c
}

// WithPollLimit sets the maximum number of messages polled from the task queue
//
// default value: 10
func (c *Consumer) WithPollLimit(pollLimit int) *Consumer {
	c.pollLimit = pollLimit
	return c
}

// WithPollStrategy sets the ordering to be used when polling for tasks from the task queue
//
// default value: PollStrategyByCreatedAt
func (c *Consumer) WithPollStrategy(pollStrategy PollStrategy) *Consumer {
	c.pollStrategy = pollStrategy
	return c
}

// WithAutoDeleteOnSuccess sets whether successful tasks should be automatically deleted from the task queue
// by the consumer
//
// default value: false
func (c *Consumer) WithAutoDeleteOnSuccess(autoDeleteOnSuccess bool) *Consumer {
	c.autoDeleteOnSuccess = autoDeleteOnSuccess
	return c
}

// WithMaxActiveTasks sets the maximum number of tasks a consumer can have enqueued at the same time
// before polling for additional ones
//
// default value: 10
func (c *Consumer) WithMaxActiveTasks(maxActiveTasks int) *Consumer {
	c.maxActiveTasks = maxActiveTasks
	return c
}

// WithVisibilityTimeout sets the duration by which each ping will extend a task's visibility timeout;
// Once this timeout is up, a consumer instance may receive the task again
//
// default value: 15 seconds
func (c *Consumer) WithVisibilityTimeout(visibilityTimeout time.Duration) *Consumer {
	c.visibilityTimeout = visibilityTimeout
	return c
}

// WithQueues sets the queues from which the consumer may poll for tasks
//
// default value: empty slice of strings
func (c *Consumer) WithQueues(queues ...string) *Consumer {
	c.queues = queues
	return c
}

// Learn sets a handler function for the specified taskType;
// If override is false and a handler function is already set for the specified
// taskType, it'll return an error
func (c *Consumer) Learn(taskType string, f handlerFunc, override bool) error {
	if _, exists := c.handlerFuncMap[taskType]; exists && !override {
		return fmt.Errorf("task with the type '%s' already learned", taskType)
	}

	c.handlerFuncMap[taskType] = f

	return nil
}

// Forget removes a handler function for the specified taskType from the map of
// learned handler functions;
// If the specified taskType does not exist, it'll return an error
func (c *Consumer) Forget(taskType string) error {
	if _, exists := c.handlerFuncMap[taskType]; !exists {
		return fmt.Errorf("task with the type '%s' not found", taskType)
	}

	delete(c.handlerFuncMap, taskType)

	return nil
}

// Start launches the go routine which manages the pinging and polling of tasks
// for the consumer, or returns an error if the consumer is not properly configured
func (c *Consumer) Start() error {
	if c.isRunning() {
		return errors.New("consumer has already been started")
	}

	if c.visibilityTimeout <= c.pollInterval {
		return fmt.Errorf("visibility timeout '%v' must be longer than poll interval '%v'", c.visibilityTimeout, c.pollInterval)
	}

	c.setRunning(true)

	c.c = make(chan *func(), c.channelSize)

	ticker := c.clock.Ticker(c.pollInterval)

	go c.processLoop(ticker)

	return nil
}

// Stop sends the termination signal to the consumer so it'll no longer poll for news tasks
func (c *Consumer) Stop() error {
	if !c.isRunning() {
		return errors.New("consumer has already been stopped")
	}

	c.stop <- struct{}{}

	return nil
}

// Channel returns a read-only channel where the polled jobs can be read from
func (c *Consumer) Channel() <-chan *func() {
	return c.c
}

func (c *Consumer) isRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}

func (c *Consumer) setRunning(isRunning bool) {
	c.mu.Lock()
	c.running = isRunning
	c.mu.Unlock()
}

func (c *Consumer) registerTaskStart(task *model.Task) {
	_, err := c.client.getRepository().RegisterStart(c.client.getContext(), task)
	if err != nil {
		panic(err)
	}
}

func (c *Consumer) registerTaskError(task *model.Task, taskError error) {
	_, err := c.client.getRepository().RegisterError(c.client.getContext(), task, taskError)
	if err != nil {
		panic(err)
	}

	if task.MaxReceives > 0 && (task.ReceiveCount) >= task.MaxReceives {
		c.registerTaskFail(task)
	} else {
		c.requeueTask(task)
	}
}

func (c *Consumer) registerTaskSuccess(task *model.Task) {
	if c.autoDeleteOnSuccess {
		err := c.client.getRepository().DeleteTask(c.client.getContext(), task)
		if err != nil {
			panic(err)
		}
	} else {
		_, err := c.client.getRepository().RegisterSuccess(c.client.getContext(), task)
		if err != nil {
			panic(err)
		}
	}

	c.removeAsActive(task)
}

func (c *Consumer) registerTaskFail(task *model.Task) {
	_, err := c.client.getRepository().RegisterFailure(c.client.getContext(), task)
	if err != nil {
		panic(err)
	}

	c.removeAsActive(task)
}

func (c *Consumer) requeueTask(task *model.Task) {
	_, err := c.client.getRepository().RequeueTask(c.client.getContext(), task)
	if err != nil {
		panic(err)
	}

	c.removeAsActive(task)
}

func (c *Consumer) getActiveTaskCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.activeTasks)
}

func (c *Consumer) removeAsActive(task *model.Task) {
	c.mu.Lock()
	delete(c.activeTasks, task.ID)
	c.mu.Unlock()
}

func (c *Consumer) getActiveTaskIDs() []uuid.UUID {
	c.mu.Lock()
	defer c.mu.Unlock()

	activeTaskIDs := make([]uuid.UUID, 0, len(c.activeTasks))

	for taskID := range c.activeTasks {
		activeTaskIDs = append(activeTaskIDs, taskID)
	}

	return activeTaskIDs
}

func (c *Consumer) getKnownTaskTypes() []string {
	taskTypes := make([]string, 0, len(c.handlerFuncMap))

	for taskType := range c.handlerFuncMap {
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
	c.mu.Lock()
	defer c.mu.Unlock()

	taskCapacity := c.maxActiveTasks - len(c.activeTasks)

	if c.pollLimit < taskCapacity {
		return c.pollLimit
	}

	return taskCapacity
}

func (c *Consumer) processLoop(ticker *clock.Ticker) {
	c.wg.Add(1)
	defer c.wg.Done()
	defer c.logger.Print("processing stopped")
	defer ticker.Stop()

	var loopID int

	for {
		loopID++

		err := c.pingActiveTasks()
		if err != nil {
			c.logger.Printf("error pinging active tasks: %s", err)
		}

		if c.isRunning() {
			tasks, err := c.pollForTasks()
			if err != nil {
				c.logger.Printf("error polling for tasks: %s", err)
			}

			err = c.activateTasks(tasks)
			if err != nil {
				c.logger.Printf("error activating tasks: %s", err)
			}
		} else if c.getActiveTaskCount() == 0 {
			return
		}

		select {
		case <-c.stop:
			c.setRunning(false)
			close(c.c)
		case <-ticker.C:
			continue
		}
	}
}

func (c *Consumer) pollForTasks() ([]*model.Task, error) {
	pollOrdering, err := c.getPollOrdering()
	if err != nil {
		return nil, err
	}

	return c.client.getRepository().PollTasks(c.client.getContext(), c.getKnownTaskTypes(), c.queues, c.visibilityTimeout, pollOrdering, c.getPollQuantity())
}

func (c *Consumer) pingActiveTasks() (err error) {
	_, err = c.client.getRepository().PingTasks(c.client.getContext(), c.getActiveTaskIDs(), c.visibilityTimeout)

	return err
}

func (c *Consumer) activateTasks(tasks []*model.Task) error {
	var errors []error

	for _, task := range tasks {
		err := c.activateTask(task)
		if err != nil {
			errors = append(errors, err)
			c.registerTaskFail(task)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("%v tasks could not be activated", len(errors))
	}

	return nil
}

func (c *Consumer) activateTask(task *model.Task) error {
	job, err := c.createJobFromTask(task)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.activeTasks[task.ID] = struct{}{}
	c.mu.Unlock()

	c.c <- job

	return nil
}

func (c *Consumer) createJobFromTask(task *model.Task) (*func(), error) {
	if handlerFunc, ok := c.handlerFuncMap[task.Type]; ok {
		return c.newJob(c, handlerFunc, task), nil
	}

	return nil, fmt.Errorf("task type '%s' is not known by this consumer", task.Type)
}

func (c *Consumer) newJob(consumer *Consumer, f handlerFunc, task *model.Task) *func() {
	job := func() {
		consumer.registerTaskStart(task)

		if err := f(task); err == nil {
			consumer.registerTaskSuccess(task)
		} else {
			consumer.registerTaskError(task, err)
		}
	}

	return &job
}
