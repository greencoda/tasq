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
	"github.com/greencoda/tasq/pkg/repository"
)

var (
	ErrConsumerAlreadyRunning    = errors.New("consumer has already been started")
	ErrConsumerAlreadyStopped    = errors.New("consumer has already been stopped")
	ErrCouldNotActivateTasks     = errors.New("a number of tasks could not be activated")
	ErrTaskTypeAlreadyLearned    = errors.New("task with this type already learned")
	ErrTaskTypeNotFound          = errors.New("task with this type not found")
	ErrTaskTypeNotKnown          = errors.New("task with this type is not known by this consumer")
	ErrUnknownPollStrategy       = errors.New("unknown poll strategy")
	ErrVisibilityTimeoutTooShort = errors.New("visibility timeout must be longer than poll interval")
)

// Logger is the interface used for event logging during task consumption.
type Logger interface {
	Print(v ...any)
	Printf(format string, v ...any)
}

type handlerFunc func(task Task) error

type handlerFuncMap map[string]handlerFunc

// PollStrategy is the label assigned to the ordering by which tasks are polled for consumption.
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
// and the various parameters that define the task consumption behaviour.
type Consumer struct {
	running             bool
	autoDeleteOnSuccess bool
	channelSize         int
	pollLimit           int
	maxActiveTasks      int
	pollInterval        time.Duration
	pollStrategy        PollStrategy

	wg sync.WaitGroup

	channel chan *func()
	client  *Client
	clock   clock.Clock
	logger  Logger

	handlerFuncMap handlerFuncMap

	activeTasks map[uuid.UUID]struct{}

	visibilityTimeout time.Duration
	queues            []string

	stop chan struct{}
}

// NewCleaner creates a new consumer with a reference to the original tasq client
// and default consumer parameters.
func (c *Client) NewConsumer() *Consumer {
	return &Consumer{
		running:             false,
		autoDeleteOnSuccess: defaultAutoDeleteOnSuccess,
		channelSize:         defaultChannelSize,
		pollLimit:           defaultPollLimit,
		maxActiveTasks:      defaultMaxActiveTasks,
		pollInterval:        defaultPollInterval,
		pollStrategy:        defaultPollStrategy,

		wg: sync.WaitGroup{},

		channel: nil,
		client:  c,
		clock:   clock.New(),
		logger:  NoopLogger,

		handlerFuncMap: make(handlerFuncMap),

		activeTasks: make(map[uuid.UUID]struct{}),

		visibilityTimeout: defaultVisibilityTimeout,
		queues:            defaultQueues,

		stop: make(chan struct{}, 1),
	}
}

// WithChannelSize sets the size of the buffered channel used for outputting the polled messages to.
//
// Default value: 10.
func (c *Consumer) WithChannelSize(channelSize int) *Consumer {
	c.channelSize = channelSize

	return c
}

// WithLogger sets the Logger interface that is used for event logging during task consumption.
//
// Default value: NoopLogger.
func (c *Consumer) WithLogger(logger Logger) *Consumer {
	c.logger = logger

	return c
}

// WithPollInterval sets the interval at which the consumer will try and poll for new tasks to be executed
// must not be greater than or equal to visibility timeout.
//
// Default value: 5 seconds.
func (c *Consumer) WithPollInterval(pollInterval time.Duration) *Consumer {
	c.pollInterval = pollInterval

	return c
}

// WithPollLimit sets the maximum number of messages polled from the task queue.
//
// Default value: 10.
func (c *Consumer) WithPollLimit(pollLimit int) *Consumer {
	c.pollLimit = pollLimit

	return c
}

// WithPollStrategy sets the ordering to be used when polling for tasks from the task queue.
//
// Default value: PollStrategyByCreatedAt.
func (c *Consumer) WithPollStrategy(pollStrategy PollStrategy) *Consumer {
	c.pollStrategy = pollStrategy

	return c
}

// WithAutoDeleteOnSuccess sets whether successful tasks should be automatically deleted from the task queue
// by the consumer.
//
// Default value: false.
func (c *Consumer) WithAutoDeleteOnSuccess(autoDeleteOnSuccess bool) *Consumer {
	c.autoDeleteOnSuccess = autoDeleteOnSuccess

	return c
}

// WithMaxActiveTasks sets the maximum number of tasks a consumer can have enqueued at the same time
// before polling for additional ones.
//
// Default value: 10.
func (c *Consumer) WithMaxActiveTasks(maxActiveTasks int) *Consumer {
	c.maxActiveTasks = maxActiveTasks

	return c
}

// WithVisibilityTimeout sets the duration by which each ping will extend a task's visibility timeout;
// Once this timeout is up, a consumer instance may receive the task again.
//
// Default value: 15 seconds.
func (c *Consumer) WithVisibilityTimeout(visibilityTimeout time.Duration) *Consumer {
	c.visibilityTimeout = visibilityTimeout

	return c
}

// WithQueues sets the queues from which the consumer may poll for tasks.
//
// Default value: empty slice of strings.
func (c *Consumer) WithQueues(queues ...string) *Consumer {
	c.queues = queues

	return c
}

// Learn sets a handler function for the specified taskType.
// If override is false and a handler function is already set for the specified
// taskType, it'll return an error.
func (c *Consumer) Learn(taskType string, f handlerFunc, override bool) error {
	if _, exists := c.handlerFuncMap[taskType]; exists && !override {
		return fmt.Errorf("%w: %s", ErrTaskTypeAlreadyLearned, taskType)
	}

	c.handlerFuncMap[taskType] = f

	return nil
}

// Forget removes a handler function for the specified taskType from the map of
// learned handler functions.
// If the specified taskType does not exist, it'll return an error.
func (c *Consumer) Forget(taskType string) error {
	if _, exists := c.handlerFuncMap[taskType]; !exists {
		return fmt.Errorf("%w: %s", ErrTaskTypeNotFound, taskType)
	}

	delete(c.handlerFuncMap, taskType)

	return nil
}

// Start launches the go routine which manages the pinging and polling of tasks
// for the consumer, or returns an error if the consumer is not properly configured.
func (c *Consumer) Start() error {
	if c.isRunning() {
		return ErrConsumerAlreadyRunning
	}

	if c.visibilityTimeout <= c.pollInterval {
		return ErrVisibilityTimeoutTooShort
	}

	c.setRunning(true)

	c.channel = make(chan *func(), c.channelSize)

	ticker := c.clock.Ticker(c.pollInterval)

	go c.processLoop(ticker)

	return nil
}

// Stop sends the termination signal to the consumer so it'll no longer poll for news tasks.
func (c *Consumer) Stop() error {
	if !c.isRunning() {
		return ErrConsumerAlreadyStopped
	}

	c.stop <- struct{}{}

	return nil
}

// Channel returns a read-only channel where the polled jobs can be read from.
func (c *Consumer) Channel() <-chan *func() {
	return c.channel
}

func (c *Consumer) isRunning() bool {
	return c.running
}

func (c *Consumer) setRunning(isRunning bool) {
	c.running = isRunning
}

func (c *Consumer) registerTaskStart(task *model.Task) {
	_, err := c.client.repository.RegisterStart(c.client.getContext(), task)
	if err != nil {
		panic(err)
	}
}

func (c *Consumer) registerTaskError(task *model.Task, taskError error) {
	_, err := c.client.repository.RegisterError(c.client.getContext(), task, taskError)
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
		err := c.client.repository.DeleteTask(c.client.getContext(), task)
		if err != nil {
			panic(err)
		}
	} else {
		_, err := c.client.repository.RegisterSuccess(c.client.getContext(), task)
		if err != nil {
			panic(err)
		}
	}

	c.removeFromActiveTasks(task)
}

func (c *Consumer) registerTaskFail(task *model.Task) {
	_, err := c.client.repository.RegisterFailure(c.client.getContext(), task)
	if err != nil {
		panic(err)
	}

	c.removeFromActiveTasks(task)
}

func (c *Consumer) requeueTask(task *model.Task) {
	_, err := c.client.repository.RequeueTask(c.client.getContext(), task)
	if err != nil {
		panic(err)
	}

	c.removeFromActiveTasks(task)
}

func (c *Consumer) getActiveTaskCount() int {
	return len(c.activeTasks)
}

func (c *Consumer) removeFromActiveTasks(task *model.Task) {
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
		return nil, fmt.Errorf("%w: %s", ErrUnknownPollStrategy, c.pollStrategy)
	}
}

func (c *Consumer) getPollQuantity() int {
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
			close(c.channel)
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

	return c.client.repository.PollTasks(c.client.getContext(), c.getKnownTaskTypes(), c.queues, c.visibilityTimeout, pollOrdering, c.getPollQuantity())
}

func (c *Consumer) pingActiveTasks() error {
	_, err := c.client.repository.PingTasks(c.client.getContext(), c.getActiveTaskIDs(), c.visibilityTimeout)

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
		return fmt.Errorf("%w: %v", ErrCouldNotActivateTasks, len(errors))
	}

	return nil
}

func (c *Consumer) activateTask(task *model.Task) error {
	job, err := c.createJobFromTask(task)
	if err != nil {
		return err
	}

	c.activeTasks[task.ID] = struct{}{}

	c.channel <- job

	return nil
}

func (c *Consumer) createJobFromTask(task *model.Task) (*func(), error) {
	if handlerFunc, ok := c.handlerFuncMap[task.Type]; ok {
		return c.newJob(c, handlerFunc, task), nil
	}

	return nil, fmt.Errorf("%w: %s", ErrTaskTypeNotKnown, task.Type)
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
