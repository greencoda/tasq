package tasq

import (
	"bytes"
	"context"
	"errors"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/google/uuid"
	"github.com/greencoda/tasq/internal/model"
	mock_repository "github.com/greencoda/tasq/pkg/mocks/repository"
	"github.com/greencoda/tasq/pkg/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func (c *Consumer) setClock(clock clock.Clock) *Consumer {
	c.clock = clock
	return c
}

func (c *Consumer) getWaitGroup() *sync.WaitGroup {
	return &c.wg
}

type ConsumerTestSuite struct {
	suite.Suite
	mockRepository *mock_repository.IRepository
	mockClock      *clock.Mock
	tasqClient     *Client
	tasqConsumer   *Consumer
	logBuffer      bytes.Buffer
}

func (s *ConsumerTestSuite) SetupTest() {
	s.mockRepository = mock_repository.NewIRepository(s.T())
	s.mockClock = clock.NewMock()
	s.mockClock.Set(time.Now())

	s.tasqClient = NewClient(context.Background(), s.mockRepository)
	require.NotNil(s.T(), s.tasqClient)

	s.tasqConsumer = s.tasqClient.NewConsumer().WithLogger(log.New(&s.logBuffer, "", 0)).setClock(s.mockClock)
	require.NotNil(s.T(), s.tasqConsumer)

	s.logBuffer.Truncate(0)
}

func (s *ConsumerTestSuite) TestNewConsumer() {
	assert.NotNil(s.T(), s.tasqConsumer.
		WithAutoDeleteOnSuccess(true).
		WithChannelSize(10).
		WithMaxActiveTasks(10).
		WithPollInterval(10*time.Second).
		WithPollLimit(10).
		WithPollStrategy(PollStrategyByCreatedAt).
		WithQueues("testQueue").
		WithVisibilityTimeout(30*time.Second))

	assert.Empty(s.T(), s.logBuffer.String())
}

func (s *ConsumerTestSuite) TestLearnAndForget() {
	// Learning a new task execution method is successful
	err := s.tasqConsumer.Learn("testTask", func(task Task) error {
		return nil
	}, false)
	assert.Nil(s.T(), err)

	// Learning an already learned task execution method returns an error
	err = s.tasqConsumer.Learn("testTask", func(task Task) error {
		return nil
	}, false)
	assert.NotNil(s.T(), err)

	// Learning an already learned task execution method with override being true is successful
	err = s.tasqConsumer.Learn("testTask", func(task Task) error {
		return nil
	}, true)
	assert.Nil(s.T(), err)

	// Forgetting an already learned task execution method is successful
	err = s.tasqConsumer.Forget("testTask")
	assert.Nil(s.T(), err)

	// Forgetting an unknown execution method is successful
	err = s.tasqConsumer.Forget("anotherTestTask")
	assert.NotNil(s.T(), err)

	assert.Empty(s.T(), s.logBuffer.String())
}

func (s *ConsumerTestSuite) TestStartWithInvalidVisibilityTimeoutParam() {
	s.tasqConsumer.
		WithVisibilityTimeout(time.Second).
		WithPollInterval(5 * time.Second)

	// Start up the consumer
	err := s.tasqConsumer.Start()
	assert.NotNil(s.T(), err)

	assert.Empty(s.T(), s.logBuffer.String())
}

func (s *ConsumerTestSuite) TestStartStopTwice() {
	s.tasqConsumer.
		WithQueues("testQueue")

	err := s.tasqConsumer.Learn("testTask", func(task Task) error {
		return nil
	}, false)
	assert.Nil(s.T(), err)

	// Getting tasks
	s.mockRepository.On("PingTasks", s.tasqClient.getContext(), []uuid.UUID{}, 15*time.Second).Return([]*model.Task{}, nil)
	s.mockRepository.On("PollTasks", s.tasqClient.getContext(), []string{"testTask"}, []string{"testQueue"}, 15*time.Second, repository.OrderingCreatedAtFirst, 10).Return([]*model.Task{}, nil)

	// Start up the consumer
	err = s.tasqConsumer.Start()
	assert.Nil(s.T(), err)

	// Start up the consumer again
	err = s.tasqConsumer.Start()
	assert.NotNil(s.T(), err)

	// Stop the consumer
	err = s.tasqConsumer.Stop()
	assert.Nil(s.T(), err)

	s.mockClock.Add(5 * time.Second)

	// Stop the consumer again
	err = s.tasqConsumer.Stop()
	assert.NotNil(s.T(), err)

	// Wait for goroutine to actually return and output log message
	s.tasqConsumer.getWaitGroup().Wait()

	assert.Equal(s.T(), "processing stopped\n", s.logBuffer.String())
}

func (s *ConsumerTestSuite) TestConsumption() {
	s.tasqConsumer.
		WithQueues("testQueue")

	var (
		successTestArgs = "success"
		failTestArgs    = "fail"

		successTestTask       = model.NewTask("testTask", successTestArgs, "testQueue", 100, 5)
		failTestTask          = model.NewTask("testTask", failTestArgs, "testQueue", 100, 5)
		failNoRequeueTestTask = model.NewTask("testTask", failTestArgs, "testQueue", 100, 1)

		errTaskFail   = errors.New("task failed")
		errRepository = errors.New("repository error")
	)

	err := s.tasqConsumer.Learn("testTask", func(task Task) error {
		var args string

		err := task.UnmarshalArgs(&args)
		require.Nil(s.T(), err)

		if args == successTestArgs {
			return nil
		} else {
			return errTaskFail
		}
	}, false)
	require.Nil(s.T(), err)

	// Increment receive counts
	for _, task := range []*model.Task{
		successTestTask,
		failTestTask,
		failNoRequeueTestTask,
	} {
		task.ReceiveCount++
	}

	// Getting tasks

	// First try - pinging fails
	s.mockRepository.On("PingTasks", s.tasqClient.getContext(), []uuid.UUID{}, 15*time.Second).Once().Return([]*model.Task{}, errRepository)

	// First try - pinging succeeds
	s.mockRepository.On("PingTasks", s.tasqClient.getContext(), []uuid.UUID{}, 15*time.Second).Once().Return([]*model.Task{}, nil)
	s.mockRepository.On("PollTasks", s.tasqClient.getContext(), []string{"testTask"}, []string{"testQueue"}, 15*time.Second, repository.OrderingCreatedAtFirst, 10).Return([]*model.Task{
		successTestTask,
		failTestTask,
		failNoRequeueTestTask,
	}, nil)

	// Start up the consumer
	err = s.tasqConsumer.Start()
	assert.Nil(s.T(), err)

	// Read the successful job from the consumer channel
	successJob := <-s.tasqConsumer.Channel()
	assert.NotNil(s.T(), successJob)

	// First try - registering task start fails
	s.mockRepository.On("RegisterStart", s.tasqClient.getContext(), successTestTask).Once().Return(nil, errRepository)
	assert.Panics(s.T(), func() {
		(*successJob)()
	})

	// Second try - registering task success fails
	s.mockRepository.On("RegisterStart", s.tasqClient.getContext(), successTestTask).Once().Return(successTestTask, nil)
	s.mockRepository.On("RegisterSuccess", s.tasqClient.getContext(), successTestTask).Once().Return(nil, errRepository)
	assert.Panics(s.T(), func() {
		(*successJob)()
	})

	// Third try - repository succeeds
	s.mockRepository.On("RegisterStart", s.tasqClient.getContext(), successTestTask).Once().Return(successTestTask, nil)
	s.mockRepository.On("RegisterSuccess", s.tasqClient.getContext(), successTestTask).Once().Return(successTestTask, nil)
	assert.NotPanics(s.T(), func() {
		(*successJob)()
	})

	// Read the failing job from the consumer channel
	failJob := <-s.tasqConsumer.Channel()
	assert.NotNil(s.T(), failJob)

	// First try - registering task error fails
	s.mockRepository.On("RegisterStart", s.tasqClient.getContext(), failTestTask).Once().Return(failTestTask, nil)
	s.mockRepository.On("RegisterError", s.tasqClient.getContext(), failTestTask, errTaskFail).Once().Return(nil, errRepository)
	assert.Panics(s.T(), func() {
		(*failJob)()
	})

	// Second try - requeuing task fails
	s.mockRepository.On("RegisterStart", s.tasqClient.getContext(), failTestTask).Once().Return(failTestTask, nil)
	s.mockRepository.On("RegisterError", s.tasqClient.getContext(), failTestTask, errTaskFail).Once().Return(failTestTask, nil)
	s.mockRepository.On("RequeueTask", s.tasqClient.getContext(), failTestTask).Once().Return(nil, errRepository)
	assert.Panics(s.T(), func() {
		(*failJob)()
	})

	// Third try - repository succeeds
	s.mockRepository.On("RegisterStart", s.tasqClient.getContext(), failTestTask).Once().Return(failTestTask, nil)
	s.mockRepository.On("RegisterError", s.tasqClient.getContext(), failTestTask, errTaskFail).Once().Return(failTestTask, nil)
	s.mockRepository.On("RequeueTask", s.tasqClient.getContext(), failTestTask).Once().Return(failTestTask, nil)
	assert.NotPanics(s.T(), func() {
		(*failJob)()
	})

	// Read the failing job that shouldn't be requeued from the consumer channel
	failNoRequeueJob := <-s.tasqConsumer.Channel()
	assert.NotNil(s.T(), failNoRequeueJob)

	// First try - registering task failure fails
	s.mockRepository.On("RegisterStart", s.tasqClient.getContext(), failNoRequeueTestTask).Once().Return(failNoRequeueTestTask, nil)
	s.mockRepository.On("RegisterError", s.tasqClient.getContext(), failNoRequeueTestTask, errTaskFail).Once().Return(nil, errRepository)
	assert.Panics(s.T(), func() {
		(*failNoRequeueJob)()
	})

	// Second try - registering task failure fails
	s.mockRepository.On("RegisterStart", s.tasqClient.getContext(), failNoRequeueTestTask).Once().Return(failNoRequeueTestTask, nil)
	s.mockRepository.On("RegisterError", s.tasqClient.getContext(), failNoRequeueTestTask, errTaskFail).Once().Return(failNoRequeueTestTask, nil)
	s.mockRepository.On("RegisterFailure", s.tasqClient.getContext(), failNoRequeueTestTask).Once().Return(nil, errRepository)
	assert.Panics(s.T(), func() {
		(*failNoRequeueJob)()
	})

	// Third try - repository succeeds
	s.mockRepository.On("RegisterStart", s.tasqClient.getContext(), failNoRequeueTestTask).Once().Return(failNoRequeueTestTask, nil)
	s.mockRepository.On("RegisterError", s.tasqClient.getContext(), failNoRequeueTestTask, errTaskFail).Once().Return(failNoRequeueTestTask, nil)
	s.mockRepository.On("RegisterFailure", s.tasqClient.getContext(), failNoRequeueTestTask).Once().Return(failNoRequeueTestTask, nil)
	assert.NotPanics(s.T(), func() {
		(*failNoRequeueJob)()
	})

	// Stop the consumer
	err = s.tasqConsumer.Stop()
	assert.Nil(s.T(), err)

	// Wait until channel is closed
	<-s.tasqConsumer.Channel()

	s.mockClock.Add(10 * time.Second)

	// Wait for goroutine to actually return and output log message
	s.tasqConsumer.getWaitGroup().Wait()

	assert.Equal(s.T(), "error pinging active tasks: repository error\nprocessing stopped\n", s.logBuffer.String())
}

func (s *ConsumerTestSuite) TestConsumptionWithAutoDeleteOnSuccess() {
	s.tasqConsumer.
		WithQueues("testQueue").
		WithAutoDeleteOnSuccess(true)

	var (
		successTestTask = model.NewTask("testTask", true, "testQueue", 100, 5)
		errRepository   = errors.New("repository error")
	)

	err := s.tasqConsumer.Learn("testTask", func(task Task) error {
		return nil
	}, false)
	require.Nil(s.T(), err)

	successTestTask.ReceiveCount++

	// Getting tasks
	s.mockRepository.On("PingTasks", s.tasqClient.getContext(), []uuid.UUID{}, 15*time.Second).Twice().Return([]*model.Task{}, nil)
	s.mockRepository.On("PollTasks", s.tasqClient.getContext(), []string{"testTask"}, []string{"testQueue"}, 15*time.Second, repository.OrderingCreatedAtFirst, 10).Return([]*model.Task{
		successTestTask,
	}, nil)

	// Start up the consumer
	err = s.tasqConsumer.Start()
	assert.Nil(s.T(), err)

	// Read the successful job from the consumer channel
	successJob := <-s.tasqConsumer.Channel()
	assert.NotNil(s.T(), successJob)

	// First try - deleting task fails
	s.mockRepository.On("RegisterStart", s.tasqClient.getContext(), successTestTask).Once().Return(successTestTask, nil)
	s.mockRepository.On("DeleteTask", s.tasqClient.getContext(), successTestTask).Once().Return(errRepository)
	assert.Panics(s.T(), func() {
		(*successJob)()
	})

	// Second try - deleting task succeeds
	s.mockRepository.On("RegisterStart", s.tasqClient.getContext(), successTestTask).Once().Return(successTestTask, nil)
	s.mockRepository.On("DeleteTask", s.tasqClient.getContext(), successTestTask).Once().Return(nil)
	assert.NotPanics(s.T(), func() {
		(*successJob)()
	})

	// Stop the consumer
	err = s.tasqConsumer.Stop()
	assert.Nil(s.T(), err)

	// Wait until channel is closed
	<-s.tasqConsumer.Channel()

	s.mockClock.Add(5 * time.Second)

	// Wait for goroutine to actually return and output log message
	s.tasqConsumer.getWaitGroup().Wait()

	assert.Equal(s.T(), "processing stopped\n", s.logBuffer.String())
}

func (s *ConsumerTestSuite) TestConsumptionWithPollStrategyByPriority() {
	s.tasqConsumer.
		WithQueues("testQueue").
		WithPollStrategy(PollStrategyByPriority)

	successTestTask := model.NewTask("testTask", true, "testQueue", 100, 5)

	err := s.tasqConsumer.Learn("testTask", func(task Task) error {
		return nil
	}, false)
	require.Nil(s.T(), err)

	successTestTask.ReceiveCount++

	// Getting tasks
	s.mockRepository.On("PingTasks", s.tasqClient.getContext(), []uuid.UUID{}, 15*time.Second).Twice().Return([]*model.Task{}, nil)
	s.mockRepository.On("PollTasks", s.tasqClient.getContext(), []string{"testTask"}, []string{"testQueue"}, 15*time.Second, repository.OrderingPriorityFirst, 10).Return([]*model.Task{
		successTestTask,
	}, nil)

	// Start up the consumer
	err = s.tasqConsumer.Start()
	assert.Nil(s.T(), err)

	// Read the successful job from the consumer channel
	successJob := <-s.tasqConsumer.Channel()
	assert.NotNil(s.T(), successJob)

	// First try - repository succeeds
	s.mockRepository.On("RegisterStart", s.tasqClient.getContext(), successTestTask).Once().Return(successTestTask, nil)
	s.mockRepository.On("RegisterSuccess", s.tasqClient.getContext(), successTestTask).Once().Return(successTestTask, nil)
	assert.NotPanics(s.T(), func() {
		(*successJob)()
	})

	// Stop the consumer
	err = s.tasqConsumer.Stop()
	assert.Nil(s.T(), err)

	// Wait until channel is closed
	<-s.tasqConsumer.Channel()

	s.mockClock.Add(5 * time.Second)

	// Wait for goroutine to actually return and output log message
	s.tasqConsumer.getWaitGroup().Wait()

	assert.Equal(s.T(), "processing stopped\n", s.logBuffer.String())
}

func (s *ConsumerTestSuite) TestConsumptionWithUnknownPollStrategy() {
	s.tasqConsumer.
		WithQueues("testQueue").
		WithPollStrategy(PollStrategy("pollByMagic"))

	// Getting tasks
	s.mockRepository.On("PingTasks", s.tasqClient.getContext(), []uuid.UUID{}, 15*time.Second).Twice().Return([]*model.Task{}, nil)

	// Start up the consumer
	err := s.tasqConsumer.Start()
	assert.Nil(s.T(), err)

	// Stop the consumer
	err = s.tasqConsumer.Stop()
	assert.Nil(s.T(), err)

	// Wait until channel is closed
	<-s.tasqConsumer.Channel()

	s.mockClock.Add(5 * time.Second)

	// Wait for goroutine to actually return and output log message
	s.tasqConsumer.getWaitGroup().Wait()

	assert.Equal(s.T(), "error polling for tasks: unknown poll strategy 'pollByMagic'\nprocessing stopped\n", s.logBuffer.String())
}

func (s *ConsumerTestSuite) TestConsumptionOfUnknownTaskType() {
	s.tasqConsumer.
		WithQueues("testQueue")

	anotherTestTask := model.NewTask("anotherTestTask", true, "testQueue", 100, 5)

	err := s.tasqConsumer.Learn("testTask", func(task Task) error {
		return nil
	}, false)
	require.Nil(s.T(), err)

	// Getting tasks
	s.mockRepository.On("PingTasks", s.tasqClient.getContext(), []uuid.UUID{}, 15*time.Second).Twice().Return([]*model.Task{}, nil)
	s.mockRepository.On("PollTasks", s.tasqClient.getContext(), []string{"testTask"}, []string{"testQueue"}, 15*time.Second, repository.OrderingCreatedAtFirst, 10).Return([]*model.Task{
		anotherTestTask,
	}, nil)

	// Start up the consumer
	err = s.tasqConsumer.Start()
	assert.Nil(s.T(), err)

	s.mockRepository.On("RegisterFailure", s.tasqClient.getContext(), anotherTestTask).Return(anotherTestTask, nil)

	// Stop the consumer
	err = s.tasqConsumer.Stop()
	assert.Nil(s.T(), err)

	// Wait until channel is closed
	<-s.tasqConsumer.Channel()

	s.mockClock.Add(5 * time.Second)

	// Wait for goroutine to actually return and output log message
	s.tasqConsumer.getWaitGroup().Wait()

	assert.Equal(s.T(), "error activating tasks: 1 tasks could not be activated\nprocessing stopped\n", s.logBuffer.String())
}

func (s *ConsumerTestSuite) TestLoopingConsumption() {
	s.tasqConsumer.
		WithQueues("testQueue").
		WithPollInterval(5 * time.Second).
		WithPollLimit(1).
		WithMaxActiveTasks(2)

	var (
		testTaskID_1 = uuid.MustParse("1ada263f-61d5-44ac-b99d-2d5ad4f249de")
		testTaskID_2 = uuid.MustParse("28032675-bc13-4dcd-8ec6-6aa430fc466a")

		testTasks = map[uuid.UUID]*model.Task{
			testTaskID_1: {
				ID:           testTaskID_1,
				Type:         "testTask",
				Args:         []uint8{0x3, 0x2, 0x0, 0x1},
				Queue:        "testQueue",
				Priority:     100,
				Status:       model.StatusNew,
				ReceiveCount: 0,
				MaxReceives:  5,
				CreatedAt:    s.mockClock.Now(),
				VisibleAt:    s.mockClock.Now(),
			},
			testTaskID_2: {
				ID:           testTaskID_2,
				Type:         "testTask",
				Args:         []uint8{0x3, 0x2, 0x0, 0x1},
				Queue:        "testQueue",
				Priority:     100,
				Status:       model.StatusNew,
				ReceiveCount: 0,
				MaxReceives:  5,
				CreatedAt:    s.mockClock.Now(),
				VisibleAt:    s.mockClock.Now(),
			},
		}
	)

	err := s.tasqConsumer.Learn("testTask", func(task Task) error {
		return nil
	}, false)
	require.Nil(s.T(), err)

	// Respond to pings
	pingCall := s.mockRepository.On("PingTasks", s.tasqClient.getContext(), mock.AnythingOfType("[]uuid.UUID"), 15*time.Second)
	pingCall.Run(func(args mock.Arguments) {
		inputTestIDs, ok := args[1].([]uuid.UUID)
		require.True(s.T(), ok)

		returnTasks := make([]*model.Task, 0, len(inputTestIDs))
		for _, taskID := range args[1].([]uuid.UUID) {
			returnTask, ok := testTasks[taskID]
			require.True(s.T(), ok)

			returnTasks = append(returnTasks, returnTask)
		}

		pingCall.ReturnArguments = mock.Arguments{returnTasks, nil}
	})

	// Respond to polls
	// First call
	s.mockRepository.On("PollTasks", s.tasqClient.getContext(), []string{"testTask"}, []string{"testQueue"}, 15*time.Second, repository.OrderingCreatedAtFirst, 1).Once().
		Return([]*model.Task{
			testTasks[testTaskID_1],
		}, nil)

	// Second call
	s.mockRepository.On("PollTasks", s.tasqClient.getContext(), []string{"testTask"}, []string{"testQueue"}, 15*time.Second, repository.OrderingCreatedAtFirst, 1).Once().
		Return([]*model.Task{
			testTasks[testTaskID_2],
		}, nil)

	// Start up the consumer
	err = s.tasqConsumer.Start()
	assert.Nil(s.T(), err)

	s.mockClock.Add(5 * time.Second)

	// Stop the consumer
	err = s.tasqConsumer.Stop()
	assert.Nil(s.T(), err)

	// Mock job handling
	s.mockRepository.On("RegisterStart", s.tasqClient.getContext(), testTasks[testTaskID_1]).Once().
		Return(testTasks[testTaskID_1], nil)
	s.mockRepository.On("RegisterSuccess", s.tasqClient.getContext(), testTasks[testTaskID_1]).Once().
		Return(testTasks[testTaskID_1], nil)

	s.mockRepository.On("RegisterStart", s.tasqClient.getContext(), testTasks[testTaskID_2]).Once().
		Return(testTasks[testTaskID_2], nil)
	s.mockRepository.On("RegisterSuccess", s.tasqClient.getContext(), testTasks[testTaskID_2]).Once().
		Return(testTasks[testTaskID_2], nil)

	// Drain channel of jobs
	for job := range s.tasqConsumer.Channel() {
		assert.NotPanics(s.T(), func() {
			(*job)()
		})
	}

	// Let 5 seconds pass so that the goroutine has a chance to finish its last loop
	s.mockClock.Add(5 * time.Second)

	// Wait for goroutine to actually return and output log message
	s.tasqConsumer.getWaitGroup().Wait()

	assert.Equal(s.T(), "processing stopped\n", s.logBuffer.String())
}

func TestConsumerTestSuite(t *testing.T) {
	suite.Run(t, new(ConsumerTestSuite))
}
