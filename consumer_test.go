package tasq_test

import (
	"bytes"
	"context"
	"errors"
	"log"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/google/uuid"
	"github.com/greencoda/tasq"
	"github.com/greencoda/tasq/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

var (
	errTaskFail   = errors.New("task failed")
	errRepository = errors.New("repository error")
)

type ConsumerTestSuite struct {
	suite.Suite

	mockRepository *mocks.IRepository
	mockClock      *clock.Mock
	tasqClient     *tasq.Client
	tasqConsumer   *tasq.Consumer
	logBuffer      bytes.Buffer
}

func TestConsumerTestSuite(t *testing.T) {
	t.Parallel()

	suite.Run(t, new(ConsumerTestSuite))
}

func (s *ConsumerTestSuite) SetupTest() {
	s.mockRepository = mocks.NewIRepository(s.T())
	s.mockClock = clock.NewMock()
	s.mockClock.Set(time.Now())

	s.tasqClient = tasq.NewClient(s.mockRepository)
	s.Require().NotNil(s.tasqClient)

	s.tasqConsumer = s.tasqClient.NewConsumer().WithLogger(log.New(&s.logBuffer, "", 0)).SetClock(s.mockClock)
	s.Require().NotNil(s.tasqConsumer)

	s.logBuffer.Reset()
}

func (s *ConsumerTestSuite) TestNewConsumer() {
	s.NotNil(s.tasqConsumer.
		WithAutoDeleteOnSuccess(true).
		WithChannelSize(10).
		WithMaxActiveTasks(10).
		WithPollInterval(10 * time.Second).
		WithPollLimit(10).
		WithPollStrategy(tasq.PollStrategyByCreatedAt).
		WithQueues("testQueue").
		WithVisibilityTimeout(30 * time.Second))

	s.Empty(s.logBuffer.String())
}

func (s *ConsumerTestSuite) TestLearnAndForget() {
	// Learning a new task execution method is successful
	err := s.tasqConsumer.Learn("testTask", func(task *tasq.Task) error {
		return nil
	}, false)
	s.NoError(err)

	// Learning an already learned task execution method returns an error
	err = s.tasqConsumer.Learn("testTask", func(task *tasq.Task) error {
		return nil
	}, false)
	s.Error(err)

	// Learning an already learned task execution method with override being true is successful
	err = s.tasqConsumer.Learn("testTask", func(task *tasq.Task) error {
		return nil
	}, true)
	s.NoError(err)

	// Forgetting an already learned task execution method is successful
	err = s.tasqConsumer.Forget("testTask")
	s.NoError(err)

	// Forgetting an unknown execution method is successful
	err = s.tasqConsumer.Forget("anotherTestTask")
	s.Error(err)

	s.Empty(s.logBuffer.String())
}

func (s *ConsumerTestSuite) TestStartWithInvalidVisibilityTimeoutParam() {
	ctx := context.Background()

	s.tasqConsumer.
		WithVisibilityTimeout(time.Second).
		WithPollInterval(5 * time.Second)

	// Start up the consumer
	err := s.tasqConsumer.Start(ctx)
	s.Error(err)

	s.Empty(s.logBuffer.String())
}

func (s *ConsumerTestSuite) TestStartStopTwice() {
	ctx := context.Background()

	s.tasqConsumer.
		WithQueues("testQueue")

	err := s.tasqConsumer.Learn("testTask", func(task *tasq.Task) error {
		return nil
	}, false)
	s.NoError(err)

	// Getting tasks
	s.mockRepository.On("PingTasks", ctx, []uuid.UUID{}, 15*time.Second).Return([]*tasq.Task{}, nil)

	// Polling fails on the first time
	s.mockRepository.On("PollTasks", ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, tasq.OrderingCreatedAtFirst, 10).Return([]*tasq.Task{}, errRepository).Once()

	// Polling succeeds on subsequent attempts
	s.mockRepository.On("PollTasks", ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, tasq.OrderingCreatedAtFirst, 10).Return([]*tasq.Task{}, nil)

	// Start up the consumer
	err = s.tasqConsumer.Start(ctx)
	s.NoError(err)

	// Start up the consumer again
	err = s.tasqConsumer.Start(ctx)
	s.Error(err)

	// Stop the consumer
	err = s.tasqConsumer.Stop()
	s.NoError(err)

	s.mockClock.Add(5 * time.Second)

	// Stop the consumer again
	err = s.tasqConsumer.Stop()
	s.Error(err)

	// Wait for goroutine to actually return and output log message
	s.tasqConsumer.GetWaitGroup().Wait()

	s.Equal("error polling for tasks: could not poll tasks: repository error\nprocessing stopped\n", s.logBuffer.String())
}

func (s *ConsumerTestSuite) TestConsumption() {
	ctx := context.Background()

	s.tasqConsumer.
		WithQueues("testQueue")

	var (
		successTestArgs = "success"
		failTestArgs    = "fail"
	)

	successTestTask, err := tasq.NewTask("testTask", successTestArgs, "testQueue", 100, 5)
	s.Require().NotNil(successTestTask)
	s.Require().NoError(err)

	failTestTask, err := tasq.NewTask("testTask", failTestArgs, "testQueue", 100, 5)
	s.Require().NotNil(failTestTask)
	s.Require().NoError(err)

	failNoRequeueTestTask, err := tasq.NewTask("testTask", failTestArgs, "testQueue", 100, 1)
	s.Require().NotNil(failNoRequeueTestTask)
	s.Require().NoError(err)

	err = s.tasqConsumer.Learn("testTask", func(task *tasq.Task) error {
		var args string

		err := task.UnmarshalArgs(&args)
		s.Require().NoError(err)

		if args == successTestArgs {
			return nil
		}

		return errTaskFail
	}, false)
	s.Require().NoError(err)

	// Increment receive counts
	for _, task := range []*tasq.Task{
		successTestTask,
		failTestTask,
		failNoRequeueTestTask,
	} {
		task.ReceiveCount++
	}

	// Getting tasks

	// First try - pinging fails
	s.mockRepository.On("PingTasks", ctx, []uuid.UUID{}, 15*time.Second).Once().Return([]*tasq.Task{}, errRepository)

	// Second try - pinging succeeds
	s.mockRepository.On("PingTasks", ctx, []uuid.UUID{}, 15*time.Second).Once().Return([]*tasq.Task{}, nil)
	s.mockRepository.On("PollTasks", ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, tasq.OrderingCreatedAtFirst, 10).Return([]*tasq.Task{
		successTestTask,
		failTestTask,
		failNoRequeueTestTask,
	}, nil)

	// Start up the consumer
	err = s.tasqConsumer.Start(ctx)
	s.NoError(err)

	// Read the successful job from the consumer channel
	successJob := <-s.tasqConsumer.Channel()
	s.NotNil(successJob)

	// First try - registering task start fails
	s.mockRepository.On("RegisterStart", ctx, successTestTask).Once().Return(nil, errRepository)
	s.Panics(func() {
		(*successJob)()
	})

	// Second try - registering task success fails
	s.mockRepository.On("RegisterStart", ctx, successTestTask).Once().Return(successTestTask, nil)
	s.mockRepository.On("RegisterFinish", ctx, successTestTask, tasq.StatusSuccessful).Once().Return(nil, errRepository)
	s.Panics(func() {
		(*successJob)()
	})

	// Third try - repository succeeds
	s.mockRepository.On("RegisterStart", ctx, successTestTask).Once().Return(successTestTask, nil)
	s.mockRepository.On("RegisterFinish", ctx, successTestTask, tasq.StatusSuccessful).Once().Return(successTestTask, nil)
	s.NotPanics(func() {
		(*successJob)()
	})

	// Read the failing job from the consumer channel
	failJob := <-s.tasqConsumer.Channel()
	s.NotNil(failJob)

	// First try - registering task error fails
	s.mockRepository.On("RegisterStart", ctx, failTestTask).Once().Return(failTestTask, nil)
	s.mockRepository.On("RegisterError", ctx, failTestTask, errTaskFail).Once().Return(nil, errRepository)
	s.Panics(func() {
		(*failJob)()
	})

	// Second try - requeuing task fails
	s.mockRepository.On("RegisterStart", ctx, failTestTask).Once().Return(failTestTask, nil)
	s.mockRepository.On("RegisterError", ctx, failTestTask, errTaskFail).Once().Return(failTestTask, nil)
	s.mockRepository.On("RequeueTask", ctx, failTestTask).Once().Return(nil, errRepository)
	s.Panics(func() {
		(*failJob)()
	})

	// Third try - repository succeeds
	s.mockRepository.On("RegisterStart", ctx, failTestTask).Once().Return(failTestTask, nil)
	s.mockRepository.On("RegisterError", ctx, failTestTask, errTaskFail).Once().Return(failTestTask, nil)
	s.mockRepository.On("RequeueTask", ctx, failTestTask).Once().Return(failTestTask, nil)
	s.NotPanics(func() {
		(*failJob)()
	})

	// Read the failing job that shouldn't be requeued from the consumer channel
	failNoRequeueJob := <-s.tasqConsumer.Channel()
	s.NotNil(failNoRequeueJob)

	// First try - registering task failure fails
	s.mockRepository.On("RegisterStart", ctx, failNoRequeueTestTask).Once().Return(failNoRequeueTestTask, nil)
	s.mockRepository.On("RegisterError", ctx, failNoRequeueTestTask, errTaskFail).Once().Return(nil, errRepository)
	s.Panics(func() {
		(*failNoRequeueJob)()
	})

	// Second try - registering task failure fails
	s.mockRepository.On("RegisterStart", ctx, failNoRequeueTestTask).Once().Return(failNoRequeueTestTask, nil)
	s.mockRepository.On("RegisterError", ctx, failNoRequeueTestTask, errTaskFail).Once().Return(failNoRequeueTestTask, nil)
	s.mockRepository.On("RegisterFinish", ctx, failNoRequeueTestTask, tasq.StatusFailed).Once().Return(nil, errRepository)
	s.Panics(func() {
		(*failNoRequeueJob)()
	})

	// Third try - repository succeeds
	s.mockRepository.On("RegisterStart", ctx, failNoRequeueTestTask).Once().Return(failNoRequeueTestTask, nil)
	s.mockRepository.On("RegisterError", ctx, failNoRequeueTestTask, errTaskFail).Once().Return(failNoRequeueTestTask, nil)
	s.mockRepository.On("RegisterFinish", ctx, failNoRequeueTestTask, tasq.StatusFailed).Once().Return(failNoRequeueTestTask, nil)
	s.NotPanics(func() {
		(*failNoRequeueJob)()
	})

	// Stop the consumer
	err = s.tasqConsumer.Stop()
	s.NoError(err)

	// Wait until channel is closed
	<-s.tasqConsumer.Channel()

	s.mockClock.Add(10 * time.Second)

	// Wait for goroutine to actually return and output log message
	s.tasqConsumer.GetWaitGroup().Wait()

	s.Equal("error pinging active tasks: could not ping tasks: repository error\nprocessing stopped\n", s.logBuffer.String())
}

func (s *ConsumerTestSuite) TestConsumptionWithAutoDeleteOnSuccess() {
	ctx := context.Background()

	s.tasqConsumer.
		WithQueues("testQueue").
		WithAutoDeleteOnSuccess(true)

	successTestTask, err := tasq.NewTask("testTask", true, "testQueue", 100, 5)
	s.Require().NotNil(successTestTask)
	s.Require().NoError(err)

	err = s.tasqConsumer.Learn("testTask", func(task *tasq.Task) error {
		return nil
	}, false)
	s.Require().NoError(err)

	successTestTask.ReceiveCount++

	// Getting tasks
	s.mockRepository.On("PingTasks", ctx, []uuid.UUID{}, 15*time.Second).Twice().Return([]*tasq.Task{}, nil)
	s.mockRepository.On("PollTasks", ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, tasq.OrderingCreatedAtFirst, 10).Return([]*tasq.Task{
		successTestTask,
	}, nil)

	// Start up the consumer
	err = s.tasqConsumer.Start(ctx)
	s.NoError(err)

	// Read the successful job from the consumer channel
	successJob := <-s.tasqConsumer.Channel()
	s.NotNil(successJob)

	// First try - deleting task fails
	s.mockRepository.On("RegisterStart", ctx, successTestTask).Once().Return(successTestTask, nil)
	s.mockRepository.On("DeleteTask", ctx, successTestTask, false).Once().Return(errRepository)
	s.Panics(func() {
		(*successJob)()
	})

	// Second try - deleting task succeeds
	s.mockRepository.On("RegisterStart", ctx, successTestTask).Once().Return(successTestTask, nil)
	s.mockRepository.On("DeleteTask", ctx, successTestTask, false).Once().Return(nil)
	s.NotPanics(func() {
		(*successJob)()
	})

	// Stop the consumer
	err = s.tasqConsumer.Stop()
	s.NoError(err)

	// Wait until channel is closed
	<-s.tasqConsumer.Channel()

	s.mockClock.Add(5 * time.Second)

	// Wait for goroutine to actually return and output log message
	s.tasqConsumer.GetWaitGroup().Wait()

	s.Equal("processing stopped\n", s.logBuffer.String())
}

func (s *ConsumerTestSuite) TestConsumptionWithPollStrategyByPriority() {
	ctx := context.Background()

	s.tasqConsumer.
		WithQueues("testQueue").
		WithPollStrategy(tasq.PollStrategyByPriority)

	successTestTask, err := tasq.NewTask("testTask", true, "testQueue", 100, 5)
	s.Require().NotNil(successTestTask)
	s.Require().NoError(err)

	err = s.tasqConsumer.Learn("testTask", func(task *tasq.Task) error {
		return nil
	}, false)
	s.Require().NoError(err)

	successTestTask.ReceiveCount++

	// Getting tasks
	s.mockRepository.On("PingTasks", ctx, []uuid.UUID{}, 15*time.Second).Twice().Return([]*tasq.Task{}, nil)
	s.mockRepository.On("PollTasks", ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, tasq.OrderingPriorityFirst, 10).Return([]*tasq.Task{
		successTestTask,
	}, nil)

	// Start up the consumer
	err = s.tasqConsumer.Start(ctx)
	s.NoError(err)

	// Read the successful job from the consumer channel
	successJob := <-s.tasqConsumer.Channel()
	s.NotNil(successJob)

	// First try - repository succeeds
	s.mockRepository.On("RegisterStart", ctx, successTestTask).Once().Return(successTestTask, nil)
	s.mockRepository.On("RegisterFinish", ctx, successTestTask, tasq.StatusSuccessful).Once().Return(successTestTask, nil)
	s.NotPanics(func() {
		(*successJob)()
	})

	// Stop the consumer
	err = s.tasqConsumer.Stop()
	s.NoError(err)

	// Wait until channel is closed
	<-s.tasqConsumer.Channel()

	s.mockClock.Add(5 * time.Second)

	// Wait for goroutine to actually return and output log message
	s.tasqConsumer.GetWaitGroup().Wait()

	s.Equal("processing stopped\n", s.logBuffer.String())
}

func (s *ConsumerTestSuite) TestConsumptionWithUnknownPollStrategy() {
	ctx := context.Background()

	s.tasqConsumer.
		WithQueues("testQueue").
		WithPollStrategy(tasq.PollStrategy("pollByMagic"))

	// Getting tasks
	s.mockRepository.On("PingTasks", ctx, []uuid.UUID{}, 15*time.Second).Twice().Return([]*tasq.Task{}, nil)

	// Start up the consumer
	err := s.tasqConsumer.Start(ctx)
	s.NoError(err)

	// Stop the consumer
	err = s.tasqConsumer.Stop()
	s.NoError(err)

	// Wait until channel is closed
	<-s.tasqConsumer.Channel()

	s.mockClock.Add(5 * time.Second)

	// Wait for goroutine to actually return and output log message
	s.tasqConsumer.GetWaitGroup().Wait()

	s.Equal("error polling for tasks: unknown poll strategy: pollByMagic\nprocessing stopped\n", s.logBuffer.String())
}

func (s *ConsumerTestSuite) TestConsumptionOfUnknownTaskType() {
	ctx := context.Background()

	s.tasqConsumer.
		WithQueues("testQueue")

	anotherTestTask, err := tasq.NewTask("anotherTestTask", true, "testQueue", 100, 5)
	s.Require().NotNil(anotherTestTask)
	s.Require().NoError(err)

	err = s.tasqConsumer.Learn("testTask", func(task *tasq.Task) error {
		return nil
	}, false)
	s.Require().NoError(err)

	// Getting tasks
	s.mockRepository.On("PingTasks", ctx, []uuid.UUID{}, 15*time.Second).Twice().Return([]*tasq.Task{}, nil)
	s.mockRepository.On("PollTasks", ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, tasq.OrderingCreatedAtFirst, 10).Return([]*tasq.Task{
		anotherTestTask,
	}, nil)

	// Start up the consumer
	err = s.tasqConsumer.Start(ctx)
	s.NoError(err)

	s.mockRepository.On("RegisterFinish", ctx, anotherTestTask, tasq.StatusFailed).Return(anotherTestTask, nil)

	// Stop the consumer
	err = s.tasqConsumer.Stop()
	s.NoError(err)

	// Wait until channel is closed
	<-s.tasqConsumer.Channel()

	s.mockClock.Add(5 * time.Second)

	// Wait for goroutine to actually return and output log message
	s.tasqConsumer.GetWaitGroup().Wait()

	s.Equal("error activating tasks: a number of tasks could not be activated: 1\nprocessing stopped\n", s.logBuffer.String())
}

func (s *ConsumerTestSuite) TestLoopingConsumption() {
	s.tasqConsumer.
		WithQueues("testQueue").
		WithPollInterval(5 * time.Second).
		WithPollLimit(1).
		WithMaxActiveTasks(2)

	var (
		ctx = context.Background()

		testTaskID1 = uuid.MustParse("1ada263f-61d5-44ac-b99d-2d5ad4f249de")
		testTaskID2 = uuid.MustParse("28032675-bc13-4dcd-8ec6-6aa430fc466a")

		testTasks = map[uuid.UUID]*tasq.Task{
			testTaskID1: {
				ID:           testTaskID1,
				Type:         "testTask",
				Args:         []uint8{0x3, 0x2, 0x0, 0x1},
				Queue:        "testQueue",
				Priority:     100,
				Status:       tasq.StatusNew,
				ReceiveCount: 0,
				MaxReceives:  5,
				CreatedAt:    s.mockClock.Now(),
				VisibleAt:    s.mockClock.Now(),
			},
			testTaskID2: {
				ID:           testTaskID2,
				Type:         "testTask",
				Args:         []uint8{0x3, 0x2, 0x0, 0x1},
				Queue:        "testQueue",
				Priority:     100,
				Status:       tasq.StatusNew,
				ReceiveCount: 0,
				MaxReceives:  5,
				CreatedAt:    s.mockClock.Now(),
				VisibleAt:    s.mockClock.Now(),
			},
		}
	)

	err := s.tasqConsumer.Learn("testTask", func(task *tasq.Task) error {
		return nil
	}, false)
	s.Require().NoError(err)

	// Respond to pings
	pingCall := s.mockRepository.On("PingTasks", ctx, mock.AnythingOfType("[]uuid.UUID"), 15*time.Second)
	pingCall.Run(func(args mock.Arguments) {
		inputTestIDs, ok := args[1].([]uuid.UUID)
		s.Require().True(ok)

		taskIDs, ok := args[1].([]uuid.UUID)
		s.Require().True(ok)

		returnTasks := make([]*tasq.Task, 0, len(inputTestIDs))

		for _, taskID := range taskIDs {
			returnTask, ok := testTasks[taskID]
			s.Require().True(ok)

			returnTasks = append(returnTasks, returnTask)
		}

		pingCall.ReturnArguments = mock.Arguments{returnTasks, nil}
	})

	// Respond to polls
	// First call
	s.mockRepository.On("PollTasks", ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, tasq.OrderingCreatedAtFirst, 1).Once().
		Return([]*tasq.Task{
			testTasks[testTaskID1],
		}, nil)

	// Second call
	s.mockRepository.On("PollTasks", ctx, []string{"testTask"}, []string{"testQueue"}, 15*time.Second, tasq.OrderingCreatedAtFirst, 1).Once().
		Return([]*tasq.Task{
			testTasks[testTaskID2],
		}, nil)

	// Start up the consumer
	err = s.tasqConsumer.Start(ctx)
	s.NoError(err)

	s.mockClock.Add(5 * time.Second)

	// Stop the consumer
	err = s.tasqConsumer.Stop()
	s.NoError(err)

	// Mock job handling
	s.mockRepository.On("RegisterStart", ctx, testTasks[testTaskID1]).Once().
		Return(testTasks[testTaskID1], nil)
	s.mockRepository.On("RegisterFinish", ctx, testTasks[testTaskID1], tasq.StatusSuccessful).Once().
		Return(testTasks[testTaskID1], nil)

	s.mockRepository.On("RegisterStart", ctx, testTasks[testTaskID2]).Once().
		Return(testTasks[testTaskID2], nil)
	s.mockRepository.On("RegisterFinish", ctx, testTasks[testTaskID2], tasq.StatusSuccessful).Once().
		Return(testTasks[testTaskID2], nil)

	// Drain channel of jobs
	for job := range s.tasqConsumer.Channel() {
		currentJob := job

		s.NotPanics(func() {
			(*currentJob)()
		})
	}

	// Let 5 seconds pass so that the goroutine has a chance to finish its last loop
	s.mockClock.Add(5 * time.Second)

	// Wait for goroutine to actually return and output log message
	s.tasqConsumer.GetWaitGroup().Wait()

	s.Equal("processing stopped\n", s.logBuffer.String())
}
