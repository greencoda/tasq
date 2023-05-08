package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/greencoda/tasq"
	tasqMySQL "github.com/greencoda/tasq/repository/mysql"
)

const (
	channelSize             = 10
	taskType                = "sampleTask"
	taskQueue               = "sampleQueue"
	taskPriority            = 20
	taskScanLimit           = 20
	taskMaxReceives         = 5
	deletionCutoff          = 0.5
	migrationTimeout        = 10 * time.Second
	pollInterval            = 10 * time.Second
	consumerShutdownTimeout = 30 * time.Second
)

// SampleTaskArgs is a struct that represents the arguments for the sample task.
type SampleTaskArgs struct {
	ID    int
	Value float64
}

func processSampleTask(task *tasq.Task) error {
	var args SampleTaskArgs

	err := task.UnmarshalArgs(&args)
	if err != nil {
		return fmt.Errorf("failed to unmarshal value: %w", err)
	}

	// do something here with the task arguments as input
	// for purposes of the sample, we'll just log its details here
	log.Printf("executed task '%s' with args '%+v'", task.ID, args)

	return nil
}

func consumeTasks(consumer *tasq.Consumer, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		job := <-consumer.Channel()
		if job == nil {
			return
		}

		// execute the job right away or feed it into a workerpool
		// such as workerpool.Add(*job)
		(*job)()
	}
}

func produceTasks(ctx context.Context, producer *tasq.Producer) {
	taskTicker := time.NewTicker(1 * time.Second)

	for taskIndex := 0; true; taskIndex++ {
		<-taskTicker.C

		seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))

		taskArgs := SampleTaskArgs{
			ID:    taskIndex,
			Value: seededRand.Float64(),
		}

		t, err := producer.Submit(ctx, taskType, taskArgs, taskQueue, taskPriority, taskMaxReceives)
		if err != nil {
			log.Panicf("error while submitting task to tasq: %s", err)
		} else {
			log.Printf("successfully submitted task '%s'", t.ID)
		}
	}
}

func inspectTasks(ctx context.Context, inspector *tasq.Inspector) {
	taskTicker := time.NewTicker(1 * time.Second)

	for taskIndex := 0; true; taskIndex++ {
		<-taskTicker.C

		taskCount, err := inspector.Count(ctx, nil, []string{taskType}, []string{taskQueue})
		if err != nil {
			log.Panicf("error while counting tasks: %s", err)
		}

		log.Printf("successfully counted %d tasks total", taskCount)

		tasks, err := inspector.Scan(ctx, []tasq.TaskStatus{tasq.StatusNew}, []string{taskType}, []string{taskQueue}, tasq.OrderingCreatedAtFirst, taskScanLimit)
		if err != nil {
			log.Panicf("error while scanning tasks: %s", err)
		}

		log.Printf("successfully scanned %d new tasks", len(tasks))

		for _, task := range tasks {
			var args SampleTaskArgs

			err := task.UnmarshalArgs(&args)
			if err != nil {
				log.Printf("failed to unmarshal value for task '%s'", task.ID)

				continue
			}

			if args.Value < deletionCutoff {
				continue
			}

			err = inspector.Delete(ctx, task)
			if err != nil {
				log.Printf("failed to remove task '%s'", task.ID)

				continue
			}

			log.Printf("successfully removed task '%s'", task.ID)
		}
	}
}

func main() {
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	db, err := sql.Open("mysql", "root:root@/test")
	if err != nil {
		log.Panicf("failed to open DB connection: %v", err)
	}

	// instantiate tasq repository to manage the database connection
	// you can also have it set up the sql DB for you if you provide the dsn string
	// instead of the *sql.DB instance
	tasqRepository, err := tasqMySQL.NewRepository(db, "tasq")
	if err != nil {
		log.Panicf("failed to create tasq repository: %s", err)
	}

	migrationCtx, migrationCancelCtx := context.WithTimeout(context.Background(), migrationTimeout)
	defer migrationCancelCtx()

	err = tasqRepository.Migrate(migrationCtx)
	if err != nil {
		log.Panicf("failed to migrate tasq repository: %s", err)
	}

	log.Print("database migrated successfully")

	// instantiate tasq client
	tasqClient := tasq.NewClient(tasqRepository)

	// set up tasq cleaner
	cleaner := tasqClient.NewCleaner().
		WithTaskAge(time.Second)

	cleanedTaskCount, err := cleaner.Clean(ctx)
	if err != nil {
		log.Panicf("failed to clean old tasks from queue: %s", err)
	}

	log.Printf("cleaned %d finished tasks from the queue on startup", cleanedTaskCount)

	// set up tasq consumer
	consumer := tasqClient.NewConsumer().
		WithQueues(taskQueue).
		WithChannelSize(channelSize).
		WithPollInterval(pollInterval).
		WithPollStrategy(tasq.PollStrategyByPriority).
		WithAutoDeleteOnSuccess(false).
		WithLogger(log.Default())

	// teach the consumer to handle tasks with the type "sampleTask" with the function "processSampleTask"
	err = consumer.Learn(taskType, processSampleTask, false)
	if err != nil {
		log.Panicf("failed to teach tasq consumer task handler: %s", err)
	}

	// start the consumer
	err = consumer.Start(ctx)
	if err != nil {
		log.Panicf("failed to start tasq consumer: %s", err)
	}

	var consumerWg sync.WaitGroup

	// start the goroutine which handles the tasq jobs received from the consumer
	consumerWg.Add(1)

	go consumeTasks(consumer, &consumerWg)

	// set up tasq inspector
	inspector := tasqClient.NewInspector()

	go inspectTasks(ctx, inspector)

	// set up tasq producer
	producer := tasqClient.NewProducer()

	// start the goroutine which produces the tasks and submits them to the tasq queue
	go produceTasks(ctx, producer)

	// block the execution
	<-time.After(consumerShutdownTimeout)

	err = consumer.Stop()
	if err != nil {
		log.Panicf("failed to stop tasq consumer: %s", err)
	}

	// wait until consumer go routine exits
	consumerWg.Wait()
}
