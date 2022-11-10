package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"math/rand"
	"time"

	"github.com/greencoda/tasq"
)

const (
	taskType  = "sampleTask"
	taskQueue = "sampleQueue"
)

type SampleTaskArgs struct {
	ID    int
	Value float64
}

func processSampleTask(ctx context.Context, task tasq.Task) error {
	var args SampleTaskArgs

	err := task.UnmarshalArgs(&args)
	if err != nil {
		return errors.New("failed to unmarshal value")
	}

	// do something here with the task arguments as input
	// for purposes of the sample, we'll just log its details here
	taskDetails := task.GetDetails()

	log.Printf("executed task '%s' with args '%+v'", taskDetails.ID, args)

	return nil
}

func consumeTasks(consumer *tasq.Consumer) {
	for {
		job := <-consumer.Channel()
		if job == nil {
			return
		}

		// execute the job right away...
		(*job)()
		// or feed it into a workerpool
		// workerpool.Add(*job)
	}
}

func produceTasks(producer *tasq.Producer, ctx context.Context) {
	taskTicker := time.NewTicker(1 * time.Second)

	for taskIndex := 0; true; taskIndex++ {
		<-taskTicker.C

		rand.Seed(time.Now().UnixNano())
		taskArgs := SampleTaskArgs{
			ID:    taskIndex,
			Value: rand.Float64(),
		}

		t, err := producer.Submit(ctx, taskType, taskArgs, taskQueue, 20, 5)
		if err != nil {
			log.Printf("error while submitting task to tasq: %s", err)
		} else {
			log.Printf("successfully submitted task %s submitted to tasq", t.ID)
		}
	}
}

func main() {
	ctx := context.Background()

	db, err := sql.Open("postgres", "host=127.0.0.1 user=test password=test dbname=test port=5432 sslmode=disable")
	if err != nil {
		log.Fatalf("failed to open DB connection: %v", err)
	}

	// instantiate tasq repository to manage the database connection
	// you can also have it set up the sql DB for you if you provide the dsn string
	// instead of the *sql.DB instance
	tasqRepository, err := tasq.NewRepository(ctx, db, "postgres", "tasq", true)
	if err != nil {
		log.Fatalf("failed to create tasq repository: %s", err)
	}

	// instantiate tasq client
	tasqClient := tasq.NewClient(ctx, tasqRepository)

	// set up tasq cleaner
	cleaner := tasqClient.NewCleaner().
		WithTaskAge(time.Second)

	cleanedTaskCount, err := cleaner.Clean(ctx)
	if err != nil {
		log.Fatalf("failed to clean old tasks from queue: %s", err)
	}

	log.Printf("cleaned %d finished tasks from the queue on startup", cleanedTaskCount)

	// set up tasq consumer
	consumer := tasqClient.NewConsumer().
		WithQueues(taskQueue).
		WithChannelSize(10).
		WithPollInterval(10 * time.Second).
		WithPollStrategy(tasq.PollStrategyByPriority).
		WithAutoDeleteOnSuccess(false)

	// teach the consumer to handle tasks with the type "sampleTask" with the function "processSampleTask"
	err = consumer.Learn(taskType, processSampleTask, false)
	if err != nil {
		log.Fatalf("failed to teach tasq consumer task handler: %s", err)
	}

	// start the consumer
	err = consumer.Start(ctx)
	if err != nil {
		log.Fatalf("failed to start tasq consumer: %s", err)
	}

	// start the goroutine which handles the tasq jobs received from the consumer
	go consumeTasks(consumer)

	// set up tasq producer
	producer := tasqClient.NewProducer()

	// start the goroutine which produces the tasks and submits them to the tasq queue
	go produceTasks(producer, ctx)

	// block the execution
	select {}
}