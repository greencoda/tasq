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

func processSampleTask(ctx context.Context, task *tasq.Task) error {
	var args SampleTaskArgs

	err := task.UnmarshalArgs(&args)
	if err != nil {
		return errors.New("failed to unmarshal value")
	}

	// do something here with the task arguments as input
	// for purposes of the sample, we'll just log its details here
	log.Printf("executed task '%s' with args '%+v'", task.ID, args)

	return nil
}

func consumeTasks(consumer tasq.IConsumer) {
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

func produceTasks(producer tasq.IProducer, ctx context.Context) {
	taskTicker := time.NewTicker(1 * time.Second)

	for n := 0; true; n++ {
		<-taskTicker.C

		rand.Seed(time.Now().UnixNano())
		taskArgs := SampleTaskArgs{
			ID:    n,
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

	db.SetMaxOpenConns(1)

	// set up tasq client which manages the DB transactions
	tasqClient, err := tasq.New(ctx, db, "tasq")
	if err != nil {
		log.Fatalf("failed to create tasq client: %s", err)
	}

	// set up tasq consumer
	consumer := tasqClient.NewConsumer().
		WithQueues(taskQueue).
		WithChannelSize(10).
		WithPollStrategy(tasq.PollStrategyByPriority).
		WithAutoDeleteOnSuccess(true)

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
