package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/greencoda/tasq"
	tasqModel "github.com/greencoda/tasq/pkg"
	tasqMySQL "github.com/greencoda/tasq/repository/mysql"
)

const (
	taskType  = "sampleTask"
	taskQueue = "sampleQueue"
)

type SampleTaskArgs struct {
	ID    int
	Value float64
}

func processSampleTask(task *tasqModel.Task) error {
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

func consumeTasks(consumer *tasq.Consumer, wg *sync.WaitGroup) {
	defer wg.Done()

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

func produceTasks(ctx context.Context, producer *tasq.Producer) {
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
			log.Panicf("error while submitting task to tasq: %s", err)
		} else {
			log.Printf("successfully submitted task '%s'", t.ID)
		}
	}
}

func main() {
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	db, err := sql.Open("mysql", "root:root@/test")
	if err != nil {
		log.Fatalf("failed to open DB connection: %v", err)
	}

	// instantiate tasq repository to manage the database connection
	// you can also have it set up the sql DB for you if you provide the dsn string
	// instead of the *sql.DB instance
	tasqRepository, err := tasqMySQL.NewRepository(db, "tasq")
	if err != nil {
		log.Fatalf("failed to create tasq repository: %s", err)
	}

	migrationCtx, migrationCancelCtx := context.WithTimeout(context.Background(), 10*time.Second)
	defer migrationCancelCtx()

	err = tasqRepository.Migrate(migrationCtx)
	if err != nil {
		log.Fatalf("failed to migrate tasq repository: %s", err)
	}

	log.Print("database migrated successfully")

	// instantiate tasq client
	tasqClient := tasq.NewClient(tasqRepository)

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
		WithAutoDeleteOnSuccess(false).
		WithLogger(log.Default())

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

	var consumerWg sync.WaitGroup

	// start the goroutine which handles the tasq jobs received from the consumer
	consumerWg.Add(1)
	go consumeTasks(consumer, &consumerWg)

	// set up tasq producer
	producer := tasqClient.NewProducer()

	// start the goroutine which produces the tasks and submits them to the tasq queue
	go produceTasks(ctx, producer)

	// block the execution
	<-time.After(30 * time.Second)
	err = consumer.Stop()
	if err != nil {
		log.Fatalf("failed to stop tasq consumer: %s", err)
	}

	// wait until consumer go routine exits
	consumerWg.Wait()
}
