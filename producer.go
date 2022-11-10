package tasq

import (
	"context"
	"fmt"

	"github.com/greencoda/tasq/internal/model"
)

type Producer struct {
	client *Client
}

func (c *Client) NewProducer() *Producer {
	return &Producer{
		client: c,
	}
}

func (p *Producer) Submit(ctx context.Context, taskType string, taskArgs any, queue string, priority int16, maxReceives int32) (submittedTask Task, err error) {
	newTask := model.NewTask(taskType, taskArgs, queue, priority, maxReceives)
	if newTask == nil {
		return nil, fmt.Errorf("error creating task: %s", err)
	}

	submittedTask, err = p.client.Repository().SubmitTask(ctx, newTask)
	if err != nil {
		return nil, err
	}

	return submittedTask, nil
}
