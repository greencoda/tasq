package tasq

import (
	"context"
	"fmt"
)

// Producer is a service instance created by a Client with reference to that client
// with the purpose of enabling the submission of new tasks.
type Producer struct {
	client *Client
}

// NewProducer creates a new consumer with a reference to the original tasq client.
func (c *Client) NewProducer() *Producer {
	return &Producer{
		client: c,
	}
}

// Submit constructs and submits a new task to the queue based on the supplied arguments.
func (p *Producer) Submit(ctx context.Context, taskType string, taskArgs any, queue string, priority int16, maxReceives int32) (*Task, error) {
	newTask, err := NewTask(taskType, taskArgs, queue, priority, maxReceives)
	if err != nil {
		return nil, fmt.Errorf("error creating task: %w", err)
	}

	submittedTask, err := p.client.repository.SubmitTask(ctx, newTask)
	if err != nil {
		return nil, fmt.Errorf("error submitting task: %w", err)
	}

	return submittedTask, nil
}
