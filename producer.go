package tasq

import (
	"fmt"

	"github.com/greencoda/tasq/internal/model"
)

// Cleaner is a service instance created by a Client with reference to that client
// with the purpose of enabling the submission of new tasks.
type Producer struct {
	client *Client
}

// NewCleaner creates a new consumer with a reference to the original tasq client.
func (c *Client) NewProducer() *Producer {
	return &Producer{
		client: c,
	}
}

// Submit constructs and submits a new task to the queue based on the supplied arguments.
func (p *Producer) Submit(taskType string, taskArgs any, queue string, priority int16, maxReceives int32) (*model.Task, error) {
	newTask, err := model.NewTask(taskType, taskArgs, queue, priority, maxReceives)
	if err != nil {
		return nil, fmt.Errorf("error creating task: %w", err)
	}

	submittedTask, err := p.client.repository.SubmitTask(p.client.getContext(), newTask)
	if err != nil {
		return nil, fmt.Errorf("error submitting task: %w", err)
	}

	return submittedTask, nil
}
