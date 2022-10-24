package tasq

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type IProducer interface {
	Submit(ctx context.Context, taskType string, taskArgs any, queue string, priority TaskPriority, maxReceives int32) (submittedTask *Task, err error)
}

type Producer struct {
	dao iDAO
}

func (t *Client) NewProducer() IProducer {
	return &Producer{
		dao: t.dao,
	}
}

func (p *Producer) Submit(ctx context.Context, taskType string, taskArgs any, queue string, priority TaskPriority, maxReceives int32) (submittedTask *Task, err error) {
	newTask := p.NewTask(taskType, taskArgs, queue, priority, maxReceives)
	if newTask == nil {
		return nil, fmt.Errorf("error creating task: %s", err)
	}

	submittedTask, err = p.dao.submitTask(ctx, newTask)
	if err != nil {
		return nil, err
	}

	return submittedTask, nil
}

func (p *Producer) NewTask(taskType string, taskArgs any, queue string, priority TaskPriority, maxReceives int32) *Task {
	taskID, err := uuid.NewRandom()
	if err != nil {
		return nil
	}

	encodedArgs, err := p.encodeTaskArgs(taskArgs)
	if err != nil {
		return nil
	}

	return &Task{
		ID:          taskID,
		Type:        taskType,
		Args:        encodedArgs,
		Queue:       queue,
		Priority:    priority,
		MaxReceives: maxReceives,
		Status:      StatusNew,
		CreatedAt:   time.Now(),
	}
}

func (p *Producer) encodeTaskArgs(taskArgs any) ([]byte, error) {
	var (
		buffer  bytes.Buffer
		encoder = gob.NewEncoder(&buffer)
	)

	err := encoder.Encode(taskArgs)
	if err != nil {
		return []byte{}, err
	}

	return buffer.Bytes(), nil
}
