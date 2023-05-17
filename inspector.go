package tasq

import (
	"context"
	"fmt"
)

// Inspector is a service instance created by a Client with reference to that client
// with the purpose of enabling the observation of tasks.
type Inspector struct {
	client *Client
}

// NewInspector creates a new inspector with a reference to the original tasq client.
func (c *Client) NewInspector() *Inspector {
	return &Inspector{
		client: c,
	}
}

// Count returns a the total number of tasks based on the supplied filter arguments.
func (o *Inspector) Count(ctx context.Context, taskStatuses []TaskStatus, taskTypes, queues []string) (int64, error) {
	count, err := o.client.repository.CountTasks(ctx, taskStatuses, taskTypes, queues)
	if err != nil {
		return 0, fmt.Errorf("error counting tasks: %w", err)
	}

	return count, nil
}

// Scan returns a list of tasks based on the supplied filter arguments.
func (o *Inspector) Scan(ctx context.Context, taskStatuses []TaskStatus, taskTypes, queues []string, ordering Ordering, limit int) ([]*Task, error) {
	tasks, err := o.client.repository.ScanTasks(ctx, taskStatuses, taskTypes, queues, ordering, limit)
	if err != nil {
		return nil, fmt.Errorf("error scanning tasks: %w", err)
	}

	return tasks, nil
}

// Purge will remove all tasks based on the supplied filter arguments.
func (o *Inspector) Purge(ctx context.Context, safeDelete bool, taskStatuses []TaskStatus, taskTypes, queues []string) (int64, error) {
	count, err := o.client.repository.PurgeTasks(ctx, taskStatuses, taskTypes, queues, safeDelete)
	if err != nil {
		return 0, fmt.Errorf("error purging tasks: %w", err)
	}

	return count, nil
}

// Delete will remove the supplied tasks.
func (o *Inspector) Delete(ctx context.Context, safeDelete bool, tasks ...*Task) error {
	for _, task := range tasks {
		if err := o.client.repository.DeleteTask(ctx, task, safeDelete); err != nil {
			return fmt.Errorf("error removing task: %w", err)
		}
	}

	return nil
}
