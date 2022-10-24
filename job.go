package tasq

import (
	"context"
)

type job func()

func newJob(ctx context.Context, taskFunc TaskFunc, task *Task) *job {
	job := job(func() {
		task.start(ctx)

		taskError := taskFunc(ctx, task)
		if taskError != nil {
			task.error(ctx, taskError)
		} else {
			task.success(ctx)
		}
	})

	return &job
}
