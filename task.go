package tasq

import (
	"github.com/greencoda/tasq/internal/model"
)

// Task is the public interface for accessing details of the consumed task
// and unmarshaling its arguments so the handler function can use it
type Task interface {
	GetDetails() *model.Task
	UnmarshalArgs(v any) error
}
