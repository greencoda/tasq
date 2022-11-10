package tasq

import (
	"github.com/greencoda/tasq/internal/model"
)

type Task interface {
	GetDetails() *model.Task
	UnmarshalArgs(v any) error
}
