package tasq

import (
	"context"
	"fmt"

	"github.com/greencoda/tasq/internal/repository"
	"github.com/greencoda/tasq/internal/repository/postgres"
)

func NewRepository(ctx context.Context, dataSource any, driver, prefix string, migrate bool) (repository repository.IRepository, err error) {
	switch driver {
	case "postgres":
		repository, err = postgres.NewRepository(ctx, dataSource, driver, prefix)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported database driver %s", driver)
	}

	if !migrate {
		return
	}

	err = repository.Migrate(ctx)
	if err != nil {
		return nil, err
	}

	return
}
