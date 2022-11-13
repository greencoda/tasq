package tasq

import (
	"context"
	"fmt"
	"time"

	"github.com/greencoda/tasq/internal/repository"
	"github.com/greencoda/tasq/internal/repository/postgres"
)

func NewRepository(dataSource any, driver, prefix string, migrate bool, migrationTimeout time.Duration) (repository repository.IRepository, err error) {
	switch driver {
	case "postgres":
		repository, err = postgres.NewRepository(dataSource, driver, prefix)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported database driver %s", driver)
	}

	if !migrate {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), migrationTimeout)
	defer cancel()

	err = repository.Migrate(ctx)
	if err != nil {
		return nil, err
	}

	return
}
