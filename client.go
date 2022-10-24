package tasq

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

type IClient interface {
	NewCleaner() ICleaner
	NewConsumer() IConsumer
	NewProducer() IProducer
}

type Client struct {
	dao iDAO
}

func New(ctx context.Context, db *sql.DB, prefix string) (IClient, error) {
	switch db.Driver().(type) {
	case *pq.Driver:
		dao, err := newPostgresDAO(ctx, db, prefix)
		if err != nil {
			return nil, err
		}

		return &Client{
			dao: dao,
		}, nil
	}

	return nil, fmt.Errorf("unsupported database driver %T", db.Driver())
}
