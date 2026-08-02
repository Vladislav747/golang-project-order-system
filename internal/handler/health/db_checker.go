package health

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DBChecker struct {
	pool *pgxpool.Pool
}

func NewDBChecker(pool *pgxpool.Pool) *DBChecker {
	return &DBChecker{pool: pool}
}

func (c *DBChecker) Ready(ctx context.Context) error {
	return c.pool.Ping(ctx)
}
