package worker

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

type OutboxCleanerRepository interface {
	CleanOutboxMessages(ctx context.Context, tx pgx.Tx, retention time.Duration, maxAttempts int) error
}

type OutboxCleaner struct {
	repo        OutboxCleanerRepository
	logger      *zap.Logger
	txManager   TxManager
	interval    atomic.Int64
	retention   atomic.Int64
	maxAttempts atomic.Int64
}

func NewOutboxCleaner(
	repo OutboxCleanerRepository,
	logger *zap.Logger,
	interval, retention time.Duration,
	maxAttempts int,
	txManager TxManager,
) *OutboxCleaner {
	c := &OutboxCleaner{repo: repo, logger: logger, txManager: txManager}
	c.interval.Store(int64(interval))
	c.retention.Store(int64(retention))
	c.maxAttempts.Store(int64(maxAttempts))
	return c
}

func (c *OutboxCleaner) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(c.interval.Load())):
			c.clean(ctx)
		}
	}
}

func (c *OutboxCleaner) clean(ctx context.Context) {
	err := pgx.BeginFunc(ctx, c.txManager, func(tx pgx.Tx) error {
		return c.repo.CleanOutboxMessages(
			ctx, tx,
			time.Duration(c.retention.Load()),
			int(c.maxAttempts.Load()),
		)
	})
	if err != nil {
		c.logger.Error("outbox cleaner failed", zap.Error(err))
	}
}

func (c *OutboxCleaner) SetInterval(d time.Duration)  { c.interval.Store(int64(d)) }
func (c *OutboxCleaner) SetRetention(d time.Duration) { c.retention.Store(int64(d)) }
func (c *OutboxCleaner) SetMaxAttempts(n int)         { c.maxAttempts.Store(int64(n)) }
