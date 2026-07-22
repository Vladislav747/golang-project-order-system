//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func setupPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	container, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("orders"),
		postgres.WithUsername("orders"),
		postgres.WithPassword("orders"),
		postgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	migration, err := os.ReadFile(migrationFile(t))
	require.NoError(t, err)
	upSQL := strings.SplitN(string(migration), "-- +goose Down", 2)[0]
	_, err = pool.Exec(ctx, upSQL)
	require.NoError(t, err)

	t.Cleanup(pool.Close)

	return pool
}

// migrationFile ищет migrations/00001_init.sql от cwd вверх до корня модуля.
// go test запускается из каталога пакета (tests/integration), не из корня репо.
func migrationFile(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	require.NoError(t, err)

	for {
		candidate := filepath.Join(dir, "migrations", "00001_init.sql")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("migrations/00001_init.sql not found from ", dir)
		}
		dir = parent
	}
}