//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"sort"
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

	for _, path := range migrationFiles(t) {
		migration, err := os.ReadFile(path)
		require.NoError(t, err, path)
		upSQL := strings.SplitN(string(migration), "-- +goose Down", 2)[0]
		_, err = pool.Exec(ctx, upSQL)
		require.NoError(t, err, path)
	}

	t.Cleanup(pool.Close)

	return pool
}

// migrationFiles ищет migrations/*.sql от cwd вверх до корня модуля
// и возвращает пути в лексикографическом порядке (00001, 00002, …).
func migrationFiles(t *testing.T) []string {
	t.Helper()

	dir, err := os.Getwd()
	require.NoError(t, err)

	for {
		migrationsDir := filepath.Join(dir, "migrations")
		entries, err := os.ReadDir(migrationsDir)
		if err == nil {
			var files []string
			for _, e := range entries {
				if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
					continue
				}
				files = append(files, filepath.Join(migrationsDir, e.Name()))
			}
			sort.Strings(files)
			require.NotEmpty(t, files, "no *.sql in %s", migrationsDir)
			return files
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("migrations/ not found from ", dir)
		}
		dir = parent
	}
}
