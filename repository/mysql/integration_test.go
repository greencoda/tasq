package mysql_test

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/greencoda/tasq"
	"github.com/greencoda/tasq/repository/mysql"
	"github.com/stretchr/testify/require"
)

// integrationDSNEnv names the environment variable holding the DSN of a real
// MySQL database. The integration tests are skipped when it is unset.
const integrationDSNEnv = "TASQ_TEST_MYSQL_DSN"

func newIntegrationRepo(t *testing.T) (*mysql.Repository, *sql.DB, string) {
	t.Helper()

	dsn := os.Getenv(integrationDSNEnv)
	if dsn == "" {
		t.Skipf("%s not set, skipping MySQL integration test", integrationDSNEnv)
	}

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)

	prefix := "it_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	table := prefix + "_tasks"

	repo, err := mysql.NewRepository(db, prefix)
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		_ = db.Close()
	})

	require.NoError(t, repo.Migrate(ctx))

	return repo, db, table
}

func newKeyedTask(t *testing.T, taskType, key string) *tasq.Task {
	t.Helper()

	task, err := tasq.NewTask(taskType, "args", "queue", 1, 3)
	require.NoError(t, err)

	if key != "" {
		task.WithDedupKey(key)
	}

	return task
}

func TestIntegrationDedup(t *testing.T) {
	repo, _, _ := newIntegrationRepo(t)

	first, err := repo.SubmitTask(ctx, newKeyedTask(t, "a", "k"))
	require.NoError(t, err)
	require.Equal(t, "k", *first.DedupKey)

	// same type and key while open: existing task returned
	dup, err := repo.SubmitTask(ctx, newKeyedTask(t, "a", "k"))
	require.ErrorIs(t, err, tasq.ErrDuplicateTask)
	require.Equal(t, first.ID, dup.ID)

	// different type or key, and unkeyed tasks, are independent
	for _, task := range []*tasq.Task{
		newKeyedTask(t, "b", "k"), newKeyedTask(t, "a", "k2"),
		newKeyedTask(t, "a", ""), newKeyedTask(t, "a", ""),
	} {
		_, err = repo.SubmitTask(ctx, task)
		require.NoError(t, err)
	}

	// once finished, the key is free again, and requeueing the old one conflicts
	_, err = repo.RegisterFinish(ctx, first, tasq.StatusSuccessful)
	require.NoError(t, err)

	second, err := repo.SubmitTask(ctx, newKeyedTask(t, "a", "k"))
	require.NoError(t, err)
	require.NotEqual(t, first.ID, second.ID)

	_, err = repo.RequeueTask(ctx, first)
	require.ErrorIs(t, err, tasq.ErrDuplicateTask)
}

func TestIntegrationDedupConcurrent(t *testing.T) {
	repo, _, _ := newIntegrationRepo(t)

	const workers = 20

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		created  int
		dups     int
		failures []error
	)

	for range workers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_, err := repo.SubmitTask(ctx, newKeyedTask(t, "a", "race"))

			mu.Lock()
			defer mu.Unlock()

			switch {
			case err == nil:
				created++
			case errorsIsDup(err):
				dups++
			default:
				failures = append(failures, err)
			}
		}()
	}

	wg.Wait()
	require.Empty(t, failures)
	require.Equal(t, 1, created)
	require.Equal(t, workers-1, dups)
}

func TestIntegrationManualUpgradeFromPreviousVersion(t *testing.T) {
	repo, db, table := newIntegrationRepo(t)

	// simulate a table created by a previous version
	_, err := db.Exec(fmt.Sprintf("ALTER TABLE %s DROP INDEX %s_open_dedup_idx, DROP COLUMN open_dedup_hash, DROP COLUMN dedup_key", table, table))
	require.NoError(t, err)

	// Migrate does not upgrade it, and submitting fails
	require.NoError(t, repo.Migrate(ctx))

	_, err = repo.SubmitTask(ctx, newKeyedTask(t, "a", ""))
	require.Error(t, err)

	// the manual steps documented in UPGRADING.md
	for _, ddl := range []string{
		"ALTER TABLE %[1]s ADD COLUMN dedup_key VARCHAR(255)",
		`ALTER TABLE %[1]s ADD COLUMN open_dedup_hash CHAR(64) GENERATED ALWAYS AS (
			CASE WHEN dedup_key IS NOT NULL AND status IN ('NEW', 'ENQUEUED', 'IN_PROGRESS')
			THEN SHA2(CONCAT_WS(CHAR(0), type, dedup_key), 256) END
		) VIRTUAL`,
		"CREATE UNIQUE INDEX %[1]s_open_dedup_idx ON %[1]s (open_dedup_hash)",
	} {
		_, err = db.Exec(fmt.Sprintf(ddl, table))
		require.NoError(t, err)
	}

	_, err = repo.SubmitTask(ctx, newKeyedTask(t, "a", "k"))
	require.NoError(t, err)

	_, err = repo.SubmitTask(ctx, newKeyedTask(t, "a", "k"))
	require.ErrorIs(t, err, tasq.ErrDuplicateTask)
}

func errorsIsDup(err error) bool {
	return errors.Is(err, tasq.ErrDuplicateTask)
}
