package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/greencoda/tasq"
	"github.com/greencoda/tasq/repository/postgres"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// integrationDSNEnv names the environment variable holding the DSN of a real
// PostgreSQL database. The integration tests are skipped when it is unset.
const integrationDSNEnv = "TASQ_TEST_POSTGRES_DSN"

type integrationEnv struct {
	db        *sql.DB
	repo      *postgres.Repository
	tableName string
}

func newIntegrationEnv(t *testing.T) *integrationEnv {
	t.Helper()

	dsn := os.Getenv(integrationDSNEnv)
	if dsn == "" {
		t.Skipf("%s not set, skipping PostgreSQL integration test", integrationDSNEnv)
	}

	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)

	tableName := "it_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]

	repo, err := postgres.NewRepository(db, postgres.WithTableName(tableName))
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
		_ = db.Close()
	})

	return &integrationEnv{db: db, repo: repo, tableName: tableName}
}

func (e *integrationEnv) migrated(t *testing.T) *integrationEnv {
	t.Helper()
	require.NoError(t, e.repo.Migrate(context.Background()))

	return e
}

func (e *integrationEnv) rowCount(t *testing.T, where string) int {
	t.Helper()

	var count int

	require.NoError(t, e.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", e.tableName, where)).Scan(&count))

	return count
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

func TestIntegrationSubmitDuplicate(t *testing.T) {
	t.Parallel()

	var (
		env = newIntegrationEnv(t).migrated(t)
		ctx = context.Background()
	)

	first, err := env.repo.SubmitTask(ctx, newKeyedTask(t, "delete", "import-1"))
	require.NoError(t, err)
	require.NotNil(t, first.DedupKey)
	require.Equal(t, "import-1", *first.DedupKey)

	second, err := env.repo.SubmitTask(ctx, newKeyedTask(t, "delete", "import-1"))
	require.ErrorIs(t, err, tasq.ErrDuplicateTask)
	require.NotNil(t, second)
	require.Equal(t, first.ID, second.ID)
	require.Equal(t, 1, env.rowCount(t, "TRUE"))

	// duplicate also detected while the first task is ENQUEUED and IN_PROGRESS
	polled, err := env.repo.PollTasks(ctx, []string{"delete"}, []string{"queue"}, 0, tasq.OrderingCreatedAtFirst, 1)
	require.NoError(t, err)
	require.Len(t, polled, 1)

	_, err = env.repo.SubmitTask(ctx, newKeyedTask(t, "delete", "import-1"))
	require.ErrorIs(t, err, tasq.ErrDuplicateTask)

	_, err = env.repo.RegisterStart(ctx, first)
	require.NoError(t, err)

	existing, err := env.repo.SubmitTask(ctx, newKeyedTask(t, "delete", "import-1"))
	require.ErrorIs(t, err, tasq.ErrDuplicateTask)
	require.Equal(t, tasq.StatusInProgress, existing.Status)
	require.Equal(t, 1, env.rowCount(t, "TRUE"))
}

func TestIntegrationResubmitAfterFinish(t *testing.T) {
	t.Parallel()

	for _, status := range tasq.GetTaskStatuses(tasq.FinishedTasks) {
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()

			var (
				env = newIntegrationEnv(t).migrated(t)
				ctx = context.Background()
			)

			first, err := env.repo.SubmitTask(ctx, newKeyedTask(t, "delete", "import-1"))
			require.NoError(t, err)

			_, err = env.repo.RegisterFinish(ctx, first, status)
			require.NoError(t, err)

			second, err := env.repo.SubmitTask(ctx, newKeyedTask(t, "delete", "import-1"))
			require.NoError(t, err)
			require.NotEqual(t, first.ID, second.ID)
			require.Equal(t, 2, env.rowCount(t, "TRUE"))
		})
	}
}

func TestIntegrationDifferentTypeIsNotDuplicate(t *testing.T) {
	t.Parallel()

	var (
		env = newIntegrationEnv(t).migrated(t)
		ctx = context.Background()
	)

	_, err := env.repo.SubmitTask(ctx, newKeyedTask(t, "delete", "import-1"))
	require.NoError(t, err)

	_, err = env.repo.SubmitTask(ctx, newKeyedTask(t, "export", "import-1"))
	require.NoError(t, err)
	require.Equal(t, 2, env.rowCount(t, "TRUE"))
}

func TestIntegrationNoKeyIsNeverDeduplicated(t *testing.T) {
	t.Parallel()

	var (
		env = newIntegrationEnv(t).migrated(t)
		ctx = context.Background()
	)

	for range 3 {
		task, err := env.repo.SubmitTask(ctx, newKeyedTask(t, "delete", ""))
		require.NoError(t, err)
		require.Nil(t, task.DedupKey)
	}

	require.Equal(t, 3, env.rowCount(t, "dedup_key IS NULL"))
}

func TestIntegrationConcurrentSubmit(t *testing.T) {
	t.Parallel()

	const workers = 20

	var (
		env        = newIntegrationEnv(t).migrated(t)
		ctx        = context.Background()
		wg         sync.WaitGroup
		mu         sync.Mutex
		winners    int
		duplicates int
		unexpected []error
		ids        = map[uuid.UUID]struct{}{}
		start      = make(chan struct{})
	)

	for range workers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			task := newKeyedTask(t, "delete", "import-1")
			<-start

			result, err := env.repo.SubmitTask(ctx, task)

			mu.Lock()
			defer mu.Unlock()

			switch {
			case err == nil:
				winners++
				ids[result.ID] = struct{}{}
			case errors.Is(err, tasq.ErrDuplicateTask):
				duplicates++
				ids[result.ID] = struct{}{}
			default:
				unexpected = append(unexpected, err)
			}
		}()
	}

	close(start)
	wg.Wait()

	require.Empty(t, unexpected)
	require.Equal(t, 1, winners)
	require.Equal(t, workers-1, duplicates)
	require.Len(t, ids, 1, "every caller must see the same task")
	require.Equal(t, 1, env.rowCount(t, "TRUE"))
}

func TestIntegrationManualUpgradeFromPreviousVersion(t *testing.T) {
	t.Parallel()

	var (
		env = newIntegrationEnv(t)
		ctx = context.Background()
	)

	// the status type is needed by the legacy table definition
	require.NoError(t, env.repo.Migrate(ctx))
	_, err := env.db.Exec(fmt.Sprintf("DROP TABLE %s", env.tableName))
	require.NoError(t, err)

	// table exactly as created by the previous version
	_, err = env.db.Exec(fmt.Sprintf(`CREATE TABLE %s (
			"id" UUID NOT NULL PRIMARY KEY,
			"type" TEXT NOT NULL,
			"args" BYTEA NOT NULL,
			"queue" TEXT NOT NULL,
			"priority" SMALLINT NOT NULL,
			"status" tasq_task_status NOT NULL,
			"receive_count" INTEGER NOT NULL DEFAULT 0,
			"max_receives" INTEGER NOT NULL DEFAULT 0,
			"last_error" TEXT,
			"created_at" TIMESTAMPTZ NOT NULL DEFAULT '0001-01-01 00:00:00.000000',
			"started_at" TIMESTAMPTZ,
			"finished_at" TIMESTAMPTZ,
			"visible_at" TIMESTAMPTZ NOT NULL DEFAULT '0001-01-01 00:00:00.000000'
		)`, env.tableName))
	require.NoError(t, err)

	// a legacy row, and the old-style submit works before migrating
	legacy, err := env.repo.SubmitTask(ctx, newKeyedTask(t, "delete", ""))
	require.NoError(t, err)

	// Migrate does not upgrade old tables: it fails because the dedup_key column is missing
	require.Error(t, env.repo.Migrate(ctx))

	// the manual steps documented in UPGRADING.md
	_, err = env.db.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN "dedup_key" TEXT`, env.tableName))
	require.NoError(t, err)

	_, err = env.db.Exec(fmt.Sprintf(`CREATE UNIQUE INDEX %[1]s_open_dedup_key_idx ON %[1]s ("type", "dedup_key") WHERE "dedup_key" IS NOT NULL AND "status" IN ('NEW','ENQUEUED','IN_PROGRESS')`, env.tableName))
	require.NoError(t, err)

	// afterwards Migrate is a no-op
	require.NoError(t, env.repo.Migrate(ctx))

	// legacy row has a NULL key and still round-trips
	scanned, err := env.repo.ScanTasks(ctx, nil, nil, nil, tasq.OrderingCreatedAtFirst, 10)
	require.NoError(t, err)
	require.Len(t, scanned, 1)
	require.Equal(t, legacy.ID, scanned[0].ID)
	require.Nil(t, scanned[0].DedupKey)

	// and the feature works on the upgraded table
	_, err = env.repo.SubmitTask(ctx, newKeyedTask(t, "delete", "k"))
	require.NoError(t, err)

	_, err = env.repo.SubmitTask(ctx, newKeyedTask(t, "delete", "k"))
	require.ErrorIs(t, err, tasq.ErrDuplicateTask)
}

func TestIntegrationConcurrentMigrate(t *testing.T) {
	t.Parallel()

	var (
		env  = newIntegrationEnv(t)
		ctx  = context.Background()
		wg   sync.WaitGroup
		errs = make(chan error, 8)
	)

	for range 8 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			errs <- env.repo.Migrate(ctx)
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}

func TestIntegrationRequeue(t *testing.T) {
	t.Parallel()

	var (
		env = newIntegrationEnv(t).migrated(t)
		ctx = context.Background()
	)

	first, err := env.repo.SubmitTask(ctx, newKeyedTask(t, "delete", "import-1"))
	require.NoError(t, err)

	// requeueing the open task that holds the key works and round-trips the key
	requeued, err := env.repo.RequeueTask(ctx, first)
	require.NoError(t, err)
	require.NotNil(t, requeued.DedupKey)
	require.Equal(t, "import-1", *requeued.DedupKey)

	// finish it, submit a new task with the same key, then try to requeue the old one
	_, err = env.repo.RegisterFinish(ctx, first, tasq.StatusFailed)
	require.NoError(t, err)

	second, err := env.repo.SubmitTask(ctx, newKeyedTask(t, "delete", "import-1"))
	require.NoError(t, err)

	_, err = env.repo.RequeueTask(ctx, first)
	require.ErrorIs(t, err, tasq.ErrDuplicateTask)

	// the old task stays FAILED and the open one is untouched
	require.Equal(t, 1, env.rowCount(t, fmt.Sprintf("id = '%s' AND status = 'FAILED'", first.ID)))
	require.Equal(t, 1, env.rowCount(t, fmt.Sprintf("id = '%s' AND status = 'NEW'", second.ID)))

	// once the open task finishes, the old one can be requeued again
	_, err = env.repo.RegisterFinish(ctx, second, tasq.StatusSuccessful)
	require.NoError(t, err)

	_, err = env.repo.RequeueTask(ctx, first)
	require.NoError(t, err)
}
