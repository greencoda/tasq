[![godoc for greencoda/tasq][godoc-badge]][godoc-url]
[![Go 1.22][goversion-badge]][goversion-url]
[![Build Status][actions-badge]][actions-url]
[![Go Coverage][gocoverage-badge]][gocoverage-url]
[![Go Report card][goreportcard-badge]][goreportcard-url]

<p align="center"><img src=".github/splash_image.png" width="500"></p>

# tasq

Tasq is Golang task queue using SQL database for persistence.
Currently supports: 
- PostgreSQL
- MySQL

# Dev Requirements

- [Go 1.22](https://golang.org/doc/go1.22)
- [Mockery](https://github.com/vektra/mockery) - Mock generation

## Install

```shell
go get -u github.com/greencoda/tasq
```

## Usage Example

To try tasq locally, you'll need a local DB running on your machine. You may use the supplied docker-compose.yml file to start a local instance
```shell
docker-compose -f _examples/<example_repo_name>/docker-compose.yml up -d
```

Afterwards simply run the example.go file
```shell
go run _examples/<example_repo_name>/main.go
```

## Task de-duplication (PostgreSQL and MySQL)

A producer can attach a dedup key to a task. While a task with the same `(type, dedup key)` is still **open** (`NEW`, `ENQUEUED` or `IN_PROGRESS`), submitting another one does not create a second task. Instead the existing open task is returned together with an error wrapping `tasq.ErrDuplicateTask`. Once the earlier task is `SUCCESSFUL` or `FAILED`, the same key can be submitted again.

```go
task, err := tasq.NewTask("deleteImport", args, "imports", 1, 3)
if err != nil {
	return err
}

submitted, err := producer.SubmitTask(ctx, task.WithDedupKey("import-42"))
switch {
case errors.Is(err, tasq.ErrDuplicateTask):
	// nothing was created, `submitted` is the existing open task
case err != nil:
	return err
}
```

- The check is atomic (`INSERT ... ON CONFLICT DO NOTHING` against a partial unique index), so concurrent submitters cannot create duplicates.
- Tasks without a key behave exactly as before and are never de-duplicated. `Producer.Submit` is unchanged; to use a key, build the task with `NewTask` and submit it with `SubmitTask`.
- `Migrate` creates new tables with the dedup columns and index but does **not** alter existing ones. **Upgrading from v1.1.4 requires running a few `ALTER`/`CREATE INDEX` statements by hand before deploying v1.2.0**: see [repository/postgres/UPGRADING.md](repository/postgres/UPGRADING.md) and [repository/mysql/UPGRADING.md](repository/mysql/UPGRADING.md). On MySQL an unaltered table makes every `SubmitTask` fail, not only keyed ones.
- `RequeueTask` on a finished task whose key has since been claimed by another open task fails with an error wrapping `ErrDuplicateTask` and leaves the task untouched.
- MySQL has neither partial indexes nor `ON CONFLICT`, so it uses a `dedup_key VARCHAR(255)` column (longer keys are rejected), a virtual generated column `open_dedup_hash` that is `NULL` unless the task has a key and is open, and a unique index `<table>_open_dedup_idx` on it. Requires MySQL 5.7.8+.

## Running the MySQL integration tests

`repository/mysql/integration_test.go` is skipped unless `TASQ_TEST_MYSQL_DSN` is set:

```shell
TASQ_TEST_MYSQL_DSN="user:pass@tcp(localhost:3306)/db?parseTime=true" go test ./repository/mysql/
```

## Running the PostgreSQL integration tests

The tests in `repository/postgres/integration_test.go` run against a real database and are skipped unless `TASQ_TEST_POSTGRES_DSN` is set:

```shell
TASQ_TEST_POSTGRES_DSN="postgres://user:pass@localhost:5432/db?sslmode=disable" go test ./repository/postgres/
```

[godoc-badge]: https://pkg.go.dev/badge/github.com/greencoda/tasq
[godoc-url]: https://pkg.go.dev/github.com/greencoda/tasq
[actions-badge]: https://github.com/greencoda/tasq/actions/workflows/main.yml/badge.svg
[actions-url]: https://github.com/greencoda/tasq/actions/workflows/main.yml
[goversion-badge]: https://img.shields.io/badge/Go-1.22-%2300ADD8?logo=go
[goversion-url]: https://golang.org/doc/go1.22
[goreportcard-badge]: https://goreportcard.com/badge/github.com/greencoda/tasq
[goreportcard-url]: https://goreportcard.com/report/github.com/greencoda/tasq
[gocoverage-badge]: https://github.com/greencoda/tasq/wiki/coverage.svg
[gocoverage-url]: https://raw.githack.com/wiki/greencoda/tasq/coverage.html