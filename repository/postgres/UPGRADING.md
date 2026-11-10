# Upgrading the PostgreSQL tasks table

`Migrate` only runs `CREATE TABLE IF NOT EXISTS`, so it never changes a table that already exists.
When a release changes the table definition, existing tables have to be altered by hand with the statements below.

Replace `<table>` with your tasks table, including the schema if you use one
(e.g. `my_schema.tasks`), and `<table_name>` with the table name alone (e.g. `tasks`).

## v1.1.4 → v1.2.0: task de-duplication

**Required.** From v1.2.0 `Migrate` creates the dedup index, which fails on a table without the
`dedup_key` column, and tasks submitted with a dedup key cannot be stored. Run the statements below
before deploying v1.2.0. They are safe to run while v1.1.4 is still running (the new column is nullable
and v1.1.4 ignores it), and existing rows keep a `NULL` key.

```sql
ALTER TABLE <table> ADD COLUMN IF NOT EXISTS "dedup_key" TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS <table_name>_open_dedup_key_idx ON <table> ("type", "dedup_key")
    WHERE "dedup_key" IS NOT NULL AND "status" IN ('NEW', 'ENQUEUED', 'IN_PROGRESS');
```

The index name must be exactly `<table_name>_open_dedup_key_idx`, and its predicate must match the one above:
`SubmitTask` relies on it for `ON CONFLICT` inference. On a large table you may prefer
`CREATE UNIQUE INDEX CONCURRENTLY` (outside a transaction) to avoid blocking writes.
