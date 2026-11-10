# Upgrading the MySQL tasks table

`Migrate` only runs `CREATE TABLE IF NOT EXISTS`, so it never changes a table that already exists.
When a release changes the table definition, existing tables have to be altered by hand with the statements below.

Replace `<table>` with your tasks table, including the schema if you use one
(e.g. `my_schema.my_prefix_tasks`), and `<table_name>` with the table name alone (e.g. `my_prefix_tasks`).

## v1.1.4 → v1.2.0: task de-duplication

**Required.** From v1.2.0 every task insert writes the `dedup_key` column, so on an unaltered table
**all** `SubmitTask` calls fail with `Unknown column 'dedup_key'`, whether or not you use dedup keys.
Run the statements below before deploying v1.2.0. They are safe to run while v1.1.4 is still running
(the new column is nullable and v1.1.4 ignores it), and existing rows keep a `NULL` key.

Requires MySQL 5.7.8 or later (generated columns).

```sql
ALTER TABLE <table> ADD COLUMN dedup_key VARCHAR(255);

ALTER TABLE <table> ADD COLUMN open_dedup_hash CHAR(64) GENERATED ALWAYS AS (
    CASE WHEN dedup_key IS NOT NULL AND status IN ('NEW', 'ENQUEUED', 'IN_PROGRESS')
    THEN SHA2(CONCAT_WS(CHAR(0), type, dedup_key), 256) END
) VIRTUAL;

CREATE UNIQUE INDEX <table_name>_open_dedup_idx ON <table> (open_dedup_hash);
```

The index name must be exactly `<table_name>_open_dedup_idx`: the repository recognises duplicate-key errors by it.
