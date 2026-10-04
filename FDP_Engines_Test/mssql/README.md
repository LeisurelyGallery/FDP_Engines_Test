# SQL Server target (192.168.1.83:1433)

For the full end-to-end walkthrough, read `mssql/CONTAINER_PIPELINE.md` — it is
the runbook for the whole stack: SSMS connection, CDC setup, the Docker services,
generating the SQL files for a new table, submitting the Flink job, operating it
day to day, rebuilding a table from Kafka, and a troubleshooting table.

The Flink consolidation job can write to the existing SQL Server instance on your
LAN instead of (or in addition to) ClickHouse. Nothing runs on this machine for
SQL Server; the project only connects to it as a client.

## 1. Create the schema in SSMS

1. SSMS -> Connect -> Database Engine -> Server name `192.168.1.83`, port `1433`.
2. Authentication must be **SQL Server Authentication** (Windows-only instances
   reject the JDBC driver). Use `sa` or a login you create.
3. Run `mssql/init/001_consolidated_users.sql` (creates database `fdp` and
   `dbo.consolidated_users`).
4. Optional: run `mssql/init/002_sink_login.sql` to get a least-privilege
   `flink_sink` login with `db_datareader` + `db_datawriter` on `fdp`.

## 2. Point the Flink job at it

Edit `flink/sql/consolidate_users_mssql.sql`:

- `username` / `password` in the `mssql_sink` DDL.
- `url` if the database name or port differs:
  `jdbc:sqlserver://192.168.1.83:1433;databaseName=fdp;encrypt=true;trustServerCertificate=true`

`trustServerCertificate=true` keeps the job running against a server whose
certificate is self-signed; drop it if the instance uses a trusted certificate.

## 3. Rebuild and submit

The `mssql-jdbc` driver is baked into the image by `flink/Dockerfile`, so an
already-built container must be rebuilt:

```
docker compose up -d --build
docker exec -it fdp-engine-flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/consolidate_users_mssql.sql
python Producer/publish_csv_topics.py --limit 1000
```

`flink/sql` is mounted read-only at `/opt/flink/usrlib/sql` in the JobManager, so
edited SQL is picked up without copying files into the container.

## 4. Query the results

```sql
SELECT TOP 10 * FROM fdp.dbo.consolidated_users ORDER BY joined_at DESC;
SELECT id, COUNT(*) AS rows_per_id FROM fdp.dbo.consolidated_users GROUP BY id HAVING COUNT(*) > 1;
```

The sink uses at-least-once delivery (same as the ClickHouse sink), so a job
restart can replay buffered rows and duplicates are expected — the second query
shows them.

## 5. Streaming `OLTPSemi_55.OLTP55_Schema.[Container]` with Flink CDC

`mssql/tools/introspect_container.py` reads the live table, so nothing about its
columns is hardcoded here.

### 5.1 Enable CDC on the SQL Server (once)

Run `mssql/init/003_enable_cdc.sql` in SSMS from a login with `db_owner`
(`sp_cdc_enable_db` / `sp_cdc_enable_table` require it). It creates the
`flink_cdc` login, grants `VIEW SERVER STATE`, runs `sp_cdc_enable_db` on
`OLTPSemi_55`, runs `sp_cdc_enable_table` for `OLTP55_Schema.Container` (capture
instance `cdc_oltp`), adds `flink_cdc` to `db_datareader` and the `cdc_reader`
role, and ends with three verification queries.

Two prerequisites the script cannot fix: **SQL Server Agent must be running**
(it drives the capture job), and the table needs a primary key or unique index —
without one `sp_cdc_enable_table` fails, since SQL Server CDC has no other
capture mode.

### 5.2 Generate the job from the live schema

Either run it from a machine with `pyodbc` (`pip install pyodbc`; ODBC Driver 17
and 18 are already installed here):

```
set MSSQL_USER=flink_cdc
set MSSQL_PASSWORD=...
python mssql/tools/introspect_container.py --write --mode cdc
```

or skip Python entirely: run `mssql/introspect_container_schema.sql` in SSMS
and send the result grid back, and the job is written from that.

`--mode batch` instead of `--mode cdc` generates a one-shot JDBC read: the job
finishes after a single pass and only needs `SELECT` permission, so it works
before CDC is enabled. Re-submit it to pick up newer rows.

Either way the script prints the columns, primary key, row count, sample rows,
and a CDC prerequisite check (database enabled, capture instance, SQL Server
Agent). Any `[ ]` line there means the job will fail — fix it with
`003_enable_cdc.sql` first. `--write` generates:

- `clickhouse/init/002_oltp55_schema_container.sql` — MergeTree table plus the
  Kafka engine table and materialized view that feed it
- `flink/sql/generated/oltp55_schema_container_cdc.sql` — `sqlserver-cdc` source
  writing to a Kafka topic (gitignored, it holds the password)

The sink goes through Kafka for the same reason as `consolidate_users.sql`:
Flink's JDBC connector has no ClickHouse dialect.

Defaults are `--database OLTPSemi_55 --schema OLTP55_Schema --table Container`.

### 5.3 Create the ClickHouse table, the topic, then submit

The generated sink topic is `oltp55_schema_container`. `kafka-init` creates
whatever is listed in `kafka/topics.txt`, so add it there and re-run that service
(the Flink producer could create the topic itself, but the ClickHouse Kafka
engine starts consuming before that happens):

```
echo oltp55_schema_container >> kafka/topics.txt
docker compose up -d --force-recreate kafka-init
```

`clickhouse/init` only runs against a fresh volume, so apply the generated DDL to
the running container explicitly:

```
Get-Content -Raw clickhouse/init/002_oltp55_schema_container.sql | docker exec -i fdp-engine-clickhouse clickhouse-client --multiquery
docker compose up -d --build
docker exec -it fdp-engine-flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/generated/oltp55_schema_container_cdc.sql
```

Order matters: the ClickHouse DDL first, otherwise the sink has nowhere to write
and the job restarts.

### 5.4 Verify the stream

```
SELECT count() FROM fdp.oltp55_schema_container;
```

`scan.startup.mode = 'initial'` snapshots the existing rows first, so the count
jumps to the full row count of `[Container]` and then tracks every insert, update
and delete. Watch the snapshot phase in `http://localhost:8081`; the source
exposes `<database>.<schema>.<table>` metrics such as `numSplitsRemaining`.

CDC events are appended, not merged: an `UPDATE` shows up as a second row in
ClickHouse. Swap the generated MergeTree for `ReplacingMergeTree` ordered by the
primary key to collapse updates, or filter on the Flink side.

### 5.5 Streaming into SQL Server instead

Flink CDC has no SQL Server sink, only sources. To land CDC output in SQL
Server, either read from ClickHouse over an ODBC linked server with a scheduled
`INSERT ... SELECT`, or point the generated job's sink at
`jdbc:sqlserver://192.168.1.83:1433` — but that sink is append-only, so updates
and deletes captured from SQL Server would not be applied back.

### 5.2b OLTPSemi_55.OLTP55_Schema.[Container] — already generated

The job for this table is checked in, generated from its DDL (201 columns) with
the same type mapping:

- `flink/sql/container_to_clickhouse.sql` — `sqlserver-cdc` source → Kafka topic
  `oltp55_schema_container`. Put the credentials in the source DDL
  (`'username'` / `'password'`) before submitting.
- `clickhouse/init/004_oltp55_schema_container.sql` — MergeTree table, Kafka
  engine table and materialized view; already applied to the running container,
  and it also runs automatically on a fresh volume

```
docker exec -it fdp-engine-flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/container_to_clickhouse.sql
docker exec -it fdp-engine-clickhouse clickhouse-client --query "SELECT count() FROM fdp.oltp55_schema_container"
```

Three things about this job that are not obvious:

- The sink uses `'value.format' = 'debezium-json'`, not `'format' = 'json'`. A CDC
  source emits update and delete rows, and a Kafka sink rejects changelog input
  with `"doesn't support consuming update and delete changes"` unless the value
  format can carry it. Debezium-json wraps the row as
  `{"before":...,"after":{...},"op":"c|u|d"}`.
- The ClickHouse side therefore keeps the raw message
  (`kafka_format = 'JSONAsString'`, one `msg String` column) and the materialized
  view unpacks the `after` object. `op = 'd'` events are filtered out, so deletes
  are dropped and updates add a row rather than replacing one. The target table is
  a `ReplacingMergeTree` ordered by the primary key, so those extra rows collapse:
  read with `FINAL`, or run `OPTIMIZE TABLE ... FINAL` to compact. A re-run of the
  initial snapshot (a cancelled job resubmitted before its first checkpoint)
  re-emits every row, and `ReplacingMergeTree` is what keeps the table at one row
  per key instead of doubling it.
- Datetime columns are `Nullable(DateTime64(3))` and parsed with
  `parseDateTime64BestEffortOrNull`. The non-`OrNull` variant throws on an empty
  string, which makes ClickHouse drop the entire message.

It runs with `parallelism.default = 1` because the TaskManager has four slots and
`consolidated_users`, `oltp55_schema_container` and `oltp55_schema_workflowstep`
share them; raise `taskmanager.numberOfTaskSlots` in `Docker-compose.yml` to run
more jobs side by side. A job that cannot get slots does not queue, it fails with
`NoResourceAvailableException: Could not acquire the minimum required resources`
and then loops between `RUNNING` and `RESTARTING`.

Credentials and CDC both have to be in place: a wrong login shows up as
`Login failed for user 'sa'`, and a table without a capture instance fails with
`Failed to discover captured tables`. Check with:

```sql
SELECT is_cdc_enabled FROM sys.databases WHERE name = 'OLTPSemi_55';
SELECT capture_instance FROM cdc.change_tables
WHERE source_object_id = OBJECT_ID(N'OLTP55_Schema.Container');
```

## 6. Monitoring Flink from SSMS

Flink exposes job state only over HTTP (`http://localhost:8081`), not SQL, so it
cannot be watched from a table directly. Options:

- Browse `http://localhost:8081` for job/vertex/checkpoint status.
- To keep a history in SQL Server, poll the REST API (`/jobs`,
  `/jobs/<jobid>/vertices`) on a schedule and insert into a table. SQL Server can
  do this with a SQL Agent job calling `sp_invoke_external_script` (Python) or
  `xp_cmdshell` (curl + `bcp`), or you can let a small Flink/Python timer do the
  polling and reuse the same JDBC sink.

## 7. Pointing the pipeline at a different table

There is no single config file — the SQL is generated, so change the generator's
inputs and regenerate:

| What to change | Where |
| --- | --- |
| Source database, schema, table | `mssql/tools/introspect_container.py --database --schema --table`, or `MSSQL_DATABASE` / `MSSQL_SCHEMA` / `MSSQL_TABLE`, or the three variables at the top of `mssql/introspect_container_schema.sql` |
| CDC capture | `mssql/init/003_enable_cdc.sql` — `@source_schema` and `@source_name`; run it once per table |
| Kafka topic | derived as `<schema>_<table>` in lower case. Add it to `kafka/topics.txt` and re-run `docker compose up -d --force-recreate kafka-init` |
| ClickHouse target | the same slug: `fdp.<schema>_<table>`, plus `fdp.<schema>_<table>_kafka` and `fdp.mv_<schema>_<table>`, all in the generated `clickhouse/init/002_*.sql` |
| Columns | regenerated: Flink DDL, ReplacingMergeTree table, Kafka engine table and materialized view all come from the live schema |

### Adding another CDC table

```powershell
$env:MSSQL_USER = '...'; $env:MSSQL_PASSWORD = '...'
python mssql/tools/introspect_container.py --table WorkflowStep --write --mode cdc
./mssql/run_pipeline.ps1 -Slug oltp55_schema_workflowstep -CreateTopic -ApplyDdl -Submit `
    -JobFile generated/oltp55_schema_workflowstep_cdc.sql
```

`-JobFile` is relative to `flink/sql`, and the generator writes the job to
`flink/sql/generated/`, so the default `$Slug`_cdc.sql` only resolves for a job
file that sits directly in `flink/sql/`. Run the CDC setup first
(`mssql/init/003_enable_cdc.sql` with the new `@source_name`); the generator
reports `[[ ]] SQL Server Agent: Stopped` when the agent is down, which still lets
the initial snapshot through but means no change is captured afterwards.

The Kafka/CSV pipeline is hand written instead of generated: topic names and
columns live in `flink/sql/consolidate_users.sql`, and the ClickHouse side has to
stay column aligned with it in `clickhouse/init/001_consolidated_users.sql`
(MergeTree), `clickhouse/init/003_consolidated_users_kafka.sql` (Kafka engine
table + materialized view) and `mssql/init/001_consolidated_users.sql` (SQL
Server sink). Change a column in one and it must change in the others.

### Changing columns after a job is running

The pipeline is schema static: Flink does not apply DDL from the source. After an
`ALTER TABLE` on SQL Server:

1. Re-run the generator with `--write` for the new column list.
2. For added columns, `ALTER TABLE ... ADD COLUMN` on the MergeTree table, the
   `_kafka` table and the materialized view, in that order.
3. Cancel the running job and resubmit it — checkpoints from the old schema
   cannot be restored.