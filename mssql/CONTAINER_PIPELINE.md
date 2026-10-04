# SQL Server → Kafka → ClickHouse with Flink CDC — setup and operations runbook

The complete recipe for this repository, written to be followed end to end on a
new machine and then reused every time a table is added.

```
SQL Server 192.168.1.83:1433              Kafka (Docker)          ClickHouse (Docker)
OLTPSemi_55.OLTP55_Schema.Container  ──┐
                                        ├→ topic oltp55_schema_container  ─→ fdp.oltp55_schema_container
OLTPSemi_55.OLTP55_Schema.WorkflowStep ─┘
                                        └→ topic oltp55_schema_workflowstep ─→ fdp.oltp55_schema_workflowstep
```

Two tables are live today, and a third needs nothing but the procedure in
[Part B](#part-b--adding-a-table-the-repeating-procedure).

| Table | ClickHouse | Rows synced (17:54, 2026-10-04) |
| --- | --- | --- |
| `OLTP55_Schema.Container` (201 cols) | `fdp.oltp55_schema_container` | 9660 |
| `OLTP55_Schema.WorkflowStep` (27 cols) | `fdp.oltp55_schema_workflowstep` | 1077 |

---

## Table of contents

- [What each piece does](#what-each-piece-does)
- [Naming rule](#naming-rule)
- [Part A — one-time setup](#part-a--one-time-setup)
  - [A1. Prerequisites](#a1-prerequisites)
  - [A2. Connect in SSMS](#a2-connect-in-ssms)
  - [A3. Enable CDC on the SQL Server](#a3-enable-cdc-on-the-sql-server)
  - [A4. Start the Docker stack](#a4-start-the-docker-stack)
  - [A5. Decide how many Flink jobs can run](#a5-decide-how-many-flink-jobs-can-run)
- [Part B — adding a table: the repeating procedure](#part-b--adding-a-table-the-repeating-procedure)
- [Part C — what the generated SQL does](#part-c--what-the-generated-sql-does)
- [Part D — operating the pipeline](#part-d--operating-the-pipeline)
- [Part E — troubleshooting](#part-e--troubleshooting)
- [Part F — known gaps](#part-f--known-gaps)

---

## What each piece does

| Piece | Job it does |
| --- | --- |
| SQL Server CDC change tables | Records every INSERT/UPDATE/DELETE on a captured table. This is what "CDC" means. Requires the SQL Server Agent. |
| Flink job (`flink/sql/…`) | `sqlserver-cdc` source reads the change stream, each row is wrapped in a Debezium envelope, written to a Kafka topic. Checkpoints every 30 s. |
| Kafka topic | Durable buffer between Flink and ClickHouse. Survives a Flink restart, which is why the sink is Kafka and not a direct database write. |
| ClickHouse Kafka engine table | One `msg String` column, `kafka_format = 'JSONAsString'`. Pulls messages off the topic. |
| ClickHouse materialized view | Unpacks each Debezium message into a typed row and inserts it into the `ReplacingMergeTree` table. |
| `ReplacingMergeTree` table | The table you query. Collapses repeated and updated versions of the same primary key to one row. |

There is no direct Flink → ClickHouse sink because Flink's JDBC connector ships
no ClickHouse dialect (only mysql, postgres, oracle, db2, derby, sqlserver,
trino, oceanbase, cratedb). Kafka is the workaround.

## Naming rule

Everything is derived from one **slug**: `<schema>_<table>` in lower case.
`OLTP55_Schema.WorkflowStep` → `oltp55_schema_workflowstep`, and the generator
emits:

| Object | Name |
| --- | --- |
| Kafka topic | `oltp55_schema_workflowstep` |
| ClickHouse table | `fdp.oltp55_schema_workflowstep` |
| ClickHouse Kafka engine table | `fdp.oltp55_schema_workflowstep_kafka` |
| ClickHouse materialized view | `fdp.mv_oltp55_schema_workflowstep` |
| Flink job file | `flink/sql/generated/oltp55_schema_workflowstep_cdc.sql` |
| ClickHouse DDL file | `clickhouse/init/002_oltp55_schema_workflowstep.sql` |

Kafka topics are case sensitive. Do not hand-edit generated files to rename
anything — change the table you point the generator at instead.

---

# Part A — one-time setup

Everything in Part A is done once per machine. Part B is the part you repeat.

## A1. Prerequisites

| Requirement | Check |
| --- | --- |
| Docker Desktop running | `docker compose ps` |
| Python with `pyodbc` | `python -c "import pyodbc; print(pyodbc.version)"` |
| ODBC Driver 18 or 17 | `python -c "import pyodbc; print(pyodbc.drivers())"` |
| SSMS 19 or 20 | Start menu |
| Credentials | `sa` password, or a `flink_cdc` login you created |

If `pyodbc` is missing:

```powershell
pip install pyodbc
python -c "import pyodbc; print(pyodbc.drivers())"
# ('SQL Server', 'ODBC Driver 17 for SQL Server', 'ODBC Driver 18 for SQL Server', ...)
```

Run the generator's commands in **PowerShell**. In CMD the syntax is
`set MSSQL_USER=sa`; in PowerShell it is `$env:MSSQL_USER = 'sa'`. Using the CMD
form in PowerShell sets nothing and the script exits with
`Set MSSQL_USER and MSSQL_PASSWORD before running this script.`

## A2. Connect in SSMS

1. SSMS → **Connect** → **Database Engine**.
2. **Server name**: `192.168.1.83,1433`
   (host and port separated by a comma; the default 1433 may be left off).
3. **Authentication**: **SQL Server Authentication** — not Windows
   Authentication. This machine is not on the server's domain, so Windows auth
   cannot succeed.
4. **Login / Password**: `sa` and its password.
5. In **Connection Properties**, tick **Trust server certificate**. The instance
   uses a self-signed certificate; without this, encrypted connections fail with
   a certificate chain error.

Confirm you are on the right server:

```sql
SELECT @@VERSION;
SELECT SERVERPROPERTY('InstanceName') AS instance_name,
       SERVERPROPERTY('Edition')      AS edition,
       SERVERPROPERTY('ProductVersion') AS version;
```

Expected: SQL Server 2019, instance `MSSQLSERVER2017`.

From PowerShell, an equivalent connectivity test:

```powershell
Test-NetConnection -ComputerName 192.168.1.83 -Port 1433
```

`TcpTestSucceeded : True` means the port answers. This is also the quickest way
to tell a firewall problem apart from a credential problem — if it goes
`False`, nothing else in this document will work until the network path is back.
Occasionally it flaps for a few seconds on this LAN; re-run it before
concluding the server is down.

If the instance refuses connections entirely, remote TCP/IP is probably disabled
on the server side. That cannot be fixed from the client: SQL Server
Configuration Manager → Network Configuration → TCP/IP → Enabled, then restart
the SQL Server service.

## A3. Enable CDC on the SQL Server

CDC has to be switched on twice: for the database, and for each table. Only a
login with `db_owner` (or `sysadmin`) can do this.

**Once per database**, run `mssql/init/003_enable_cdc.sql` in SSMS after
replacing the `<Flink_Cdc_Password>` placeholder. It creates the `flink_cdc`
login, grants `VIEW SERVER STATE` (Debezium needs it), runs `sp_cdc_enable_db`,
creates the `flink_cdc` database user, and grants it `db_datareader` plus the
`cdc_reader` role.

**Once per table**, add a block like this (this is the same call the script makes
for `Container`):

```sql
USE [OLTPSemi_55];
GO

IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables
    WHERE source_object_id = OBJECT_ID(N'OLTP55_Schema.WorkflowStep'))
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema          = N'OLTP55_Schema',
        @source_name            = N'WorkflowStep',
        @role_name              = N'cdc_reader',
        @supports_net_changes   = 1;
END;
GO
```

`@supports_net_changes = 1` is optional but keeps the connector from falling
back to reading the whole log on restart.

Two prerequisites the script cannot fix for you:

- **The table needs a primary key or unique index.** SQL Server CDC has no other
  capture mode, so `sp_cdc_enable_table` fails without one. Check with
  `EXEC sys.sp_helpindex N'OLTP55_Schema.WorkflowStep';`. The index name is
  recorded in `cdc.change_tables.index_name`.
- **The SQL Server Agent must be running.** It drives the capture job. See
  [Part F](#part-f--known-gaps) — on this instance it is currently stopped and
  has to be started on the server host.

Verify, in SSMS:

```sql
USE [OLTPSemi_55];
GO
SELECT is_cdc_enabled FROM sys.databases WHERE name = N'OLTPSemi_55';
-- 1

SELECT OBJECT_NAME(source_object_id) AS source_table,
       capture_instance,
       supports_net_changes,
       index_name
FROM cdc.change_tables
WHERE source_object_id = OBJECT_ID(N'OLTP55_Schema.WorkflowStep');
-- OLTP55_Schema.WorkflowStep | OLTP55_Schema_Workflowstep | 0 | WorkflowStep624

SELECT servicename, status_desc
FROM sys.dm_server_services
WHERE servicename LIKE 'SQL Server Agent%';
-- SQL Server Agent (MSSQLSERVER2017): Running   <- must say Running

SELECT name, enabled FROM msdb.dbo.sysjobs WHERE name LIKE N'cdc%capture%';
-- cdc.OLTPSemi_55_capture | 1
```

`capture_instance` is generated by SQL Server as `<schema>_<table>`, and
`index_name` is whichever index you happen to have — read both from the query
rather than assuming. `supports_net_changes` is `0` for both tables on this
instance because they were captured without `@supports_net_changes`; that only
affects `sp_cdc_getnetchanges`, and the Flink connector streams the log directly,
so the jobs run fine either way. Pass `@supports_net_changes = 1` when enabling a
new table if you also want net-change support from the capture instance.

Note `cdc.change_tables` has `source_object_id`, **not** `source_object`. Using
the wrong name gives `Invalid column name 'source_object'` and it is easy to
misread that as "CDC is not enabled".

## A4. Start the Docker stack

```powershell
docker compose up -d --build
docker compose ps
```

Expect `clickhouse`, `kafka`, `kafka-init`, `kafka-ui`, `flink-jobmanager` and
`flink-taskmanager` to be `Up`. `kafka-init` creates every topic listed in
`kafka/topics.txt`, one partition each, and finishes.

| What | Where |
| --- | --- |
| Flink jobs | http://localhost:8081 |
| Kafka messages | http://localhost:8181 |
| ClickHouse HTTP | http://localhost:8124 (user `admin`, password `admin123`) |
| ClickHouse native | `localhost:9001` |

`flink/sql` is mounted read-only into the JobManager at
`/opt/flink/usrlib/sql`, so edited or generated SQL is picked up with no copy
step. `clickhouse/init` is mounted as the initdb directory, which means those
scripts run **only against a brand-new data volume** — on an existing stack you
apply DDL by hand (step B6).

## A5. Decide how many Flink jobs can run

`Docker-compose.yml` sets `TASK_MANAGER_NUMBER_OF_TASK_SLOTS=4`. Each job
consumes `parallelism.default` slots: the generated jobs use **2**, the
hand-written `container_to_clickhouse.sql` uses **1**. So today: 2 (container) +
2 (workflowstep) = 4, the cluster is full.

```powershell
(Invoke-WebRequest -UseBasicParsing http://localhost:8081/overview).Content
# slots-total=4  slots-available=2  taskmanagers=1
```

A job that cannot get its slots does not queue politely — it fails with

```
NoResourceAvailableException: Could not acquire the minimum required resources
```

and then loops between `RUNNING` and `RESTARTING` forever. To add a job, raise
the slot count and recreate the TaskManager:

```powershell
# edit TASK_MANAGER_NUMBER_OF_TASK_SLOTS in Docker-compose.yml first
docker compose up -d --force-recreate flink-taskmanager
```

**This cancels every running Flink job** — the jobs do not migrate, they come
back as `CANCELED`. Re-submit each one afterwards (step D2). Alternatively set
`SET 'parallelism.default' = '1';` in the new job before submitting to fit it in
the slots you already have.

---

# Part B — adding a table: the repeating procedure

Worked example: streaming `OLTPSemi_55.OLTP55_Schema.Spec` to ClickHouse. The
slug would be `oltp55_schema_spec`. Steps B1–B3 are SQL Server work; B4–B7 are
on this machine. Budget a couple of minutes plus the snapshot time.

## B1. Confirm the table has a primary key

SSMS, against `OLTPSemi_55`:

```sql
EXEC sys.sp_helpindex N'OLTP55_Schema.Spec';
```

No clustered or unique nonclustered index → CDC cannot capture it. Fix that on
the SQL Server side first; there is no way around it.

## B2. Enable CDC for the table

In SSMS, `USE [OLTPSemi_55]; GO` and run the `sp_cdc_enable_table` block from
[A3](#a3-enable-cdc-on-the-sql-server) with `@source_name = N'Spec'`.

You can also edit `mssql/init/003_enable_cdc.sql` (`@source_schema`,
`@source_name`) and re-run it; it is idempotent.

## B3. Confirm the SQL Server Agent is running

```sql
SELECT servicename, status_desc
FROM sys.dm_server_services
WHERE servicename LIKE 'SQL Server Agent%';
```

If it says `Stopped`, the snapshot in B7 will still work — the Flink source
reads the base table directly — but **no INSERT/UPDATE/DELETE will ever reach
Kafka**, because nothing writes the change tables. Fix this before trusting any
"it is not live" conclusion. On this instance the Agent cannot be started over
T-SQL (see [Part F](#part-f--known-gaps)); start the Windows service
`SQLSERVERAGENT` on the server host.

## B4. Generate the SQL files

The generator reads the live table, so nothing about the columns is hardcoded.

```powershell
$env:MSSQL_USER = 'sa'
$env:MSSQL_PASSWORD = 'your-password'

python mssql/tools/introspect_container.py `
    --database OLTPSemi_55 `
    --schema OLTP55_Schema `
    --table Spec `
    --write `
    --mode cdc
```

Flags: `--host`, `--port` (default `192.168.1.83`, `1433`), `--database`,
`--schema`, `--table`, `--mode cdc|batch`, `--sample N`, `--write`. Each also
has an `MSSQL_*` environment variable equivalent. `--mode batch` generates a
one-shot JDBC read instead of a stream: it finishes after a single pass and only
needs `SELECT`, so it works before CDC is enabled.

Pass `--table` **once**. Adding it twice gives
`argument --table: conflicting option string`.

Output looks like this — read it, because every `[ ]` is a problem:

```
== OLTPSemi_55.OLTP55_Schema.[Spec] ==
  ContainerId                    int
  ...
  primary key: SpecId
  rows: 412

== CDC prerequisites ==
  [x] CDC enabled on database OLTPSemi_55
  [x] capture instance for OLTP55_Schema.Spec: OLTP55_Schema_Spec
  [[ ]] SQL Server Agent: Stopped

Wrote clickhouse\init\002_oltp55_schema_spec.sql
Wrote flink\sql\generated\oltp55_schema_spec_cdc.sql (contains the SQL Server password)
```

Two files, both derived from the live schema:

- `flink/sql/generated/<slug>_cdc.sql` — source table, Kafka sink table and the
  `INSERT INTO`. Contains the password in clear text, which is why
  `flink/sql/generated/` is in `.gitignore`. Do not commit it, do not copy it
  out of the machine.
- `clickhouse/init/00N_<slug>.sql` — `ReplacingMergeTree` table, Kafka engine
  table, materialized view. Safe to commit.

If `pyodbc` cannot reach the server, run `mssql/introspect_container_schema.sql`
in SSMS instead (change its three variables at the top: `@database`, `@schema`,
`@table`), send the result grid back, and the two files get written by hand.

## B5. Create the Kafka topic

Append the slug to `kafka/topics.txt` — one topic per line, no commas — then
recreate the init service:

```powershell
docker compose up -d --force-recreate kafka-init
docker exec fdp-engine-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list
```

Do not skip this. The Flink source uses an AdminClient and never creates topics
on demand, so a missing topic surfaces as `UnknownTopicOrPartitionException`
with the job stuck in `RESTARTING`.

## B6. Apply the ClickHouse DDL

```powershell
Get-Content -Raw clickhouse\init\002_oltp55_schema_spec.sql |
    docker exec -i fdp-engine-clickhouse clickhouse-client --multiquery
```

Two traps here:

- **PowerShell has no `<` redirection.** `docker exec ... < file.sql` fails with
  *"The '<' operator is reserved for future use"*. Always pipe.
- **Every object is `IF NOT EXISTS`.** Applying the file to a stack where the
  table already exists silently changes nothing. For a table whose columns
  changed you must drop and recreate instead:

```powershell
docker exec fdp-engine-clickhouse clickhouse-client --multiquery --query "DROP VIEW IF EXISTS fdp.mv_oltp55_schema_spec; DROP TABLE IF EXISTS fdp.oltp55_schema_spec;"
```

Check what exists:

```powershell
docker exec fdp-engine-clickhouse clickhouse-client --query "SELECT name, engine FROM system.tables WHERE database='fdp' ORDER BY name"
```

Do not `SELECT` from a `_kafka` engine table directly to check its contents.
ClickHouse refuses with
`Direct select is not allowed ... stream_like_engine_allow_direct_select`, and
even if allowed it would consume the messages you are trying to count.

## B7. Submit the job

```powershell
./mssql/run_pipeline.ps1 -Slug oltp55_schema_spec -CreateTopic -ApplyDdl -Submit `
    -JobFile generated/oltp55_schema_spec_cdc.sql
```

That helper is a thin wrapper over B5 + B6 + submit, and it prints the job list
and row count at the end. Its parameters:

| Parameter | Meaning |
| --- | --- |
| `-Slug` | mandatory, `<schema>_<table>` lower case |
| `-Topic` | Kafka topic; defaults to `-Slug` |
| `-CreateTopic` | append the topic to `kafka/topics.txt` if absent, recreate `kafka-init` |
| `-ApplyDdl` | apply `clickhouse/init/*_<slug>.sql` to the running ClickHouse |
| `-Submit` | run the job file through the Flink SQL client |
| `-JobFile` | job file **relative to `flink/sql`**; defaults to `<slug>_cdc.sql` |

`-JobFile` matters: generated jobs land in `flink/sql/generated/`, so the default
only resolves for a job file sitting directly in `flink/sql/`. The Container job
is checked in at `flink/sql/container_to_clickhouse.sql`, so for it:

```powershell
./mssql/run_pipeline.ps1 -Slug oltp55_schema_container -Submit -JobFile container_to_clickhouse.sql
```

To submit by hand instead:

```powershell
docker exec -it fdp-engine-flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/generated/oltp55_schema_spec_cdc.sql
```

`-f` submits and returns; the job keeps running in the cluster. Watch the source
vertex's `numSplitsRemaining` metric at http://localhost:8081 to see the
snapshot progress.

## B8. Verify, then prove it is live

The count should reach the exact row count the generator printed:

```powershell
docker exec fdp-engine-clickhouse clickhouse-client --query "SELECT count() AS rows, uniqExact(SpecId) AS uniq FROM fdp.oltp55_schema_spec"
docker exec fdp-engine-clickhouse clickhouse-client --query "SELECT SpecId, Description FROM fdp.oltp55_schema_spec LIMIT 10"
```

`rows` should equal `uniq` on a fresh build. If it does not, see
[D4](#d4-re-running-a-job-replays-the-snapshot).

Then prove change capture works — in SSMS, on the live table:

```sql
UPDATE [OLTP55_Schema].[Spec]
SET [Description] = 'touched by flink'
WHERE [SpecId] = (SELECT MIN([SpecId]) FROM [OLTP55_Schema].[Spec]);
```

Re-run the `count()` query: with the Agent running, a new row appears within a
second or two. If nothing arrives and the Agent is running, check the Flink
source vertex metrics and `cdc.lsn_time_mapping` advancing in SQL Server.

---

# Part C — what the generated SQL does

### C1. The Flink job

```sql
SET 'execution.checkpointing.interval' = '30 s';
SET 'execution.checkpointing.tolerable-failed-checkpoints' = '100';
SET 'restart-strategy' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '2147483647';
SET 'parallelism.default' = '2';

CREATE TABLE <slug>_source ( ... ) WITH (
  'connector' = 'sqlserver-cdc',
  'hostname' = '192.168.1.83', 'port' = '1433',
  'username' = '...', 'password' = '...',
  'database-name' = 'OLTPSemi_55',
  'table-name' = 'OLTP55_Schema.Spec',
  'scan.startup.mode' = 'initial');

CREATE TABLE <slug>_sink ( ... ) WITH (
  'connector' = 'kafka', 'topic' = '<slug>',
  'properties.bootstrap.servers' = 'kafka:19092',
  'value.format' = 'debezium-json');

INSERT INTO <slug>_sink SELECT ... FROM <slug>_source;
```

Things that are not obvious:

- **`'value.format' = 'debezium-json'`, not `'format' = 'json'`.** A CDC source
  emits update and delete rows, and a Kafka sink rejects changelog input with
  `doesn't support consuming update and delete changes` unless the format can
  carry it. Debezium wraps each row as
  `{"before":...,"after":{...},"op":"c|u|d"}`.
- **No primary key on the Flink source, on purpose.** With a declared key Flink
  plans the changelog as upsert, and the sink format then has to carry retractions.
  The container job instead sets
  `'scan.incremental.snapshot.chunk.key-column' = 'ContainerId'` to chunk the
  initial snapshot.
- **`scan.startup.mode = 'initial'`** snapshots the existing rows first, then
  switches to the change stream.
- Checkpointing every 30 s is what makes restarts resume instead of replaying
  from the beginning.

### C2. The ClickHouse objects

```sql
CREATE TABLE fdp.<slug> ( ... ) ENGINE = ReplacingMergeTree ORDER BY <pk>;
CREATE TABLE fdp.<slug>_kafka (msg String) ENGINE = Kafka SETTINGS kafka_format = 'JSONAsString', ...;
CREATE MATERIALIZED VIEW fdp.mv_<slug> TO fdp.<slug> AS
SELECT CAST(JSONExtractInt(msg, 'after', 'Qty') AS Int32) AS Qty, ...
FROM fdp.<slug>_kafka
WHERE JSONExtractString(msg, 'op') IN ('c', 'u', 'r');
```

- **Keep the raw message.** The Kafka engine table holds one `msg String` and the
  view does the parsing, so a column can be added downstream without touching
  the topic.
- **`op = 'd'` is filtered out.** A ClickHouse row cannot be removed by a stream
  insert, so deletes are dropped rather than faked. This is a known gap.
- **Datetime columns must be `Nullable(...)` parsed with
  `parseDateTime64BestEffortOrNull`.** The non-`OrNull` variant throws on an
  empty string, which makes ClickHouse drop the entire message — the symptom is
  "messages in Kafka, ClickHouse empty".
- **`ReplacingMergeTree` ordered by the primary key** collapses the extra rows
  that updates and snapshot replays produce. Read with `FINAL`, or compact with
  `OPTIMIZE TABLE fdp.<slug> FINAL`.
- **Text columns become `''` for SQL NULL**; only datetime columns stay NULL.

---

# Part D — operating the pipeline

## D1. Health check

```powershell
# jobs
(Invoke-WebRequest -UseBasicParsing http://localhost:8081/jobs/overview).Content |
    ConvertFrom-Json | Select-Object -ExpandProperty jobs |
    Format-Table jid, state, name -AutoSize

# slots
(Invoke-WebRequest -UseBasicParsing http://localhost:8081/overview).Content

# checkpoints for one job
(Invoke-WebRequest -UseBasicParsing "http://localhost:8081/jobs/<jobid>/checkpoints/config").Content
# {"mode":"exactly_once","interval":30000,...}

# ClickHouse row counts
docker exec fdp-engine-clickhouse clickhouse-client --query "SELECT count() FROM fdp.oltp55_schema_container"
docker exec fdp-engine-clickhouse clickhouse-client --query "SELECT count() FROM fdp.oltp55_schema_workflowstep"

# topics and how many messages they hold
docker exec fdp-engine-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:19092 --topic oltp55_schema_container
```

## D2. Cancel a job

```powershell
Invoke-WebRequest -UseBasicParsing -Method PATCH `
    -Body '{"cmd":"cancel"}' -ContentType 'application/json' `
    "http://localhost:8081/jobs/<jobid>"
```

`POST /jobs/<jobid>/cancel` returns `Not found` on Flink 1.20 — use the PATCH
form above.

Stopping everything:

```powershell
docker compose down        # containers only, data volumes kept
docker compose down -v     # also deletes the Kafka and ClickHouse volumes
```

`down -v` destroys all ClickHouse and Kafka data. Re-running `up -d --build`
afterwards replays every init script, so the ClickHouse tables come back empty
and only fill as jobs re-snapshot.

## D3. Resubmit a job

Resubmitting is safe and is the normal response to `CANCELED`, `FAILED` or a
TaskManager restart:

```powershell
./mssql/run_pipeline.ps1 -Slug oltp55_schema_workflowstep -Submit -JobFile generated/oltp55_schema_workflowstep_cdc.sql
./mssql/run_pipeline.ps1 -Slug oltp55_schema_container -Submit -JobFile container_to_clickhouse.sql
```

A resubmitted job starts a **fresh initial snapshot** — the source reads the
base table again rather than resuming from the topic — so the whole table is
re-emitted into Kafka and the materialized view ingests it a second time. With
`ReplacingMergeTree` the duplicates collapse; compact them with:

```powershell
docker exec fdp-engine-clickhouse clickhouse-client --query "OPTIMIZE TABLE fdp.oltp55_schema_container FINAL"
```

Only cancel and resubmit for a reason. Every resubmit re-reads the whole source
table.

## D4. Re-running a job replays the snapshot

Symptoms: `SELECT count()` returns exactly twice the source row count, or
`uniqExact(pk)` is half of `count()`.

```powershell
docker exec fdp-engine-clickhouse clickhouse-client --query "SELECT count() AS rows, uniqExact(ContainerId) AS uniq FROM fdp.oltp55_schema_container"
```

Cause: the job was cancelled and resubmitted before its first checkpoint
completed, or the TaskManager was recreated, so the snapshot was emitted twice
into a sink that appends. Confirm the diagnosis:

```powershell
docker exec fdp-engine-clickhouse clickhouse-client --query "OPTIMIZE TABLE fdp.oltp55_schema_container FINAL"
docker exec fdp-engine-clickhouse clickhouse-client --query "SELECT count() AS rows, uniqExact(ContainerId) AS uniq FROM fdp.oltp55_schema_container"
```

`OPTIMIZE … FINAL` collapses them; the count drops back to the source row count.

`TRUNCATE` is not the answer. Truncating while the materialized view keeps
consuming leaves you with a permanently short table, because the consumer group
has already committed past those messages and will not re-read them.

## D5. Rebuild a table from Kafka

The recovery that actually works if a table has lost rows and the topic still has
them: let a *second* materialized view consume the topic from the beginning under
a **new consumer group**, into an empty table, then swap it in. It took about 2
minutes for 28 980 messages.

```powershell
$slug = 'oltp55_schema_container'
$sql = Get-Content -Raw "clickhouse\init\004_$slug.sql"
$sql = $sql -replace "kafka_group_name = 'fdp-$slug'", "kafka_group_name = 'fdp-rebuild-1'"
$sql = $sql -replace "${slug}_kafka", "${slug}_kafka_rebuild"
$sql = $sql -replace "mv_$slug", "mv_${slug}_rebuild"
$sql | docker exec -i fdp-engine-clickhouse clickhouse-client --multiquery

# wait until the row count stops growing
docker exec fdp-engine-clickhouse clickhouse-client --query "SELECT count() FROM fdp.$slug"

# collapse the duplicate passes, then swap back
docker exec fdp-engine-clickhouse clickhouse-client --query "OPTIMIZE TABLE fdp.$slug FINAL"
docker exec fdp-engine-clickhouse clickhouse-client --multiquery --query "DROP VIEW IF EXISTS fdp.mv_${slug}_rebuild; DROP TABLE IF EXISTS fdp.${slug}_kafka_rebuild;"
Get-Content -Raw "clickhouse\init\004_$slug.sql" | docker exec -i fdp-engine-clickhouse clickhouse-client --multiquery
```

Verify with `count()` **and** `uniqExact(<pk>)`; they must match the source row
count. Leave the rebuild objects in place and they keep consuming a second copy
of every message.

Two mistakes worth knowing about, both made while building this:

- `CREATE TABLE new AS old ENGINE = ReplacingMergeTree ORDER BY pk` copies the
  **structure but not the rows** on ClickHouse 26.x. Renaming that empty table
  into place destroyed the data that `CREATE TABLE … AS` was supposed to copy.
  Rebuild from the topic instead, as above.
- Reading a `_kafka` engine table with `SELECT count()` to check progress fails
  with `Direct select is not allowed` (and would consume the data).

## D6. Changing columns on a source table

The pipeline is schema static — Flink does not apply DDL from the source. After
an `ALTER TABLE` on SQL Server:

1. Re-run the generator with `--write` (B4) for the new column list.
2. For **added** columns, `ALTER TABLE … ADD COLUMN` on the ClickHouse table, the
   `_kafka` table and the materialized view, in that order.
3. For **removed or retyped** columns, drop the view and the table and re-apply
   the regenerated DDL (B6) — that is the only way to rebuild them correctly.
4. Cancel and resubmit the job; checkpoints from the old schema cannot be
   restored.

---

# Part E — troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `Could not find any jdbc dialect factory ... 'jdbc:clickhouse://...'` | Flink's JDBC connector has no ClickHouse dialect | Use the Kafka topic path in this document |
| `UnknownTopicOrPartitionException`, job stuck in `RESTARTING` | Topic does not exist | Add the slug to `kafka/topics.txt`, `docker compose up -d --force-recreate kafka-init` (B5) |
| `doesn't support consuming update and delete changes` | Sink format cannot carry the CDC changelog | Keep `'value.format' = 'debezium-json'` on the Kafka sink |
| `Login failed for user 'sa'` | Wrong credentials | Fix `'username'`/`'password'` in the source DDL, or `$env:MSSQL_USER`/`$env:MSSQL_PASSWORD` for the generator |
| `Failed to discover captured tables` | CDC is not enabled on that table | Run `sp_cdc_enable_table` (B2); check `cdc.change_tables` |
| `Invalid column name 'source_object'` | `cdc.change_tables` key is `source_object_id` | Use `source_object_id` in the query |
| `NoResourceAvailableException: Could not acquire the minimum required resources` | Not enough TaskManager slots; job loops RUNNING/RESTARTING | Raise `TASK_MANAGER_NUMBER_OF_TASK_SLOTS` and recreate the TaskManager (A5), or set `parallelism.default = '1'` |
| `argument --table: conflicting option string` | `--table` passed twice | Pass it once |
| `Set MSSQL_USER and MSSQL_PASSWORD before running this script.` | CMD syntax used in PowerShell | `$env:MSSQL_USER = '...'` |
| `ModuleNotFoundError: No module named 'pyodbc'` | Not installed for that interpreter | `pip install pyodbc`, then check `python -c "import pyodbc; print(pyodbc.version)"` |
| `The '<' operator is reserved for future use` | bash-style input redirection in PowerShell | `Get-Content -Raw f.sql \| docker exec -i … clickhouse-client --multiquery` |
| `Login timeout expired` / `Server is not found or not accessible` | Network path to 1433 is down or flapping | `Test-NetConnection -ComputerName 192.168.1.83 -Port 1433`, retry; check firewall/VPN |
| `Could not find stored procedure 'sys.sp_start_server'` | The Agent cannot be started over T-SQL on this instance | Start `SQLSERVERAGENT` on the server host |
| Messages in Kafka, ClickHouse stays empty | MV insert throws on a column | `docker logs fdp-engine-clickhouse`; datetime columns must be `Nullable` + `parseDateTime64BestEffortOrNull` |
| `Direct select is not allowed` | `SELECT` on a Kafka engine table | Count the target table instead |
| Row count doubled, `uniqExact` half of `count()` | Snapshot replayed into an appending sink | `OPTIMIZE TABLE … FINAL` (D4) |
| Job shows `CANCELED` after a compose change | TaskManager was recreated; jobs do not migrate | Resubmit (D3) |
| Applied the DDL and nothing happened | Every object is `IF NOT EXISTS` | Drop and recreate (B6) |
| Generated files missing from `git status` | `flink/sql/generated/` is gitignored — by design, it holds the password | Leave them ignored |

---

# Part F — known gaps

1. **The SQL Server Agent on 192.168.1.83 is stopped**, so the CDC capture jobs
   (`cdc.OLTPSemi_55_capture`, `cdc.Medi_Dec_BI_capture`) are not running. Both
   Flink jobs are `RUNNING` and the initial snapshots are complete and correct,
   but **no INSERT/UPDATE/DELETE is reaching Kafka or ClickHouse**. Both capture
   jobs exist and are `enabled = 1`, so capture begins the moment the Agent
   starts.

   `EXEC sp_start_server 'SQLSERVERAGENT'` does **not** work from SQL on this
   instance — the procedure is not present on it, even though the service is
   registered and shows up in `sys.dm_server_services` as *SQL Server Agent
   (MSSQLSERVER2017)*. It has to be started on the server host
   (`Start-Service SQLSERVERAGENT`, or Services.msc). Note that roughly 11 other
   Agent jobs on that instance, including `Camstar Summary Tables
   (OLTPSemi_55)`, are also idle for the same reason.

2. **Deletes are dropped.** The materialized views filter `op = 'd'`; a stream
   insert cannot remove a row. A proper fix means emitting tombstones or
   periodically reconciling from the source, which is a deliberate design change
   to the generator and both views.

3. **No freshness monitoring.** Nothing alerts when the row count stops moving
   or the job leaves `RUNNING`. The health check in D1 is manual; wiring it into
   the Flink/ClickHouse side would catch the Agent-stopped case automatically.