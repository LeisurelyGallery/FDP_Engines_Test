# Flink users/Test consolidation

This first pipeline reads JSON records from Kafka topics `users` and `Test`,
joins them on `id`, selects the required user and test columns, and writes the
result to the Kafka topic `consolidated_users`. ClickHouse then stores it:
`clickhouse/init/003_consolidated_users_kafka.sql` defines a Kafka engine table
plus a materialized view that moves rows into the MergeTree table
`fdp.consolidated_users`.

The hop through Kafka is not a workaround you can avoid with a config change:
Flink's JDBC connector ships dialects for mysql, postgres, oracle, db2, derby,
sqlserver, trino, oceanbase and cratedb only, so there is no ClickHouse dialect
to load (`Could not find any jdbc dialect factory ...` is what you get instead).
The SQL Server sink works over JDBC because that dialect exists.

`clickhouse/init` scripts only run against a fresh volume, so apply the Kafka
engine table and materialized view to a running container with:

```
Get-Content -Raw clickhouse/init/003_consolidated_users_kafka.sql | docker exec -i fdp-engine-clickhouse clickhouse-client --multiquery
```

Start the infrastructure with `docker compose up -d --build`; the `kafka-init`
service creates the topics `users`, `Test` and `consolidated_users`, which the
Flink sources require to exist before the job starts. Submit the SQL job from
the Flink SQL client/container using `flink/sql/consolidate_users.sql`, then run
`python Producer/publish_csv_topics.py --limit 1000` from the host.

The join is an inner streaming join. A record is emitted when matching IDs are
available on both Kafka topics. Kafka retains the source events, so the job can
be restarted using its consumer groups and checkpoints.

```
docker exec -it fdp-engine-flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/consolidate_users.sql
python Producer/publish_csv_topics.py --limit 1000
docker exec -it fdp-engine-clickhouse clickhouse-client --query "SELECT count() FROM fdp.consolidated_users"
```

Submitting the file with `-f` returns immediately and the job keeps running in
the cluster.

## SQL Server sink

`flink/sql/consolidate_users_mssql.sql` is the same pipeline writing to the SQL
Server at `192.168.1.83:1433` (`fdp.dbo.consolidated_users`) instead of
ClickHouse. Set the username/password in the sink DDL, create the table with
`mssql/init/001_consolidated_users.sql`, then submit:

```
docker compose up -d --build
docker exec -it fdp-engine-flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/consolidate_users_mssql.sql
```

See `mssql/README.md` for the SSMS steps and how to read Flink state from SQL
Server.

## SQL Server CDC source

`flink/Dockerfile` also installs `flink-sql-connector-sqlserver-cdc` (Flink CDC
3.6.0-1.20). The job that streams `OLTPSemi_55.OLTP55_Schema.[Container]` into
ClickHouse is generated from the live table schema by
`mssql/tools/introspect_container.py` into `flink/sql/generated/` (gitignored,
it contains the SQL Server password). Setup steps and the CDC enablement script
are in `mssql/README.md`.
