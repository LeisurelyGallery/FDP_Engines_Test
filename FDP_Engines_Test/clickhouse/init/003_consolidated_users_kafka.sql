-- Streaming path into fdp.consolidated_users.
--
-- Flink's JDBC connector has no ClickHouse dialect (only mysql, postgres, oracle,
-- db2, derby, sqlserver, trino, oceanbase and cratedb are shipped), so
-- flink/sql/consolidate_users.sql writes to the Kafka topic `consolidated_users`
-- and these tables move it into ClickHouse.
--
-- clickhouse/init only runs against a fresh volume, so apply this to the running
-- container with:
--   Get-Content -Raw clickhouse/init/003_consolidated_users_kafka.sql | docker exec -i fdp-engine-clickhouse clickhouse-client --multiquery

CREATE TABLE IF NOT EXISTS fdp.consolidated_users_kafka
(
    id String,
    first_name String,
    last_name String,
    email String,
    username String,
    phone String,
    zip String,
    gender String,
    test_first_name String,
    test_last_name String,
    test_email String,
    test_username String,
    test_phone String,
    test_zip String,
    test_gender String,
    joined_at String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'consolidated_users',
    kafka_group_name = 'fdp-consolidated-users',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 1000,
    kafka_max_block_size = 1048576,
    kafka_skip_broken_messages = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS fdp.mv_consolidated_users
TO fdp.consolidated_users
AS
SELECT
    id,
    first_name,
    last_name,
    email,
    username,
    phone,
    zip,
    gender,
    test_first_name,
    test_last_name,
    test_email,
    test_username,
    test_phone,
    test_zip,
    test_gender,
    parseDateTime64BestEffort(joined_at, 3) AS joined_at
FROM fdp.consolidated_users_kafka;