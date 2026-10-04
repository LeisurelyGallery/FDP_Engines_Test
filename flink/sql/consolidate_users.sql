-- Flink's JDBC connector has no ClickHouse dialect, so this job writes the joined
-- rows to the Kafka topic `consolidated_users`; clickhouse/init/003_consolidated_users_kafka.sql
-- (Kafka engine table + materialized view) moves them into fdp.consolidated_users.

SET 'execution.checkpointing.interval' = '10 s';
SET 'parallelism.default' = '1';

CREATE TABLE users_source (
  id STRING,
  firstName STRING,
  lastName STRING,
  email STRING,
  username STRING,
  phone STRING,
  zip STRING,
  gender STRING,
  ingested_at STRING,
  `partition` BIGINT METADATA FROM 'partition' VIRTUAL,
  `offset` BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'users',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'flink-users-consolidation',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE test_source (
  id STRING,
  firstName STRING,
  lastName STRING,
  email STRING,
  username STRING,
  phone STRING,
  zip STRING,
  gender STRING,
  ingested_at STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'Test',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'flink-test-consolidation',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE consolidated_users_sink (
  id STRING,
  first_name STRING,
  last_name STRING,
  email STRING,
  username STRING,
  phone STRING,
  zip STRING,
  gender STRING,
  test_first_name STRING,
  test_last_name STRING,
  test_email STRING,
  test_username STRING,
  test_phone STRING,
  test_zip STRING,
  test_gender STRING,
  joined_at STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'consolidated_users',
  'properties.bootstrap.servers' = 'kafka:19092',
  'format' = 'json'
);

INSERT INTO consolidated_users_sink
SELECT
  u.id, u.firstName, u.lastName, u.email, u.username, u.phone, u.zip, u.gender,
  t.firstName, t.lastName, t.email, t.username, t.phone, t.zip, t.gender,
  DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS')
FROM users_source AS u
JOIN test_source AS t ON u.id = t.id;
=======
SET 'execution.checkpointing.interval' = '10 s';
SET 'parallelism.default' = '1';

CREATE TABLE users_source (
  id STRING,
  firstName STRING,
  lastName STRING,
  email STRING,
  username STRING,
  phone STRING,
  zip STRING,
  gender STRING,
  ingested_at STRING,
  `partition` BIGINT METADATA FROM 'partition' VIRTUAL,
  `offset` BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'users',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'flink-users-consolidation',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE test_source (
  id STRING,
  firstName STRING,
  lastName STRING,
  email STRING,
  username STRING,
  phone STRING,
  zip STRING,
  gender STRING,
  ingested_at STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'Test',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'flink-test-consolidation',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE clickhouse_sink (
  id STRING,
  first_name STRING,
  last_name STRING,
  email STRING,
  username STRING,
  phone STRING,
  zip STRING,
  gender STRING,
  test_first_name STRING,
  test_last_name STRING,
  test_email STRING,
  test_username STRING,
  test_phone STRING,
  test_zip STRING,
  test_gender STRING,
  joined_at TIMESTAMP(3)
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:clickhouse://clickhouse:8123/fdp',
  'table-name' = 'consolidated_users',
  'username' = 'admin',
  'password' = 'admin123',
  'sink.buffer-flush.max-rows' = '1000',
  'sink.buffer-flush.interval' = '1s'
);

INSERT INTO clickhouse_sink
SELECT
  u.id, u.firstName, u.lastName, u.email, u.username, u.phone, u.zip, u.gender,
  t.firstName, t.lastName, t.email, t.username, t.phone, t.zip, t.gender,
  CURRENT_TIMESTAMP
FROM users_source AS u
JOIN test_source AS t ON u.id = t.id;