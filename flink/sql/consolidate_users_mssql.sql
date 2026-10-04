-- Writes the users/Test consolidation to SQL Server at 192.168.1.83:1433.
-- Replace <MSSQL_USERNAME> / <MSSQL_PASSWORD> before submitting.
-- Consumer groups differ from consolidate_users.sql so this job and the ClickHouse
-- job can run at the same time without splitting the Kafka partitions.
-- Create the database/table first with mssql/init/001_consolidated_users.sql.

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
  'properties.group.id' = 'flink-users-consolidation-mssql',
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
  'properties.group.id' = 'flink-test-consolidation-mssql',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE mssql_sink (
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
  'url' = 'jdbc:sqlserver://192.168.1.83:1433;databaseName=fdp;encrypt=true;trustServerCertificate=true',
  'table-name' = 'dbo.consolidated_users',
  'driver' = 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
  'username' = '<sa>',
  'password' = '<dts@123>',
  'sink.buffer-flush.max-rows' = '1000',
  'sink.buffer-flush.interval' = '1s'
);

INSERT INTO mssql_sink
SELECT
  u.id, u.firstName, u.lastName, u.email, u.username, u.phone, u.zip, u.gender,
  t.firstName, t.lastName, t.email, t.username, t.phone, t.zip, t.gender,
  CURRENT_TIMESTAMP
FROM users_source AS u
JOIN test_source AS t ON u.id = t.id;