-- Optional least-privilege login for the Flink JDBC sink.
-- Run against 192.168.1.83 (server-level, target database master) AFTER
-- mssql/init/001_consolidated_users.sql has created the [fdp] database.
-- Replace <SINK_LOGIN_PASSWORD> before running, then use flink_sink as the
-- username/password in flink/sql/consolidate_users_mssql.sql.

USE master;
GO

IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'flink_sink')
BEGIN
    CREATE LOGIN flink_sink
        WITH PASSWORD = N'<SINK_LOGIN_PASSWORD>', CHECK_POLICY = OFF;
END;
GO

USE [fdp];
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'flink_sink')
BEGIN
    CREATE USER flink_sink FOR LOGIN flink_sink;
    ALTER ROLE db_datareader ADD MEMBER flink_sink;
    ALTER ROLE db_datawriter ADD MEMBER flink_sink;
END;
GO