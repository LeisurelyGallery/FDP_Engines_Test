-- Enables Change Data Capture on OLTPSemi_55.OLTP55_Schema.[Container] so the
-- Flink CDC job in flink/sql/generated/ can read it. Run in SSMS as a login with
-- sysadmin/db_owner rights (sp_cdc_enable_* require db_owner).
-- Replace <Flink_Cdc_Password> before running, then use flink_cdc as the
-- username/password in the generated Flink DDL.

USE master;
GO

IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'flink_cdc')
BEGIN
    CREATE LOGIN flink_cdc
        WITH PASSWORD = N'<Flink_Cdc_Password>', CHECK_POLICY = OFF;
END;
GO

-- Debezium (and therefore the Flink CDC connector) needs this server permission.
GRANT VIEW SERVER STATE TO flink_cdc;
GO

-- On an Always On availability group instance the login also needs:
-- GRANT ALTER AVAILABILITY GROUP TO flink_cdc;

USE [OLTPSemi_55];
GO

IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = N'OLTPSemi_55') = 0
BEGIN
    EXEC sys.sp_cdc_enable_db;
END;
GO

IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables
    WHERE source_object_id = OBJECT_ID(N'OLTP55_Schema.Container'))
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'OLTP55_Schema',
        @source_name = N'Container',
        @role_name = N'cdc_reader',
        @supports_net_changes = 1;
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'flink_cdc')
BEGIN
    CREATE USER flink_cdc FOR LOGIN flink_cdc;
    ALTER ROLE db_datareader ADD MEMBER flink_cdc;
    EXEC sys.sp_addrolemember N'cdc_reader', N'flink_cdc';
END;
GO

-- Verification -------------------------------------------------------------
-- 1. SQL Server Agent must be running, it drives the capture job.
SELECT servicename, status_desc
FROM sys.dm_server_services
WHERE servicename LIKE 'SQL Server Agent%';
GO

-- 2. The capture job must be running.
SELECT j.name, j.enabled
FROM msdb.dbo.sysjobs AS j
INNER JOIN msdb.dbo.syscategories AS c ON j.category_id = c.category_id
WHERE c.name = N'CDC';
GO

-- 3. This must return a row for OLTP55_Schema.Container, otherwise the Flink
--    job will fail with "no capture instance".
SELECT OBJECT_NAME(source_object_id) AS source_table,
       capture_instance,
       supports_net_changes,
       index_name
FROM cdc.change_tables;
GO