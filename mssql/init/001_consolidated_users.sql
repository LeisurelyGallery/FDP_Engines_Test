-- Target: SQL Server 192.168.1.83:1433
-- Run this in SSMS (connect to 192.168.1.83 first, master as the target database).
-- It is the SQL Server equivalent of clickhouse/init/001_consolidated_users.sql and
-- matches the column order written by flink/sql/consolidate_users_mssql.sql.

IF DB_ID(N'fdp') IS NULL
BEGIN
    CREATE DATABASE [fdp];
END;
GO

USE [fdp];
GO

IF OBJECT_ID(N'dbo.consolidated_users', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.consolidated_users
    (
        id             NVARCHAR(64)   NOT NULL,
        first_name     NVARCHAR(256)  NULL,
        last_name      NVARCHAR(256)  NULL,
        email          NVARCHAR(256)  NULL,
        username       NVARCHAR(256)  NULL,
        phone          NVARCHAR(64)   NULL,
        zip            NVARCHAR(32)   NULL,
        gender         NVARCHAR(32)   NULL,
        test_first_name NVARCHAR(256) NULL,
        test_last_name  NVARCHAR(256) NULL,
        test_email      NVARCHAR(256) NULL,
        test_username   NVARCHAR(256) NULL,
        test_phone      NVARCHAR(64)  NULL,
        test_zip        NVARCHAR(32)  NULL,
        test_gender     NVARCHAR(32)  NULL,
        joined_at       DATETIME2(3)   NOT NULL
            CONSTRAINT DF_consolidated_users_joined_at DEFAULT SYSUTCDATETIME()
    );

    CREATE CLUSTERED INDEX CX_consolidated_users
        ON dbo.consolidated_users (id, joined_at);
END;
GO