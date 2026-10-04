-- Run this in SSMS against the SQL Server, then send the result grid back so the
-- Flink job can be generated without installing pyodbc locally.
-- It is the T-SQL equivalent of mssql/tools/introspect_container.py.
--
-- To inspect a different table, change the three variables below; every query in
-- this script follows them.

USE [OLTPSemi_55];
GO

DECLARE @database sysname = N'OLTPSemi_55';
DECLARE @schema   sysname = N'OLTP55_Schema';
DECLARE @table    sysname = N'Container';
GO

-- 1. Columns with the Flink and ClickHouse types they map to -----------------
SELECT
    c.ORDINAL_POSITION                AS pos,
    c.COLUMN_NAME                     AS column_name,
    c.DATA_TYPE                       AS sql_type,
    c.CHARACTER_MAXIMUM_LENGTH        AS char_len,
    c.NUMERIC_PRECISION               AS num_precision,
    c.NUMERIC_SCALE                   AS num_scale,
    c.DATETIME_PRECISION              AS dt_precision,
    c.IS_NULLABLE                     AS nullable,
    CASE WHEN pk.COLUMN_NAME IS NULL THEN '' ELSE 'PK' END AS key_flag,
    CASE
        WHEN c.DATA_TYPE IN ('char', 'nchar') THEN CONCAT('CHAR(', COALESCE(c.CHARACTER_MAXIMUM_LENGTH, 1), ')')
        WHEN c.DATA_TYPE IN ('varchar', 'nvarchar') AND c.CHARACTER_MAXIMUM_LENGTH >= 0
            THEN CONCAT('VARCHAR(', c.CHARACTER_MAXIMUM_LENGTH, ')')
        WHEN c.DATA_TYPE IN ('text', 'ntext', 'xml', 'uniqueidentifier') THEN 'STRING'
        WHEN c.DATA_TYPE IN ('decimal', 'numeric', 'money', 'smallmoney')
            THEN CONCAT('DECIMAL(', COALESCE(c.NUMERIC_PRECISION, 38), ',', COALESCE(c.NUMERIC_SCALE, 0), ')')
        WHEN c.DATA_TYPE IN ('float', 'real') THEN 'DOUBLE'
        WHEN c.DATA_TYPE = 'bit' THEN 'BOOLEAN'
        WHEN c.DATA_TYPE = 'int' THEN 'INT'
        WHEN c.DATA_TYPE IN ('tinyint', 'smallint') THEN 'SMALLINT'
        WHEN c.DATA_TYPE = 'bigint' THEN 'BIGINT'
        WHEN c.DATA_TYPE = 'date' THEN 'DATE'
        WHEN c.DATA_TYPE = 'time' THEN CONCAT('TIME(', COALESCE(c.DATETIME_PRECISION, 7), ')')
        WHEN c.DATA_TYPE IN ('datetime', 'datetime2') THEN CONCAT('TIMESTAMP(', COALESCE(c.DATETIME_PRECISION, 3), ')')
        WHEN c.DATA_TYPE = 'smalldatetime' THEN 'TIMESTAMP(0)'
        WHEN c.DATA_TYPE = 'datetimeoffset' THEN 'TIMESTAMP_LTZ(3)'
        WHEN c.DATA_TYPE IN ('binary', 'varbinary', 'image', 'rowversion', 'timestamp') THEN 'BYTES'
        ELSE 'STRING'
    END AS flink_type,
    CASE
        WHEN c.DATA_TYPE IN ('char', 'nchar', 'varchar', 'nvarchar', 'text', 'ntext', 'xml',
                             'uniqueidentifier', 'time', 'binary', 'varbinary', 'image',
                             'rowversion', 'timestamp') THEN 'String'
        WHEN c.DATA_TYPE IN ('decimal', 'numeric', 'money', 'smallmoney')
            THEN CONCAT('Decimal(', COALESCE(c.NUMERIC_PRECISION, 38), ',', COALESCE(c.NUMERIC_SCALE, 0), ')')
        WHEN c.DATA_TYPE IN ('float', 'real') THEN 'Float64'
        WHEN c.DATA_TYPE = 'bit' THEN 'Bool'
        WHEN c.DATA_TYPE = 'int' THEN 'Int32'
        WHEN c.DATA_TYPE IN ('tinyint', 'smallint') THEN 'Int16'
        WHEN c.DATA_TYPE = 'bigint' THEN 'Int64'
        WHEN c.DATA_TYPE = 'date' THEN 'Date'
        WHEN c.DATA_TYPE IN ('datetime', 'datetime2') THEN 'DateTime64(3)'
        WHEN c.DATA_TYPE = 'smalldatetime' THEN 'DateTime64(0)'
        WHEN c.DATA_TYPE = 'datetimeoffset' THEN 'DateTime64(3, ''UTC'')'
        ELSE 'String'
    END AS clickhouse_type
FROM INFORMATION_SCHEMA.COLUMNS AS c
LEFT JOIN (
    SELECT kc.name AS schema_name, col.name AS column_name
    FROM sys.key_constraints AS kc
    INNER JOIN sys.index_columns AS ic
        ON ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id
    INNER JOIN sys.columns AS col
        ON col.object_id = ic.object_id AND col.column_id = ic.column_id
    WHERE kc.type = 'PK' AND SCHEMA_NAME(kc.schema_id) = @schema
) AS pk ON pk.schema_name = c.TABLE_SCHEMA AND pk.column_name = c.COLUMN_NAME
WHERE c.TABLE_CATALOG = @database
  AND c.TABLE_SCHEMA = @schema
  AND c.TABLE_NAME = @table
ORDER BY c.ORDINAL_POSITION;
GO

-- 2. Row count --------------------------------------------------------------
-- A FROM clause cannot be built from variables, so this one needs dynamic SQL.
EXEC sys.sp_executesql
    N'SELECT COUNT_BIG(*) AS container_rows
      FROM ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@table) + N';';
GO

-- 3. First rows, so you can see the shape of the data ------------------------
EXEC sys.sp_executesql
    N'SELECT TOP (5) *
      FROM ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@table) + N';';
GO

-- 4. CDC prerequisites ------------------------------------------------------
-- The CDC Flink job needs all three of these. [ ] means the job will fail.
SELECT 'database CDC enabled' AS check_name,
       CASE WHEN is_cdc_enabled = 1 THEN '[x]' ELSE '[ ]' END AS ok,
       CAST(is_cdc_enabled AS VARCHAR(10)) AS detail
FROM sys.databases WHERE name = @database
UNION ALL
SELECT 'capture instance',
       CASE WHEN COUNT(*) > 0 THEN '[x]' ELSE '[ ]' END,
       ISNULL(MAX(capture_instance), 'run sp_cdc_enable_table')
FROM cdc.change_tables
WHERE source_object_id = OBJECT_ID(QUOTENAME(@schema) + N'.' + QUOTENAME(@table))
UNION ALL
SELECT 'SQL Server Agent',
       CASE WHEN status_desc LIKE '%running%' THEN '[x]' ELSE '[ ]' END,
       ISNULL(MAX(status_desc), 'not visible to this login')
FROM sys.dm_server_services
WHERE servicename LIKE 'SQL Server Agent%';
GO