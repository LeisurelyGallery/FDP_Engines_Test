"""Introspect a SQL Server table and generate the Flink CDC pipeline for it.

Reads the live schema of <database>.<schema>.<table> on 192.168.1.83, prints the
definition, row count and sample rows, then writes:

    clickhouse/init/002_<schema>_<table>.sql   ClickHouse table (no credentials)
    flink/sql/generated/<schema>_<table>_cdc.sql  sqlserver-cdc -> clickhouse job

Credentials come from the environment, so nothing secret lands in the repo:

    set MSSQL_USER=... / set MSSQL_PASSWORD=...

The generated Flink job contains the password in clear text and is gitignored.

Requires: pip install pyodbc  (plus "ODBC Driver 18 for SQL Server" on Windows)
"""
import argparse
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GENERATED_JOB_DIR = ROOT / 'flink' / 'sql' / 'generated'
CLICKHOUSE_INIT_DIR = ROOT / 'clickhouse' / 'init'

SIMPLE_IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
BYTE_TYPES = ('binary', 'varbinary', 'image', 'rowversion', 'timestamp')
# Transported as a string and parsed back by the materialized view, because Flink's
# JSON format writes timestamps as ISO strings and ClickHouse's JSONEachRow parser
# does not accept the 'T' separator reliably.
TEMPORAL_TYPES = ('datetime', 'datetime2', 'smalldatetime', 'datetimeoffset')


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def quote(name):
    return name if SIMPLE_IDENTIFIER.match(name) else '`' + name.replace('`', '``') + '`'


def clickhouse_type(data_type, precision, scale, datetime_precision):
    kind = data_type.lower()
    if kind in ('char', 'nchar', 'varchar', 'nvarchar', 'text', 'ntext', 'xml', 'uniqueidentifier'):
        return 'String'
    if kind in ('decimal', 'numeric', 'money', 'smallmoney'):
        return f'Decimal({precision or 38},{scale or 0})'
    if kind in ('float', 'real'):
        return 'Float64'
    if kind == 'bit':
        return 'Bool'
    if kind == 'int':
        return 'Int32'
    if kind in ('tinyint', 'smallint'):
        return 'Int16'
    if kind == 'bigint':
        return 'Int64'
    if kind == 'date':
        return 'Date'
    if kind == 'time':
        return 'String'
    if kind in ('datetime', 'datetime2'):
        return 'DateTime64(3)'
    if kind == 'smalldatetime':
        return 'DateTime64(0)'
    if kind == 'datetimeoffset':
        return "DateTime64(3, 'UTC')"
    if kind in BYTE_TYPES:
        return 'String'
    return 'String'


def flink_type(data_type, length, precision, scale, datetime_precision):
    kind = data_type.lower()
    if kind in ('char', 'nchar', 'varchar', 'nvarchar'):
        if length is None or length < 0:
            return 'STRING'
        return f'{"CHAR" if kind in ("char", "nchar") else "VARCHAR"}({length})'
    if kind in ('text', 'ntext', 'xml', 'uniqueidentifier'):
        return 'STRING'
    if kind in ('decimal', 'money', 'smallmoney'):
        return f'DECIMAL({precision or 38},{scale or 0})'
    if kind == 'numeric':
        return f'NUMERIC({precision or 38},{scale or 0})'
    if kind in ('float', 'real'):
        return 'DOUBLE'
    if kind == 'bit':
        return 'BOOLEAN'
    if kind == 'int':
        return 'INT'
    if kind in ('tinyint', 'smallint'):
        return 'SMALLINT'
    if kind == 'bigint':
        return 'BIGINT'
    if kind == 'date':
        return 'DATE'
    if kind == 'time':
        return f'TIME({datetime_precision if datetime_precision is not None else 7})'
    if kind in ('datetime', 'datetime2'):
        precision = datetime_precision if datetime_precision is not None else 3
        return f'TIMESTAMP({precision})'
    if kind == 'smalldatetime':
        return 'TIMESTAMP(0)'
    if kind == 'datetimeoffset':
        return 'TIMESTAMP_LTZ(3)'
    if kind in BYTE_TYPES:
        return 'BYTES'
    return 'STRING'


def flink_sink_type(column):
    """Type of the column in the Kafka sink; timestamps travel as strings."""
    if column['data_type'].lower() in TEMPORAL_TYPES:
        return 'STRING'
    return flink_type(column['data_type'], column['length'], column['precision'],
                      column['scale'], column['datetime_precision'])


def clickhouse_transport_type(column):
    """ClickHouse type of the Kafka engine table, matching flink_sink_type."""
    sink_type = flink_sink_type(column)
    if sink_type in ('STRING', 'VARCHAR', 'CHAR', 'BYTES'):
        return 'String'
    if sink_type == 'BOOLEAN':
        return 'Bool'
    if sink_type == 'INT':
        return 'Int32'
    if sink_type == 'SMALLINT':
        return 'Int16'
    if sink_type == 'BIGINT':
        return 'Int64'
    if sink_type == 'DOUBLE':
        return 'Float64'
    if sink_type == 'DATE':
        return 'Date'
    if sink_type.startswith(('DECIMAL', 'NUMERIC')):
        return sink_type.replace('DECIMAL', 'Decimal').replace('NUMERIC', 'Decimal')
    return 'String'


def is_temporal(column):
    return column['data_type'].lower() in TEMPORAL_TYPES


def clickhouse_target_type(data_type, precision, scale, datetime_precision):
    """CDC payloads keep SQL NULLs, so datetime columns have to be Nullable:
    parseDateTime64BestEffortOrNull returns NULL and inserting it into a non
    Nullable DateTime64 fails the whole block."""
    target = clickhouse_type(data_type, precision, scale, datetime_precision)
    if data_type.lower() in TEMPORAL_TYPES:
        return f'Nullable({target})'
    return target


def clickhouse_extract_expression(column):
    """Unpacks one column from the 'after' object of a debezium-json payload."""
    path = f"msg, 'after', {sql_literal(column['name'])}"
    kind = column['data_type'].lower()
    if kind in ('char', 'nchar', 'varchar', 'nvarchar', 'text', 'ntext', 'xml',
                'uniqueidentifier', 'time'):
        return f'JSONExtractString({path})'
    if kind == 'bit':
        return f'JSONExtractBool({path})'
    if kind == 'int':
        return f'CAST(JSONExtractInt({path}) AS Int32)'
    if kind == 'smallint':
        return f'CAST(JSONExtractInt({path}) AS Int16)'
    if kind == 'bigint':
        return f'JSONExtractInt({path})'
    if kind in ('float', 'real'):
        return f'JSONExtractFloat({path})'
    if kind in ('decimal', 'numeric', 'money', 'smallmoney'):
        return f"CAST(JSONExtractString({path}) AS Decimal({column['precision'] or 38},{column['scale'] or 0}))"
    if kind == 'date':
        return f'toDate(JSONExtractString({path}))'
    if kind in TEMPORAL_TYPES:
        return f'parseDateTime64BestEffortOrNull(JSONExtractString({path}), 3)'
    if kind in BYTE_TYPES:
        return f'JSONExtractString({path})'
    return f'JSONExtractString({path})'


def connect(host, port, database, user, password):
    try:
        import pyodbc
    except ModuleNotFoundError:
        sys.exit('pyodbc is not installed. Run: pip install pyodbc')

    errors = []
    for driver in ('ODBC Driver 18 for SQL Server', 'ODBC Driver 17 for SQL Server', 'SQL Server'):
        connection_string = (
            f'DRIVER={{{driver}}};'
            f'SERVER={host},{port};DATABASE={database};'
            f'UID={user};PWD={password};'
            'Encrypt=yes;TrustServerCertificate=yes;'
        )
        try:
            return pyodbc.connect(connection_string, autocommit=True), pyodbc
        except pyodbc.Error as error:
            errors.append(f'{driver}: {error}')
    sys.exit('Could not connect to SQL Server.\n' + '\n'.join(errors))


def read_columns(cursor, database, schema, table):
    sql = """
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
               NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = ? AND TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """
    rows = cursor.execute(sql, database, schema, table).fetchall()
    if not rows:
        sys.exit(f'{database}.{schema}.{table} not found (or not visible to this login).')
    return [
        {
            'name': row[0],
            'data_type': row[1],
            'length': row[2],
            'precision': row[3],
            'scale': row[4],
            'datetime_precision': row[5],
            'nullable': row[6] == 'YES',
        }
        for row in rows
    ]


def read_primary_key(cursor, database, schema, table):
    sql = """
        SELECT c.name
        FROM sys.key_constraints AS kc
        INNER JOIN sys.index_columns AS ic
            ON ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id
        INNER JOIN sys.columns AS c
            ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE kc.type = 'PK'
          AND kc.parent_object_id = OBJECT_ID(?)
          AND SCHEMA_NAME(kc.schema_id) = ?
        ORDER BY ic.key_ordinal
    """
    qualified = f'[{database}].[{schema}].[{table}]'
    return [row[0] for row in cursor.execute(sql, qualified, schema).fetchall()]


def print_report(cursor, database, schema, table, columns, primary_key, sample):
    print(f'== {database}.{schema}.[{table}] ==')
    for column in columns:
        marks = []
        if not column['nullable']:
            marks.append('NOT NULL')
        if column['name'] in primary_key:
            marks.append('PK')
        suffix = (' ' + ', '.join(marks)) if marks else ''
        print(f"  {column['name']:<28} {column['data_type']}{suffix}")
    print(f"  primary key: {', '.join(primary_key) if primary_key else 'none'}")

    qualified = f'[{schema}].[{table}]'
    total = cursor.execute(f'SELECT COUNT_BIG(*) FROM {qualified}').fetchone()[0]
    print(f'  rows: {total}')

    if sample:
        print(f'  first {sample} rows:')
        for row in cursor.execute(f'SELECT TOP ({sample}) * FROM {qualified}').fetchall():
            print('   ', dict(zip((column['name'] for column in columns), row)))

    if not primary_key:
        print('  NOTE: no primary key. Upserts are not possible downstream and the '
              'CDC job needs scan.incremental.snapshot.chunk.key-column.')
    binary_columns = [column['name'] for column in columns if column['data_type'].lower() in BYTE_TYPES]
    if binary_columns:
        print('  NOTE: binary columns are written as ClickHouse String: ' + ', '.join(binary_columns))


def print_cdc_status(cursor, database, schema, table):
    """Flink's sqlserver-cdc source reads the SQL Server change tables, so the
    database and the table must be CDC enabled before the job can start."""
    print('\n== CDC prerequisites ==')
    try:
        row = cursor.execute(
            'SELECT is_cdc_enabled FROM sys.databases WHERE name = ?', database).fetchone()
        if row is None or not row[0]:
            print(f'  [ ] CDC is not enabled on database {database}')
            print('      run: EXEC sys.sp_cdc_enable_db;  (see mssql/init/003_enable_cdc.sql)')
        else:
            print(f'  [x] CDC enabled on database {database}')
    except Exception as error:
        print(f'  [?] could not read sys.databases: {error}')

    try:
        row = cursor.execute(
            'SELECT capture_instance FROM cdc.change_tables WHERE source_object_id = OBJECT_ID(?)',
            f'{schema}.{table}').fetchone()
        if row is None:
            print(f'  [ ] no capture instance for {schema}.{table}')
            print('      run mssql/init/003_enable_cdc.sql (sp_cdc_enable_table)')
        else:
            print(f'  [x] capture instance for {schema}.{table}: {row[0]}')
    except Exception as error:
        print(f'  [?] could not read cdc.change_tables: {error}')

    try:
        rows = cursor.execute(
            "SELECT status_desc FROM sys.dm_server_services "
            "WHERE servicename LIKE 'SQL Server Agent%'").fetchall()
        if rows:
            print(f'  [{"[x]" if "running" in rows[0][0].lower() else "[ ]"}] SQL Server Agent: {rows[0][0]}')
        else:
            print('  [?] SQL Server Agent status is not visible to this login')
    except Exception:
        print('  [?] SQL Server Agent status is not visible to this login')


def build_clickhouse_ddl(table_name, columns, primary_key, mode='cdc'):
    target = clickhouse_target_type if mode == 'cdc' else clickhouse_type
    definitions = ',\n'.join(
        f"    {quote(column['name'])} {target(column['data_type'], column['precision'], column['scale'], column['datetime_precision'])}"
        for column in columns
    )
    order_by = ', '.join(quote(name) for name in primary_key) or 'tuple()'
    return (
        'CREATE DATABASE IF NOT EXISTS fdp;\n\n'
        f'CREATE TABLE IF NOT EXISTS fdp.{table_name}\n'
        f'(\n{definitions}\n)\n'
        'ENGINE = ReplacingMergeTree\n'
        f'ORDER BY {order_by};\n'
    )


def kafka_settings(topic, group):
    return (
        'SETTINGS\n'
        "    kafka_broker_list = 'kafka:19092',\n"
        f'    kafka_topic_list = {sql_literal(topic)},\n'
        f'    kafka_group_name = {sql_literal(group)},\n'
    )


def build_clickhouse_streaming_ddl(table_name, topic, group, columns, mode='cdc'):
    """Flink's JDBC connector has no ClickHouse dialect, so every sink lands on Kafka
    and ClickHouse reads the topic with a Kafka engine table and a materialized view.
    CDC jobs use debezium-json (the only value format that can carry the update and
    delete changelog a CDC source produces), so the engine table keeps the raw
    message and the view unpacks the 'after' object. Delete events are filtered out."""
    if mode == 'cdc':
        projection = ',\n'.join(
            f'    {clickhouse_extract_expression(column)} AS {quote(column["name"])}'
            for column in columns
        )
        return (
            f'CREATE TABLE IF NOT EXISTS fdp.{table_name}_kafka\n'
            '(\n    msg String\n) ENGINE = Kafka\n'
            + kafka_settings(topic, group)
            + "    kafka_format = 'JSONAsString',\n"
            '    kafka_num_consumers = 1,\n'
            '    kafka_flush_interval_ms = 1000,\n'
            '    kafka_max_block_size = 1048576,\n'
            '    kafka_skip_broken_messages = 1;\n\n'
            f'CREATE MATERIALIZED VIEW IF NOT EXISTS fdp.mv_{table_name}\n'
            f'TO fdp.{table_name}\n'
            'AS\n'
            'SELECT\n'
            f'{projection}\n'
            f'FROM fdp.{table_name}_kafka\n'
            "WHERE JSONExtractString(msg, 'op') IN ('c', 'u', 'r');\n"
        )

    transport = ',\n'.join(
        f"    {quote(column['name'])} {clickhouse_transport_type(column)}" for column in columns
    )
    projection = ',\n'.join(
        '    {expression}'.format(
            expression=f'parseDateTime64BestEffortOrNull({quote(column["name"])}, 3) AS {quote(column["name"])}'
            if is_temporal(column) else quote(column['name'])
        )
        for column in columns
    )
    return (
        f'CREATE TABLE IF NOT EXISTS fdp.{table_name}_kafka\n'
        f'(\n{transport}\n) ENGINE = Kafka\n'
        + kafka_settings(topic, group)
        + "    kafka_format = 'JSONEachRow',\n"
        '    kafka_num_consumers = 1,\n'
        '    kafka_flush_interval_ms = 1000,\n'
        '    kafka_max_block_size = 1048576,\n'
        '    kafka_skip_broken_messages = 1;\n\n'
        f'CREATE MATERIALIZED VIEW IF NOT EXISTS fdp.mv_{table_name}\n'
        f'TO fdp.{table_name}\n'
        f'AS\nSELECT\n{projection}\nFROM fdp.{table_name}_kafka;\n'
    )


def build_source_ddl(mode, source_name, database, schema, table, host, port, user, password,
                     columns, primary_key, chunk_key):
    source_columns = ',\n'.join(
        '  {name} {type}{null}'.format(
            name=quote(column['name']),
            type=flink_type(column['data_type'], column['length'], column['precision'], column['scale'], column['datetime_precision']),
            null='' if column['nullable'] else ' NOT NULL',
        )
        for column in columns
    )

    if mode == 'batch':
        # One pass over the table, no SQL Server side prerequisites beyond SELECT.
        options = [
            "  'connector' = 'jdbc'",
            f"  'url' = 'jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=true'",
            f'  \'table-name\' = {sql_literal(f"{schema}.{table}")}',
            "  'driver' = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'",
            f'  \'username\' = {sql_literal(user)}',
            f'  \'password\' = {sql_literal(password)}',
        ]
        return f'CREATE TABLE {source_name} (\n{source_columns}\n) WITH (\n' + ',\n'.join(options) + '\n);\n'

    if primary_key:
        keys = ', '.join(quote(name) for name in primary_key)
        source_columns += f',\n  PRIMARY KEY ({keys}) NOT ENFORCED'

    options = [
        "  'connector' = 'sqlserver-cdc'",
        f'  \'hostname\' = {sql_literal(host)}',
        f"  'port' = '{port}'",
        f'  \'username\' = {sql_literal(user)}',
        f'  \'password\' = {sql_literal(password)}',
        f'  \'database-name\' = {sql_literal(database)}',
        f'  \'table-name\' = {sql_literal(f"{schema}.{table}")}',
        "  'scan.startup.mode' = 'initial'",
    ]
    if not primary_key and chunk_key:
        options.append(f'  \'scan.incremental.snapshot.chunk.key-column\' = {sql_literal(chunk_key)}')
    return f'CREATE TABLE {source_name} (\n{source_columns}\n) WITH (\n' + ',\n'.join(options) + '\n);\n'


def build_flink_job(job_name, topic, sink_name, source_name, database, schema, table,
                    host, port, user, password, columns, primary_key, chunk_key, mode='cdc'):
    sink_columns = ',\n'.join(
        '  {name} {type}'.format(name=quote(column['name']), type=flink_sink_type(column))
        for column in columns
    )
    projection = ',\n'.join(
        '  {expression}'.format(
            expression=f'CAST({quote(column["name"])} AS STRING)' if is_temporal(column) else quote(column['name'])
        )
        for column in columns
    )
    source = build_source_ddl(mode, source_name, database, schema, table, host, port, user,
                              password, columns, primary_key, chunk_key)

    if mode == 'batch':
        settings = (
            "-- One-shot read: the job finishes after one pass. Re-submit it to pick up\n"
            "-- newer rows.\n"
            "SET 'execution.runtime-mode' = 'BATCH';\n"
            "SET 'parallelism.default' = '2';\n\n"
        )
    else:
        settings = (
            'SET \'execution.checkpointing.interval\' = \'30 s\';\n'
            'SET \'execution.checkpointing.tolerable-failed-checkpoints\' = \'100\';\n'
            'SET \'restart-strategy\' = \'fixed-delay\';\n'
            'SET \'restart-strategy.fixed-delay.attempts\' = \'2147483647\';\n'
            'SET \'parallelism.default\' = \'2\';\n\n'
        )

    return (
        '-- Generated by mssql/tools/introspect_container.py. Contains the SQL Server\n'
        '-- password in clear text; it is gitignored, do not commit it.\n'
        '-- Submit with:\n'
        f'--   docker exec -it fdp-engine-flink-jobmanager /opt/flink/bin/sql-client.sh \\\n'
        f'--     -f /opt/flink/usrlib/sql/generated/{job_name}\n'
        '-- Create the ClickHouse table, Kafka engine table and materialized view first\n'
        '-- (generated in clickhouse/init), otherwise the sink fails.\n'
        + settings
        + source
        + '\n'
        f'CREATE TABLE {sink_name} (\n{sink_columns}\n) WITH (\n'
        "  'connector' = 'kafka',\n"
        f'  \'topic\' = {sql_literal(topic)},\n'
        "  'properties.bootstrap.servers' = 'kafka:19092',\n"
        # A CDC source emits update and delete rows, and a Kafka sink rejects
        # changelog input unless the value format can carry it. debezium-json is
        # the only format among these that can.
        + ("  'value.format' = 'debezium-json'\n" if mode == 'cdc' else "  'format' = 'json'\n")
        + ');\n\n'
        f'INSERT INTO {sink_name}\nSELECT\n{projection}\nFROM {source_name};\n'
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--host', default=os.environ.get('MSSQL_HOST', '192.168.1.83'))
    parser.add_argument('--port', default=os.environ.get('MSSQL_PORT', '1433'))
    parser.add_argument('--database', default=os.environ.get('MSSQL_DATABASE', 'OLTPSemi_55'))
    parser.add_argument('--schema', default=os.environ.get('MSSQL_SCHEMA', 'OLTP55_Schema'))
    parser.add_argument('--table', default=os.environ.get('MSSQL_TABLE', 'Container'))
    parser.add_argument('--sample', type=int, default=5, help='rows to print, 0 to skip')
    parser.add_argument('--mode', choices=('cdc', 'batch'), default='cdc',
                        help='cdc streams changes continuously and needs CDC enabled on the '
                             'table; batch reads the table once per submission and only needs SELECT')
    parser.add_argument('--write', action='store_true', help='write the generated SQL files')
    args = parser.parse_args()

    user = os.environ.get('MSSQL_USER')
    password = os.environ.get('MSSQL_PASSWORD')
    if not user or not password:
        sys.exit('Set MSSQL_USER and MSSQL_PASSWORD before running this script.')

    connection, _ = connect(args.host, args.port, args.database, user, password)
    try:
        cursor = connection.cursor()
        columns = read_columns(cursor, args.database, args.schema, args.table)
        primary_key = read_primary_key(cursor, args.database, args.schema, args.table)
        print_report(cursor, args.database, args.schema, args.table, columns, primary_key, args.sample)
        print_cdc_status(cursor, args.database, args.schema, args.table)

        if not args.write:
            print('\nRe-run with --write to generate the Flink job and ClickHouse table.')
            return

        slug = f"{args.schema}_{args.table}".lower()
        clickhouse_table = slug
        topic = slug
        suffix = '_cdc' if args.mode == 'cdc' else '_batch'
        job_name = f'{slug}{suffix}.sql'
        clickhouse_file = CLICKHOUSE_INIT_DIR / f'002_{slug}.sql'
        job_file = GENERATED_JOB_DIR / job_name

        chunk_key = next((column['name'] for column in columns if not column['nullable']), None)
        clickhouse_ddl = (
            build_clickhouse_ddl(clickhouse_table, columns, primary_key, args.mode)
            + '\n'
            + build_clickhouse_streaming_ddl(clickhouse_table, topic, f'fdp-{slug}', columns, args.mode)
        )
        job = build_flink_job(job_name, topic, f'{slug}_sink', f'{slug}_source',
                              args.database, args.schema, args.table, args.host, args.port,
                              user, password, columns, primary_key, chunk_key, args.mode)

        clickhouse_file.parent.mkdir(parents=True, exist_ok=True)
        GENERATED_JOB_DIR.mkdir(parents=True, exist_ok=True)
        clickhouse_file.write_text(clickhouse_ddl, encoding='utf-8')
        job_file.write_text(job, encoding='utf-8')

        print(f'\nWrote {clickhouse_file.relative_to(ROOT)}')
        print(f'Wrote {job_file.relative_to(ROOT)} (contains the SQL Server password)')
    finally:
        connection.close()


if __name__ == '__main__':
    main()