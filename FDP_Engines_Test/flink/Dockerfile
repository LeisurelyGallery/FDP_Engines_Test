FROM flink:1.20.1-scala_2.12-java17

USER root
RUN wget -q -O /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar \
 && wget -q -O /opt/flink/lib/flink-connector-jdbc-3.2.0-1.19.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar \
 && wget -q -O /opt/flink/lib/clickhouse-jdbc-0.7.2.jar \
    https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.7.2/clickhouse-jdbc-0.7.2.jar \
  && wget -q -O /opt/flink/lib/mssql-jdbc-12.10.2.jre11.jar \
    https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.10.2.jre11/mssql-jdbc-12.10.2.jre11.jar \
  && wget -q -O /opt/flink/lib/flink-sql-connector-sqlserver-cdc-3.6.0-1.20.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-sqlserver-cdc/3.6.0-1.20/flink-sql-connector-sqlserver-cdc-3.6.0-1.20.jar
USER flink
=======
FROM flink:1.20.1-scala_2.12-java17

USER root
RUN wget -q -O /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar \
 && wget -q -O /opt/flink/lib/flink-connector-jdbc-3.2.0-1.19.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar \
 && wget -q -O /opt/flink/lib/clickhouse-jdbc-0.7.2.jar \
    https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.7.2/clickhouse-jdbc-0.7.2.jar
USER flink
