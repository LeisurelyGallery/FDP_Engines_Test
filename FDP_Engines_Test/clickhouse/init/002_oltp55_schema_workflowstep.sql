CREATE DATABASE IF NOT EXISTS fdp;

CREATE TABLE IF NOT EXISTS fdp.oltp55_schema_workflowstep
(
    CDOTypeId Int32,
    ChangeCount Int32,
    DefaultPathId String,
    Description String,
    ExportImportKey String,
    IconId Int32,
    IsFrozen Bool,
    IsLastStep Bool,
    Notes String,
    OnDefaultRoute Bool,
    RouteStepId String,
    SchedulingDetailId String,
    SchedulingRouteStepId String,
    Sequence Int32,
    SpecBaseId String,
    SpecId String,
    StepType Int32,
    SubWorkflowBaseId String,
    SubWorkflowId String,
    WIPMsgLabel String,
    WorkflowId String,
    WorkflowStepId String,
    WorkflowStepName String,
    Xlocation Int32,
    Ylocation Int32,
    isRouteStepName String,
    isSchdRouteStepName String
)
ENGINE = ReplacingMergeTree
ORDER BY WorkflowStepId;

CREATE TABLE IF NOT EXISTS fdp.oltp55_schema_workflowstep_kafka
(
    msg String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'oltp55_schema_workflowstep',
    kafka_group_name = 'fdp-oltp55_schema_workflowstep',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 1000,
    kafka_max_block_size = 1048576,
    kafka_skip_broken_messages = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS fdp.mv_oltp55_schema_workflowstep
TO fdp.oltp55_schema_workflowstep
AS
SELECT
    CAST(JSONExtractInt(msg, 'after', 'CDOTypeId') AS Int32) AS CDOTypeId,
    CAST(JSONExtractInt(msg, 'after', 'ChangeCount') AS Int32) AS ChangeCount,
    JSONExtractString(msg, 'after', 'DefaultPathId') AS DefaultPathId,
    JSONExtractString(msg, 'after', 'Description') AS Description,
    JSONExtractString(msg, 'after', 'ExportImportKey') AS ExportImportKey,
    CAST(JSONExtractInt(msg, 'after', 'IconId') AS Int32) AS IconId,
    JSONExtractBool(msg, 'after', 'IsFrozen') AS IsFrozen,
    JSONExtractBool(msg, 'after', 'IsLastStep') AS IsLastStep,
    JSONExtractString(msg, 'after', 'Notes') AS Notes,
    JSONExtractBool(msg, 'after', 'OnDefaultRoute') AS OnDefaultRoute,
    JSONExtractString(msg, 'after', 'RouteStepId') AS RouteStepId,
    JSONExtractString(msg, 'after', 'SchedulingDetailId') AS SchedulingDetailId,
    JSONExtractString(msg, 'after', 'SchedulingRouteStepId') AS SchedulingRouteStepId,
    CAST(JSONExtractInt(msg, 'after', 'Sequence') AS Int32) AS Sequence,
    JSONExtractString(msg, 'after', 'SpecBaseId') AS SpecBaseId,
    JSONExtractString(msg, 'after', 'SpecId') AS SpecId,
    CAST(JSONExtractInt(msg, 'after', 'StepType') AS Int32) AS StepType,
    JSONExtractString(msg, 'after', 'SubWorkflowBaseId') AS SubWorkflowBaseId,
    JSONExtractString(msg, 'after', 'SubWorkflowId') AS SubWorkflowId,
    JSONExtractString(msg, 'after', 'WIPMsgLabel') AS WIPMsgLabel,
    JSONExtractString(msg, 'after', 'WorkflowId') AS WorkflowId,
    JSONExtractString(msg, 'after', 'WorkflowStepId') AS WorkflowStepId,
    JSONExtractString(msg, 'after', 'WorkflowStepName') AS WorkflowStepName,
    CAST(JSONExtractInt(msg, 'after', 'Xlocation') AS Int32) AS Xlocation,
    CAST(JSONExtractInt(msg, 'after', 'Ylocation') AS Int32) AS Ylocation,
    JSONExtractString(msg, 'after', 'isRouteStepName') AS isRouteStepName,
    JSONExtractString(msg, 'after', 'isSchdRouteStepName') AS isSchdRouteStepName
FROM fdp.oltp55_schema_workflowstep_kafka
WHERE JSONExtractString(msg, 'op') IN ('c', 'u', 'r');
