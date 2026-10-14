CREATE DATABASE IF NOT EXISTS fdp;

CREATE TABLE IF NOT EXISTS fdp.oltp55_schema_employee_test
(
    AccessLevel Int32,
    AllowOverrideOfSessionValues Bool,
    CanLogin Bool,
    CDOTypeId Int32,
    ChangeCount Int32,
    ChangeStatusId String,
    Description String,
    DocManagerPassword String,
    DocManagerUser String,
    DomainName String,
    EmailAddress String,
    EmployeeId String,
    EmployeeLoginInfoId String,
    EmployeeName String,
    ESigRoleGroupId String,
    FilterTagAccess Int32,
    FilterTags String,
    FilterTagsSession String,
    FullName String,
    HistoryViewId String,
    IconId Int32,
    IsFrozen Bool,
    IsSuperUser Bool,
    LanguageDictionaryId String,
    LastWebLoginTimestamp Nullable(DateTime64(3)),
    LoginEmployeeGroupId String,
    MenuDefinitionId String,
    ModelerAccess Bool,
    Notes String,
    PortalMenuDefinitionId String,
    PortalMobileMenuDefinitionId String,
    PortalV8MenuDefinitionId String,
    PrimaryOrganizationId String,
    SessionValuesId String,
    SetupAccessId String,
    TerminologyDictionaryId String,
    TrainingPlanId String,
    UIPortalProfileId String,
    UserComment String,
    UserProfileId String,
    VerifySessionValues Bool,
    WebDrillDownMenuDefinitionId String,
    WebMenuDefinitionId String
)
ENGINE = ReplacingMergeTree
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS fdp.oltp55_schema_employee_test_kafka
(
    msg String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'oltp55_schema_employee_test',
    kafka_group_name = 'fdp-oltp55_schema_employee_test',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 1000,
    kafka_max_block_size = 1048576,
    kafka_skip_broken_messages = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS fdp.mv_oltp55_schema_employee_test
TO fdp.oltp55_schema_employee_test
AS
SELECT
    CAST(JSONExtractInt(msg, 'after', 'AccessLevel') AS Int32) AS AccessLevel,
    JSONExtractBool(msg, 'after', 'AllowOverrideOfSessionValues') AS AllowOverrideOfSessionValues,
    JSONExtractBool(msg, 'after', 'CanLogin') AS CanLogin,
    CAST(JSONExtractInt(msg, 'after', 'CDOTypeId') AS Int32) AS CDOTypeId,
    CAST(JSONExtractInt(msg, 'after', 'ChangeCount') AS Int32) AS ChangeCount,
    JSONExtractString(msg, 'after', 'ChangeStatusId') AS ChangeStatusId,
    JSONExtractString(msg, 'after', 'Description') AS Description,
    JSONExtractString(msg, 'after', 'DocManagerPassword') AS DocManagerPassword,
    JSONExtractString(msg, 'after', 'DocManagerUser') AS DocManagerUser,
    JSONExtractString(msg, 'after', 'DomainName') AS DomainName,
    JSONExtractString(msg, 'after', 'EmailAddress') AS EmailAddress,
    JSONExtractString(msg, 'after', 'EmployeeId') AS EmployeeId,
    JSONExtractString(msg, 'after', 'EmployeeLoginInfoId') AS EmployeeLoginInfoId,
    JSONExtractString(msg, 'after', 'EmployeeName') AS EmployeeName,
    JSONExtractString(msg, 'after', 'ESigRoleGroupId') AS ESigRoleGroupId,
    CAST(JSONExtractInt(msg, 'after', 'FilterTagAccess') AS Int32) AS FilterTagAccess,
    JSONExtractString(msg, 'after', 'FilterTags') AS FilterTags,
    JSONExtractString(msg, 'after', 'FilterTagsSession') AS FilterTagsSession,
    JSONExtractString(msg, 'after', 'FullName') AS FullName,
    JSONExtractString(msg, 'after', 'HistoryViewId') AS HistoryViewId,
    CAST(JSONExtractInt(msg, 'after', 'IconId') AS Int32) AS IconId,
    JSONExtractBool(msg, 'after', 'IsFrozen') AS IsFrozen,
    JSONExtractBool(msg, 'after', 'IsSuperUser') AS IsSuperUser,
    JSONExtractString(msg, 'after', 'LanguageDictionaryId') AS LanguageDictionaryId,
    parseDateTime64BestEffortOrNull(JSONExtractString(msg, 'after', 'LastWebLoginTimestamp'), 3) AS LastWebLoginTimestamp,
    JSONExtractString(msg, 'after', 'LoginEmployeeGroupId') AS LoginEmployeeGroupId,
    JSONExtractString(msg, 'after', 'MenuDefinitionId') AS MenuDefinitionId,
    JSONExtractBool(msg, 'after', 'ModelerAccess') AS ModelerAccess,
    JSONExtractString(msg, 'after', 'Notes') AS Notes,
    JSONExtractString(msg, 'after', 'PortalMenuDefinitionId') AS PortalMenuDefinitionId,
    JSONExtractString(msg, 'after', 'PortalMobileMenuDefinitionId') AS PortalMobileMenuDefinitionId,
    JSONExtractString(msg, 'after', 'PortalV8MenuDefinitionId') AS PortalV8MenuDefinitionId,
    JSONExtractString(msg, 'after', 'PrimaryOrganizationId') AS PrimaryOrganizationId,
    JSONExtractString(msg, 'after', 'SessionValuesId') AS SessionValuesId,
    JSONExtractString(msg, 'after', 'SetupAccessId') AS SetupAccessId,
    JSONExtractString(msg, 'after', 'TerminologyDictionaryId') AS TerminologyDictionaryId,
    JSONExtractString(msg, 'after', 'TrainingPlanId') AS TrainingPlanId,
    JSONExtractString(msg, 'after', 'UIPortalProfileId') AS UIPortalProfileId,
    JSONExtractString(msg, 'after', 'UserComment') AS UserComment,
    JSONExtractString(msg, 'after', 'UserProfileId') AS UserProfileId,
    JSONExtractBool(msg, 'after', 'VerifySessionValues') AS VerifySessionValues,
    JSONExtractString(msg, 'after', 'WebDrillDownMenuDefinitionId') AS WebDrillDownMenuDefinitionId,
    JSONExtractString(msg, 'after', 'WebMenuDefinitionId') AS WebMenuDefinitionId
FROM fdp.oltp55_schema_employee_test_kafka
WHERE JSONExtractString(msg, 'op') IN ('c', 'u', 'r');
