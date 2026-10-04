<#
.SYNOPSIS
    Runs the whole SQL Server -> Kafka -> ClickHouse pipeline for one table.

.DESCRIPTION
    Creates the Kafka topic if needed, applies the ClickHouse DDL, submits the
    Flink job and then shows the job state and the row count. Written for
    PowerShell, which has no `<` input redirection (that is bash syntax and fails
    with "The '<' operator is reserved for future use").

.EXAMPLE
    ./mssql/run_pipeline.ps1 -Slug oltp55_schema_container -CreateTopic

.EXAMPLE
    ./mssql/run_pipeline.ps1 -Slug oltp55_schema_container -Submit
#>
[CmdletBinding()]
param(
    # Lower case <schema>_<table>, the name the generator derives from the source table.
    [Parameter(Mandatory = $true)]
    [string]$Slug,

    # Topic name. Defaults to the slug, which is what the generated job uses.
    [string]$Topic,

    # Add the topic to kafka/topics.txt and run kafka-init.
    [switch]$CreateTopic,

    # Apply clickhouse/init/00X_<slug>.sql to the running container.
    [switch]$ApplyDdl,

    # Submit flink/sql/<job file> through the Flink SQL client.
    [switch]$Submit,

    # Flink job file, relative to flink/sql.
    [string]$JobFile = "$Slug`_cdc.sql"
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
if (-not $Topic) { $Topic = $Slug }

function Invoke-Step {
    param([string]$Title, [scriptblock]$Action)
    Write-Host "`n== $Title" -ForegroundColor Cyan
    # Native tools such as docker compose write progress to stderr. With
    # ErrorActionPreference 'Stop' that aborts the script, so relax it here and
    # rely on the exit code instead.
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & $Action 2>&1 | ForEach-Object { Write-Host $_ }
    } finally {
        $ErrorActionPreference = $previous
    }
    if ($LASTEXITCODE -ne 0 -and $null -ne $LASTEXITCODE) {
        throw "$Title failed with exit code $LASTEXITCODE"
    }
}

if ($CreateTopic) {
    $topicsFile = Join-Path $root 'kafka\topics.txt'
    $existing = @(Get-Content $topicsFile)
    if ($existing -notcontains $Topic) {
        Add-Content -Path $topicsFile -Value $Topic
        Write-Host "added $Topic to kafka/topics.txt"
    }
    Invoke-Step 'Creating topics' { docker compose up -d --force-recreate kafka-init }
    Start-Sleep -Seconds 5
    docker exec fdp-engine-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list
}

if ($ApplyDdl) {
    $ddl = Get-ChildItem (Join-Path $root 'clickhouse\init') -Filter "*_$Slug.sql" |
        Select-Object -First 1
    if (-not $ddl) { throw "No clickhouse/init file matches *_$Slug.sql" }
    Invoke-Step "Applying $($ddl.Name)" {
        Get-Content -Raw $ddl.FullName | docker exec -i fdp-engine-clickhouse clickhouse-client --multiquery
    }
    docker exec fdp-engine-clickhouse clickhouse-client --query "SELECT name, engine FROM system.tables WHERE database='fdp' AND name LIKE '%$($Slug -replace '^oltp55_schema_','%')%' ORDER BY name"
}

if ($Submit) {
    $job = Join-Path $root "flink\sql\$JobFile"
    if (-not (Test-Path $job)) { throw "No job file at $job" }
    Invoke-Step "Submitting $JobFile" {
        docker exec fdp-engine-flink-jobmanager /opt/flink/bin/sql-client.sh -f "/opt/flink/usrlib/sql/$JobFile"
    }
    Start-Sleep -Seconds 10
}

Write-Host "`n== Flink jobs" -ForegroundColor Cyan
$jobs = (Invoke-WebRequest -UseBasicParsing http://localhost:8081/jobs/overview).Content | ConvertFrom-Json
$jobs.jobs | Select-Object @{n = 'job'; e = { $_.jid } }, state, name | Format-Table -AutoSize

Write-Host "== ClickHouse row counts" -ForegroundColor Cyan
docker exec fdp-engine-clickhouse clickhouse-client --query "SELECT '$Slug' AS table, count() AS rows FROM fdp.$Slug"