#Requires -Modules PsAdxArchiver

<#
.Description
    For un-tagged uploads when there's non-contiguous partial-day failures,
    run this script to get an hourly breakdown for each day to spot check if there's
    a simple upload-only fix
#>

$days = @(
    '2007-04-01'
    '2007-04-03'
    '2007-12-10'
)

$bounds = @()
foreach($startOfDay in $days){
    $endOfDay = ([datetime]$startOfDay).AddDays(1).ToString('yyyy-MM-dd')
    $bounds += New-XbBatchBounds -Start $startOfDay -End $endOfDay -Step "1.00:00:00"
}

$Connection = @{
    ClusterUrl = 'https://help.kusto.windows.net;Fed=True'
    DatabaseName = 'Samples'
}

$TableName           = 'StormEvents'
$TimestampColumnName = 'StartTime'

$auditQueryTemplate = @"
let _start = datetime({0});
let _end   = datetime({1});
let _target = (
    external_table('ext${TableName}')
    | where $TimestampColumnName >= _start
    | where $TimestampColumnName <  _end
    | summarize TargetCount = count() by bin($TimestampColumnName,1h)
    | order by $TimestampColumnName
);
let _source = (
    $TableName
    | where $TimestampColumnName >= _start
    | where $TimestampColumnName <  _end
    | summarize SourceCount = count() by bin($TimestampColumnName,1h)
    | order by $TimestampColumnName
);
_source
| join kind=fullouter _target on $TimestampColumnName
| project-away *1
| extend match = SourceCount == TargetCount 
"@

$rowcounts = $bounds | ForEach-Object {
    $startStr = $_.Start
    $endStr = $_.End
    $query = $auditQueryTemplate -f $startStr, $endStr

    $queryStart = Get-Date
    Write-Host "$(Get-Date $queryStart -Format o) - polling $startStr"
    $result = Invoke-AdxCmd @Connection -Query $query
    $duration = (Get-Date) - $queryStart
    foreach($row in $result){
        [PsCustomObject]@{
            Duration             = $duration
            Date                 = $startStr.TrimEnd('Z')
            $TimestampColumnName = $row.$TimestampColumnName
            SourceCount          = $row.SourceCount
            TargetCount          = $row.TargetCount
            Match                = $row.match
        }
    }
} | Sort-Object -Property $TimestampColumnName

$rowcounts | Export-Csv "rowcounts-${TableName}-drilldown-$(Get-Date -Format FileDateTimeUniversal).csv"

$rowcounts | ConvertTo-Csv -Delimiter "`t" | clip

$rowcounts | Format-Table
