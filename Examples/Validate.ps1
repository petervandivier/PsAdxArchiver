
<#
.Synopsis
    After a large export, check rowcounts of the `external_table()`

.Description
    Reading & batching from external storage may be prohibitively
    expensive for certain validation queries after export. Batching
    the queries and running them from powershell may save signficant
    time & heartache. Compare the base & external table rowcounts in 
    Excel.
#>

$Start               = '2007-01-01'
$End                 = '2008-01-01'
$TableName           = 'StormEvents'
$TimestampColumnName = 'StartTime'

$bounds = New-XbBatchBounds -Start $Start -End $End -Step "01:00:00"

$AdxDatabase = @{
    ClusterUrl = 'https://help.kusto.windows.net;Fed=True'
    DatabaseName = 'Samples'
}

$baseTableRowCount = Invoke-AdxCmd @AdxDatabase -Query @"
$TableName
| where $TimestampColumnName >= datetime($Start)
| where $TimestampColumnName <  datetime($End)
| summarize count()
"@

$externalTableRowcounts = $bounds | ForEach-Object {
    $startStr = $_.Start
    $endStr = $_.End
    $query = @(
        "external_table('ext$TableName')"
        "| where $TimestampColumnName >= datetime($startStr)"
        "| where $TimestampColumnName <  datetime($endStr)"
        "| summarize count()"
    ) -join "`n"

    $queryStart = Get-Date
    Write-Host "$(Get-Date $queryStart -Format o) - polling $startStr"
    $result = Invoke-AdxCmd @AdxDatabase -Query $query
    $duration = (Get-Date) - $queryStart
    [PsCustomObject]@{
        Duration = $duration
        Timestamp = $startStr.TrimEnd('Z')
        Count = $result.count_
    }
} | Sort-Object -Property Timestamp

$externalTableRowcounts

$externalTableRowcounts | ConvertTo-Csv -Delimiter "`t" | clip
$baseTableRowCount | ConvertTo-Csv -Delimiter "`t" | clip
