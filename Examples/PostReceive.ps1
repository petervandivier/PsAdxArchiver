
<#
.Synopsis
    Manually receive after a crash if you still know OpId & bounds

.Description
    Adds Tags to blobs that were successfully exported after the fact.
    Makes it easier to nuke un-tagged blobs from batches you didn't mean
    to export but started exporting before you hit cancel.
#>

Push-Location $PSScriptRoot/..

Import-Module ./PsAdxArchiver.psd1 -Force

# need to manually import. PS1 classes aren't portable
. ./Classes/XbAsyncExportWaiter.ps1

$ops = @"
OperationId,StartStr,EndStr
00000000-0000-0000-0000-000000000000,2007-06-01,2007-06-02
"@ | ConvertFrom-Csv

$StorageAccountName = 'mystorageaccount'
$Container = 'storm-events'
$LogFile = Resolve-Path "~/Desktop/export.log.csv"
$ClusterUrl = 'https://help.kusto.windows.net;Fed=True'
$DatabaseName = 'Samples'
$TableName = 'StormEvents'
$TimestampColumnName = 'StartTime'

$waiters =  $ops | ForEach-Object {
    [XbAsyncExportWaiter][PSCustomObject]@{
        Start = $_.StartStr
        End = $_.EndStr
        ClusterUrl = $ClusterUrl
        DatabaseName = $DatabaseName
        TableName = $TableName
        TimestampColumnName = $TimestampColumnName
        Prefix = "$TimestampColumnName=2007-06-01"
        OperationId = $_.OperationId
    }
}

$waiters | ForEach-Object {
    Receive-XbAsyncArchive `
        -Waiter $_ `
        -StorageAccountName $StorageAccountName `
        -Container $Container `
        -LogFile $LogFile 
}

Pop-Location
