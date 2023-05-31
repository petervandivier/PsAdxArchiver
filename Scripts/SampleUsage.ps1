
Push-Location $PSScriptRoot

if($null -eq (Get-Module Invoke-AdxCmd -ListAvailable)){
    Write-Error "Module 'Invoke-AdxCmd' must be present in `$env:PsModulePath."
    return
}

Import-Module ../PsAdxArchiver.psd1

$StorageAccountName  = 'mystorageaccount'
$Container           = 'storm-events'
$LogFile             = '~/Desktop/export.log.csv'
$ClusterUrl          = 'https://help.kusto.windows.net;Fed=True'
$DatabaseName        = 'Samples'
$TableName           = 'StormEvents'
$TimestampColumnName = 'StartTime'
$Start               = '2007-01-01 00:00:00Z'
$End                 = '2008-01-01 00:00:00Z'
$Step                = [timespan]'1.00:00:00'
$Parallel            = 4

$exportSplat = @{
    StorageAccountName  = $StorageAccountName
    Container           = $Container
    LogFile             = $LogFile
    ClusterUrl          = $ClusterUrl
    DatabaseName        = $DatabaseName
    TableName           = $TableName
    TimestampColumnName = $TimestampColumnName
    Start               = $Start
    End                 = $End
    Step                = $Step
    Parallel            = $Parallel
}

Export-XbTable @exportSplat -Verbose

Pop-Location
