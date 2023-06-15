
function Export-XbTable {
<#
.Synopsis
    Wrapper function to invoke Start/Wait/Receive for a given table & time range.

.Parameter Parallel
    Specifies number of concurrent executions to allow. 
    Max 100. Defaults to 1 (one): *not* parallel.

.Parameter NoExecute
    Prints batch bounds to verbose stream but does not initiate archive command(s). 
    For validation & testing.

.Parameter Inclusive
    Adds an additional bound to the end of the timespan. Allows copy-paste from a
    KQL `summarize count() by bin(Timestamp,_step)` without needing to manually 
    increment to terminal bin by 1.
#>
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [string]
        $StorageAccountName,

        [Parameter(Mandatory)]
        [string]
        $Container,

        [Parameter()]
        [string]
        $LogFile,

        [Parameter(Mandatory)]
        [string]
        $ClusterUrl,

        [Parameter(Mandatory)]
        [string]
        $DatabaseName,

        [Parameter(Mandatory)]
        [string]
        $TableName,

        [Parameter(Mandatory)]
        [string]
        $TimestampColumnName,

        [Parameter(Mandatory)]
        [DateTimeOffset]
        $Start,

        [Parameter(Mandatory)]
        [DateTimeOffset]
        $End,

        [Parameter(Mandatory)]
        [timespan]
        $Step,

        [ValidateRange(1,100)]
        [int]
        $Parallel = 1,

        [ValidateRange(60,600)]
        [int]
        $SleepSeconds = 120,

        [switch]
        $NoExecute,

        [switch]
        $Inclusive
    )

    $DoExecute = -Not $NoExecute

    $receiveSplat = @{
        StorageAccountName = $StorageAccountName
        Container = $Container
        LogFile = $LogFile
    }

    $AdxTableSpec = @{
        ClusterUrl = $ClusterUrl
        DatabaseName = $DatabaseName
        TableName = $TableName
        TimestampColumnName = $TimestampColumnName
    }

    if($Inclusive){
        $End = $End.Add($Step)
    }

    $Bounds = New-XbBatchBounds -Start $Start -End $End -Step $Step
    if($Bounds.Count -lt 1){
        Write-Error "Count of batch boundaries must be at least 1, but was '$($Bounds.Count)'"
        return
    }
    $BatchCount = $Bounds.Count

    for($IndexStart = 0; $IndexStart -lt $BatchCount; $IndexStart += $Parallel) {
        $IndexEnd = $IndexStart + $Parallel - 1
        if($IndexEnd -ge $BatchCount){
            $IndexEnd = $BatchCount - 1
        }
        Write-Verbose "Initializing serial batch; IndexStart: '$IndexStart', IndexEnd: '$IndexEnd'"
        $Batches = $IndexStart .. $IndexEnd | ForEach-Object {
            $startStr = $Bounds[$_].Start
            $endStr   = $Bounds[$_].End
            $prefix   = $Bounds[$_].Label
            Write-Verbose "Initializing parallel batch; IndexPosition: '$_', Start: '$startStr', End: '$endStr'"
            if($DoExecute){
                $Operation = Start-XbAsyncArchive -Start $startStr -End $endStr @AdxTableSpec
                $Operation.Prefix = "${TimestampColumnName}=${prefix}"
                $Operation
            }
        }

        if($DoExecute){
            $Batches = Wait-XbAsyncArchive -ClusterUrl $ClusterUrl -DatabaseName $DatabaseName -Waiters $Batches -SleepSeconds $SleepSeconds

            $Batches | ForEach-Object {
                $_ | Receive-XbAsyncArchive @receiveSplat
            }
        }
    }
}
