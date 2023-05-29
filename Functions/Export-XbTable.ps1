
function Export-XbTable {
<#
.Synopsis
    Wrapper function to invoke Start/Wait/Receive for a given table & time range.

.Parameter Parallel
    Specifies number of concurrent executions to allow. 
    Max 100. Defaults to 1 (one): *not* parallel.
#>
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [string]
        $StorageAccountName,

        [Parameter(Mandatory)]
        [string]
        $Container,

        [Parameter(Mandatory)]
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
        [datetime]
        $Start,

        [Parameter(Mandatory)]
        [datetime]
        $End,

        [Parameter(Mandatory)]
        [timespan]
        $Step,

        [ValidateRange(1,100)]
        [int]
        $Parallel = 1
    )

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

    $Days = New-XbBatchBounds -Start $Start -End $End -Step $Step
    if($Days.Count -lt 2){
        Write-Error "Count of batch boundaries must be at least 2, but was '$($Days.Count)'"
    }
    $BatchCount = $Days.Count - 1

    for($IndexStart = 0; $IndexStart -lt $BatchCount; $IndexStart += $Parallel) {
        $IndexEnd = $IndexStart + $Parallel - 1
        if($IndexEnd -ge $BatchCount){
            $IndexEnd = $BatchCount - 1
        }
        Write-Verbose "Initializing serial batch; IndexStart: '$IndexStart', IndexEnd: '$IndexEnd'"
        $Batches = $IndexStart .. $IndexEnd | ForEach-Object {
            $Start = Get-Date $Days[$_]     -Format "yyyy-MM-dd hh:mm:ss"
            $End   = Get-Date $Days[$_ + 1] -Format "yyyy-MM-dd hh:mm:ss"
            Write-Verbose "Initializing parallel batch; IndexPosition: '$_', Start: '$Start', End: '$End'"
            $Operation = Start-XbAsyncArchive -Start $Start -End $End @AdxTableSpec
            $Operation.Prefix = "start=$($Days[$_].ToString("yyyy-MM-dd"))"
            $Operation
        }

        $Batches = Wait-XbAsyncArchive -ClusterUrl $ClusterUrl -DatabaseName $DatabaseName -Waiters $Batches

        $Batches | ForEach-Object {
            $_ | Receive-XbAsyncArchive @receiveSplat
        }
    }
}
