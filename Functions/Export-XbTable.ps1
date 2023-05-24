
function Export-XbTable {
    [CmdletBinding()]
    param (
        [Parameter()]
        [string]
        $StorageAccountName,

        [string]
        $Container,

        [string]
        $LogFile,

        [Parameter(Mandatory)]
        [datetime]
        $Start,

        [Parameter(Mandatory)]
        [datetime]
        $End,

        [Parameter(Mandatory)]
        [timespan]
        $Step
    )

    $days = New-XbBatchBounds -Start $Start -End $End -Step $Step

    $receiveSplat = @{
        StorageAccountName = $StorageAccountName
        Container = $Container
        LogFile = $LogFile
    }
    
    0 .. ($days.Count - 1) | ForEach-Object {
        $start  = $days[$_].ToString()
        $end    = $days[$_ + 1].ToString()
        $prefix = "start=$($days[$_].ToString("yyyy-MM-dd"))"

        $Operation = (Start-XbAsyncArchive -Start $start -End $end).ExecutionResults
        $OperationId = $Operation.OperationId

        Wait-XbAsyncArchive -OperationId $OperationId -Start $start -End $end

        Receive-XbAsyncArchive -OperationId $OperationId -Prefix $prefix @receiveSplat
    }
}
