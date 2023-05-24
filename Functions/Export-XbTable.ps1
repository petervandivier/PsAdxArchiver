
function Export-XbTable {
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
        $Step
    )

    $days = New-XbBatchBounds -Start $Start -End $End -Step $Step

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

    0 .. ($days.Count - 1) | ForEach-Object {
        $start  = $days[$_].ToString()
        $end    = $days[$_ + 1].ToString()
        $prefix = "start=$($days[$_].ToString("yyyy-MM-dd"))"

        $Operation = (
            Start-XbAsyncArchive `
                -Start $start `
                -End $end `
                @AdxTableSpec
        ).ExecutionResults
        $OperationId = $Operation.OperationId

        Wait-XbAsyncArchive `
            -OperationId $OperationId `
            -Start $start `
            -End $end `
            -ClusterUrl $ClusterUrl `
            -DatabaseName $DatabaseName

        Receive-XbAsyncArchive `
            -OperationId $OperationId `
            -Prefix $prefix `
            @receiveSplat
    }
}
