
function Receive-XbAsyncArchive {
    [CmdletBinding()]
    Param (
        [Parameter(Mandatory,ValueFromPipeline)]
        [XbAsyncExportWaiter]
        $Waiter,

        [Parameter(Mandatory)]
        [string]
        $StorageAccountName,

        [Parameter(Mandatory)]
        [string]
        $Container,

        [string]
        $LogFile
    )

    $AdxConnection = @{
        ClusterUrl = $Waiter.ClusterUrl
        DatabaseName = $Waiter.DatabaseName
    }

    $ReceiveWaiterKql = ".show operation $($Waiter.OperationId) details"

    $Result = Invoke-AdxCmd @AdxConnection -Command $ReceiveWaiterKql

    $Context = New-AzStorageContext -StorageAccountName $StorageAccountName -UseConnectedAccount

    $Blobs = $Context | Get-AzStorageBlob -Container $Container | Where-Object { 
        $_.Name -in $Result.Path
    }

    foreach($blob in $Blobs) {
        $Tags = @{
            OperationId = $Waiter.OperationId
            Start = $Waiter.Start.ToString('u')
            End = $Waiter.End.ToString('u')
        }
        Set-AzStorageBlobTag -Tag $Tags -Container $Container -Blob $blob.Name -Context $Context
    }

    $Aggregate = $Blobs | ForEach-Object {$_.Length} | Measure-Object -Sum 

    $Waiter.Timestamp = [DateTimeOffset]::Now
    $Waiter.SizeBytes = $Aggregate.Sum
    $Waiter.NumFiles  = $Aggregate.Count

    if($LogFile){
        $Waiter | Export-Csv $LogFile -Append
    }

    $Waiter
}
