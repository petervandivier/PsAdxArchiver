
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

    $ResultBlobs = $Result | ForEach-Object {
        # alt method
        # [System.Web.HttpUtility]::UrlDecode($_.Substring($_.IndexOf($Container)+$Container.Length+1))
        [PsCustomObject]@{
            Name = "$($Waiter.Prefix)/$($_.Path.Substring($_.Path.LastIndexOf('/')+1))"
            RowCount = $_.NumRecords
            SizeInBytes = $_.SizeInBytes
        }
    }

    $Context = New-AzStorageContext -StorageAccountName $StorageAccountName -UseConnectedAccount

    $Blobs = $Context | Get-AzStorageBlob -Container $Container | Where-Object { 
        $_.Name -in $ResultBlobs.Name
    }

    foreach($blob in $Blobs) {
        $ResultBlob = $ResultBlobs | Where-Object Name -eq $blob.Name
        $Tags = @{
            OperationId = $Waiter.OperationId.ToString()
            Start = $Waiter.Start.ToString('u')
            End = $Waiter.End.ToString('u')
            RowCount = $ResultBlob.RowCount.ToString()
            SizeInBytes = $ResultBlob.SizeInBytes.ToString()
        }
        Set-AzStorageBlobTag -Tag $Tags -Container $Container -Blob $blob.Name -Context $Context | Out-Null
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
