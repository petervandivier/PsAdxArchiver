
function Receive-XbAsyncArchive {
<#
.Parameter NoTag
    Do not tag blobs with metadata. If you know your blobs cannot receive tags but do not
    otherwise meet the criteria to abort the tag flow, use this switch to suppress errors.
#>
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
        $LogFile,

        [switch]
        $NoTag
    )

    $AdxConnection = @{
        ClusterUrl = $Waiter.ClusterUrl
        DatabaseName = $Waiter.DatabaseName
    }

    $ReceiveWaiterKql = ".show operation $($Waiter.OperationId) details"

    $Result = Invoke-AdxCmd @AdxConnection -Command $ReceiveWaiterKql

    if($LogFile){
        $Result | Export-Csv "$LogFile-blobs.csv" -Append
    }

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

    $StorageAccount = Get-AzStorageAccount | Where-Object StorageAccountName -eq $StorageAccountName

    $Blobs = $Context | Get-AzStorageBlob -Container $Container -Prefix $Waiter.Prefix | Where-Object { 
        $_.Name -in $ResultBlobs.Name
    }

    if($NoTag) {
        Write-Verbose "Skipping tagging by user request."
    } else {
        foreach($blob in $Blobs) {
            $ResultBlob = $ResultBlobs | Where-Object Name -eq $blob.Name
            $Tags = @{
                OperationId = $Waiter.OperationId.ToString()
                Start = $Waiter.Start.ToString('u')
                End = $Waiter.End.ToString('u')
                RowCount = $ResultBlob.RowCount.ToString()
                SizeInBytes = $ResultBlob.SizeInBytes.ToString()
            }
            if(
                $Waiter.Prefix.Contains("/") -Or
                $true -eq $StorageAccount.EnableHierarchicalNamespace
            ){
                Write-Warning "Blob API is not yet supported for hierarchical namespace accounts. Skipping tagging."
            }else{
                Set-AzStorageBlobTag -Tag $Tags -Container $Container -Blob $blob.Name -Context $Context | Out-Null
            }
        }
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
