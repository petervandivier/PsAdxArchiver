
function Receive-XbAsyncArchive {
    [CmdletBinding()]
    Param (
        [Parameter(Mandatory)]
        [guid]
        $OperationId,

        [Parameter(Mandatory)]
        [string]
        $StorageAccountName,

        [Parameter(Mandatory)]
        [string]
        $Container,

        [Parameter(Mandatory)]
        [string]
        $Prefix,

        [string]
        $LogFile,

        [Parameter(Mandatory)]
        [string]
        $ClusterUrl,

        [Parameter(Mandatory)]
        [string]
        $DatabaseName
    )

    $AdxConnection = @{
        ClusterUrl = $ClusterUrl
        DatabaseName = $DatabaseName
    }

    $operation = Invoke-AdxCmd @AdxConnection -Command ".show operations $OperationId"

    if(($operation).Count -ne 1){
        Write-Error "$(Get-Date -Format o): Expected exactly one operation, but got '$(($operation).Count)'. Aborting await"
        $operation
        return
    }

    $Context = New-AzStorageContext -StorageAccountName $StorageAccountName -UseConnectedAccount

    $Blobs = $Context | Get-AzStorageBlob -Container $Container | Where-Object { 
        $_.Name.StartsWith($Prefix)
    }

    $Aggregate = $Blobs | ForEach-Object {$_.Length} | Measure-Object -Sum 

    $receiver = [PsCustomObject]@{
        Timestamp   = Get-Date -Format o
        OperationId = $OperationId
        Duration    = $operation.Duration
        SizeBytes   = $Aggregate.Sum
        NumFiles    = $Aggregate.Count
    }

    if($LogFile){
        $receiver | Export-Csv $LogFile -Append
    }

    $receiver
}

