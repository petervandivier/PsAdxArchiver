
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
        $LogFile,

        $VerbosePreference = $PSCmdlet.GetVariableValue('VerbosePreference')
    )

    $Context = New-AzStorageContext -StorageAccountName $StorageAccountName -UseConnectedAccount

    $Blobs = $Context | Get-AzStorageBlob -Container $Container | Where-Object { 
        $_.Name.StartsWith($Waiter.Prefix)
    }

    $Aggregate = $Blobs | ForEach-Object {$_.Length} | Measure-Object -Sum 

    $Waiter.Timestamp = Get-Date -Format u
    $Waiter.SizeBytes = $Aggregate.Sum
    $Waiter.NumFiles  = $Aggregate.Count

    if($LogFile){
        $Waiter | Export-Csv $LogFile -Append
    }

    $Waiter
}

