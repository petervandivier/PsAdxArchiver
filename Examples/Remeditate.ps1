
<#
.Synopsis
    Identify & remove blobs from a malformed export

.Description
    When validating your export, if you notice rowcount mismatch,
    retrieve all blobs by prefix for the offending range. If you 
    don't want to nuke & re-export the whole range, you can further 
    filter by blob tags (assuming the range was subdivided). If you 
    able to identify an offending subdivision, remove only blobs
    from that sub-batch; filtering by tag properties (or absence of
    tags if the `Receive` step was skipped or cancelled)
#>

$StorageAccountName = 'mystorageaccount'
$Container = 'storm-events'
$TimestampColumnName = 'StartTime'

$Context = New-AzStorageContext -StorageAccountName $StorageAccountName -UseConnectedAccount

$Blobs = $Context | Get-AzStorageBlob -Container $Container -Prefix "$TimestampColumnName=2007-06-01"

$tags = $Blobs | ForEach-Object { 
    $blob = $_
    $Context = $blob.Context
    $Name = $blob.Name
    $obj = $blob | Get-AzStorageBlobTag -ErrorAction SilentlyContinue
    if($obj.Count -gt 0){
        $obj | Select-Object *,@{l='context';e={$Context}},@{l='name';e={$Name}}
    }
}

# Sometimes you just nuke the whole day and re-run
# even if not, untagged blobs are pretty useless for debugging
# fastest fix is to nuke all untagged blobs and fill in the gaps
$Blobs | Where-Object TagCount -eq 0 | Remove-AzStorageBlob

# inventory what you actually _do_ have tagged and recheck counts
$tags | ForEach-Object {
    $_.Start + " - " + $_.End
} | Group-Object | Sort-Object name | Select-Object count,name

# Re-run the export for the batch(es) you just nuked then re-run validation
