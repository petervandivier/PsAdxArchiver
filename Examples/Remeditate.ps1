
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

$tags.Start | Group-Object | Sort-Object name | Select-Object count,name

# if the offending batch(es) is (are) not found above, you may be able to
# remove all blobs where `.TagCount -eq 0`

$Blobs | Where-Object TagCount -eq 0 | Remove-AzStorageBlob

# Re-run the export for the batch(es) you just nuked then re-run validation
