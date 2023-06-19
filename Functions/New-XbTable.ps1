
function New-XbTable {
<#
.Synopsis
    Given an existing ADX Table & Storage Account, create the external table.

.Link
    https://learn.microsoft.com/en-us/azure/data-explorer/kusto/management/external-tables-azurestorage-azuredatalake

.Parameter NoDeploy
    Only print the KQL `create external table...` command, do not execute the command.
#>
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [string]
        $ResourceGroupName,

        [Parameter(Mandatory)]
        [string]
        $StorageAccountName,

        [Parameter(Mandatory)]
        [string]
        $Container,

        [Parameter(Mandatory)]
        [ValidateScript({$_.EndsWith(';Fed=True')})]
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

        [switch]
        $NoDeploy
    )

    $Keys = Get-AzStorageAccountKey -ResourceGroupName $ResourceGroupName -Name $StorageAccountName
    # TODO: ¿is it possible to `catch [Microsoft.Rest.Azure.CloudException], [Microsoft.Azure.Commands.Management.Storage.GetAzureStorageAccountKeyCommand]`?
    if($Keys){
        $AccessKey = $Keys[0].Value
    }

    if([string]::IsNullOrEmpty($AccessKey)){
        Write-Error "AccessKey could not be retrived for Storage Account '$StorageAccountName' in Resource Group '$ResourceGroupName'. Do you need to activate PIM?"
    }

    $Connection = @{
        ClusterUrl = $ClusterUrl
        DatabaseName = $DatabaseName
    }

    $TableSchema = Invoke-AdxCmd @Connection -Command ".show table ['$TableName'] cslschema"

    $TableSchema.TableName = "ext${TableName}"
    $TableSchema.Folder = $null
    $TableSchema.DocString = $null

    $TableDdl = ConvertTo-AdxCreateTableCmd $TableSchema
    $TableDdl = $TableDdl.Replace('.create-merge table','.create external table')
    $TableDdl += @(
        ""
        "kind = blob "
        "partitionby (${TimestampColumnName}:datetime = startofday($TimestampColumnName))"
        "pathformat = (`"$TimestampColumnName=`" datetime_pattern(`"yyyyMMdd`", $TimestampColumnName))"
        "dataformat = parquet "
        "("
        "    h@'https://${StorageAccountName}.blob.core.windows.net/${Container}/;${AccessKey}' " 
        ") "
        "with ( "
        "    compressed = true,"
        "    folder = 'External',"
        "    docstring = 'archive copy of ${TableName}'"
        ")"
    ) -join "`n"

    if($NoDeploy){
        $TableDdl 
    } else {
        Invoke-AdxCmd @Connection -Command $TableDdl
    }
}
