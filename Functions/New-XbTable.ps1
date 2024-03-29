
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

        [Parameter()]
        [string]
        $Directory,

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

        [Parameter()]
        [ValidateNotNullOrEmpty()]
        [string]
        $ExternalTableName,

        [Parameter(Mandatory)]
        [string]
        $TimestampColumnName,

        [Parameter()]
        [ValidateNotNullOrEmpty()]
        [string]
        $PathFormatColumnName,

        [ValidateSet('second','millisecond')]
        [AllowNull()]
        [string]
        $UnixTime,

        [Alias('TextOnly','DdlOnly','AsText')]
        [switch]
        $NoDeploy
    )

    # if StorageAccount not exists
    # if Container not exists

    $Keys = Get-AzStorageAccountKey -ResourceGroupName $ResourceGroupName -Name $StorageAccountName
    # TODO: ¿is it possible to `catch [Microsoft.Rest.Azure.CloudException], [Microsoft.Azure.Commands.Management.Storage.GetAzureStorageAccountKeyCommand]`?
    if($Keys){
        $AccessKey = $Keys[0].Value
    }

    if([string]::IsNullOrEmpty($AccessKey)){
        Write-Error "AccessKey could not be retrived for Storage Account '$StorageAccountName' in Resource Group '$ResourceGroupName'. Do you need to activate PIM?"
        $AccessKey = '******'
    }

    $Connection = @{
        ClusterUrl = $ClusterUrl
        DatabaseName = $DatabaseName
    }

    $TableSchema = Invoke-AdxCmd @Connection -Command ".show table ['$TableName'] cslschema"

    if($PsBoundParameters.Keys -contains 'ExternalTableName'){
        $TableSchema.TableName = $ExternalTableName
    } else {
        $TableSchema.TableName = "ext${TableName}"
    }
    
    $TableSchema.Folder = $null
    $TableSchema.DocString = $null

    $TableDdl = ConvertTo-AdxCreateTableCmd $TableSchema
    $TableDdl = $TableDdl.Replace('.create-merge table','.create external table')

    if(-Not $PsBoundParameters.Keys.Contains('PathFormatColumnName')){
        $PathFormatColumnName = $TimestampColumnName
    }
    if([string]::IsNullOrEmpty($UnixTime)){
        $PartitionBy = "partition by (${TimestampColumnName}:datetime = startofday($TimestampColumnName))"
        $PathFormat = "pathformat = (`"${PathFormatColumnName}=`" datetime_pattern(`"yyyy-MM-dd`", ${PathFormatColumnName}))"
    } else {
        $TableDdl = $TableDdl.Replace("table ext${TableName} (","table ext${TableName} (`n    ${TimestampColumnName}_DT: datetime,")
        $PartitionBy = "partition by (${TimestampColumnName}_DT:datetime = startofday(${TimestampColumnName}_DT))"
        $PathFormat = "pathformat = (`"${PathFormatColumnName}_DT=`" datetime_pattern(`"yyyy-MM-dd`", ${PathFormatColumnName}_DT))"
    }

    if([string]::IsNullOrEmpty($Directory)){
        $UriPath = $Container
    } else {
        $UriPath = "$Container/$Directory"
    }

    $TableDdl += @(
        ""
        "kind = blob "
        "$PartitionBy"
        "$PathFormat"
        "dataformat = parquet "
        "("
        "    h@'https://${StorageAccountName}.blob.core.windows.net/${UriPath}/;${AccessKey}' " 
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
