
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
        $Prefix,

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

    $TableSchema.TableName = "ext${TableName}"
    $TableSchema.Folder = $null
    $TableSchema.DocString = $null

    $TableDdl = ConvertTo-AdxCreateTableCmd $TableSchema
    $TableDdl = $TableDdl.Replace('.create-merge table','.create external table')
    if([string]::IsNullOrEmpty($UnixTime)){
        $PartitionBy = "partition by (${TimestampColumnName}:datetime = startofday($TimestampColumnName))"
        $PathFormat = "pathformat = (`"$TimestampColumnName=`" datetime_pattern(`"yyyy-MM-dd`", $TimestampColumnName))"
    } else {
        $TableDdl = $TableDdl.Replace("table ext${TableName} (","table ext${TableName} (`n    ${TimestampColumnName}_DT: datetime,")
        $PartitionBy = "partition by (${TimestampColumnName}_DT:datetime = startofday(${TimestampColumnName}_DT))"
        $PathFormat = "pathformat = (`"${TimestampColumnName}_DT=`" datetime_pattern(`"yyyy-MM-dd`", ${TimestampColumnName}_DT))"
    }

    if([string]::IsNullOrEmpty($Prefix)){
        $UriPath = $Container
    } else {
        $UriPath = "$Container/$Prefix"
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
