
function Start-XbAsyncArchive {
<#
.Synopsis
    Executes the .`export Async` command for an input table & returns the operation id

.Outputs 
    [guid]$OperationId

.Parameter StartStr
    Inclusive lower bound of the export batch.
    Deliberately a string to bypass client localization issues. 
    Enforced to be a valid datetime.

.Parameter EndStr
    Exclusive upper bound of the export batch.
    Deliberately a string to bypass client localization issues. 
    Enforced to be a valid datetime.
#>
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [ValidateScript({Get-Date $_})]
        [string]
        $StartStr,

        [Parameter(Mandatory)]
        [ValidateScript({Get-Date $_})]
        [string]
        $EndStr,

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
        $TimestampColumnName
    )

    $exportAsyncCmd = @(
        ".export async to table ext$TableName <|"
        "$TableName"
        "| where $TimestampColumnName >= datetime($startStr)"
        "| where $TimestampColumnName <  datetime($endStr)"
    ) -join "`n"

    $AdxConnection = @{
        ClusterUrl = $ClusterUrl
        DatabaseName = $DatabaseName
    }

    $command = Invoke-AdxCmd @AdxConnection -Command $exportAsyncCmd

    if(-not [guid]::TryParse(
                $command.OperationId,
                [Management.Automation.PSReference][guid]::empty
            )
        )
    {
        Write-Warning "Returned OperationId: '$($command.OperationId)' is not a GUID. "
    }

    Write-Verbose "$(Get-Date -Format o): Started Operation: '$($command.OperationId)', Start: '$StartStr', End: '$EndStr'"

    [XbAsyncExportWaiter][PsCustomObject]@{
        Start               = [DateTimeOffset]$StartStr
        End                 = [DateTimeOffset]$EndStr
        ClusterUrl          = $ClusterUrl
        DatabaseName        = $DatabaseName
        TableName           = $TableName
        TimestampColumnName = $TimestampColumnName 
        OperationId         = $command.OperationId
    }
}
