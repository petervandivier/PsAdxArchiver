
function Start-XbAsyncArchive {
<#
.SYNOPSIS
    Executes the .`export Async` command for an input table & returns the operation id
.OUTPUTS 
    [guid]$OperationId
#>
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [datetime]
        $Start,

        [Parameter(Mandatory)]
        [datetime]
        $End,

        [Parameter(Mandatory)]
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

    $startStr = $Start.ToString('u')
    $endStr   = $End.ToString('u')

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

    Write-Verbose "$(Get-Date -Format u): Started Operation: '$($command.OperationId)', Start: '$startStr', End: '$endStr'"

    [XbAsyncExportWaiter][PsCustomObject]@{
        OperationId = $command.OperationId
        Start = $Start
        End = $End
    }
}
