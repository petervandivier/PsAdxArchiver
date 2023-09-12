
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

        [Parameter()]
        [string]
        $ExternalTableName,

        [Parameter(Mandatory)]
        [string]
        $TimestampColumnName,

        [ValidateSet('second','millisecond','')]
        [AllowNull()]
        [AllowEmptyString()]
        [string]
        $UnixTime,

        [switch]
        $NoExecute
    )

    $UnixTime = $UnixTime.ToLower()
    $EpochOffset = switch ($UnixTime) {
        'second'      { 62135596800 }
        'millisecond' { 62135596800000 }
    }

    if(-not [string]::IsNullOrEmpty($UnixTime)){
        $LowBound = "tolong((datetime($startStr)/timespan(1 $UnixTime))-$EpochOffset)"
        $HighBound = "tolong((datetime($endStr)/timespan(1 $UnixTime))-$EpochOffset)"
    } else {
        $LowBound = "datetime($startStr)"
        $HighBound = "datetime($endStr)"
    }

    if([string]::IsNullOrEmpty($ExternalTableName)){
        $ExternalTableName = "ext$TableName"
    }

    $exportAsyncCmd = @(
        ".export async to table $ExternalTableName <|"
        "$TableName"
        "| where $TimestampColumnName >= $LowBound"
        "| where $TimestampColumnName <  $HighBound"
    ) -join "`n"

    if(-not [string]::IsNullOrEmpty($UnixTime)){
        $exportAsyncCmd += "`n"
        $exportAsyncCmd += @(
            "| extend ${TimestampColumnName}_DT = unixtime_${UnixTime}s_todatetime(${TimestampColumnName})"
            "| project-reorder ${TimestampColumnName}_DT"
        ) -join "`n"
    }

    if($NoExecute){
        Write-Host $exportAsyncCmd
        return
    }

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
        ExternalTableName   = $ExternalTableName
        TimestampColumnName = $TimestampColumnName 
        OperationId         = $command.OperationId
    }
}
