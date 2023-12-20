
function Export-XbTable {
<#
.Synopsis
    Wrapper function to invoke Start/Wait/Receive for a given table & time range.

.Parameter Parallel
    Specifies number of concurrent executions to allow. 
    Max 100. Defaults to 1 (one): *not* parallel.

.Parameter NoExecute
    Prints batch bounds to verbose stream but does not initiate archive command(s). 
    For validation & testing.

.Parameter Inclusive
    Adds an additional bound to the end of the timespan. Allows copy-paste from a
    KQL `summarize count() by bin(Timestamp,_step)` without needing to manually 
    increment to terminal bin by 1.

.Parameter BatchBounds
    Explicit batches rather than start/end/step. Useful for backfilling non-contiguous 
    gaps. _Must be_ an [object] matching the output of `New-XbBatchBounds`.
    ?TODO: enforce typing with a class?
#>
    [CmdletBinding(DefaultParameterSetName='Timespan')]
    param (
        [Parameter(Mandatory)]
        [string]
        $StorageAccountName,

        [Parameter(Mandatory)]
        [string]
        $Container,

        [Parameter()]
        [string]
        $Directory,

        [Parameter()]
        [string]
        $LogFile,

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
        [string]
        $PathFormatColumnName,

        [Parameter(
            Mandatory,
            ParameterSetName='Timespan'
        )]
        [DateTimeOffset]
        $Start,

        [Parameter(
            Mandatory,
            ParameterSetName='Timespan'
        )]
        [DateTimeOffset]
        $End,

        [Parameter(
            Mandatory,
            ParameterSetName='Timespan'
        )]
        [timespan]
        $Step,

        [Parameter(ParameterSetName='Timespan')]
        [switch]
        $Inclusive,

        [Parameter(
            Mandatory,
            ParameterSetName='BatchBounds'
        )]
        $BatchBounds,

        [ValidateRange(1,100)]
        [int]
        $Parallel = 1,

        [ValidateRange(60,600)]
        [int]
        $SleepSeconds = 120,

        [ValidateSet('second','millisecond')]
        [AllowNull()]
        [string]
        $UnixTime,

        [switch]
        $NoExecute,

        [switch]
        $NoTag
    )

    $DoExecute = -Not $NoExecute

    if(-not [string]::IsNullOrWhiteSpace($LogFile)){
        Try { 
            [io.file]::OpenWrite($LogFile).close() 
        } Catch { 
            throw "Unable to write to output file '$LogFile'";
            return
        }
    }

    $receiveSplat = @{
        StorageAccountName = $StorageAccountName
        Container = $Container
        LogFile = $LogFile
        NoTag = $NoTag
    }

    $AdxTableSpec = @{
        ClusterUrl = $ClusterUrl
        DatabaseName = $DatabaseName
        TableName = $TableName
        ExternalTableName = $ExternalTableName
        TimestampColumnName = $TimestampColumnName
    }

    $Bounds = switch($PsCmdlet.ParameterSetName){
        'Timespan' {
            if($Inclusive){
                $End = $End.Add($Step)
            }
            New-XbBatchBounds -Start $Start -End $End -Step $Step
        }
        'BatchBounds' {
            $BatchBounds
        }
    }

    if($Bounds.Count -lt 1){
        Write-Error "Count of batch boundaries must be at least 1, but was '$($Bounds.Count)'"
        return
    }
    $BatchCount = $Bounds.Count

    for($IndexStart = 0; $IndexStart -lt $BatchCount; $IndexStart += $Parallel) {
        $StopWatch = New-Object System.Diagnostics.Stopwatch
        $StopWatch.Start()

        $IndexEnd = $IndexStart + $Parallel - 1
        if($IndexEnd -ge $BatchCount){
            $IndexEnd = $BatchCount - 1
        }
        Write-Verbose "Initializing serial batch; IndexStart: '$IndexStart', IndexEnd: '$IndexEnd'"
        $Batches = $IndexStart .. $IndexEnd | ForEach-Object {
            $startStr = $Bounds[$_].Start
            $endStr   = $Bounds[$_].End
            $label    = $Bounds[$_].Label
            if(
                $PsBoundParameters.Keys.Contains('PathFormatColumnName') -And
                -Not [string]::IsNullOrWhiteSpace($PathFormatColumnName)
            ){
                $prefix = "${PathFormatColumnName}=${label}"
            } else {
                $prefix = "${TimestampColumnName}=${label}"
            }
            if(
                $PsBoundParameters.Keys.Contains('Directory') -And
                -Not [string]::IsNullOrWhiteSpace($Directory)
            ){
                $prefix = "$Directory/$prefix"
            }
            Write-Verbose "Initializing parallel batch; IndexPosition: '$_', Start: '$startStr', End: '$endStr'"
            if($DoExecute){
                $Operation = Start-XbAsyncArchive -Start $startStr -End $endStr -UnixTime $UnixTime @AdxTableSpec
                $Operation.Prefix = $prefix
                $Operation
            } else {
                Start-XbAsyncArchive -Start $startStr -End $endStr -UnixTime $UnixTime @AdxTableSpec -NoExecute
            }
        }

        if($DoExecute){
            $Batches = Wait-XbAsyncArchive -Waiters $Batches -SleepSeconds $SleepSeconds

            $StopWatch.Stop()
            $WaitDurationMin = $StopWatch.Elapsed.TotalMinutes
            $WaitDurationStr = $StopWatch.Elapsed.ToString()
            $StopWatch.Restart()

            $Batches | ForEach-Object {
                $_ | Receive-XbAsyncArchive @receiveSplat
            }

            $StopWatch.Stop()
            $ReceiveDurationMin = $StopWatch.Elapsed.TotalMinutes
            $ReceiveDurationStr = $StopWatch.Elapsed.ToString()

            $TotalMinutes = $WaitDurationMin + $ReceiveDurationMin
            $TotalMinutesStr = [TimeSpan]::FromMinutes($TotalMinutes).ToString()

            $ExportedMb = ($Batches.SizeBytes | Measure-Object -Sum).Sum / 1mb
            $BlobCount = ($Batches.NumFiles | Measure-Object -Sum).Sum

            $WaitSerialMbPerSec = $ExportedMb / ($WaitDurationMin * 60)
            $TotalSerialMbPerSec = $ExportedMb / ($TotalMinutes * 60)
            $AverageMbPerSec = ($Batches | ForEach-Object {
                $DurationSeconds = $_.Duration.TotalSeconds
                if($DurationSeconds -gt 0){
                    $_.SizeBytes / (1mb * $DurationSeconds)
                } else {
                    $null
                }
            } | Measure-Object -Average).Average

            $ExportedMb          = $ExportedMb.ToString('N2')
            $BlobCount           = $BlobCount.ToString('N0')
            $WaitSerialMbPerSec  = $WaitSerialMbPerSec.ToString('N2')
            $TotalSerialMbPerSec = $TotalSerialMbPerSec.ToString('N2')
            $AverageMbPerSec     = $AverageMbPerSec.ToString('N2')

            $Status = @(
                "Completed '$($Batches.Count)' batches for range '$($Bounds[$IndexStart].Start)' t '$($Bounds[$IndexEnd].End)'. "
                "- Exported Mb: '$ExportedMb'. "
                "- Count blobs: '$BlobCount'. "
                "- Serial export duration: '$WaitDurationStr'. "
                "  - Average speed (per-thread): '$AverageMbPerSec' Mb/sec. "
                "  - Overall speed (export only): '$WaitSerialMbPerSec' Mb/sec. "
                "- Blob tagging duration: '$ReceiveDurationStr'. " 
                "- Total runtime: '$TotalMinutesStr'. "
                "  - Overall speed (including tagging): '$TotalSerialMbPerSec' Mb/sec. "
            ) -join "`n"

            New-BurntToastNotification -Text $Status
            Write-Host $Status -ForegroundColor Green
        }
    }
}
