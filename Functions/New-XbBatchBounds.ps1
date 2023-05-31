
function New-XbBatchBounds {
<#
.Synopsis
    Returns an array of [datetime] values for each $Step betwen $Start & $End. 

.Example
    $batches = @{
        Start = [DateTimeOffset]'2023-01-01 00:00'
        End   = [DateTimeOffset]'2023-01-01 03:00'
        Step  = [timespan]"01:00:00"
    }
    New-XbBatchBounds @batches

.Outputs
    [PsCustomObject[]]@{
        [string]$StartStr
        [string]$EndStr
        [string]$Label
    }
#>
    [CmdletBinding()]
    Param(
        [Parameter(Mandatory)]
        [DateTimeOffset]
        $Start,

        [Parameter(Mandatory)]
        [DateTimeOffset]
        $End,

        [Parameter(Mandatory)]
        [timespan]
        $Step
    )

    if($Start.Offset -ne [timespan]'00:00'){
        Write-Verbose "Parameter `$Start was supplied with localization '$($Start.Offset)'. Stripping and setting to UTC timezone *without* offset."
        $Start = [DateTimeOffset]"$($Start.ToString('yyyy-MM-dd HH:mm:ss'))Z"
    }
    if($End.Offset -ne [timespan]'00:00'){
        Write-Verbose "Parameter `$End was supplied with localization '$($End.Offset)'. Stripping and setting to UTC timezone *without* offset."
        $End = [DateTimeOffset]"$($End.ToString('yyyy-MM-dd HH:mm:ss'))Z"
    }

    if($Start -Gt $End){
        Write-Error "Start must come before end. You supplied: Start: '$Start', End: '$End'"
        return
    }

    if($Step -gt ($End - $Start)){
        Write-Error "Step is greater than total timespan. You supplied: Start: '$Start', End: '$End', Step: '$Step'"
        return
    }

    $boundary = $Start.Add(0)

    [DateTimeOffset[]]$bounds = $boundary

    while($End -Gt $boundary){
        $boundary = $boundary.Add($Step)
        $bounds += $boundary
    }

    $bounds | Sort-Object | Where-Object {
        $_ -Lt $End
    } | ForEach-Object {
        [PSCustomObject]@{
            Start = $_.ToString('u')
            End   = $_.Add($Step).ToString('u')
            Label = $_.ToString('yyyy-MM-dd')
        }
    }
}
