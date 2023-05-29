
function New-XbBatchBounds {
<#
.Synopsis
    Returns an array of [datetime] values for each $Step betwen $Start & $End. 

.Example
    $batches = @{
        Start = [datetime]::Now
        End   = [datetime]::Now.AddDays(1)
        Step  = [timespan]"01:00:00"
    }
    New-XbBatch @batches

.Outputs
    [datetime[]]
#>
    [CmdletBinding()]
    Param(
        [Parameter(Mandatory)]
        [datetime]
        $Start,

        [Parameter(Mandatory)]
        [datetime]
        $End,

        [Parameter(Mandatory)]
        [timespan]
        $Step
    )

    if($Start -Gt $End){
        Write-Error "Start must come before end. You supplied: Start: '$Start', End: '$End'"
        return
    }

    if($Step -gt ($End - $Start)){
        Write-Error "Step is greater than total timespan. You supplied: Start: '$Start', End: '$End', Step: '$Step'"
        return
    }

    $boundary = $Start.Add(0)

    [datetime[]]$bounds = $boundary

    while($End -Gt $boundary){
        $boundary = $boundary.Add($Step)
        $bounds += $boundary
    }

    $bounds | Sort-Object
}
