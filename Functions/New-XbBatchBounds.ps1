
function New-XbBatchBounds {
<#
.EXAMPLE
    $batches = @{
        Start = [datetime]::Now.ToShortDateString()
        End   = [datetime]::Now.AddDays(1).ToShortDateString()
        Step  = [timespan]"01:00:00"
    }
    New-XbBatch @batches
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