
function Wait-XbAsyncArchive {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory,ValueFromPipeline)]
        [XbAsyncExportWaiter]
        $Waiter,

        [ValidateRange(60,600)]
        [int]
        $SleepSeconds = 60,

        [Parameter(Mandatory)]
        [string]
        $ClusterUrl,

        [Parameter(Mandatory)]
        [string]
        $DatabaseName
    )

    begin{
        $AdxConnection = @{
            ClusterUrl = $ClusterUrl
            DatabaseName = $DatabaseName
        }
    }

    process{
        $OperationName = "Operation: '$($Waiter.OperationId)'"
        if($Waiter.Start){
            $OperationName += ", Start: '$($Waiter.Start)'"
        }
        if($Waiter.End){
            $OperationName += ", End: '$($Waiter.End)'"
        }

        :awaitCompletion while($true){
            $operation = Invoke-AdxCmd @AdxConnection -Command ".show operations $($Waiter.OperationId)"

            if(($operation).Count -ne 1){
                Write-Error "Expected exactly one operation, but got '$(($operation).Count)'. Aborting await"
                $operation
                return
            }

            switch($operation.State){
                'InProgress'{
                    Write-Verbose "$(Get-Date -Format u): Awaiting $OperationName. Current wait time: $($operation.Duration)" 
                    Start-Sleep -Seconds $SleepSeconds
                    continue
                }
                'Completed'{
                    Write-Verbose "$(Get-Date -Format u): Completed $OperationName." 
                    New-BurntToastNotification -Text "Completed $OperationName"
                    break awaitCompletion
                }
                'Throttled'{
                    Write-Warning "$(Get-Date -Format u): Throttled operation $OperationName" 
                    Start-Sleep -Seconds $SleepSeconds
                    # TODO: resubmit, needs Table & Column data from Start-Cmd AFAICT
                    # $NewArchiveCmd = @{
                    #     Start = $Waiter.Start
                    #     End = $Waiter.End
                    # }
                    # $NewWaiter = Start-XbAsyncArchive @NewArchiveCmd
                    # Write-Warning "$(Get-Date -Format u): Re-submitting operation for Start: '$($Waiter.Start)', End: '$($Waiter.End)'. Old OperationId: '$($Waiter.OperationId)', New OperationId: '$($NewWaiter.OperationId)'" 
                    # $Waiter.OperationId = $NewWaiter.OperationId
                    continue
                }
                default{
                    Write-Error "Unexpected state occured: '$($operation.State)' for $OperationName"
                    $operation | ConvertTo-Json -Depth 0 | Write-Error
                    break awaitCompletion
                }
            }
        }
        $Waiter.State = $operation.State
        $Waiter.Duration = $operation.Duration
        $Waiter
    }
}
