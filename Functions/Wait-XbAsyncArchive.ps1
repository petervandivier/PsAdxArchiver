
function Wait-XbAsyncArchive {
<#
.Description
    `ForEach-Object -Parallel` does not inherit caller scope. Therefore, `Invoke-AdxCmd` _must be_
    on the PsModulePath (it will get autoloaded in each child process).
#>
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [XbAsyncExportWaiter[]]
        $Waiters,

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

    $Waiters | ForEach-Object -Parallel {
        $VerbosePreference = $using:VerbosePreference
        $ClusterUrl = $using:ClusterUrl
        $DatabaseName = $using:DatabaseName
        $SleepSeconds = $using:SleepSeconds

        $AdxConnection = @{
            ClusterUrl = $ClusterUrl
            DatabaseName = $DatabaseName
        }

        $Waiter = $_
        $OperationName = "Operation: '$($Waiter.OperationId)'"
        if($Waiter.Start){
            $OperationName += ", Start: '$($Waiter.Start)'"
        }
        if($Waiter.End){
            $OperationName += ", End: '$($Waiter.End)'"
        }

        while($true){
            $operation = Invoke-AdxCmd @AdxConnection -Command ".show operations $($Waiter.OperationId)"

            if(($operation).Count -ne 1){
                Write-Error "Expected exactly one operation, but got '$(($operation).Count)'. Aborting await"
                $operation
                return
            }

            if($operation.State -eq 'InProgress'){
                Write-Verbose "$(Get-Date -Format u): Awaiting $OperationName. Current wait time: $($operation.Duration)" 
                Start-Sleep -Seconds $SleepSeconds
                continue
            }elseif($operation.State -eq 'Completed') {
                Write-Verbose "$(Get-Date -Format u): Completed $OperationName." 
                New-BurntToastNotification -Text "Completed $OperationName"
                break 
            }elseif($operation.State -eq 'Throttled') {
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
            }else{
                Write-Error "Unexpected state occured: '$($operation.State)' for $OperationName"
                $operation | ConvertTo-Json -Depth 0 | Write-Error
                break 
            }
        }
        $Waiter.State = $operation.State
        $Waiter.Duration = $operation.Duration
        $Waiter
    }
}
