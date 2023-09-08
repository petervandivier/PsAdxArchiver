
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
        $SleepSeconds = 120
    )

    $Waiters | ForEach-Object -Parallel {
        $VerbosePreference = $using:VerbosePreference
        $SleepSeconds = $using:SleepSeconds

        $Waiter = $_

        $AdxConnection = @{
            ClusterUrl = $Waiter.ClusterUrl
            DatabaseName = $Waiter.DatabaseName
        }

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
                Write-Verbose "$(Get-Date -Format o): Awaiting $OperationName. Current wait time: $($operation.Duration)" 
                Start-Sleep -Seconds $SleepSeconds
                continue
            }elseif($operation.State -eq 'Completed') {
                Write-Host "$(Get-Date -Format o): Completed $OperationName." -ForegroundColor Green
                break 
            }elseif($operation.State -eq 'Throttled') {
                Write-Error "$(Get-Date -Format o): Throttled operation $OperationName" 
                Start-Sleep -Seconds $SleepSeconds
                break
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
