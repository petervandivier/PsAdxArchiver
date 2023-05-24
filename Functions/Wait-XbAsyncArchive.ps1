
function Wait-XbAsyncArchive {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [guid]
        $OperationId,

        [ValidateRange(60,600)]
        [int]
        $SleepSeconds = 60,

        [datetime]
        $Start,

        [datetime]
        $End,

        [string]
        $ClusterUrl,

        [string]
        $DatabaseName
    )

    $AdxConnection = @{
        ClusterUrl = $ClusterUrl
        DatabaseName = $DatabaseName
    }

    $OperationName = "Operation: '$OperationId'"
    if($Start){
        $OperationName += ", Start: '$Start'"
    }
    if($End){
        $OperationName += ", End: '$End'"
    }

    while($true){
        $operation = Invoke-AdxCmd @AdxConnection -Command ".show operations $OperationId"

        if(($operation).Count -ne 1){
            Write-Error "Expected exactly one operation, but got '$(($operation).Count)'. Aborting await"
            $operation
            return
        }

        if($operation.State -eq 'InProgress'){
            Write-Host "$(Get-Date -Format o): Awaiting $OperationName" -ForegroundColor Yellow
            Start-Sleep -Seconds $SleepSeconds
            continue
        }elseif($operation.State -eq 'Completed'){
            Write-Host "$(Get-Date -Format o): Completed $OperationName." -ForegroundColor Green
            New-BurntToastNotification -Text "Completed $OperationName"
            break
        }else{
            Write-Error "Unexpected state occured: '$($operation.State)' for $OperationName"
            $operation
            return
        }
    }
}
