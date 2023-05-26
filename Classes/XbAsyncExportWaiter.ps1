<#
.Synopsis
    Holds all the information about a single `.export async` command for transit through start/wait/receive.
#>
class XbAsyncExportWaiter {
    [datetime]$Timestamp = [datetime]::Now
    [ValidateNotNullOrEmpty()][datetime]$Start
    [ValidateNotNullOrEmpty()][datetime]$End
    [string]$Prefix
    [ValidateNotNullOrEmpty()][guid]$OperationId
    [string]$State
    [ValidateNotNullOrEmpty()][timespan]$Duration = "00:00:00"
    [bigint]$SizeBytes
    [int]$NumFiles
    XbAsyncExportWaiter([PsCustomObject]$InputObject){
        $this.Timestamp   = if($InputObject.Timestamp){$InputObject.Timestamp}else{[datetime]::Now}
        $this.Start       = $InputObject.Start
        $this.End         = $InputObject.End
        $this.Prefix      = $InputObject.Prefix
        $this.OperationId = $InputObject.OperationId
        $this.State       = $InputObject.State
        $this.Duration    = if($InputObject.Duration){$InputObject.Duration}else{[timespan]"00:00:00"}
        $this.SizeBytes   = $InputObject.SizeBytes
        $this.NumFiles    = $InputObject.NumFiles
    }
}