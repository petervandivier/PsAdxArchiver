
<#
.Synopsis
    Runs Pester tests on current build.
.Description
    Force reload PsAdxArchiver, force Pester v4, runs the tests.
    Adds about 10 second delay to commit :\
#>

Import-Module $PsScriptRoot/../../PsAdxArchiver.psd1 -Force

Remove-Module Pester -ErrorAction SilentlyContinue
Import-Module Pester -MaximumVersion '4.99'

$PesterResult = Invoke-Pester $PsScriptRoot/../../Tests -PassThru

if($PesterResult.FailedCount -gt 0){
    Write-Error "A pre-commit condition failed. The commit will abort. Check Pester output."
    exit 1
}
