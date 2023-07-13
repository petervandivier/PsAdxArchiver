
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

Invoke-Pester $PsScriptRoot/../../Tests
