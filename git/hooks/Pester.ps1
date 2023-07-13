
Import-Module $PsScriptRoot/../../PsAdxArchiver.psd1 -Force

Remove-Module Pester -ErrorAction SilentlyContinue
Import-Module Pester -MaximumVersion '4.99'

Invoke-Pester $PsScriptRoot/../../Tests
