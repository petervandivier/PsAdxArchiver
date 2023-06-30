#Requires -Modules @{ModuleName='Pester';MaximumVersion='4.99';Guid='a699dea5-2c73-4616-a270-1f7abb777e71'}

$caseName = "New-XbTable.1"
$case1 = Get-Content "$PsScriptRoot/Configuration/${caseName}.json" | ConvertFrom-Json -AsHashtable
New-XbTable @case1 -NoDeploy -ErrorAction SilentlyContinue | Set-Content "$PsScriptRoot/Output/${caseName}.kql"

Describe "New-XbTable" {
    $output = Get-Content "$PsScriptRoot/Output/${caseName}.kql" -Raw
    $control = Get-Content "$PsScriptRoot/Control/${caseName}.kql" -Raw

    It "Case 1: Samples.StormEvents, non-existent storage account (blank AccessKey) " {
        $output | Should -BeExactly $control
    }
}
