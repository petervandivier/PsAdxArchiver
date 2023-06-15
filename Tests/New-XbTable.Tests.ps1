
$caseName = "New-XbTable.1"
$case1 = Get-Content "$PsScriptRoot/Configuration/${caseName}.json" | ConvertFrom-Json -AsHashtable
New-XbTable @case1 -ErrorAction SilentlyContinue | Set-Content "$PsScriptRoot/Output/${caseName}.kql"

Describe "New-XbTable" {
    $output = Get-Content "$PsScriptRoot/Output/${caseName}.kql" -Raw
    $control = Get-Content "$PsScriptRoot/Control/${caseName}.kql" -Raw

    It "Case 1: Samples.StormEvents, non-existent storage account (blank AccessKey) " {
        $output | Should -BeExactly $control
    }
}
