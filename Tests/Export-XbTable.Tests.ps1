
Describe "Planned steps should remain consistent between " {
    Push-Location $PsScriptRoot

    $caseName = "Export-XbTable.1"
    $case1 = Get-Content "Configuration/${caseName}.json" | ConvertFrom-Json -AsHashtable
    Export-XbTable @case1 -Verbose -NoExecute 4>&1 | Set-Content "Output/${caseName}.txt"
    $output = Get-Content "Output/${caseName}.txt" -Raw
    $control = Get-Content "Control/${caseName}.txt" -Raw

    It "Case 1: calendar year 2007, 1 day (24 hour) step size, DOP 31. " {
        $output | Should -BeExactly $control
    }

    Pop-Location
}