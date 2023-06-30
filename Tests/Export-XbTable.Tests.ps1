#Requires -Modules @{ModuleName='Pester';MaximumVersion='4.99';Guid='a699dea5-2c73-4616-a270-1f7abb777e71'}

Describe "Planned steps should remain consistent between " {
    $caseName = "Export-XbTable.1"
    $case = Get-Content "$PsScriptRoot/Configuration/${caseName}.json" | ConvertFrom-Json -AsHashtable
    Export-XbTable @case -Verbose -NoExecute 4>&1 | Set-Content "$PsScriptRoot/Output/${caseName}.txt"
    $output = Get-Content "$PsScriptRoot/Output/${caseName}.txt" -Raw
    $control = Get-Content "$PsScriptRoot/Control/${caseName}.txt" -Raw

    It "Case 1: calendar year 2007, 1 day (24 hour) step size, DOP 31. " {
        $output | Should -BeExactly $control
    }

    $caseName = "Export-XbTable.2"
    $case = Get-Content "$PsScriptRoot/Configuration/${caseName}.json" | ConvertFrom-Json -AsHashtable
    $bounds =  New-XbBatchBounds -Start '2007-01-01' -End '2007-02-01' -Step '1.00:00'
    $bounds += New-XbBatchBounds -Start '2007-03-01' -End '2007-04-01' -Step '7.00:00'
    $case.Add('BatchBounds',$bounds)
    Export-XbTable @case -Verbose -NoExecute 4>&1 | Set-Content "$PsScriptRoot/Output/${caseName}.txt"
    $output = Get-Content "$PsScriptRoot/Output/${caseName}.txt" -Raw
    $control = Get-Content "$PsScriptRoot/Control/${caseName}.txt" -Raw

    It "Case 2: Non-contiguous batch bounds, CY 2007, January (by day) & March (by week), DOP 4. " {
        $output | Should -BeExactly $control
    }

    Pop-Location
}
