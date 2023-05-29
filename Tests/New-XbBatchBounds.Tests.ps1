#Requires -Modules @{ModuleName='PsAdxArchiver';ModuleVersion='0.0.1';Guid='6db49c4c-0073-44fe-95d8-907f056c1645'}
#Requires -Modules @{ModuleName='Pester';MaximumVersion='4.99';Guid='a699dea5-2c73-4616-a270-1f7abb777e71'}

Describe "New-XbBatchBounds creates an expected array" {
    $Start = '2023-01-01 00:00'
    $End = '2023-01-01 04:00'
    $BatchSpec = @{
        Start = $Start
        End   = $End
        Step  = '01:00:00'
    }
    $BatchBounds = New-XbBatchBounds @BatchSpec
    It "Should have the right number of batches" {
        $BatchBounds.Count | Should -BeExactly 5
    }
    It "Should be sorted" {
        $Test    = ($BatchBounds | ConvertTo-Json) 
        $Control = ($BatchBounds | Sort-Object | ConvertTo-Json)
        $Test | Should -Be $Control
    }
    It "Should have the correct first batch" {
        $Test    = (Get-Date $BatchBounds[0] -Format u) 
        $Control = (Get-Date $Start -Format u)
        $Test | Should -Be $Control
    }
    It "Should have the correct last batch" {
        $Test    = (Get-Date $BatchBounds[-1] -Format u) 
        $Control = (Get-Date $End -Format u)
        $Test | Should -Be $Control
    }
}
