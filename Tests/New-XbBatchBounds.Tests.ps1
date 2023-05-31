#Requires -Modules @{ModuleName='Pester';MaximumVersion='4.99';Guid='a699dea5-2c73-4616-a270-1f7abb777e71'}

Describe "New-XbBatchBounds creates an expected array" {
    $Start = [DateTimeOffset]'2023-01-01 00:00Z'
    $End   = [DateTimeOffset]'2023-01-01 04:00Z'
    $BatchSpec = @{
        Start = $Start
        End   = $End
        Step  = [timespan]'01:00:00'
    }
    $BatchBounds = New-XbBatchBounds @BatchSpec
    It "Should have the right number of batches" {
        $BatchBounds.Count | Should -BeExactly 4
    }
    It "Should be sorted" {
        $Test    = $BatchBounds | ConvertTo-Csv
        $Control = $BatchBounds | Sort-Object Start | ConvertTo-Csv
        $Test | Should -Be $Control
    }
    It "Should have the correct lower bound" {
        $Test    = $BatchBounds[0].Start
        $Control = $Start.ToString('u')
        $Test | Should -Be $Control
    }
    It "Should have the correct upper bound" {
        $Test    = $BatchBounds[-1].End
        $Control = $End.ToString('u')
        $Test | Should -Be $Control
    }
}
