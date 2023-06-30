#
# Module manifest for module 'PsAdxArchiver'
# Generated by: Peter.Vandivier
# Generated on: 2023-05-24
#

@{
    RootModule = 'PsAdxArchiver.psm1'
    ModuleVersion = '0.0.3'
    Guid = '6db49c4c-0073-44fe-95d8-907f056c1645'
    Author = 'Peter Vandivier'
    Copyright = '(c) Peter Vandivier. All rights reserved.'
    Description = 'Generic exporter. Sends an Azure Data Explorer table to blob storage as an external table.'
    RequiredModules = @(
        @{
            ModuleName = 'Invoke-AdxCmd'
            ModuleVersion = '0.0.9'
            Guid = '688fd570-0253-491b-beff-385ecc05cef2'
        }
        # @{
        #     ModuleName = 'BurntToast'
        #     ModuleVersion = '1.0.0'
        #     Guid = '751a2aeb-a68f-422e-a2ea-376bdd81612a'
        # }
        @{
            ModuleName = 'Az.Storage'
            ModuleVersion = '5.6.0'
            Guid = 'dfa9e4ea-1407-446d-9111-79122977ab20'
        }
    )
    FunctionsToExport = @(
        'Export-XbTable'
        'New-XbBatchBounds'
        'New-XbTable'
        'Receive-XbAsyncArchive'
        'Start-XbAsyncArchive'
        'Wait-XbAsyncArchive'
    )
}
