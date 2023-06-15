
$newTableSplat = @{
    ResourceGroupName = 'my-resource-group'
    StorageAccountName = 'mystorageaccount'
    Container = 'storm-events'
    ClusterUrl = 'https://help.kusto.windows.net;Fed=True'
    DatabaseName = 'Samples'
    TableName = 'StormEvents'
    TimestampColumnName = 'StartTime'
}

New-XbTable @newTableSplat -NoDeploy -ErrorAction SilentlyContinue
