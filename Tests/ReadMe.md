# PsAdxArchiver Pester Tests

These are written for Pester v4 because v5 hurts my brain. 

The `#Requires` blocks at the top of the `*.Tests` files don't _actually_ do much here (since `Invoke-Kusto` bypasses it & Pester will self-upgrade v4→v5 in certain situations anyhow) but they are deliberately included as a reminder to the maintainer. 

## StormEvents

`StormEvents` is a table in the `Samples` database ([help cluster](https://help.kusto.windows.net/Samples)) from the [KQL tutorial docs](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/tutorials/learn-common-operators)

An Azure portal login is required to access the help cluster. 
