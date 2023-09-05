# PS ADX Archiver

Wrapper scripts to simplify export of a large table to azure blob storage.

Assumes external table has already been provisioned.

Requires that the table to be exported has some sort of timestamp column, either datetime or numeric unix time. 

## Stay Awake

Because this module is designed to be long-running and bound to a laptop session, leveraging [PowerToys Awake](https://learn.microsoft.com/en-us/windows/powertoys/awake) is recommended.

## But why?

Anecdotally, [`.export async`](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/management/data-export/export-data-to-storage) only gives about 5 mb/sec throughput per thread. Additionally, there is a [60-minute hard limit timeout](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/concepts/querylimits#limit-execution-timeout) after which any `.export` command will be terminated by the cluster.

You need to batch & parallelize your `.export` commands to maximize throughput and minimize errors. This module provides functions and helper scripts for that.

## What's with the `Xb` prefix?

Just needed to quickly pick something short & distinct. `Xb` == "Export Batch".
