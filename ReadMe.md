# PS ADX Archiver

Wrapper scripts to simplify export of a large table to azure blob storage.

Assumes external table has already been provisioned.

## But why?

Anecdotally, [`.export async`](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/management/data-export/export-data-to-storage) only gives about 5 mb/sec throughput per thread. It can be parallelized but you need to manage it. That's what this module is for.

## What's with the `Xb` prefix?

Just needed to quickly pick something short & distinct. `Xb` == "Export Batch".
