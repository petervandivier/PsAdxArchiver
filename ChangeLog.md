# Change Log

## 0.1.2

Tuesday, 14th November, 2023

- supports user suppression of storage tagging flow

## 0.1.1

Thursday, 7th September, 2023

- adds support for subdirectories in container path
- bugfixes standard/non-UnixTime table DDL from New-XbTable
- supports external name that does NOT exactly match source table
    - previously, the external table name needed to be exactly the source table name prefixed with "ext"
- filters on known prefix to speed up pre-tag blob retreival in Receive phase

## 0.1.0

Monday, 21st August, 2023

- Adds support for the timestamp column to be numeric unix time
