# PythonDataIntegration
How to use Python for efficient data integration

## Scope

 1. Use DuckDB to read data, e.g. a CSV file, do some transformations using SQL and write the result into e.g. Parquet using DuckDB. Source --> SQL --> Target
 2. Delta Detection: Compare the incoming data with a target table, which has the same (or more) columns and find out all new rows, all changed rows and optionally all deleted rows. Source --> SQL --> Compare with Target --> Target
 3. Target shall be a Slowly Changing Dimension Type 2
