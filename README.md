# Ducktape
A data integration tool based on Python for flexibility and DuckDB for performance.

## Scope

 1. Python and DuckDB can be used to read data into DuckDB. Within DuckDB, SQL based transformations are applied. The result is written into a Target, again either using DuckDB's native features or Python. `Source --> DuckDB --> SQL --> Target`
 2. Delta handling as a first class citizen, meaning the structure supports before and after image, change indicators, can create the delta by comparing with the target and can be connected to CDC readers
 3. Certain transformations are hard to do in SQL, either because the SQL statement is complicated, or it can be expressed in 3GL code more easily or it needs multiple SQL statements. For these Python methods are implemented to achieve these transformations with a single function call.
 4. User interaction levels are
    - Via code - The user writes the Python code and thanks to the APIs, can be very quick.
    - (planned) Via UI - The user does drag and drop the transformations and with that configures the SQL statements and the sequence of transformations. Important: Code changes the UI representation and UI representation changes the code. The only thing stored is the code, the UI is just a visualization of the code in a different format.
    - (planned) Via intend - The user tells the source and the target properties and the transformations required to achieve that target, are obvious. Example: Read the customer master and create a Slow-Changing-Dimension table with its data. It's obvious what has to be done.
    - (planned) Via prompt - The user feeds the functional spec and the code is generated.

## Principles

A Dataflow is a graph of nodes, where each node is either a SQL Query, a Table or a Transform. The goal is to have as few persistencies as possible for performance reasons, e.g. Source --> Query1 --> Query2 --> Target is one large insert...select statement.
Reasons for persisting the data explicitly is when multiple SQL statements must be executed to achieve a goal. DuckDB might also decide to pick an execution plan where data is cached, e.g. when the same data is used in different sub selects of a large statement.

For the same reason some transforms require a Table as input whereas others support Tables or Queries. From a OOP point of view the hierarchy is Step --> Dataset --> Table --> Loader

Some transforms require primary key information, which can either be the physical primary key or a logical primary key. For example the physical primary key is an ID column, but the logical primary key is the Customer_Id.
This is a powerful concept as it allows a great degree of freedom - see Comparison transform as a concrete example.
The primary key can be provided by various ways. If specified in the method call, this has the highest priority. Second level is the primary key information of the upstream dataset. And if that is not specified either, the table's primary key definition is used.

A CDC enabled dataset is one that has a `__change_type` column. A CDC dataset is either coming from the source already or the Comparison transform creates it. If the target table has a CDC enabled dataset as input, it will insert the `I` records, update the `U` records etc.

All of this is to make the usage of the library as simple as possible.

An example of copying a source table into a target:

Create the source and target table with a primary key (actually only the target needs one).
The source is a Table representing the DuckDB table `csv_data`
The target has the source as input and is a Loader object, to be more precise a Loader writing into DuckDB.
Both are added to a Dataflow and the dataflow is started.
Because the target table has a primary key and the source is not a CDC table but a regular table, the entire dataset is upserted.


```
        duckdb.execute("create or replace table csv_data as (SELECT * FROM 'testdata/customers-100000.csv')")
        duckdb.execute("alter table csv_data add primary key (\"Customer Id\")")
        duckdb.execute("create or replace table csv_data_copy as (SELECT * FROM csv_data) with no data")
        duckdb.execute("alter table csv_data_copy add primary key (\"Customer Id\")")

        df = Dataflow()
        source_table = df.add(Table('csv_data', 'csv_data'))
        target_table = df.add(DuckDBTable(source_table, "csv_data_copy", logger=logger))
        df.start(duckdb)
        target_table.show(duckdb, logger, "Target table after apply")
```
When the logging.loglevel=DEBUG, the SQL statements are logged and this would reveal what has been executed

```
with source as (select * from "csv_data") 
INSERT OR REPLACE INTO "csv_data_copy"("Customer Id", "Country", "Company", "City", ...)
SELECT "Customer Id", "Country", "Company", "City", ... from source
```

Thanks to DuckDB we have all options:

 - Create a table and load data into it using DuckDB methods - like above.
 - Create a table and use Python to get data into it.
 - Do not create a source table but a query selecting directly from a supported source. That can be a CSV file, a Parquet file, a SQL Server table, a Delta Lake table,... 
 - Similar in the DuckDBLoader.

## Transforms
 - Comparison: Take an input dataset (normal or CDC), compare with the target and create a delta dataset. This tells which records must be inserted, updated, optionally deleted and does remove all records that have not been changed.
 - SCD2: Take a CDC input and provide the changes to be applied in the target, e.g. Change the current version to an end-date of today and create create a new version with start-date today.
 - Pivot/Unpivot: Turn a dataset with multiple columns into a dataset with multiple rows and vice versa, including multiple sets. Example: Source has 12 columns for REVENUE_JAN, REVENUE_FEB,... and 12 columns for QTY_JAN, QTY_FEB,... Result should be 12 rows for each input row.
 - Hierarchy handling: Multiple transforms to deal with parent_child hierarchy tables, hierarchy column (City, region, country) and convert between these representations. Also validate hierarchies to ensure there are no loops in a parent child based hierarchy.
 - Temporal Join: Two datasets must be joined and each as a valid-from/valid-to date. Create a list of dates when something changed and join the data from both tables.
 - CDC: A transform capable of changing the CDC information and do calculations on the change data. Example: If the ACCOUNT_BALANCE changed from 100USD to 500USD, what is the increase? The after image value minus the before image value.
 - Address validation: Pass an address to an external service like Google Maps API to validate, correct and standardize the address.
 - Data Validation: Apply rules to the data and check if the record does as PASS/FAIL/WARN for each rule and overall.
 - Lookup: Find the matching record in another table and handle cases where the lookup table returns multiple candidates.
 - Union All: All rows from different inputs are combined into one large data set.

## Qualities

 - (Schema evolution)
 - automatic create table
 - Logical validation

## Operational Metadata and Validation
 - Each dataflow creates operational statistics, rows read, rows outputted, execution time, number of PASS/FAIL/WARN records.
 - The dataflow can have safeguard based on the statistics, e.g. if less than 100'000 rows are processed, consider the dataflow as failed.
