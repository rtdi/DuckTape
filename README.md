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
        duckdb.execute("create or replace table csv_data as "
                       "(SELECT * FROM 'testdata/customers-100000.csv')")
        duckdb.execute("alter table csv_data add primary key (\"Customer Id\")")
        duckdb.execute("create or replace table csv_data_copy as "
                       "(SELECT * FROM csv_data) with no data")
        duckdb.execute("alter table csv_data_copy add primary key (\"Customer Id\")")

        df = Dataflow()
        source_table = df.add(Table('csv_data', 'csv_data'))
        target_table = df.add(DuckDBTable(source_table, "csv_data_copy"))
        df.start(duckdb)
        target_table.show(duckdb, "Target table after apply")
```
When the logging.loglevel=DEBUG, the SQL statements are logged and reveals what has been executed
`logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)`

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

## Examples

### Upsert

Read a source CSV file and upsert it into a target table based on the target table's primary key

```
duckdb.execute("create or replace table csv_data as "
               "(SELECT * FROM 'testdata/customers-100000.csv')")
duckdb.execute("alter table csv_data add primary key (\"Customer Id\")")
duckdb.execute("create or replace table csv_data_copy as (SELECT * FROM csv_data)")
duckdb.execute("alter table csv_data_copy add primary key (\"Customer Id\")")
df = Dataflow()
source_table = df.add(Table('csv_data', 'csv_data'))
target_table = df.add(DuckDBTable(source_table, "csv_data_copy"))
df.start(duckdb)
```

### Slow Changing Dimension Table Type 2

Create a target table with all columns of the source plus
 - a generated key column
 - a start/end date
 - a current indicator

Goal is to have all the different versions, each with a validity period.

A tricky situation is when a source record got deleted - it is no longer valid, hence go an end-date, and later it is added again - with a new start date hence.

```
termination_date = datetime.strptime('9999-12-31', '%Y-%m-%d')

duckdb.execute("create or replace table csv_data as "
               "(SELECT * FROM 'testdata/customers-100000.csv')")
df = Dataflow()
source_table = df.add(Table('csv_data', 'csv_data', pk_list=['Customer Id']))
tc = df.add(Comparison(source_table, end_date_column="end_date",
                       termination_date=termination_date,
                       detect_deletes=True, order_column="version_id"))
scd2 = df.add(SCD2(tc, 'start_date', 'end_date',
            termination_date=termination_date,
            current_flag_column='current', current_flag_set='Y',
            current_flag_unset='N'))
target_table = df.add(DuckDBTable(scd2, "customer_output",
                      generated_key_column='version_id',
                      pk_list=['version_id']))

# create the target table with all needed columns
target_table.add_all_columns(source_table, duckdb)
scd2.add_default_columns(target_table)
target_table.add_default_columns()
target_table.create_table(duckdb)

tc.set_comparison_table(target_table)
df.start(duckdb)
```

On 2025-05-28 two records got added. They were marked as current and valid until 9999-12-31.
On 2025-07-06 Fritz was added as new customer, Terrance deleted and Barry changed name to Berry. Therefore, Fritz had a valid from date of 2025-07-06 to 9999-12-31 on this day, Terrance' end date was updated and a new version for Barry/Berry was created, marking the old version as outdated.
On 2025-08-09 the Customer Id of Terrance was added again. So we have the old version valid from 2025-05-28 to 2025-07-06 and a new version valid since 2025-08-09. The name of Berry/Barry got corrected again.

```
│ 56b3cEA1E6A49F1 │ Barry      │ 2025-05-28 00:00:00 │ 2025-07-06 00:00:00 │ N       │
│ eF43a70995dabAB │ Terrance   │ 2025-05-28 00:00:00 │ 2025-07-06 00:00:00 │ N       │
│ FaE5E3c1Ea0dAf6 │ Fritz      │ 2025-07-06 00:00:00 │ 2025-08-09 00:00:00 │ N       │
│ 56b3cEA1E6A49F1 │ Berry      │ 2025-07-06 00:00:00 │ 2025-08-09 00:00:00 │ N       │
│ eF43a70995dabAB │ Terrance   │ 2025-08-09 00:00:00 │ 9999-12-31 00:00:00 │ Y       │
│ 56b3cEA1E6A49F1 │ Barry      │ 2025-08-09 00:00:00 │ 9999-12-31 00:00:00 │ Y       │
```

### History table

Read a source CSV file, compare it with the latest version in the target and append the differences.

```
duckdb.execute("create or replace table csv_data as (SELECT *, '?' as __change_type, "
               "current_localtimestamp() as change_date 
               "FROM 'testdata/customers-100000.csv')")
df = Dataflow()
source_table = df.add(Table('csv_data', 'csv_data'))
tc = df.add(Comparison(source_table, detect_deletes=True, logical_pk_list=['Customer Id'],
                       columns_to_ignore=['change_date'],
                       order_column='change_date'))
duckdb.execute("create or replace table customer_output as "
               "(SELECT * FROM csv_data) with no data")

target_table = df.add(DuckDBTable(tc, "customer_output"))
tc.set_comparison_table(target_table)
df.start(duckdb)
```

After a couple of changes and executions, the target table contains the complete history of changed records.

```
│ 56b3cEA1E6A49F1 │ Barry      │ I             │ 2025-08-17 06:07:32.213 │
│ eF43a70995dabAB │ Terrance   │ I             │ 2025-08-17 06:07:32.213 │
│ FaE5E3c1Ea0dAf6 │ Fritz      │ I             │ 2025-08-17 13:45:32.521 │
│ 56b3cEA1E6A49F1 │ Berry      │ U             │ 2025-08-17 13:45:32.521 │
│ 56b3cEA1E6A49F1 │ Barry      │ B             │ 2025-08-17 17:24:29.193 │
│ eF43a70995dabAB │ Terrance   │ D             │ 2025-08-17 17:24:29.193 │
```

### Lookup

For cases where a source should be augmented with more data but the lookup table has multiple records matching the join condition, writing the proper SQL is difficult. The Lookup transform takes care of that.

In this example, the sales order table should get joined with the customer_history table. Trouble is, the fact only knows the customer_id and the customer_history table has many records.
We want to know the primary key - the version_id - of the history table for the customer_id that was valid at the order date.
Meaning, find all versions with a change_date less or equal the order_date, then sort these by change_date descending (`{"change_date": True}`) and pick the first found.
Yes, a tricky SQL join...

```
lookup = df.add(Lookup(sales_orders, customer_history,
                       {"version_id": "current_version_id"},
                       's.sold_to_customer_id = l."Customer Id" and s.order_date >= l.change_date",
                       {"change_date": True}
                       ))
```

### Query

And of course any valid SQL select can also be used at any place in the dataflow.

```
query_1 = df.add(Query("Query_1", "select * from delta_scan('tmp/deltalake/csv_data_copy')))
```

### Deltalake write

Writing the data into a DuckDB target table and then copying the contents into the target is one option. Common targets should be supported by the library.

DuckDB can and must be used to read the Deltalake table, here it is used as input to the Comparison transform, but writing it cannot. The DeltaLakeTable class can.

```
duckdb.execute("create or replace table csv_data as "
               "(SELECT * FROM 'testdata/customers-100000.csv')")
duckdb.execute("alter table csv_data add primary key (\"Customer Id\")")
df = Dataflow()
source_table = df.add(Table('csv_data', 'csv_data', pk_list=["Customer Id"]))
tc = df.add(Comparison(source_table, detect_deletes=True))
target_table = df.add(DeltaLakeTable("tmp/deltalake", tc, "csv_data_copy"))

# Create the table in deltalake
target_table.add_all_columns(source_table, duckdb)
target_table.create_table(duckdb)

comparison_table = Query('deltalake_table', 
                         "SELECT * FROM delta_scan('tmp/deltalake/csv_data_copy')")
tc.set_comparison_table(comparison_table)

df.start(duckdb)
```

## Transforms
 - Comparison: Take an input dataset (normal or CDC), compare with the target and create a delta dataset. This tells which records must be inserted, updated, optionally deleted and does remove all records that have not been changed.
 - SCD2: Take a CDC input and provide the changes to be applied in the target, e.g. Change the current version to an end-date of today and create a new version with start-date today.
 - Pivot/Unpivot: Supported by DuckDB natively. Turn a dataset with multiple columns into a dataset with multiple rows and vice versa, including multiple sets. Example: Source has 12 columns for REVENUE_JAN, REVENUE_FEB,... and 12 columns for QTY_JAN, QTY_FEB,... Result should be 12 rows for each input row.
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
