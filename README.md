# PythonDataIntegration
Let's build a data integration tool based on Python for flexibility and DuckDB for performance.

## Scope

 1. Python and DuckDB can be used to read data into DuckDB. Within DuckDB SQL based transformations are applied. The result is written into a Target, again either using DuckDB's native features or Python. `Source --> DuckDB --> SQL --> Target`
 2. Delta handling as a first class citicen, meaning the structure supports before and after image, change indicators, can create the delta by comparing with the target and can be connected to CDC readers
 3. Certain transformations are hard to do in SQL, either because the SQL statement is complicated, or it can be expressed in 3GL code more easily or it needs multiple SQL statements. For these Python methods are implemented to achieve these transformations with a single function call.
 4. User interaction levels are
    - Via code - The user writes the Python code and thanks to the APIs, can be very quick.
    - Via UI - The user does drag and drop the transformations and with that configures the SQL statements and the sequence of transformations. Important: Code changes the UI representation and UI represeentation changes the code. The only thing stored is the code, the UI is just a visualization of the code in a different format.
    - Via intend - The user tells the source and the target properties and the transformations required to achieve that target, are obvious. Example: Read the customer master and create a Slow-Changing-Dimension table with its data. It's obvious what has to be done.
    - Via prompt - The user feeds the functional spec and the code is generated.

## Transforms
 - Table Comparison: Take an input dataset (normal or CDC), compare with the target and create a delta dataset. This tells which records must be inserted, updated, optionally deleted and does remove all records that gave not been changed.
 - SCD2 Generation: Take a CDC input and provide the changes to be applied in the target, e.g. Change the current version to an end-date of today and create create a new version with start-date today.
 - Pivot/Unpivot: Turn a dataset with multiple columnms into a dataset with multiple rows and vice versa, including multiple sets. Example: Source has 12 columns for REVENUE_JAN, REVENUE_FEB,... and 12 columns for QTY_JAN, QTY_FEB,... Result should be 12 rows for each input row.
 - Hierarchy handling: Multiple transforms to deal with parent_child hierarchy tables, hierarchy column (City, region, country) and convert between these representations. Also validate hierarchies to ensure there are no loops in a parent child based hierarchy.
 - Temporal Join: Two datasets must be joined and each as a valid-from/valid-to date. Create a list of dates when something changed and join the data from both tables.
 - CDC operations: A transform capable of changing the CDC information and do calculations on the change data. Example: If the ACCOUNT_BALANCE changed from 100USD to 500USD, what is the increase? The after image value minus the before image value.
 - Address validation: Pass an address to an external service like Google Maps API to validate, correct and standardize the address.
 - Data Validation: Apply rules to the data and check if the record does as PASS/FAIL/WARN for each rule and overall.

## Operational Metadata and Validation
 - Each dataflow creates operational statistics, rows read, rows outputted, execution time, number of PASS/FAIL/WARN records.
 - The dataflow can have safeguard based on the statistics, e.g. if less than 100'000 rows are processed, consider the dataflow as failed.
