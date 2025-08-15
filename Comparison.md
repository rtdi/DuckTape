# Comparison

## Qualities

 - Inputs: One Dataset (table or query)
 - Output: One table in CDC format
 - Output structure: The full structure of the comparison table, instead of just the input columns

## Use cases

 - Input is compared with the target table of same structure to find out all changed records.
 - Input has less columns than the comparison table, hence only the columns of the input count.
 - Input has a column last_changed_timestamp set to current_timestamp. Hence all records will be different because at least the timestamp will be different always. By setting adding that column to the columns_to_ignore parameter, this column is not considered in the comparison logic.
 - Input has the primary key production_order_id with the current status, target table should have all status values. Hence the comparison will find multiple record where source.production_order_id = comparison.production_order_id and it should compare with the row having the highest ID value. Hence the order_column parameter is set to ID.
 - Target table has a valid-from and valid-to date range. Similar to above, one input record has multiple matching records in the comparison table, but it should be compared with the last but with the one that has a valid-to date of 9999-12-31.  

## Example

```
        # manually create source and target table, with or without primary keys
        duckdb.execute("create or replace table csv_data as (SELECT * FROM 'testdata/customers-100000.csv')")
        duckdb.execute("create or replace table customer_output as (SELECT * FROM csv_data) with no data")
        
        df = Dataflow()
        
        # add the source table and specify a logical PK as the table has none
        source_table = df.add(Table('csv_data', 'csv_data', pk_list=['"Customer Id"']))
        
        # Comparison gets the source table as input
        tc = df.add(Comparison(source_table, detect_deletes=True, logger=logger))
        
        # and the target table has the comparison as input
        target_table = df.add(DuckDBTable(tc, "customer_output", logger=logger))
        
        # last step is setting the Comparison's table to compare with
        tc.set_comparison_table(target_table)
        
        # run the entire dataflow
        df.start(duckdb)

```