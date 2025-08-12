import unittest
import logging
from datetime import datetime

import duckdb

from rtdi_ducktape.CDCTransforms import TableComparison, SCD2, GenerateKey
from rtdi_ducktape.Dataflow import Dataflow
from rtdi_ducktape.Loaders import DuckDBApplier
from rtdi_ducktape.Metadata import Table, Query

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
logger = logging.getLogger("rtdi_ducktape")


class MyTestCase(unittest.TestCase):
    def test_all(self):
        termination_date = datetime.strptime('9999-12-31', '%Y-%m-%d')

        duckdb.execute("create table csv_data as (SELECT * FROM 'testdata/customers-100000.csv')")
        df = Dataflow()
        source_table = df.add(Table('csv_data', 'csv_data', pk_list=['"Customer Id']))
        source_query_1 = df.add(Query("query_1", "SELECT * FROM csv_data",
                                      inputs=[source_table], logical_pk_list=['"Customer Id"']))
        target_table = df.add(Table("customer_output", "customer_output"))

        tc = df.add(TableComparison(source_query_1, target_table, end_date_column="end_date",
                             termination_date=termination_date,
                             detect_deletes=True, order_column="version_id", logger=logger))

        scd2 = df.add(SCD2(tc, 'start_date', 'end_date',
                    termination_date=termination_date,
                    current_flag_column='current', current_flag_set='Y', current_flag_unset='N', logger=logger))
        gk = df.add(GenerateKey(scd2, target_table, surrogate_key_column="version_id", logger=logger))
        df.add(DuckDBApplier(scd2, target_table, logger=logger))
        target_table.add_all_columns(source_table, duckdb)
        scd2.add_default_columns(target_table)
        gk.add_default_columns(target_table)
        target_table.create_table(duckdb)

        tc.set_show_columns(
            ['"Customer Id"', '"First Name"', "version_id", "start_date", "end_date", "current", "__change_type"])
        tc.set_show_where_clause(
            "\"Customer Id\" in ('FaE5E3c1Ea0dAf6', '56b3cEA1E6A49F1', 'eF43a70995dabAB')")

        target_table.set_show_columns(
            ['"Customer Id"', '"First Name"', "version_id", "start_date", "end_date", "current"])
        target_table.set_show_where_clause(
            "\"Customer Id\" in ('FaE5E3c1Ea0dAf6', '56b3cEA1E6A49F1', 'eF43a70995dabAB')")

        df.start(duckdb)
        tc.show(duckdb, logger, "CDC table after execution")
        target_table.show(duckdb, logger, "Target table after apply")
        tc.completed()

        source_query_1 = Query("query_1", "SELECT * FROM 'testdata/customers-100000_change_01.csv'")
        tc.set_source(source_query_1)
        tc.start(duckdb)
        tc.show(duckdb, logger, "CDC table after execution")
        target_table.show(duckdb, logger, "Target table after apply")
        tc.completed()

        source_query_1 = Query("query_1", "SELECT * FROM 'testdata/customers-100000.csv'")
        tc.set_source(source_query_1)
        tc.start(duckdb)
        tc.show(duckdb, logger, "CDC table after execution")
        target_table.show(duckdb, logger, "Target table after apply")
        tc.completed()

        # ┌─────────────────┬────────────┬────────────────────────────┬────────────────────────────┬─────────┐
        # │   Customer Id   │ First Name │         start_date         │          end_date          │ current │
        # │     varchar     │  varchar   │         timestamp          │         timestamp          │ varchar │
        # ├─────────────────┼────────────┼────────────────────────────┼────────────────────────────┼─────────┤
        # │ 56b3cEA1E6A49F1 │ Barry      │ 2025-08-09 19:01:42.431554 │ 2025-08-09 19:01:43.591056 │ N       │
        # │ eF43a70995dabAB │ Terrance   │ 2025-08-09 19:01:42.431554 │ 2025-08-09 19:01:43.591056 │ N       │
        # │ FaE5E3c1Ea0dAf6 │ Fritz      │ 2025-08-09 19:01:43.591056 │ 2025-08-09 19:01:44.369191 │ N       │
        # │ 56b3cEA1E6A49F1 │ Berry      │ 2025-08-09 19:01:43.591056 │ 2025-08-09 19:01:44.369191 │ N       │
        # │ eF43a70995dabAB │ Terrance   │ 2025-08-09 19:01:44.369191 │ 9999-12-31 00:00:00        │ Y       │
        # │ 56b3cEA1E6A49F1 │ Barry      │ 2025-08-09 19:01:44.369191 │ 9999-12-31 00:00:00        │ Y       │
        # └─────────────────┴────────────┴────────────────────────────┴────────────────────────────┴─────────┘
        target_table.set_show_columns(['"Customer Id"', '"First Name"', "start_date", "end_date", "current"])
        actual = target_table.get_show_data(duckdb)
        start_dates = {row[2] for row in actual}
        sorted_start_dates = sorted(start_dates)
        expected = [
            # run 1: record was created
            ('56b3cEA1E6A49F1', 'Barry',    sorted_start_dates[0], sorted_start_dates[1], 'N'),
            # run 1: record was created, run 2 record got deleted
            ('eF43a70995dabAB', 'Terrance', sorted_start_dates[0], sorted_start_dates[1], 'N'),
            # run 2: record was created
            ('FaE5E3c1Ea0dAf6', 'Fritz',    sorted_start_dates[1], sorted_start_dates[2], 'N'),
            # run 2: firstname changed
            ('56b3cEA1E6A49F1', 'Berry',    sorted_start_dates[1], sorted_start_dates[2], 'N'),
            # run 3: record was created again
            ('eF43a70995dabAB', 'Terrance', sorted_start_dates[2], termination_date, 'Y'),
            # run 3: firstname changed back to the original value
            ('56b3cEA1E6A49F1', 'Barry',    sorted_start_dates[2], termination_date, 'Y')
        ]
        self.assertEqual(actual, expected, "Datasets are different")

if __name__ == '__main__':
    unittest.main()
