import logging
import unittest

import duckdb

from rtdi_ducktape.CDCTransforms import Comparison
from rtdi_ducktape.Dataflow import Dataflow
from rtdi_ducktape.LoaderDeltaLake import DeltaLakeTable
from rtdi_ducktape.Metadata import Table, Query

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
logger = logging.getLogger("rtdi_ducktape")


class DuckDBTests(unittest.TestCase):

    def test_cdc(self):
        """
        Target table has the same fields and a primary key specified
        :return:
        """
        duckdb.execute("create or replace table csv_data as (SELECT * FROM 'testdata/customers-100000.csv')")
        duckdb.execute("alter table csv_data add primary key (\"Customer Id\")")
        df = Dataflow()
        source_table = df.add(Table('csv_data', 'csv_data'))
        tc = df.add(Comparison(source_table, detect_deletes=True, logger=logger))
        duckdb.execute("create or replace table csv_data_copy as (SELECT * FROM csv_data) with no data")
        duckdb.execute("alter table csv_data_copy add primary key (\"Customer Id\")")
        target_table = df.add(DeltaLakeTable("./tmp/deltalake", tc, "csv_data_copy", logger=logger))
        target_table.add_all_columns(source_table)
        target_table.create_table(duckdb)

        comparison_table = Query('deltalake_table', "SELECT * FROM delta_scan('file://./tmp/deltalake')")
        tc.set_comparison_table(comparison_table)

        tc.set_show_columns(
            ['"Customer Id"', '"First Name"', "__change_type"])
        tc.set_show_where_clause(
            "\"Customer Id\" in ('FaE5E3c1Ea0dAf6', '56b3cEA1E6A49F1', 'eF43a70995dabAB')")

        df.start(duckdb)
        tc.show(duckdb, logger, "CDC table after execution")

        duckdb.execute("create or replace table csv_data as (SELECT * FROM 'testdata/customers-100000_change_01.csv')")
        df.start(duckdb)
        tc.show(duckdb, logger, "CDC table after execution")

        actual = set(target_table.get_show_data(duckdb))
        expected = {('eF43a70995dabAB', 'Terrance'), ('56b3cEA1E6A49F1', 'Barry')}
        self.assertEqual(actual, expected, "Datasets are different")


if __name__ == '__main__':
    unittest.main()
