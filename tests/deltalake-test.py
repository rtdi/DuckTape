import logging
import shutil
import unittest

import duckdb

from rtdi_ducktape.CDCTransforms import Comparison
from rtdi_ducktape.Dataflow import Dataflow
from rtdi_ducktape.LoaderDeltaLake import DeltaLakeTable
from rtdi_ducktape.Metadata import Table, Query

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)


class DeltalakeTests(unittest.TestCase):

    def test_cdc(self):
        """
        Target table has the same fields and a primary key specified
        :return:
        """
        duckdb.execute("create or replace table csv_data as (SELECT * FROM 'testdata/customers-100000.csv')")
        duckdb.execute("alter table csv_data add primary key (\"Customer Id\")")
        df = Dataflow()
        source_table = df.add(Table('csv_data', 'csv_data', pk_list=["Customer Id"]))
        tc = df.add(Comparison(source_table, detect_deletes=True))
        target_table = df.add(DeltaLakeTable("tmp/deltalake", tc, "csv_data_copy"))
        shutil.rmtree('./tmp/deltalake', ignore_errors=True)
        target_table.add_all_columns(source_table, duckdb)
        target_table.create_table(duckdb)

        comparison_table = Query('deltalake_table', "SELECT * FROM delta_scan('tmp/deltalake/csv_data_copy')")
        tc.set_comparison_table(comparison_table)

        tc.set_show_columns(
            ['"Customer Id"', '"First Name"', "__change_type"])
        tc.set_show_where_clause(
            "\"Customer Id\" in ('FaE5E3c1Ea0dAf6', '56b3cEA1E6A49F1', 'eF43a70995dabAB')")

        target_table.set_show_columns(
            ['"Customer Id"', '"First Name"'])
        target_table.set_show_where_clause(
            "\"Customer Id\" in ('FaE5E3c1Ea0dAf6', '56b3cEA1E6A49F1', 'eF43a70995dabAB')")

        target_table.show(duckdb, "Target table before start")
        df.start(duckdb)
        tc.show(duckdb, "CDC table after execution")
        target_table.show(duckdb, "Target table")

        duckdb.execute("create or replace table csv_data as (SELECT * FROM 'testdata/customers-100000_change_01.csv')")
        df.start(duckdb)
        tc.show(duckdb, "CDC table after execution")
        target_table.show(duckdb, "Target table")

        actual = set(target_table.get_show_data(duckdb))
        expected = {('56b3cEA1E6A49F1', 'Berry'), ('FaE5E3c1Ea0dAf6', 'Fritz')}
        self.assertEqual(expected, actual, "Datasets are different")


if __name__ == '__main__':
    unittest.main()
