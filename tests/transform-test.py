import logging
import unittest
from datetime import datetime

import duckdb

from rtdi_ducktape.CDCTransforms import Comparison, SCD2
from rtdi_ducktape.Dataflow import Dataflow
from rtdi_ducktape.Loaders import DuckDBTable
from rtdi_ducktape.Metadata import Table
from rtdi_ducktape.RowTransformations import Lookup

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)


class DuckDBTests(unittest.TestCase):

    def test_scd2(self):
        termination_date = datetime.strptime('9999-12-31', '%Y-%m-%d')

        duckdb.execute("create or replace table csv_data as (SELECT * FROM 'testdata/customers-100000.csv')")
        df = Dataflow()
        source_table = df.add(Table('csv_data', 'csv_data', pk_list=['Customer Id']))
        tc = df.add(Comparison(source_table, end_date_column="end_date",
                               termination_date=termination_date,
                               detect_deletes=True, order_column="version_id"))

        scd2 = df.add(SCD2(tc, 'start_date', 'end_date',
                    termination_date=termination_date,
                    current_flag_column='current', current_flag_set='Y', current_flag_unset='N'))
        target_table = df.add(DuckDBTable(scd2, "customer_output", generated_key_column='version_id',
                                          pk_list=['version_id']))
        target_table.add_all_columns(source_table, duckdb)
        scd2.add_default_columns(target_table)
        target_table.add_default_columns()
        target_table.create_table(duckdb)
        tc.set_comparison_table(target_table)

        source_table.set_show_columns(
            ['"Customer Id"', '"First Name"'])
        source_table.set_show_where_clause(
            "\"Customer Id\" in ('FaE5E3c1Ea0dAf6', '56b3cEA1E6A49F1', 'eF43a70995dabAB')")

        tc.set_show_columns(
            ['"Customer Id"', '"First Name"', "version_id", "start_date", "end_date", "current", "__change_type"])
        tc.set_show_where_clause(
            "\"Customer Id\" in ('FaE5E3c1Ea0dAf6', '56b3cEA1E6A49F1', 'eF43a70995dabAB')")

        target_table.set_show_columns(
            ['"Customer Id"', '"First Name"', "version_id", "start_date", "end_date", "current"])
        target_table.set_show_where_clause(
            "\"Customer Id\" in ('FaE5E3c1Ea0dAf6', '56b3cEA1E6A49F1', 'eF43a70995dabAB')")

        source_table.show(duckdb, "Source")
        df.start(duckdb)
        tc.show(duckdb, "CDC table after execution")
        target_table.show(duckdb, "Target table after apply")

        duckdb.execute("create or replace table csv_data as (SELECT * FROM 'testdata/customers-100000_change_01.csv')")
        source_table.show(duckdb, "Source")
        df.start(duckdb)
        tc.show(duckdb, "CDC table after execution")
        target_table.show(duckdb, "Target table after apply")

        duckdb.execute("create or replace table csv_data as (SELECT * FROM 'testdata/customers-100000.csv')")
        source_table.show(duckdb, "Source")
        df.start(duckdb)
        tc.show(duckdb, "CDC table after execution")
        target_table.show(duckdb, "Target table after apply")

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
        actual = set(target_table.get_show_data(duckdb))
        start_dates = {row[2] for row in actual}
        start_dates = start_dates.union({row[3] for row in actual})
        sorted_start_dates = sorted(start_dates)
        expected = {
            # run 1: record was created
            ('56b3cEA1E6A49F1', 'Barry',    sorted_start_dates[0], sorted_start_dates[1], 'N'),
            # run 1: record was created, run 2 record got deleted
            ('eF43a70995dabAB', 'Terrance', sorted_start_dates[0], sorted_start_dates[1], 'N'),
            # run 2: record was created
            ('FaE5E3c1Ea0dAf6', 'Fritz',    sorted_start_dates[1], sorted_start_dates[2], 'N'),
            # run 2: firstname changed
            ('56b3cEA1E6A49F1', 'Berry',    sorted_start_dates[1], sorted_start_dates[2], 'N'),
            # run 3: record was created again
            ('eF43a70995dabAB', 'Terrance', sorted_start_dates[2], sorted_start_dates[3], 'Y'),
            # run 3: firstname changed back to the original value
            ('56b3cEA1E6A49F1', 'Barry',    sorted_start_dates[2], sorted_start_dates[3], 'Y')
        }
        self.assertEqual(expected, actual, "Datasets are different")

    def test_tc_same(self):
        """
        Target table has the same fields and a primary key specified
        :return:
        """
        duckdb.execute("create or replace table csv_data as (SELECT * FROM 'testdata/customers-100000.csv')")
        df = Dataflow()
        source_table = df.add(Table('csv_data', 'csv_data', pk_list=['Customer Id']))
        tc = df.add(Comparison(source_table, detect_deletes=True))

        target_table = df.add(DuckDBTable(tc, "customer_output", pk_list=['Customer Id']))
        target_table.add_all_columns(source_table, duckdb)
        target_table.create_table(duckdb)
        tc.set_comparison_table(target_table)

        tc.set_show_columns(
            ['"Customer Id"', '"First Name"', "__change_type"])
        tc.set_show_where_clause(
            "\"Customer Id\" in ('FaE5E3c1Ea0dAf6', '56b3cEA1E6A49F1', 'eF43a70995dabAB')")

        target_table.set_show_columns(
            ['"Customer Id"', '"First Name"'])
        target_table.set_show_where_clause(
            "\"Customer Id\" in ('FaE5E3c1Ea0dAf6', '56b3cEA1E6A49F1', 'eF43a70995dabAB')")

        df.start(duckdb)
        tc.show(duckdb, "CDC table after execution")
        target_table.show(duckdb, "Target table after apply")

        duckdb.execute("create or replace table csv_data as (SELECT * FROM 'testdata/customers-100000_change_01.csv')")
        df.start(duckdb)
        tc.show(duckdb, "CDC table after execution")
        target_table.show(duckdb, "Target table after apply")

        duckdb.execute("create or replace table csv_data as (SELECT * FROM 'testdata/customers-100000.csv')")
        df.start(duckdb)
        tc.show(duckdb, "CDC table after execution")
        target_table.show(duckdb, "Target table after apply")

        actual = set(target_table.get_show_data(duckdb))
        expected = {('eF43a70995dabAB', 'Terrance'), ('56b3cEA1E6A49F1', 'Barry')}
        self.assertEqual(expected, actual, "Datasets are different")

    def test_tc_with_history(self):
        """
        Target table has the same fields and a primary key specified
        :return:
        """
        duckdb.execute("create or replace table csv_data as (SELECT *, '?' as __change_type, "
                       "current_localtimestamp() as change_date FROM 'testdata/customers-100000.csv')")
        df = Dataflow()
        source_table = df.add(Table('csv_data', 'csv_data'))
        tc = df.add(Comparison(source_table, detect_deletes=True, logical_pk_list=['Customer Id'],
                               columns_to_ignore=['change_date'], order_column='change_date'))
        duckdb.execute("create or replace table customer_output as (SELECT * FROM csv_data) with no data")

        target_table = df.add(DuckDBTable(tc, "customer_output"))
        tc.set_comparison_table(target_table)

        tc.set_show_columns(['"Customer Id"', '"First Name"', "__change_type"])
        tc.set_show_where_clause("\"Customer Id\" in ('FaE5E3c1Ea0dAf6', '56b3cEA1E6A49F1', 'eF43a70995dabAB')")

        target_table.set_show_columns(['"Customer Id"', '"First Name"', "__change_type", "change_date"])
        target_table.set_show_where_clause("\"Customer Id\" in ('FaE5E3c1Ea0dAf6', '56b3cEA1E6A49F1', "
                                           "'eF43a70995dabAB')")

        df.start(duckdb)
        tc.show(duckdb, "CDC table after execution")
        target_table.show(duckdb, "Target table after apply")

        duckdb.execute("create or replace table csv_data as (SELECT *, '?' as __change_type, "
                       "current_localtimestamp() as change_date FROM 'testdata/customers-100000_change_01.csv')")
        df.start(duckdb)
        tc.show(duckdb, "CDC table after execution")
        target_table.show(duckdb, "Target table after apply")

        duckdb.execute("create or replace table csv_data as (SELECT *, '?' as __change_type, "
                       "current_localtimestamp() as change_date FROM 'testdata/customers-100000.csv')")
        df.start(duckdb)
        tc.show(duckdb, "CDC table after execution")
        target_table.show(duckdb, "Target table after apply")

        target_table.set_show_columns(['"Customer Id"', '"First Name"', "__change_type"])

        actual = set(target_table.get_show_data(duckdb))
        # ├─────────────────┼────────────┼───────────────┤
        # │ 56b3cEA1E6A49F1 │ Barry      │ I             │
        # │ eF43a70995dabAB │ Terrance   │ I             │
        # │ FaE5E3c1Ea0dAf6 │ Fritz      │ I             │
        # │ 56b3cEA1E6A49F1 │ Berry      │ U             │
        # │ 56b3cEA1E6A49F1 │ Barry      │ B             │
        # │ eF43a70995dabAB │ Terrance   │ D             │
        # │ 56b3cEA1E6A49F1 │ Barry      │ U             │
        # │ 56b3cEA1E6A49F1 │ Berry      │ B             │
        # │ FaE5E3c1Ea0dAf6 │ Fritz      │ D             │
        # └─────────────────┴────────────┴───────────────┘
        expected = {('56b3cEA1E6A49F1', 'Barry', 'B'),
             ('56b3cEA1E6A49F1', 'Barry', 'I'),
             ('56b3cEA1E6A49F1', 'Barry', 'U'),
             ('56b3cEA1E6A49F1', 'Berry', 'B'),
             ('56b3cEA1E6A49F1', 'Berry', 'U'),
             ('FaE5E3c1Ea0dAf6', 'Fritz', 'D'),
             ('FaE5E3c1Ea0dAf6', 'Fritz', 'I'),
             ('eF43a70995dabAB', 'Terrance', 'D'),
             ('eF43a70995dabAB', 'Terrance', 'I')
        }
        self.assertEqual(expected, actual, "Datasets are different")

    def test_upsert(self):
        """
        Target table has a PK, hence an upsert statement can be used
        :return:
        """
        duckdb.execute("create or replace table csv_data as (SELECT * FROM 'testdata/customers-100000.csv')")
        duckdb.execute("alter table csv_data add primary key (\"Customer Id\")")
        duckdb.execute("create or replace table csv_data_copy as (SELECT * FROM csv_data) with no data")
        duckdb.execute("alter table csv_data_copy add primary key (\"Customer Id\")")
        df = Dataflow()
        source_table = df.add(Table('csv_data', 'csv_data'))
        target_table = df.add(DuckDBTable(source_table, "csv_data_copy"))

        target_table.set_show_columns(
            ['"Customer Id"', '"First Name"'])
        target_table.set_show_where_clause(
            "\"Customer Id\" in ('FaE5E3c1Ea0dAf6', '56b3cEA1E6A49F1', 'eF43a70995dabAB')")

        df.start(duckdb)
        target_table.show(duckdb, "Target table after apply")

        df.start(duckdb)
        target_table.show(duckdb, "Target table after apply")

        actual = set(target_table.get_show_data(duckdb))
        expected = {('eF43a70995dabAB', 'Terrance'), ('56b3cEA1E6A49F1', 'Barry')}
        self.assertEqual(expected, actual, "Datasets are different")

    def test_lookup(self):
        """
        Simple table copy with a lookup
        :return:
        """
        duckdb.execute("create or replace table csv_data as (SELECT * FROM 'testdata/customers-100000.csv')")
        duckdb.execute("create or replace table csv_data_copy as (SELECT * FROM csv_data) with no data")
        duckdb.execute("alter table csv_data_copy add min_customer_id string")
        duckdb.execute("alter table csv_data_copy add min_id_name string")
        df = Dataflow()
        source_table = df.add(Table('csv_data', 'csv_data'))
        lookup = df.add(Lookup(source_table, source_table,
                               {"Customer Id": "min_customer_id", "First Name": "min_id_name"},
                               "s.\"Customer Id\" = l.\"Customer Id\"",
                               {"Customer Id": True},
                               "min customer lookup"
                               ))
        target_table = df.add(DuckDBTable(lookup, "csv_data_copy"))

        target_table.set_show_columns(
            ['"Customer Id"', '"First Name"', "min_customer_id", "min_id_name"])
        target_table.set_show_where_clause(
            "\"Customer Id\" in ('FaE5E3c1Ea0dAf6', '56b3cEA1E6A49F1', 'eF43a70995dabAB')")

        df.start(duckdb)
        target_table.show(duckdb, "Target table after apply")

        actual = set(target_table.get_show_data(duckdb))
        expected = {
            ('56b3cEA1E6A49F1', 'Barry', '56b3cEA1E6A49F1', 'Barry'),
            ('eF43a70995dabAB', 'Terrance', 'eF43a70995dabAB', 'Terrance')
        }
        self.assertEqual(expected, actual, "Datasets are different")


if __name__ == '__main__':
    unittest.main()
