import logging
from datetime import datetime, timezone
from typing import Iterable, Union
import duckdb

from CDCTransforms import TableComparison, SCD2, GenerateKey
from Metadata import Dataset
from SQLUtils import quote_str, convert_list_to_str, get_table_cols, get_table_primary_key, empty

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
logger = logging.getLogger("ducktape")


def process():

    termination_date = datetime.strptime('9999-12-31', '%Y-%m-%d')

    duckdb.execute("CREATE TABLE customer_input AS SELECT * FROM 'testdata/customers-100000.csv'")
    duckdb.execute("ALTER TABLE customer_input add primary key (\"Customer Id\")")
    duckdb.execute("CREATE TABLE customer_output AS FROM customer_input with no data")
    duckdb.execute("ALTER TABLE customer_output add version_id integer")
    duckdb.execute("ALTER TABLE customer_output add start_date datetime")
    duckdb.execute("ALTER TABLE customer_output add end_date datetime")
    duckdb.execute("ALTER TABLE customer_output add current varchar(1)")
    duckdb.execute("ALTER TABLE customer_output add primary key (version_id)")
    source_table = Dataset("customer_input", "customer_input")
    target_table = Dataset("customer_output", "customer_output")

    tc = TableComparison(source_table, target_table, end_date_column="end_date", termination_date=termination_date,
                         detect_deletes=True, order_column="version_id", logger=logger)
    output_table = tc.output_table

    scd2 = SCD2(output_table, 'start_date', 'end_date', datetime.now(timezone.utc),
         termination_date=termination_date,
         current_flag_column='current', current_flag_set='Y', current_flag_unset='N', logger=logger)
    gk = GenerateKey(output_table, target=target_table, logger=logger)

    source_table.set_show_columns(['"Customer Id"', '"First Name"'])
    source_table.set_show_where_clause("\"Customer Id\" in ('FaE5E3c1Ea0dAf6', '56b3cEA1E6A49F1', 'eF43a70995dabAB')")

    output_table.set_show_columns(['"Customer Id"', '"First Name"', "version_id", "start_date", "end_date", "current", "__change_type"])
    output_table.set_show_where_clause("\"Customer Id\" in ('FaE5E3c1Ea0dAf6', '56b3cEA1E6A49F1', 'eF43a70995dabAB')")

    target_table.set_show_columns(['"Customer Id"', '"First Name"', "version_id", "start_date", "end_date", "current"])
    target_table.set_show_where_clause("\"Customer Id\" in ('FaE5E3c1Ea0dAf6', '56b3cEA1E6A49F1', 'eF43a70995dabAB')")

    source_table.show(duckdb, logger)
    tc.execute(duckdb)
    output_table.show(duckdb, logger, "CDC table after table_comparison()")
    scd2.execute(duckdb)
    output_table.show(duckdb, logger, "CDC table after scd2()")
    gk.execute(duckdb)
    output_table.show(duckdb, logger, "CDC table after generate_key()")
    apply(output_table, target_table)
    target_table.show(duckdb, logger)

    duckdb.execute("TRUNCATE TABLE customer_input")
    duckdb.execute("INSERT INTO customer_input SELECT * FROM 'testdata/customers-100000_change_01.csv'")
    source_table.show(duckdb, logger)
    tc.execute(duckdb)
    output_table.show(duckdb, logger, "CDC table after table_comparison()")
    scd2.execute(duckdb)
    output_table.show(duckdb, logger, "CDC table after scd2()")
    gk.execute(duckdb)
    output_table.show(duckdb, logger, "CDC table after generate_key()")
    apply(output_table, target_table)
    target_table.show(duckdb, logger)

    duckdb.execute("TRUNCATE TABLE customer_input")
    duckdb.execute("INSERT INTO customer_input SELECT * FROM 'testdata/customers-100000.csv'")
    source_table.show(duckdb, logger)
    tc.execute(duckdb)
    output_table.show(duckdb, logger, "CDC table after table_comparison()")
    scd2.execute(duckdb)
    output_table.show(duckdb, logger, "CDC table after scd2()")
    gk.execute(duckdb)
    output_table.show(duckdb, logger, "CDC table after generate_key()")
    apply(output_table, target_table)
    target_table.show(duckdb, logger)


def apply(source: Dataset, target: Dataset, logical_pk_list: Union[None, Iterable[str]] = None):
    if not target.is_persisted():
        raise RuntimeError(f"Target {target} must be a table")
    target_table = target.table_name
    if logical_pk_list is None:
        logger.debug(f"apply() - No logical primary key provided, reading the pk of the target table {target_table}...")
        logical_pk_list = get_table_primary_key(duckdb, logger, target_table)
        logger.debug(f"apply() - Target table {target_table} has the primary key columns {logical_pk_list}")
    if logical_pk_list is None or empty(logical_pk_list):
        logger.error("apply() - Applier requires primary keys to know the where-condition of the update and delete statement")
        raise RuntimeError("Applier requires primary keys to know the where-condition of the update and delete statement")

    cols = get_table_cols(duckdb, target_table)
    cols_str = convert_list_to_str(cols)
    update_set_str = ""
    for col in cols:
        if col not in logical_pk_list:
            if len(update_set_str) > 0:
                update_set_str += ", "
            update_set_str += f"{col} = s.{col}"
    pk_list_str = convert_list_to_str(logical_pk_list)
    join_condition = ""
    for col in logical_pk_list:
        if len(join_condition) > 0:
            join_condition += " and "
        join_condition += f"s.{col} = {quote_str(target_table)}.{col}"
    sql = (f"with source as ({source.get_sub_select_clause()}) "
           f"INSERT INTO {quote_str(target_table)}({cols_str}) "
           f"SELECT {cols_str} from source "
           f"where \"__change_type\" = 'I'")
    logger.debug(f"apply() - Insert all __change_type='I' rows via the SQL <{sql}>")
    duckdb.execute(sql)
    sql = (f"with source as ({source.get_sub_select_clause()}) "
           f"UPDATE {quote_str(target_table)} set {update_set_str} from source s "
           f"where {join_condition} and \"__change_type\" = 'U'")
    logger.debug(f"apply() - Update all __change_type='U' rows in the target via the SQL <{sql}>")
    duckdb.execute(sql)
    sql = (f"with source as ({source.get_sub_select_clause()}) "
           f"DELETE FROM {quote_str(target_table)} "
           f"where {pk_list_str} in (SELECT {pk_list_str} from source where \"__change_type\" = 'D')")
    logger.debug(f"apply() - Delete all __change_type='D' rows in the target via the SQL <{sql}>")
    duckdb.execute(sql)


if __name__ == '__main__':
    process()