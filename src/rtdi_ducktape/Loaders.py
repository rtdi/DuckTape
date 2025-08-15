import logging
from logging import Logger
from typing import Union, Iterable

from .CDCTransforms import CHANGE_TYPE
from .Metadata import Dataset, Table, OperationalMetadata, create_join_condition
from .SQLUtils import convert_list_to_str, quote_str
import pyarrow as pa


class Loader(Table):

    def __init__(self, source: Dataset, table_name: str, name: Union[None, str] = None,
                 pk_list: Union[None, Iterable[str]] = None, allow_evolution: bool = False,
                 is_cdc: bool = False, generated_key_column: Union[None, str] = None, start_value: Union[None, int] = None,
                 logger: Union[None, Logger] = None):
        """
        A Loader is the last object in a dataflow, the target table.
        - If the source is a CDC table and this table loader is not, then the changes are applied.
        - If both are CDC tables, the data is appended.
        - If the target table has a primary and the source is not a CDC table, an upsert is performed
        - If source is not a CDC table and no primary key information is available, only an append is possible
        The Loader also has the option to fill a surrogate key if the generated key column is specified.

        :param source: The input of this step
        :param table_name: the table name of this target table
        :param name: the step name
        :param pk_list: the target table's (assumed) primary key
        :param allow_evolution: does the table support schema evolution?
        :param is_cdc: is this table one with CDC information
        :param generated_key_column: optional name of the generated key column, which is filled for all insert records
        :param start_value: If provided, this is the start value for the generated key column, else the max() is read from the table
        :param logger: logger to use
        """
        if name is None:
            name = f"Target table {table_name}"
        super().__init__(name, table_name, is_cdc=is_cdc, pk_list=pk_list, allow_evolution=allow_evolution)
        self.add_input(source)
        self.source = source
        self.generated_key_column = generated_key_column
        self.start_value = start_value
        if logger is None:
            self.logger = logging.getLogger("DuckDBTable")
        else:
            self.logger = logger

    def add_default_columns(self):
        if self.generated_key_column is not None:
            self.add_column(pa.field(self.generated_key_column, pa.int32(), True))
            self.set_pk_list([self.generated_key_column])
        if self.is_cdc:
            self.add_column(pa.field(CHANGE_TYPE, pa.string()))


class DuckDBTable(Loader):

    def __init__(self, source: Dataset, table_name: str, name: Union[None, str] = None,
                 pk_list: Union[None, Iterable[str]] = None, allow_evolution: bool = False,
                 is_cdc: bool = False, generated_key_column: Union[None, str] = None, start_value: Union[None, int] = None,
                 logger: Union[None, Logger] = None):
        """
        Write the data into the target table. If the source dataset is a CDC source, perform the insert-update-delete
        statements, else an upsert.
        The primary key is either read from the target table or must be provided. The logical pk of the source is
        not used, it must be the physical pk of the target!

        :param source: source dataset
        :param pk_list: optional pk list in case the target does not support PKs
        :param logger: logger
        """
        super().__init__(source, table_name, name, pk_list, allow_evolution,
                         is_cdc, generated_key_column, start_value, logger)

    def get_generated_key_start(self, duckdb):
        if self.start_value is not None:
            return self.start_value
        elif self.generated_key_column is not None:
            sql = f"select max({quote_str(self.generated_key_column)}) from {quote_str(self.table_name)}"
            self.logger.debug(
                f"DuckDBTable() - No start value provided, reading the max({self.generated_key_column}) value "
                f"from {self.table_name}: <{sql}>")
            res = duckdb.execute(sql).fetchall()
            start_value = res[0][0]
            if start_value is None:
                start_value = 1
            else:
                start_value += 1
            return start_value

    def execute(self, duckdb):
        self.last_execution = OperationalMetadata()
        target_table = self.table_name
        target_table_name = quote_str(target_table)
        table_pk_list = self.get_table_primary_key(duckdb)
        if self.pk_list is None:
            self.logger.debug(f"DuckDBTable() - No logical primary key provided, reading the pk "
                              f"of the target table {target_table}...")
            self.pk_list = table_pk_list
            if self.pk_list is None:
                self.logger.debug(f"DuckDBTable() - Target table {target_table} has no primary "
                                  f"key columns - data will be appended")
                use_table_pk = False
            else:
                self.logger.debug(f"DuckDBTable() - Target table {target_table} has the primary "
                                  f"key columns {self.pk_list}")
                use_table_pk = True
        elif self.pk_list == table_pk_list:
            use_table_pk = True
        else:
            use_table_pk = False

        cols = set(self.source.get_cols(duckdb))
        if CHANGE_TYPE not in self.get_cols(duckdb):
            cols.discard(CHANGE_TYPE)

        gen_key_str = ""
        seq_value_str = ""
        if self.generated_key_column is not None:
            sequence_name = self.table_name + "_seq"
            sql = f"create or replace sequence {quote_str(sequence_name)} start {self.get_generated_key_start(duckdb)}"
            self.logger.debug(f"DuckDBTable() - Creating the sequence for the key: <{sql}>")
            duckdb.execute(sql)
            gen_key_str = ", " + quote_str(self.generated_key_column)
            seq_value_str = f", nextval('{sequence_name}')"
            cols.discard(self.generated_key_column)
        cols_str = convert_list_to_str(cols)

        if self.source.is_cdc and not self.is_cdc and self.pk_list is not None:
            update_set_str = ""
            for col in cols:
                if col not in self.pk_list and col != self.generated_key_column:
                    if len(update_set_str) > 0:
                        update_set_str += ", "
                    update_set_str += f"{quote_str(col)} = s.{quote_str(col)}"
            pk_list_str = convert_list_to_str(self.pk_list)
            join_condition = create_join_condition(self.pk_list, 's', target_table_name)
            sql = f"""with source as {self.source.get_sub_select_clause()} 
                   INSERT INTO {target_table_name}({cols_str}{gen_key_str})
                   SELECT {cols_str}{seq_value_str} from source
                   where \"__change_type\" = 'I'
                """
            self.logger.debug(f"DuckDBTable() - Insert all __change_type='I' rows via the SQL <{sql}>")
            duckdb.execute(sql)
            sql = f"""with source as {self.source.get_sub_select_clause()}
                   UPDATE {target_table_name} set {update_set_str} from source s
                   where {join_condition} and s.\"__change_type\" = 'U'
                """
            self.logger.debug(f"DuckDBTable() - Update all __change_type='U' rows in the target via the SQL <{sql}>")
            duckdb.execute(sql)
            sql = f"""with source as {self.source.get_sub_select_clause()}
                   DELETE FROM {target_table_name}
                   where {pk_list_str} in (SELECT {pk_list_str} from source where \"__change_type\" = 'D')
                """
            self.logger.debug(f"DuckDBTable() - Delete all __change_type='D' rows in the target via the SQL <{sql}>")
            duckdb.execute(sql)
            res = duckdb.execute(f"""
                with source as ({self.source.get_sub_select_clause()})
                select count(*) from source""").fetchall()
            self.last_execution.processed(res[0][0])
            self.logger.info(f"DuckDBTable() - {self.last_execution}")
        else:
            if use_table_pk:
                # Upsert using the primary key
                sql = f"""with source as {self.source.get_sub_select_clause()} 
                       INSERT OR REPLACE INTO {target_table_name}({cols_str})
                       SELECT {cols_str} from source
                    """
                self.logger.debug(f"DuckDBTable() - Upsert all rows via the SQL <{sql}>")
                duckdb.execute(sql)
                res = duckdb.execute(f"""
                    with source as ({self.source.get_sub_select_clause()})
                    select count(*) from source""").fetchall()
                self.last_execution.processed(res[0][0])
                self.logger.info(f"DuckDBTable() - {self.last_execution}")
            elif self.pk_list is not None:
                # Upsert using a logical primary key
                update_set_str = ""
                for col in cols:
                    if col not in self.pk_list and col != self.generated_key_column:
                        if len(update_set_str) > 0:
                            update_set_str += ", "
                        update_set_str += f"{col} = s.{col}"
                pk_list_str = convert_list_to_str(self.pk_list)
                join_condition = create_join_condition(self.pk_list, 's', target_table_name)

                sql = f"""with source as {self.source.get_sub_select_clause()}
                       UPDATE {target_table_name} set {update_set_str} from source s
                       where {join_condition}
                    """
                self.logger.debug(f"DuckDBTable() - Updated all matching existing rows via the SQL <{sql}>")
                duckdb.execute(sql)

                sql = f"""with source as {self.source.get_sub_select_clause()} 
                       INSERT INTO {target_table_name}({cols_str})
                       SELECT {cols_str} from source 
                       where {pk_list_str} not in (select {pk_list_str} from {target_table_name})
                    """
                self.logger.debug(f"DuckDBTable() - Inserted all new rows via the SQL <{sql}>")
                duckdb.execute(sql)

                res = duckdb.execute(f"""
                    with source as ({self.source.get_sub_select_clause()})
                    select count(*) from source""").fetchall()
                self.last_execution.processed(res[0][0])
                self.logger.info(f"DuckDBTable() - {self.last_execution}")
            else:
                sql = f"""with source as {self.source.get_sub_select_clause()} 
                       INSERT INTO {target_table_name}({cols_str}{gen_key_str})
                       SELECT {cols_str}{seq_value_str} from source
                    """
                self.logger.debug(f"DuckDBTable() - Insert all rows via the SQL <{sql}>")
                duckdb.execute(sql)
                res = duckdb.execute(f"""
                    with source as {self.source.get_sub_select_clause()}
                    select count(*) from source""").fetchall()
                self.last_execution.processed(res[0][0])
                self.logger.info(f"DuckDBTable() - {self.last_execution}")


