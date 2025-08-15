import logging
from logging import Logger
from typing import Union, Iterable

from deltalake import DeltaTable, write_deltalake
import pyarrow as pa

from rtdi_ducktape.CDCTransforms import CHANGE_TYPE_COLUMN
from rtdi_ducktape.Loaders import Loader
from rtdi_ducktape.Metadata import Dataset, Table, create_join_condition
from rtdi_ducktape.SQLUtils import convert_list_to_str, quote_str


class DeltaLakeTable(Loader):

    def __init__(self, root_url: str, source: Dataset, table_name: str, name: Union[None, str] = None,
                 pk_list: Union[None, Iterable[str]] = None, allow_evolution: bool = False,
                 is_cdc: bool = False, generated_key_column: Union[None, str] = None, start_value: Union[None, int] = None,
                 logger: Union[None, Logger] = None):
        super().__init__(source, table_name, name, pk_list, allow_evolution,
                         is_cdc, generated_key_column, start_value, logger)
        self.root_url = root_url

    def execute(self, duckdb):
        cols = self.source.get_cols(duckdb)
        seq_value_str = ""
        if self.generated_key_column is not None:
            sequence_name = self.table_name + "_seq"
            sql = f"create or replace sequence {quote_str(sequence_name)} start {self.get_generated_key_start(duckdb)}"
            self.logger.debug(f"GenerateKey() - Creating the sequence for the key: <{sql}>")
            duckdb.execute(sql)
            gen_column = quote_str(self.generated_key_column)
            if self.source.is_cdc:
                seq_value_str = f", case when {CHANGE_TYPE_COLUMN} = 'I' then nextval('{sequence_name}') else {gen_column} end as {gen_column}"
            else:
                seq_value_str = f", coalesce({gen_column}, nextval('{sequence_name}')) as {gen_column}"
            cols.discard(quote_str(self.generated_key_column))
        cols_str = convert_list_to_str(cols)
        sql = f"""with source as ({self.source.get_sub_select_clause()}) 
               SELECT {cols_str}{seq_value_str} from source
            """
        data = duckdb.sql(sql).arrow()
        join_condition = create_join_condition(self.pk_list, 's', 't')
        self.logger.debug(
            f"DeltaLakeTable() - Join condition for the delta merge is <{join_condition}>")


        update_map = dict()
        for col in cols:
            if col not in self.pk_list:
                update_map[col] = f"s.{col}"
        insert_map = update_map.copy()
        if self.generated_key_column is not None:
            insert_map[self.generated_key_column] = f"s.{self.generated_key_column}"

        dt = DeltaTable(f"{self.root_url}/{self.table_name}")
        if self.source.is_cdc and not self.is_cdc:
            (dt.merge(source=data, predicate=join_condition, source_alias='s', target_alias='t')
             .when_matched_delete(predicate="s.__change_type = 'D'")
             .when_matched_update(
                updates = update_map,
                predicate="s.__change_type = 'U'"
              )
             .when_not_matched_insert(
                updates = insert_map,
                predicate="s.__change_type = 'I'"
              )
             ).execute()
        elif self.pk_list is not None:
            (dt.merge(source=data, predicate=join_condition, source_alias='s', target_alias='t')
             .when_matched_update(
                updates = update_map
              )
             .when_not_matched_insert(
                updates = insert_map
              )
             ).execute()
        else:
            write_deltalake(f"{self.root_url}/{self.table_name}", data, mode="append")

    def get_generated_key_start(self, duckdb):
        if self.start_value is not None:
            return self.start_value
        elif self.generated_key_column is not None:
            sql = f"select max({quote_str(self.generated_key_column)}) from delta_scan('{self.root_url}/{self.table_name}')"
            self.logger.debug(
                f"DeltaLakeTable() - No start value provided, reading the max({self.generated_key_column}) value "
                f"from {self.root_url}/{self.table_name}: <{sql}>")
            res = duckdb.execute(sql).fetchall()
            start_value = res[0][0]
            if start_value is None:
                start_value = 1
            else:
                start_value += 1
            return start_value

    def create_table(self, duckdb):
        table = self.schema.empty_table()
        write_deltalake(f"{self.root_url}/{self.table_name}", table, mode="overwrite")
