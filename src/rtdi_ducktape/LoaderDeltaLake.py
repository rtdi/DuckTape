from typing import Union, Iterable

from deltalake import DeltaTable, write_deltalake

from Dataflow import logger
from rtdi_ducktape.CDCTransforms import CHANGE_TYPE_COLUMN, CHANGE_TYPE
from rtdi_ducktape.Loaders import Loader
from rtdi_ducktape.Metadata import Dataset, create_join_condition
from rtdi_ducktape.SQLUtils import convert_list_to_str, quote_str, empty


class DeltaLakeTable(Loader):

    def __init__(self, root_url: str, source: Dataset, table_name: str, name: Union[None, str] = None,
                 pk_list: Union[None, Iterable[str]] = None, allow_evolution: bool = False,
                 is_cdc: bool = False, generated_key_column: Union[None, str] = None, start_value: Union[None, int] = None,
                 storage_options = None):
        super().__init__(source, table_name, name, pk_list, allow_evolution,
                         is_cdc, generated_key_column, start_value)
        self.root_url = root_url
        if empty(pk_list):
            self.pk_list = None
        if pk_list is None and source.pk_list is not None:
            self.pk_list = source.pk_list
            logger.debug(f"DeltalakeTable() - No logical primary key provided, using the pk of "
                              f"the input {source}: {source.pk_list}")
        self.storage_options = storage_options


    def execute(self, duckdb):
        cols = set(self.source.get_cols(duckdb))

        seq_value_str = ""
        if self.generated_key_column is not None:
            sequence_name = self.table_name + "_seq"
            sql = f"create or replace sequence {quote_str(sequence_name)} start {self.get_generated_key_start(duckdb)}"
            logger.debug(f"GenerateKey() - Creating the sequence for the key: <{sql}>")
            duckdb.execute(sql)
            gen_column = quote_str(self.generated_key_column)
            if self.source.is_cdc:
                seq_value_str = f", case when {CHANGE_TYPE_COLUMN} = 'I' then nextval('{sequence_name}') else {gen_column} end as {gen_column}"
            else:
                seq_value_str = f", coalesce({gen_column}, nextval('{sequence_name}')) as {gen_column}"
            cols.discard(self.generated_key_column)
        cols_str = convert_list_to_str(cols)
        update_map = dict()
        insert_map = dict()
        where_str = ""
        for col in cols:
            if col != CHANGE_TYPE:
                if col not in self.pk_list:
                    update_map[col] = f"s.{quote_str(col)}"
                insert_map[col] = f"s.{quote_str(col)}"
            else:
                where_str = "where __change_type in ('I', 'U', 'D')"
        if self.generated_key_column is not None:
            insert_map[self.generated_key_column] = f"s.{quote_str(self.generated_key_column)}"

        sql = f"""with source as ({self.source.get_sub_select_clause()}) 
               SELECT {cols_str}{seq_value_str} from source {where_str}
            """
        data = duckdb.sql(sql).arrow()

        dt = DeltaTable(f"{self.root_url}/{self.table_name}", storage_options=self.storage_options)
        if self.pk_list is not None:
            join_condition = create_join_condition(self.pk_list, 's', 't')
            logger.debug(f"DeltaLakeTable() - Join condition for the delta merge is <{join_condition}>")
            if self.source.is_cdc and not self.is_cdc:
                result = (dt.merge(source=data, predicate=join_condition, source_alias='s', target_alias='t')
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
                logger.info(f"DeltaLakeTable written from CDC: {result}")
            else:
                result = (dt.merge(source=data, predicate=join_condition, source_alias='s', target_alias='t')
                 .when_matched_update(
                    updates = update_map
                  )
                 .when_not_matched_insert(
                    updates = insert_map
                  )
                 ).execute()
                logger.info(f"DeltaLakeTable written via primary key: {result}")
        else:
            write_deltalake(f"{self.root_url}/{self.table_name}", data, mode="append")
            logger.info(f"DeltaLakeTable appended")
        logger.info(f"DeltaLakeTable compacted: {dt.optimize.compact()}")
        logger.info(f"DeltaLakeTable vacuum:"
                         f" {dt.vacuum(dry_run=False, retention_hours=0, enforce_retention_duration=False, full=True)}")


    def get_generated_key_start(self, duckdb):
        if self.start_value is not None:
            return self.start_value
        elif self.generated_key_column is not None:
            sql = f"select max({quote_str(self.generated_key_column)}) from delta_scan('{self.root_url}/{self.table_name}')"
            logger.debug(
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
        write_deltalake(f"{self.root_url}/{self.table_name}", table, mode="overwrite",
                        storage_options=self.storage_options)

    def get_cols(self, db) -> set[str]:
        dt = DeltaTable(f"{self.root_url}/{self.table_name}", storage_options=self.storage_options)
        table_schema = dt.schema()
        return set(table_schema.to_arrow().names)

    def get_table_primary_key(self, db) -> Union[None, set[str]]:
        return None

    def show(self, duckdb, heading: Union[None, str] = None):
        where = ""
        if self.where_clause is not None:
            where = " where " + self.where_clause
        sql = f"""
        select {self.show_projection} from delta_scan('{self.root_url}/{self.table_name}') {where}
        """
        if heading is not None:
            print(heading)
        print(f"Query executed: {sql}")
        duckdb.sql(sql).show(max_width=200)

    def get_show_data(self, duckdb):
        where = ""
        if self.where_clause is not None:
            where = " where " + self.where_clause
        sql = f"""
        select {self.show_projection} from delta_scan('{self.root_url}/{self.table_name}') {where}
        """
        return duckdb.execute(sql).fetchall()

    def create_schema(self, db):
        data = db.table(f"delta_scan('{self.root_url}/{self.table_name}')").arrow()
        self.schema = data.schema

    def get_schema(self, duckdb):
        if self.schema is None:
            self.create_schema(duckdb)
        return self.schema
