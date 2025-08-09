import logging
from logging import Logger
from typing import Union, Iterable

from Metadata import Dataset, Table
from SQLUtils import empty, get_table_primary_key, get_table_cols, convert_list_to_str, quote_str


class DuckDBApplier:

    def __init__(self, source: Dataset, target: Table, target_pk_list: Union[None, Iterable[str]] = None,
                 logger: Union[None, Logger] = None):
        """
        Write the data into the target table. If the source dataset is a CDC source, perform the insert-update-delete
        statements, else an upsert.
        The primary key is either read from the target table or must be provided. The logical pk of the source is
        not used, it must be the physical pk of the target!

        :param source: source dataset
        :param target: target table
        :param target_pk_list: optional pk list in case the target does not support PKs
        :param logger: logger
        """
        if not isinstance(target, Table):
            raise RuntimeError(f"Target {target} must be a table")
        if logger is None:
            self.logger = logging.getLogger("TableComparison")
        else:
            self.logger = logger

        self.source = source
        self.target = target
        self.target_pk_list = target_pk_list

    def execute(self, duckdb):
        target_table = self.target.table_name
        target_table_name = quote_str(target_table)
        if self.target_pk_list is None:
            self.logger.debug(f"DuckDBApplier() - No logical primary key provided, reading the pk "
                              f"of the target table {target_table}...")
            self.target_pk_list = get_table_primary_key(duckdb, self.logger, target_table)
            self.logger.debug(f"DuckDBApplier() - Target table {target_table} has the primary "
                              f"key columns {self.target_pk_list}")
        if self.target_pk_list is None:
            raise RuntimeError("Applier requires primary keys to know the where-condition of "
                               "the update and delete statement")

        cols = get_table_cols(duckdb, target_table)
        cols_str = convert_list_to_str(cols)
        if self.source.is_cdc:
            update_set_str = ""
            for col in cols:
                if col not in self.target_pk_list:
                    if len(update_set_str) > 0:
                        update_set_str += ", "
                    update_set_str += f"{col} = s.{col}"
            pk_list_str = convert_list_to_str(self.target_pk_list)
            join_condition = ""
            for col in self.target_pk_list:
                if len(join_condition) > 0:
                    join_condition += " and "
                join_condition += f"s.{col} = {target_table_name}.{col}"
            sql = f"""with source as ({self.source.get_sub_select_clause()}) 
                   INSERT INTO {target_table_name}({cols_str})
                   SELECT {cols_str} from source
                   where \"__change_type\" = 'I'
                """
            self.logger.debug(f"DuckDBApplier() - Insert all __change_type='I' rows via the SQL <{sql}>")
            duckdb.execute(sql)
            sql = f"""with source as ({self.source.get_sub_select_clause()})
                   UPDATE {target_table_name} set {update_set_str} from source s
                   where {join_condition} and s.\"__change_type\" = 'U'
                """
            self.logger.debug(f"DuckDBApplier() - Update all __change_type='U' rows in the target via the SQL <{sql}>")
            duckdb.execute(sql)
            sql = f"""with source as ({self.source.get_sub_select_clause()})
                   DELETE FROM {target_table_name}
                   where {pk_list_str} in (SELECT {pk_list_str} from source where \"__change_type\" = 'D')
                """
            self.logger.debug(f"DuckDBApplier() - Delete all __change_type='D' rows in the target via the SQL <{sql}>")
            duckdb.execute(sql)
        else:
            pk = get_table_primary_key(duckdb, self.logger, target_table)
            if pk is not None:
                sql = f"""with source as ({self.source.get_sub_select_clause()}) 
                       INSERT OR REPLACE INTO {target_table_name}({cols_str})
                       SELECT {cols_str} from source
                    """
                self.logger.debug(f"DuckDBApplier() - Upsert all rows via the SQL <{sql}>")
                duckdb.execute(sql)


