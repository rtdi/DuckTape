import datetime
from datetime import timezone
from logging import Logger
from typing import Union, Iterable

from SQLUtils import quote_str, convert_list_to_str


class OperationalMetadata:

    def __init__(self):
        self.rows_processed: int = 0
        self.start_time: datetime = datetime.datetime.now(timezone.utc)
        self.end_time: datetime = None
        self.execution_time: int = 0

    def processed(self, rows_processed: int):
        self.rows_processed = rows_processed
        self.end_time = datetime.datetime.now(timezone.utc)
        delta = self.end_time - self.start_time
        self.execution_time = delta.total_seconds()

    def __str__(self):
        throughput = None
        if self.execution_time > 0:
            throughput = self.rows_processed / self.execution_time
        return (f"started at {self.start_time}, ended at {self.end_time}, duration {self.execution_time}s, "
                f"rows processed {self.rows_processed}, throughput {throughput:.0f}rows/sec")


class Dataset:

    def __init__(self, dataset_name: str, from_clause: str, is_cdc: bool = False,
                 logical_pk_list: Union[None, Iterable[str]] = None):
        if from_clause is None:
            raise RuntimeError("Either the table name or the select statement must be specified as from clause")
        self.table_name = None
        self.sql = None
        if from_clause.lower().strip().startswith("select "):
            self.sql = from_clause
        else:
            self.table_name = from_clause
        self.is_cdc = is_cdc
        self.dataset_name = dataset_name
        self.show_projection = "*"
        self.where_clause = None
        self.logical_pk_list = logical_pk_list

    def is_persisted(self):
        return self.table_name is not None

    def get_sub_select_clause(self):
        if self.table_name is not None:
            return f"(select * from {quote_str(self.table_name)})"
        else:
            return f"({self.sql})"

    def set_show_columns(self, projection: list[str]):
        self.show_projection = convert_list_to_str(projection)

    def set_show_where_clause(self, clause):
        self.where_clause = clause

    def show(self, duckdb, logger: Logger, heading: Union[None, str] = None):
        where = ""
        if self.where_clause is not None:
            where = " where " + self.where_clause
        sql = f"""
        with tab as {self.get_sub_select_clause()}
        select {self.show_projection} from tab {where}
        """
        if heading is not None:
            logger.debug(heading)
        if self.is_persisted():
            logger.debug(f"Table: {self.table_name}")
        else:
            print(self.sql)
        logger.debug(duckdb.sql(sql).show(max_width=200))
