from typing import Union

from SQLUtils import quote_str, convert_list_to_str


class OperationalMetadata:

    def __init__(self):
        self.rows_processed = 0 # type int




class Dataset:

    def __init__(self, dataset_name: str, from_clause: str, is_cdc: bool = False):
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

    def show(self, duckdb):
        where = ""
        if self.where_clause is not None:
            where = " where " + self.where_clause
        sql = f"""
        with tab as {self.get_sub_select_clause()}
        select {self.show_projection} from tab {where}
        """
        if self.is_persisted():
            print(f"Table: {self.table_name}")
        else:
            print(self.sql)
        print(duckdb.sql(sql).show(max_width=200))
