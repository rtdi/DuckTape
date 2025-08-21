from typing import Union

from rtdi_ducktape.Dataflow import logger
from rtdi_ducktape.Metadata import Dataset
from rtdi_ducktape.SQLUtils import convert_list_to_str, convert_to_order_clause


class Lookup(Dataset):

    def __init__(self, source: Dataset, lookup_in: Dataset, return_columns: dict[str, str], join_condition: str,
                 order_columns: Union[None, dict[str, bool]],
                 name: Union[None, str] = None):
        """
        The idea is to return for each source record exactly one output record - the row count should not change.
        Joining the input dataset could lead to one matched record, multiple match candidates or none.
        For the non-case a left join is used, for the multiple a row_number window function.

        Important: It should not create a lookup set first but for each input row find all match candidates and pick
        the best suited one (the first thanks to the order clause). This then supports complex join clauses like

        lookup.customer_id = source.customer_id and lookup.modify_date >= source.start_date; order by start_date desc

        :param source:
        :param lookup_in:
        :param return_columns:
        :param join_condition:
        :param order_columns:
        :param name:
        """
        if name is None:
            name = f"Lookup for row in dataset {source.name}"
        super().__init__(name)
        self.add_input(source)
        self.source = source
        self.lookup_in = lookup_in
        self.return_columns = return_columns
        self.join_condition = join_condition
        self.order_columns = order_columns
        self.sql = None

    def is_persisted(self) -> bool:
        return False

    def get_sub_select_clause(self) -> str:
        return f"({self.sql})"

    def execute(self, duckdb):
        return_columns_str = convert_list_to_str(self.return_columns.keys(), qualifier="l", aliases=self.return_columns)
        source_cols_str = convert_list_to_str(self.source.get_cols(duckdb))
        return_column_alias_str = convert_list_to_str(self.return_columns.values())
        projection = source_cols_str + ", " + return_column_alias_str
        order_str = convert_to_order_clause(self.order_columns, "l")
        # create a partial cartesian product between source and lookup, a rank partitioned by source row and
        # pick the first found record as the result row
        self.sql = f"""
            select {projection} from (
                with s as (select *, row_number() over () as __row_number from {self.source.get_sub_select_clause()}),
                l as {self.lookup_in.get_sub_select_clause()}
                select s.*, {return_columns_str}, row_number() over (partition by __row_number 
                    order by {order_str}) as __version_no
                from s
                left join l on ({self.join_condition})
            ) where __version_no = 1
        """
        logger.debug(f"SQL statement for the lookup: <{self.sql}>")

