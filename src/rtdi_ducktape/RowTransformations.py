from typing import Union

from rtdi_ducktape.Metadata import Step, Dataset


class Lookup(Step):

    def __init__(self, lookup_in: Dataset, return_columns: dict[str, str], join_condition: str,
                 order_column: Union[None, dict[str, bool]]):
        super().__init__()