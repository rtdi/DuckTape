import datetime
from abc import ABC, abstractmethod
from collections import OrderedDict
from datetime import timezone
from enum import Enum
from logging import Logger
from typing import Union, Iterable
import re

from .SQLUtils import quote_str, convert_list_to_str


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
        throughput = 0
        if self.execution_time > 0:
            throughput = self.rows_processed / self.execution_time
        return (f"started at {self.start_time}, ended at {self.end_time}, duration {self.execution_time}s, "
                f"rows processed {self.rows_processed}, throughput {throughput:.0f}rows/sec")


class Step(ABC):

    def __init__(self, name: str):
        self.name: str = name
        self.description: Union[None, str] = None
        self.impact_lineage = None
        self.inputs: Union[None, set[Step]] = None
        self.outputs: Union[None, set[Step]] = None
        self.executed: bool = False
        self.execute_lock: bool = False
        self.last_execution: Union[None, OperationalMetadata] = None

    def add_input(self, step: "Step"):
        if self.inputs is None:
            self.inputs = {step}
            step.add_output(self)
        elif step in self.inputs:
            return
        else:
            self.inputs.add(step)
            step.add_output(self)

    def add_output(self, step: "Step"):
        if self.outputs is None:
            self.outputs = {step}
            step.add_input(self)
        elif step in self.outputs:
            return
        else:
            self.outputs.add(step)
            step.add_input(self)

    def start(self, duckdb):
        self.execute_lock = True
        if not self.executed:
            if self.inputs is not None:
                for step in self.inputs:
                    if not step.executed and not step.execute_lock:
                        step.start(duckdb)
            self.execute(duckdb)
            self.executed = True
        if self.outputs is not None:
            for step in self.outputs:
                if not step.executed and not step.execute_lock:
                    step.start(duckdb)


    def completed(self):
        if self.executed:
            if self.inputs is not None:
                for step in self.inputs:
                    if step.executed:
                        step.completed()
        self.executed = False
        self.execute_lock = False
        if self.outputs is not None:
            for step in self.outputs:
                if step.executed:
                    step.completed()

    @abstractmethod
    def execute(self, duckdb):
        pass

    def __str__(self):
        return self.name


class Dataset(Step, ABC):

    def __init__(self, dataset_name: str, is_cdc: bool = False,
                 logical_pk_list: Union[None, Iterable[str]] = None):
        super().__init__(dataset_name)
        self.show_projection = "*"
        self.where_clause = None
        self.logical_pk_list = logical_pk_list
        self.is_cdc = is_cdc

    @abstractmethod
    def is_persisted(self) -> bool:
        pass

    @abstractmethod
    def get_sub_select_clause(self) -> str:
        pass

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
            print(heading)
        print(f"Query executed: {self.get_sub_select_clause()}")
        duckdb.sql(sql).show(max_width=200)

    def get_show_data(self, duckdb):
        where = ""
        if self.where_clause is not None:
            where = " where " + self.where_clause
        sql = f"""
        with tab as {self.get_sub_select_clause()}
        select {self.show_projection} from tab {where}
        """
        return duckdb.execute(sql).fetchall()

    def execute(self, duckdb):
        pass


class Table(Dataset):

    def __init__(self, dataset_name: str, table_name: str, is_cdc: bool = False,
                 pk_list: Union[None, Iterable[str]] = None, allow_evolution: bool = False):
        super().__init__(dataset_name, is_cdc, pk_list)
        self.table_name = table_name
        self.cols: Union[None, OrderedDict[str, str]] = None

    def is_persisted(self):
        return True

    def get_sub_select_clause(self) -> str:
        return f"(select * from {quote_str(self.table_name)})"

    def add_column(self, column_name: str, column_datatype: str):
        if self.cols is None:
            self.cols = OrderedDict()
        self.cols[column_name] = column_datatype

    def add_all_columns(self, source: Dataset, duckdb):
        if isinstance(source, Table):
            duckdb.execute(f"SELECT column_name, data_type, is_nullable, numeric_precision, numeric_scale "
                           f"FROM duckdb_columns() "
                           f"WHERE table_name = '{source.table_name}' ORDER BY column_index")
            data = duckdb.fetchall()
            if len(data) == 0:
                raise RuntimeError("Table not found in DuckDB")
            for fields in data:
                name = fields[0]
                datatype = fields[1]
                not_null = ''
                if fields[2] == 'false':
                    not_null = 'not null'
                datatype_suffix = ''
                if datatype == 'decimal':
                    datatype_suffix = f"({fields[3]}, {fields[4]})"
                self.add_column(name, f"{datatype}{datatype_suffix} {not_null}")
        else:
            sql = f"""
                with source as {source.get_sub_select_clause()}
                select * from source;
            """
            duckdb.execute(sql)
            metadata = duckdb.description()
            for fields in metadata:
                name = fields[0]
                datatype = fields[1]
                match datatype:
                    case 'NUMBER':
                        datatype = 'decimal(38,7)' # DuckDB does not provide any additional information - it cannot
                self.add_column(name, f"{datatype}")

    def create_table(self, duckdb):
        if self.cols is None:
            raise RuntimeError("Cannot create a table without columns - use add_column to add some")
        create_str = ""
        for name, datatype in self.cols.items():
            if len(create_str) == 0:
                create_str += f"create table \"{self.table_name}\" ("
            else:
                create_str += ", "
            create_str += f"{quote_str(name)} {datatype}"
        pk_str = convert_list_to_str(self.logical_pk_list)
        if pk_str is not None:
            create_str += f", primary key ({pk_str})"
        create_str += ")"
        duckdb.execute(create_str)

    def set_logical_pk_list(self, logical_pk_list: Union[None, Iterable[str]] = None):
        self.logical_pk_list = logical_pk_list


class TableSynonym(Table):

    def __init__(self, name: str, table: Table):
        super().__init__(name, table.table_name, table.is_cdc, table.logical_pk_list)
        self.synonym_for = table

    def create_table(self, duckdb):
        raise RuntimeError(f"This is a synonym for {self.synonym_for.table_name}")

    def is_persisted(self):
        return self.synonym_for.is_persisted()

    def get_sub_select_clause(self) -> str:
        return self.synonym_for.get_sub_select_clause()

    def add_column(self, column_name: str, column_datatype: str):
        raise RuntimeError(f"This is a synonym for {self.synonym_for.table_name}")

    def add_all_columns(self, source: Dataset, duckdb):
        raise RuntimeError(f"This is a synonym for {self.synonym_for.table_name}")

    def show(self, duckdb, logger: Logger, heading: Union[None, str] = None):
        self.synonym_for.show(duckdb, logger, heading)

    def get_show_data(self, duckdb):
        self.synonym_for.get_show_data(duckdb)

    def set_logical_pk_list(self, logical_pk_list: Union[None, Iterable[str]] = None):
        self.synonym_for.set_logical_pk_list(logical_pk_list)


class Query(Dataset):

    def __init__(self, dataset_name: str, sql: str, inputs: Union[None, list[Dataset]] = None, is_cdc: bool = False,
                 logical_pk_list: Union[None, Iterable[str]] = None):
        super().__init__(dataset_name, is_cdc, logical_pk_list)
        self.sql = sql
        if inputs is not None:
            for i in inputs:
                self.add_input(i)
        regex = r"\{\w*\}"
        matches = re.finditer(regex, sql, re.MULTILINE)
        if self.inputs is not None:
            keys = {x.name for x in inputs}
            not_found_keys = []
            for match in matches:
                parameter = match.group(0)[1:-1]
                if parameter not in keys:
                    keys.discard(parameter)
            if len(not_found_keys) > 0:
                raise RuntimeError(f"The sql contains the parameters {not_found_keys} which are not found in any "
                                   f"of the input datasets with this dataset name - available dataset names "
                                   f"are {keys}")
        else:
            first = next(matches, None)
            if first is not None:
                raise RuntimeError("The sql text contains an input parameter but no input datasets are provided")

    def set_inputs(self, inputs: list[Dataset]):
        self.inputs = inputs

    def is_persisted(self):
        return False

    def get_sub_select_clause(self) -> str:
        sql = self.sql
        if self.inputs is not None:
            for i in self.inputs:
                sql = sql.replace("{" + i.name + "}", i.get_sub_select_clause())
        return f"({sql})"


class RowType(Enum):
    INSERT = 'I'
    """
    A brand new record was inserted. A record with this primary key was not present before.
    If there is no guarantee such record does not exist yet, use UPSERT instead.
    """
    UPDATE = 'U'
    """
    After image row.
    An existing record was updated. This record is the holder of the new values.
    """
    DELETE = 'D'
    """
    An existing record was deleted, the provided records contains the complete latest version with all payload fields.
    If only the primary key of the payload is known, use EXTERMINATE instead.
    """
    BEFORE = 'B'
    """
    Before image row.
    An existing record was updated. This record is the holder of the old values.
    """
    UPSERT = 'A'
    """
    In case either a new record should be created or its last version overwritten, use this 
    UPSERT RowType ("AutoCorrect").
    """
    EXTERMINATE = 'X'
    """
    When the payload of a delete has null values everywhere except for the primary key fields, then the 
    proper code is EXTERMINATE.
    A database would execute a "delete from table where pk = ?" and ignore all other fields.
    """
    TRUNCATE = 'T'
    """
    Delete a set of rows at once. An example could be to delete all records of a given patient from the diagnosis table.
    In that case the diagnosis table would get a record of type truncate with all payload fields including the 
    PK being null, only the patient field has a value.
    """
    REPLACE = 'R'
    """
    A TRUNCATE followed by the new rows.
    Example could be a case where all data of a patient should be reloaded.
    A TRUNCATE row would be sent to all tables to remove the data and all new data is inserted. But to indicate that
    this was done via a truncate-replace, the rows are not flagged as INSERT but REPLACE.
     
    Note that an UPSERT would not work in such scenarios as a patient might have had 10 diagnosis rows but
    meanwhile just 9. The UPSERT would not modify record #10, the truncate on the other hand deletes all 10 records
    and re-inserts 9 records.
    """


def create_join_condition(pk_list: Iterable[str], qualifier_left: Union[None,str], qualifier_right: Union[None,str]):
    condition_str = ""
    l = ""
    if qualifier_left is not None:
        l = qualifier_left + "."
    r = ""
    if qualifier_right is not None:
        r = qualifier_right + "."
    for pk in pk_list:
        c = quote_str(pk)
        if len(condition_str) > 0:
            condition_str += " and "
        condition_str += f"{l}{c} = {r}{c}"
    return condition_str
