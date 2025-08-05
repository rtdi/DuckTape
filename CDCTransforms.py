import logging
from datetime import datetime, timezone
from enum import Enum
from logging import Logger
from typing import Union, Iterable

from SQLUtils import quote_str, convert_list_to_str, get_cols, get_table_primary_key, empty, get_first, get_count
from Metadata import Dataset, OperationalMetadata

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

CHANGE_TYPE = "__change_type"
CHANGE_TYPE_COLUMN = '"__change_type"'



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


class TableComparison:

    def __init__(self, source: Dataset, comparison: Dataset,
                 source_pk_list: Union[None, Iterable[str]] = None, columns_to_ignore: Union[None, list[str]] = None,
                 order_column: Union[None, str] = None, before_image: bool = True, detect_deletes: bool = False,
                 end_date_column: Union[None, str] = None, termination_date: Union[None, datetime] = None,
                 logger: Logger = None
                 ):
        """
        Compare the input with another table, that has at least the same columns as the input and generate the CDC delta in a new table

        Example: Input customer(customer_id, first_name, last_name), Comparison table is dim_customer(customer_id, first_name, last_name)
        primary key is customer_id.

        The result is a table customer_tc(__change_type, customer_id, first_name, last_name) with all records
        that are in the input but have no record with the same pk in the comparison table flagged as __change_type = 'I'
        and 'U' and 'B' records for records with the same pk but different values in at least one column. The 'U' record contains the
        new version, the 'B' record the old version (before image value).

        The table_comparison can deal with additional cases, though.
        1. The comparison table might have more columns than the input. The output CDC table will have all columns of the comparison table and
        retain the values of the additional columns.
        2. The comparison table might have more rows per specified logical primary key. The comparison will happen with the row that has the
        highest value in the order_column. If the order_column is a change date for example, the most recent version will be used to compare, thus
        handling cases where a column value flipped from A --> B --> A again.
        3. Certain columns can be ignored in the comparison, e.g. source has a change_date column contained now() but that should be ignored,
        else all records would be considered as different and flagged as update.
        4. If the source contains the complete dataset always, the table comparison can detect deletes also, these are rows
        present in the target but no longer in the source.
        5. If the comparison table is a SCD2 type table, it has an end date column and a default termination date of in the far future. The
        comparison should then only consider rows where end date column has a value of termination date. This is important for cases where a
        row was deleted and then inserted again. This must be a new version, instead of comparing with the latest version, the row with an
        end date of the deletion.

        :param source: the table name of the source
        :param comparison: the name of the table to compare with
        :param source_pk_list: the list of logical primary keys, if None look for the table's primary key
        :param columns_to_ignore: optional list of columns to ignore in the comparison
        :param order_column: in case the comparison table has multiple records, pick the one with the highest value in the order column
        :param before_image: generate a before image row or not
        :param detect_deletes: full delta - find records that are in the comparison table but not in the input as these are deleted ones
        :param end_date_column: The end date column of the SCD2
        :param termination_date: The value of the end date column in case it is active
        :param logger: Logger of the dataflow
        """
        if logger is None:
            self.logger = logging.getLogger("TableComparison")
        else:
            self.logger = logger
        if empty(source_pk_list):
            source_pk_list = None
        if source_pk_list is None and not source.is_persisted():
            raise RuntimeError(
                f"table_comparison() - No logical primary key provided, and cannot be read as "
                f"the source is a select statement")
        self.source = source
        self.comparison = comparison
        self.source_pk_list = source_pk_list
        self.columns_to_ignore = columns_to_ignore
        self.order_column = order_column
        self.before_image = before_image
        self.detect_deletes = detect_deletes
        self.end_date_column = end_date_column
        self.termination_date = termination_date
        if self.termination_date is None:
            self.termination_date = datetime.strptime('9999-12-31', '%Y-%m-%d')
        self.output_table = Dataset(self.source.dataset_name + "_tc", self.source.dataset_name + "_tc",
                                    True, source_pk_list)
        self.last_execution: Union[None, OperationalMetadata] = None

    def execute(self, duckdb):
        self.logger.info(f"TableComparison() - Started for {self.source.dataset_name}")
        if self.source_pk_list is None:
            self.logger.debug(f"TableComparison() - No logical primary key provided, reading the pk of "
                              f"the source table {self.source}...")
            self.source_pk_list = get_table_primary_key(duckdb, self.logger, self.source.table_name)

            if self.source_pk_list is None:
                raise RuntimeError(
                    "Table Comparison requires the source_table_pk_list to find the matching row in the comparison table")
            else:
                self.logger.debug(f"TableComparison() - Source table {self.source} has the "
                                  f"primary key columns {self.source_pk_list}")
                self.output_table.logical_pk_list = self.source_pk_list

        self.last_execution = OperationalMetadata()

        input_pks_str = convert_list_to_str(self.source_pk_list)
        input_columns = get_cols(duckdb, self.source)
        input_columns.discard(CHANGE_TYPE_COLUMN)  # in case the target table stores the change type, do not compare on that
        input_fields_str_s = convert_list_to_str(input_columns, "s")
        comparison_table_columns = get_cols(duckdb, self.comparison)
        # The change type column, if present in the target, is always ignored in the comparison and also not selected from
        if CHANGE_TYPE_COLUMN in comparison_table_columns:
            comparison_table_has_change_type = True
            comparison_table_columns.discard(CHANGE_TYPE_COLUMN)
        else:
            comparison_table_has_change_type = False

        if self.columns_to_ignore is None or len(self.columns_to_ignore) == 0:
            compare_columns = input_columns
        else:
            compare_columns = input_columns.difference(self.columns_to_ignore)
        compare_columns_str = convert_list_to_str(compare_columns)

        additional_columns = comparison_table_columns.difference(input_columns)
        additional_columns_projection = ""
        for field in additional_columns:
            additional_columns_projection += f", null as {field}"
        additional_fields_str_t = convert_list_to_str(additional_columns, "t")
        if additional_fields_str_t is not None and len(additional_fields_str_t) > 0:
            additional_fields_str_t = ", " + additional_fields_str_t
        join_condition_s_t = create_join_condition(self.source_pk_list, "s", "t")
        join_condition_k_t = join_condition_s_t.replace('s.', 'k.')

        order_clause = ""
        if self.order_column is not None:
            order_clause = f"order by {quote_str(self.order_column)} desc"
        tc_filter = ""
        if self.end_date_column is not None:
            tc_filter = f"where \"{self.end_date_column}\" = ?"
        select = f"""
        with comparison_table as {self.comparison.get_sub_select_clause()},
        current_version as (select * from
                                (select *, row_number() over (partition by {input_pks_str} {order_clause}) as \"__rownumber\" 
                                 from comparison_table {tc_filter}
                                )
                            where \"__rownumber\" = 1
        ),
        source as {self.source.get_sub_select_clause()},
        changed as (select {compare_columns_str} from source as s
                    except
                    select {compare_columns_str} from current_version as s
        )
        select {input_fields_str_s}{additional_columns_projection}, '{RowType.INSERT.value}' as {CHANGE_TYPE_COLUMN} from source as s where ({input_pks_str}) not in (select {input_pks_str} from current_version)
        union all
        select {input_fields_str_s}{additional_fields_str_t}, '{RowType.UPDATE.value}' as {CHANGE_TYPE_COLUMN} from source as s join current_version as t on {join_condition_s_t} join changed k on {join_condition_k_t}
        """
        if self.before_image:
            select += f"""
                union all
                select {input_fields_str_s.replace('s.', 't.')}{additional_fields_str_t},
                    '{RowType.BEFORE.value}' as {CHANGE_TYPE_COLUMN}
                from source as s join current_version as t on {join_condition_s_t} join changed k on {join_condition_k_t}
            """
        if self.detect_deletes:
            select += f"""
                union all
                select {input_fields_str_s}{additional_fields_str_t.replace("t.", "s.")},
                    '{RowType.DELETE.value}' as {CHANGE_TYPE_COLUMN} from comparison_table as s
                where ({input_pks_str}) not in (select {input_pks_str} from source)
            """
        output_table_str = quote_str(self.output_table.table_name)
        sql = f"CREATE OR REPLACE TABLE {output_table_str} AS FROM {self.comparison.get_sub_select_clause()} with no data"
        self.logger.debug(f"TableComparison() - Create output table {output_table_str} via the sql statement <{sql}>")
        duckdb.execute(sql)
        if not comparison_table_has_change_type:
            sql = f"ALTER TABLE {output_table_str} add {CHANGE_TYPE_COLUMN} varchar(1)"
            self.logger.debug("TableComparison() - Adding the change_type column to the output table <{sql}>")
            duckdb.execute(sql)
        output_list = input_fields_str_s.replace('s.', "") + additional_fields_str_t.replace('t.',
                                                                                             "") + f", {CHANGE_TYPE_COLUMN}"
        sql = f"insert into {output_table_str}({output_list}) {select}"
        self.logger.debug(f"TableComparison() - Executing the SQL statement to identify the delta and "
                          f"split into insert and update records via the sql statement <{sql}>")
        duckdb.execute(sql, [self.termination_date])
        res = duckdb.execute(f"select count(*) from {output_table_str}").fetchall()
        self.last_execution.processed(res[0][0])
        self.logger.info(self.last_execution)

class SCD2:

    def __init__(self, source: Dataset,
             start_date_column: str, end_date_column: str,
             start_date: Union[None, datetime] = None, end_date: Union[None, datetime] = None,
             termination_date: Union[None, datetime] = None,
             current_flag_column: Union[None, str] = None,
             current_flag_set: Union[None, str] = None, current_flag_unset: Union[None, str] = None,
             logger: Logger = None):
        """
        The SCD2 transform takes the information created by the TableComparison and turns that into the changes
        required for the target table to contain SCD2 data.
        The task is quite simple:
        - New records get a start date and the end date is the termination date, by default 9999-12-31
        - Update (after image) are converted into new version, so an insert with start and termination date as well
        - The old values of an update (before image) get an end date
        - Delete records get the end date assigned as well

        Note: New records can get a start date by two ways, one is by providing it in the input already and the other
        is leaving it Null and using the start date parameter. The reason for the first option would be if there is
        an order_date and that should be used as start date instead of now().

        :param source: Must be a table dataset
        :param start_date_column: column name of the SCD2's start date
        :param end_date_column: column name of the SCD2's end date
        :param start_date: optional value for the start date - default is now(utc)
        :param end_date: for deletes and old version the end date is set to this value - default is same as start date
        :param termination_date: optional value for currently active records - default is 9999-12-31
        :param current_flag_column: optional column for the current flag indicator
        :param current_flag_set: if the row is the active version, the current flag column should be set to this value - default 'Y'
        :param current_flag_unset: the value for all versions not active - default 'N'
        :param logger: Logger of the dataflow
        """
        if logger is None:
            self.logger = logging.getLogger("SCD2")
        else:
            self.logger = logger
        if not source.is_persisted():
            raise RuntimeError("SCD2 requires a source table as it updates values")
        if not source.is_cdc:
            raise RuntimeError("SCD2 source must be of type CDC")
        if termination_date is None:
            termination_date = datetime.strptime('9999-12-31', '%Y-%m-%d')
        if current_flag_set is None:
            current_flag_set = 'Y'
        if current_flag_unset is None:
            current_flag_unset = 'N'
        if start_date is None:
            start_date = datetime.now(timezone.utc)
        if end_date is None:
            end_date = start_date
        self.source = source
        self.start_date_column = start_date_column
        self.end_date_column = end_date_column
        self.start_date = start_date
        self.end_date = end_date
        self.termination_date = termination_date
        self.current_flag_column = current_flag_column
        self.current_flag_set = current_flag_set
        self.current_flag_unset = current_flag_unset
        self.last_execution: Union[None, OperationalMetadata] = None

    def execute(self, duckdb):
        self.logger.info(f"SCD2() - Started for {self.source.dataset_name}")
        self.last_execution = OperationalMetadata()
        source_table = quote_str(self.source.table_name)
        if self.current_flag_column is not None:
            sql = f"""
            update {source_table} set 
            {quote_str(self.start_date_column)} = 
                case when {CHANGE_TYPE_COLUMN} = '{RowType.INSERT.value}' then ifnull({quote_str(self.start_date_column)}, $1)
                when {CHANGE_TYPE_COLUMN} = '{RowType.UPDATE.value}' then $1
                when {CHANGE_TYPE_COLUMN} = '{RowType.BEFORE.value}' or {CHANGE_TYPE_COLUMN} = '{RowType.DELETE.value}' then {quote_str(self.start_date_column)}
                end,
            {quote_str(self.end_date_column)} = 
                case when {CHANGE_TYPE_COLUMN} = '{RowType.INSERT.value}' or {CHANGE_TYPE_COLUMN} = '{RowType.UPDATE.value}' then $3
                when {CHANGE_TYPE_COLUMN} = '{RowType.BEFORE.value}' or {CHANGE_TYPE_COLUMN} = '{RowType.DELETE.value}' then $2
                end,
            {quote_str(self.current_flag_column)} = 
                case when {CHANGE_TYPE_COLUMN} = '{RowType.INSERT.value}' or {CHANGE_TYPE_COLUMN} = '{RowType.UPDATE.value}' then $4
                when {CHANGE_TYPE_COLUMN} = '{RowType.BEFORE.value}' or {CHANGE_TYPE_COLUMN} = '{RowType.DELETE.value}' then $5
                end,
            {CHANGE_TYPE_COLUMN} = 
                case when {CHANGE_TYPE_COLUMN} = '{RowType.INSERT.value}' or {CHANGE_TYPE_COLUMN} = '{RowType.UPDATE.value}' then '{RowType.INSERT.value}'
                when {CHANGE_TYPE_COLUMN} = '{RowType.BEFORE.value}' or {CHANGE_TYPE_COLUMN} = '{RowType.DELETE.value}' then '{RowType.UPDATE.value}'
                end
            """
        else:
            sql = f"""
            update {source_table} set 
            {quote_str(self.start_date_column)} = 
                case when {CHANGE_TYPE_COLUMN} = '{RowType.INSERT.value}' then ifnull({quote_str(self.start_date_column)}, $1)
                when {CHANGE_TYPE_COLUMN} = '{RowType.UPDATE.value}' then $1
                when {CHANGE_TYPE_COLUMN} = '{RowType.BEFORE.value}' or {CHANGE_TYPE_COLUMN} = '{RowType.DELETE.value}' then {quote_str(self.start_date_column)}
                end,
            {quote_str(self.end_date_column)} = 
                case when {CHANGE_TYPE_COLUMN} = '{RowType.INSERT.value}' or {CHANGE_TYPE_COLUMN} = '{RowType.UPDATE.value}' then $3
                when {CHANGE_TYPE_COLUMN} = '{RowType.BEFORE.value}' or {CHANGE_TYPE_COLUMN} = '{RowType.DELETE.value}' then $2
                end,
            {CHANGE_TYPE_COLUMN} = 
                case when {CHANGE_TYPE_COLUMN} = '{RowType.INSERT.value}' or {CHANGE_TYPE_COLUMN} = '{RowType.UPDATE.value}' then '{RowType.INSERT.value}'
                when {CHANGE_TYPE_COLUMN} = '{RowType.BEFORE.value}' or {CHANGE_TYPE_COLUMN} = '{RowType.DELETE.value}' then '{RowType.UPDATE.value}'
                end
            """
        self.logger.debug(f"SCD2() - Converting the CDC info of table {source_table} into SCD2 info: <{sql}>")
        duckdb.execute(sql, [self.start_date, self.end_date, self.termination_date, self.current_flag_set, self.current_flag_unset])
        res = duckdb.execute(f"select count(*) from {source_table}").fetchall()
        self.last_execution.processed(res[0][0])
        self.logger.info(self.last_execution)

class GenerateKey:

    def __init__(self, cdc_table: Dataset, surrogate_key_column: Union[None, str] = None,
                     start_value: Union[None, int] = None, target: Union[None, Dataset] = None,
                 logger: Logger = None):
        """
        GenerateKey() goes through all rows of the cdc table and updates the surrogate key column with new unique
        numbers. For that it must read the max(surrogate key) from the physical target table and use that as the
        start value of hte sequence.

        Note: If the target table is not accessible for DuckDb, the surrogate key column and the start value must
        be provided

        Note: new key values are set for all change type = 'I' rows, even if the row has an (old) value already.

        :param cdc_table: The table dataset to set the surrogate key values
        :param surrogate_key_column: the column name - default is the physical primary key of the target table
        :param start_value: Normally left None so that the transform read the max(surrogate key) from the target table
        :param target: The table dataset representing the target
        :param logger: Logger of the dataflow
        """
        if surrogate_key_column is None and target is None:
            raise RuntimeError("The key will be set on the target table primary key column, but neither the target "
                               "table nor surrogate key column has not been specified")
        if logger is None:
            self.logger = logging.getLogger("TableComparison")
        else:
            self.logger = logger
        self.cdc_table = cdc_table
        self.surrogate_key_column = surrogate_key_column
        self.start_value = start_value
        self.target = target
        self.last_execution: Union[None, OperationalMetadata] = None

    def execute(self, duckdb):
        self.logger.info(f"GenerateKey() - Started for {self.cdc_table.dataset_name}")
        self.last_execution = OperationalMetadata()
        if self.surrogate_key_column is None:
            pks = get_table_primary_key(duckdb, self.logger, self.target.table_name)
            if pks is None:
                raise RuntimeError(
                    f"The target table {self.target.table_name} has no primary - must specify a surrogate key column then")
            elif get_count(pks) != 1:
                raise RuntimeError(f"Generate key requires a single column to be the primary key of the "
                                   f"target table {self.target.table_name}, but has {pks} - must specify "
                                   f"a surrogate key column then")
            else:
                self.surrogate_key_column = get_first(pks)
        start_value = 0
        if self.start_value is None:
            if self.target is None:
                raise RuntimeError("To generate a key either a start value or the target table must be provided")
            else:
                sql = f"select max({quote_str(self.surrogate_key_column)}) from {quote_str(self.target.table_name)}"
                self.logger.debug(
                    f"GenerateKey() - No start value provided, reading the max({self.surrogate_key_column}) value "
                    f"from {self.target.table_name}: <{sql}>")
                res = duckdb.execute(sql).fetchall()
                start_value = res[0][0]
                if start_value is None:
                    start_value = 1
                else:
                    start_value += 1
        sequence_name = self.target.table_name + "_seq"
        sql = f"create or replace sequence {quote_str(sequence_name)} start {start_value}"
        self.logger.debug(f"GenerateKey() - Creating the sequence for the key: <{sql}>")
        duckdb.execute(sql)
        sql = (f"update {quote_str(self.cdc_table.table_name)} set "
               f"{quote_str(self.surrogate_key_column)} = nextval('{sequence_name}') "
               f"where {CHANGE_TYPE_COLUMN} = '{RowType.INSERT.value}'")
        self.logger.debug(f"GenerateKey() - Updating the key for all insert rows: <{sql}>")
        duckdb.execute(sql)
        res = duckdb.execute(f"select count(*) from {quote_str(self.cdc_table.table_name)} where {CHANGE_TYPE_COLUMN} = '{RowType.INSERT.value}'").fetchall()
        self.last_execution.processed(res[0][0])
        self.logger.info(self.last_execution)


class CDCOperation:

    def __init__(self, cdc_table: Dataset, logical_pk_list: Union[None, Iterable[str]] = None,
                 map_insert_to: str = None, map_update_to: str = None, map_before_to: str = None, map_delete_to: str = None,
                 column_expressions: Union[None, dict[str, str]] = None, logger: Union[None, Logger] = None):
        """
        Allows to modify the change type flag and to set values based on the before image.
        For example, TableComparison found out the new and changed records but all records should be inserted into
        the target. Hence the map_update_to is set to 'I'.

        :param cdc_table:
        :param logical_pk_list:
        :param map_insert_to:
        :param map_update_to:
        :param map_before_to:
        :param map_delete_to:
        :param column_expressions:
        """
        if not cdc_table.is_cdc:
            raise RuntimeError("Input dataset must be a CDC table")
        if not cdc_table.is_persisted():
            raise RuntimeError("Input dataset must be a persisted table")
        if logger is None:
            self.logger = logging.getLogger("TableComparison")
        else:
            self.logger = logger
        self.cdc_table = cdc_table
        self.map_insert_to = map_insert_to
        self.map_update_to = map_update_to
        self.map_before_to = map_before_to
        self.map_delete_to = map_delete_to
        self.column_expressions = column_expressions
        self.logical_pk_list = logical_pk_list
        if logical_pk_list is None:
            self.logical_pk_list = cdc_table.logical_pk_list
        self.last_execution = None


    def execute(self, duckdb):
        self.logger.info(f"CDCOperation() - Started for {self.cdc_table.dataset_name}")
        self.last_execution = OperationalMetadata()
        mapping_str = ""
        mappings = {
            '{RowType.INSERT.value}': self.map_insert_to,
            '{RowType.UPDATE.value}': self.map_update_to,
            '{RowType.BEFORE.value}': self.map_before_to,
            '{RowType.DELETE.value}': self.map_delete_to
        }
        for key, value in mappings.items():
            if value is not None:
                if len(mapping_str) > 0:
                    mapping_str += ", "
                mapping_str += f"when {CHANGE_TYPE_COLUMN} = '{key}' then '{value}'"
        expression_str = ""
        if self.column_expressions is not None:
            for key, value in self.column_expressions.items():
                if len(expression_str) > 0:
                    expression_str += ", "
                expression_str += f"set {quote_str(key)} = {value}"

        sql = f"""
            update {quote_str(self.cdc_table.table_name)}
        """
        if len(mapping_str) > 0:
            sql += f"""
                {CHANGE_TYPE_COLUMN} = case {mapping_str} else {CHANGE_TYPE_COLUMN} end
            """
        if self.column_expressions is not None:
            if self.logical_pk_list is None or empty(self.logical_pk_list):
                raise RuntimeError("For expressions the logical PK must be specified to know "
                                   "which before image belongs to what after image")
            join_condition = create_join_condition(self.logical_pk_list, None, "b")
            join_condition += " and b.{CHANGE_TYPE_COLUMN} = '{RowType.BEFORE.value}'"
            if len(mapping_str) > 0:
                sql += ", "
            sql += f"""
                {expression_str}
                left join {quote_str(self.cdc_table.table_name)} b on {join_condition}
            """
        duckdb.execute(sql)
        self.logger.debug(f"CDCOperation() - Updating the table with: <{sql}>")
        res = duckdb.execute(f"select count(*) from {quote_str(self.cdc_table.table_name)}").fetchall()
        self.last_execution.processed(res[0][0])
        self.logger.info(self.last_execution)
