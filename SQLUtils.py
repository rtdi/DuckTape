from typing import Iterable, Union

def quote_str(name: str) -> Union[None, str]:
    if name is None:
        return None
    if name.startswith('"'):
        return name
    else:
        return '"' + name + '"'


def convert_list_to_str(values: Iterable[str], qualifier: str = None) -> str:
    """
    Turns the list of strings into a comma separated single string, optionally with qualifier
    :param values:
    :param qualifier:
    :return:
    """
    output_str = ""
    if values is not None:
        for field in values:
            if len(output_str) > 0:
                output_str += ", "
            if qualifier is not None:
                output_str += f"{qualifier}.{field}"
            else:
                output_str += field
        return output_str
    else:
        return None


def get_table_cols(db, table_name: str) -> set[str]:
    """
    Return a set of column name, already double-quoted to be used in SQL statements
    :param db:
    :param table_name:
    :return: a set of column names in the form of ("col1", "col2",...)
    """
    output = set()
    db.execute(f"SELECT column_name FROM duckdb_columns() WHERE table_name = '{table_name}'")
    data = db.fetchall()
    for field in data:
        output.add('"' + field[0] + '"')
    return output


def get_cols(db, dataset) -> set[str]:
    """
    Return a set of column name, already double-quoted to be used in SQL statements
    :param db: DuckDB connection
    :param dataset:
    :return: a set of column names in the form of ("col1", "col2",...)
    """
    output = set()
    sql = f"""
        with source as {dataset.get_sub_select_clause()}
        select * from source;
    """
    db.execute(sql)
    metadata = db.description()
    for field in metadata:
        output.add('"' + field[0] + '"')
    return output


def get_table_primary_key(db, logger, table_name: str) -> Union[None, set[str]]:
    db.execute(f"SELECT constraint_column_names FROM duckdb_constraints() WHERE table_name = '{table_name}' and constraint_type = 'PRIMARY KEY'")
    data = db.fetchall()
    if data is None or len(data) == 0:
        logger.info(f"Table {table_name} does not have a primary key constraint")
        return None
    pk_columns = data[0][0]
    if pk_columns is None:
        return None
    else:
        output = set()
        for field in pk_columns:
            output.add('"' + field + '"')
        return output


def empty(iterable: Iterable[any]):
    if iterable is None:
        return True
    it = iter(iterable)
    return next(it, None) is None


def get_first(iterable: Iterable[any]):
    if iterable is None:
        return True
    it = iter(iterable)
    return next(it, None)


def get_count(iterable: Iterable[any]):
    if iterable is None:
        return 0
    it = iter(iterable)
    count = 0
    while next(it, None):
        count += 1
    return count
