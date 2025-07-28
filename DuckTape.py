from typing import Iterable

import duckdb

def process():
    duckdb.execute("CREATE TABLE customer_input AS SELECT * FROM 'testdata/customers-100000.csv' limit 2")
    duckdb.execute("CREATE TABLE customer_output AS SELECT * FROM customer_input where 1 = 2")
    output_table = table_comparison("customer_input", "customer_output",
                     ["Customer Id"])
    print(duckdb.execute(f"select * from \"{output_table}\"").fetchall())

def table_comparison(input_table: str, comparison_table: str,
                     input_table_pk_list: Iterable[str]) -> str:
    input_pk_list = convert_list_to_str(input_table_pk_list)
    input_columns = get_table_cols(input_table)
    input_field_list = convert_list_to_str(input_columns, "s")
    comparison_table_columns = get_table_cols(comparison_table)
    additional_columns = comparison_table_columns.difference(input_columns)
    additional_columns_projection = ""
    for field in additional_columns:
        additional_columns_projection += f", null as \"{field}\""
    additional_field_list = convert_list_to_str(additional_columns, "t")
    if additional_field_list is not None and len(additional_field_list) > 0:
        additional_field_list = ", " + additional_field_list
    join_condition = ""
    for pk in enumerate(input_table_pk_list):
        if pk[0] != 0:
            join_condition += " and"
        join_condition += f"\"s\".\"{pk[1]}\" = \"t\".\"{pk[1]}\""
    output_table_field_list = convert_list_to_str(comparison_table_columns)

    select = f"""
    with changed as (select {input_field_list} from \"{input_table}\" as \"s\"
                     except all
                     select {input_field_list} from \"{comparison_table}\" as \"s\")
    select {input_field_list}{additional_columns_projection}, 'I' as \"__change_type\" from changed as s where ({input_pk_list}) not in (select {input_pk_list} from \"{comparison_table}\")
    union all
    select {input_field_list}{additional_field_list}, 'U' as \"__change_type\" from changed as \"s\" join \"{comparison_table}\" as \"t\" on {join_condition}
    union all
    select {input_field_list.replace('"s"', '"t"')}{additional_field_list}, 'B' as \"__change_type\" from changed as \"s\" join \"{comparison_table}\" as \"t\" on {join_condition}
    """
    output_table = f"{input_table}_tc"
    duckdb.execute(f"CREATE TABLE \"{output_table}\" AS SELECT * FROM \"{comparison_table}\" where 1 = 2")
    duckdb.execute(f"ALTER TABLE \"{output_table}\" add \"__change_type\" varchar(1)")
    output_list = input_field_list.replace('"s".', "") + additional_field_list.replace('"t".', "") + ", \"__change_type\""
    duckdb.execute(f"insert into \"{output_table}\"({output_list}) {select}")
    return output_table


def convert_list_to_str(values: Iterable[str], qualifier: str = None) -> str:
    output_str = ""
    for field in enumerate(values):
        if field[0] != 0:
            output_str += ", "
        if qualifier is not None:
            output_str += f"\"{qualifier}\".\"{field[1]}\""
        else:
            output_str += f"\"{field[1]}\""
    return output_str

def get_table_cols(table_name: str) -> set[str]:
    output = set()
    duckdb.execute(f"SELECT column_name FROM duckdb_columns() WHERE table_name = '{table_name}'")
    data = duckdb.fetchall()
    for field in data:
        output.add(field[0])
    return output


if __name__ == '__main__':
    process()