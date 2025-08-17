from typing import Iterable, Union


def quote_str(name: str) -> Union[None, str]:
    if name is None:
        return None
    if name.startswith('"'):
        return name
    else:
        return '"' + name + '"'


def convert_list_to_str(values: Iterable[str], qualifier: Union[None, str] = None, aliases: Union[None, dict[str, str]] = None) -> Union[None, str]:
    """
    Turns the list of strings into a comma separated single string, optionally with qualifier and alias
    :param values:
    :param qualifier:
    :param aliases:
    :return:
    """
    output_str = ""
    if values is not None:
        for field in values:
            if len(output_str) > 0:
                output_str += ", "
            alias = ""
            if aliases is not None:
                a = aliases.get(field)
                if a is not None:
                    alias = " as " + quote_str(a)
            if qualifier is not None:
                output_str += f"{qualifier}.{quote_str(field)}{alias}"
            else:
                output_str += quote_str(field) + alias
        return output_str
    else:
        return None

def convert_to_order_clause(order_columns: dict[str, bool], qualifier: Union[None, str] = None) -> Union[None, str]:
    """
    Turns the list of strings into a comma separated single string with asc/desc info
    :return:
    """
    output_str = ""
    for field, asc in order_columns.items():
        if len(output_str) > 0:
            output_str += ", "
        desc = ""
        if not asc:
            desc = " desc"
        if qualifier is not None:
            output_str += f"{qualifier}.{quote_str(field)}{desc}"
        else:
            output_str += quote_str(field) + desc
    return output_str


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
