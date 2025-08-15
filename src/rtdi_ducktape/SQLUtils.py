from typing import Iterable, Union


def quote_str(name: str) -> Union[None, str]:
    if name is None:
        return None
    if name.startswith('"'):
        return name
    else:
        return '"' + name + '"'


def convert_list_to_str(values: Iterable[str], qualifier: str = None) -> Union[None, str]:
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
                output_str += f"{qualifier}.{quote_str(field)}"
            else:
                output_str += quote_str(field)
        return output_str
    else:
        return None


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
