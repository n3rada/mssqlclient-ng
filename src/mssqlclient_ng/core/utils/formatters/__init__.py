# mssqlclient_ng/core/utils/formatters/__init__.py

# Built-in imports
from typing import Any, Sequence

# Local library imports
from .base import IOutputFormatter
from .markdown import MarkdownFormatter
from .csv import CsvFormatter
from .grid import GridFormatter
from .formatter import OutputFormatter

__all__ = [
    "IOutputFormatter",
    "MarkdownFormatter",
    "CsvFormatter",
    "GridFormatter",
    "OutputFormatter",
    "normalize_value",
    "dict_to_markdown_table",
    "list_to_markdown_table",
    "rows_to_markdown_table",
    "table_to_markdown",
    "format_table",
]

def normalize_value(value: Any) -> str:
    """
    Normalizes a value for display in a table.
    Decodes bytes to UTF-8 strings, converts None to empty string, etc.
    """
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    elif value is None:
        return ""
    return str(value)

def dict_to_markdown_table(
    dictionary: dict[str, str], column_one_header: str, column_two_header: str
) -> str:
    """
    Converts a dictionary into a formatted table (uses current output format).
    """
    return OutputFormatter.convert_dict(
        dictionary, column_one_header, column_two_header
    )

def list_to_markdown_table(lst: Sequence[str], column_name: str) -> str:
    """
    Converts a list of strings into a formatted table with a specified column name.
    """
    return OutputFormatter.convert_list(list(lst), column_name)

def rows_to_markdown_table(rows: list[dict[str, Any]]) -> str:
    """
    Converts a list of dictionaries (rows) into a formatted table.
    Each dict should have the same keys (column names).
    """
    return OutputFormatter.convert_list_of_dicts(rows)

def table_to_markdown(
    table: list[list[Any]], headers: list[str] | None = None
) -> str:
    """
    Converts a 2D list (table) into a formatted table.
    Optionally takes a list of column headers.
    """
    if not table or (headers is not None and not headers):
        return ""

    if headers:
        rows = []
        for row in table:
            row_dict = {}
            for i, header in enumerate(headers):
                row_dict[header] = normalize_value(row[i]) if i < len(row) else ""
            rows.append(row_dict)
        return OutputFormatter.convert_list_of_dicts(rows)
    else:
        num_cols = len(table[0]) if table else 0
        gen_headers = [f"column{i}" for i in range(num_cols)]
        rows = []
        for row in table:
            row_dict = {}
            for i, header in enumerate(gen_headers):
                row_dict[header] = normalize_value(row[i]) if i < len(row) else ""
            rows.append(row_dict)
        return OutputFormatter.convert_list_of_dicts(rows)

def format_table(headers: list[str], table: list[list[Any]]) -> str:
    """
    Formats a table with headers and data rows.
    Alias for table_to_markdown for backward compatibility.
    """
    return table_to_markdown(table, headers)
