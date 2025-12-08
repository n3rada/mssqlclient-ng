# mssqlclient_ng/core/utils/formatter.py

# Built-in imports
from typing import Dict, List, Optional, Sequence, Any

# Local library imports
from .formatters import OutputFormatter


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
    dictionary: Dict[str, str], column_one_header: str, column_two_header: str
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


def rows_to_markdown_table(rows: List[Dict[str, Any]]) -> str:
    """
    Converts a list of dictionaries (rows) into a formatted table.
    Each dict should have the same keys (column names).
    """
    return OutputFormatter.convert_list_of_dicts(rows)


def table_to_markdown(
    table: List[List[Any]], headers: Optional[List[str]] = None
) -> str:
    """
    Converts a 2D list (table) into a formatted table.
    Optionally takes a list of column headers.
    """
    if not table or (headers is not None and not headers):
        return ""

    # Convert to list of dicts for consistent formatting
    if headers:
        rows = []
        for row in table:
            row_dict = {}
            for i, header in enumerate(headers):
                row_dict[header] = normalize_value(row[i]) if i < len(row) else ""
            rows.append(row_dict)
        return OutputFormatter.convert_list_of_dicts(rows)
    else:
        # No headers - create generic column names
        num_cols = len(table[0]) if table else 0
        headers = [f"column{i}" for i in range(num_cols)]
        rows = []
        for row in table:
            row_dict = {}
            for i, header in enumerate(headers):
                row_dict[header] = normalize_value(row[i]) if i < len(row) else ""
            rows.append(row_dict)
        return OutputFormatter.convert_list_of_dicts(rows)


def format_table(headers: List[str], table: List[List[Any]]) -> str:
    """
    Formats a table with headers and data rows.
    Alias for table_to_markdown for backward compatibility.

    Args:
        headers: List of column headers
        table: List of rows (each row is a list of values)

    Returns:
        Formatted table string
    """
    return table_to_markdown(table, headers)
