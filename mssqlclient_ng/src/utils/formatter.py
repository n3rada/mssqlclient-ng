"""
MarkdownFormatter: Utility functions to format data as Markdown tables.
"""

from typing import Dict, List, Optional, Sequence, Any


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
    Converts a dictionary into a Markdown-friendly table format.
    """
    if not dictionary:
        return ""

    rows = [(column_one_header, column_two_header)] + list(dictionary.items())
    col1_width = max(len(normalize_value(row[0])) for row in rows)
    col2_width = max(len(normalize_value(row[1])) for row in rows)

    lines = []
    lines.append(
        f"| {column_one_header.ljust(col1_width)} | {column_two_header.ljust(col2_width)} |"
    )
    lines.append(f"| {'-' * col1_width} | {'-' * col2_width} |")
    for k, v in dictionary.items():
        lines.append(
            f"| {normalize_value(k).ljust(col1_width)} | {normalize_value(v).ljust(col2_width)} |"
        )
    return "\n" + "\n".join(lines)


def list_to_markdown_table(lst: Sequence[str], column_name: str) -> str:
    """
    Converts a list of strings into a Markdown-friendly table format with a specified column name.
    """
    if not lst:
        return ""
    col_width = max(len(column_name), max(len(normalize_value(item)) for item in lst))
    lines = [f"| {column_name.ljust(col_width)} |", f"| {'-' * col_width} |"]
    for item in lst:
        lines.append(f"| {normalize_value(item).ljust(col_width)} |")
    return "\n" + "\n".join(lines)


def rows_to_markdown_table(rows: List[Dict[str, Any]]) -> str:
    """
    Converts a list of dictionaries (rows) into a Markdown-friendly table format.
    Each dict should have the same keys (column names).
    """
    if not rows:
        return "No data available."
    columns = list(rows[0].keys())
    col_widths = [
        max(len(str(col)), max(len(normalize_value(row.get(col, ""))) for row in rows))
        for col in columns
    ]
    header = (
        "| "
        + " | ".join(col.ljust(col_widths[i]) for i, col in enumerate(columns))
        + " |"
    )
    separator = (
        "| " + " | ".join("-" * col_widths[i] for i in range(len(columns))) + " |"
    )
    data_lines = []
    for row in rows:
        line = (
            "| "
            + " | ".join(
                normalize_value(row.get(col, "")).ljust(col_widths[i])
                for i, col in enumerate(columns)
            )
            + " |"
        )
        data_lines.append(line)
    return "\n" + "\n".join([header, separator] + data_lines)


def table_to_markdown(
    table: List[List[Any]], headers: Optional[List[str]] = None
) -> str:
    """
    Converts a 2D list (table) into a Markdown-friendly table format.
    Optionally takes a list of column headers.
    """
    if not table or (headers is not None and not headers):
        return ""
    num_cols = len(headers) if headers else len(table[0])
    col_widths = [0] * num_cols
    if headers:
        for i, h in enumerate(headers):
            col_widths[i] = max(col_widths[i], len(str(h)))
    for row in table:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(normalize_value(cell)))
    lines = []
    if headers:
        lines.append(
            "| "
            + " | ".join(str(h).ljust(col_widths[i]) for i, h in enumerate(headers))
            + " |"
        )
        lines.append(
            "| " + " | ".join("-" * col_widths[i] for i in range(num_cols)) + " |"
        )
    for row in table:
        lines.append(
            "| "
            + " | ".join(
                normalize_value(cell).ljust(col_widths[i]) for i, cell in enumerate(row)
            )
            + " |"
        )
    return "\n" + "\n".join(lines)
