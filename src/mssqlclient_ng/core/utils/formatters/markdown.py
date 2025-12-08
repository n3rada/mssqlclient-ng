# mssqlclient_ng/core/utils/formatters/markdown.py

# Built-in imports
from typing import Dict, List

# Local library imports
from .base import IOutputFormatter


class MarkdownFormatter(IOutputFormatter):
    """Formats data into Markdown-friendly table format."""

    @property
    def format_name(self) -> str:
        return "markdown"

    @staticmethod
    def _byte_array_to_hex_string(data: bytes) -> str:
        """Converts a byte array to a hexadecimal string representation."""
        if not data:
            return ""
        return "0x" + data.hex().upper()

    @staticmethod
    def _format_value(value: any) -> str:
        """Formats a value for display in markdown table."""
        if value is None:
            return "NULL"
        if isinstance(value, bytes):
            return MarkdownFormatter._byte_array_to_hex_string(value)
        if isinstance(value, (list, tuple)):
            return str(value)
        return str(value)

    def convert_dict(
        self, data: Dict[str, str], column_one_header: str, column_two_header: str
    ) -> str:
        """Converts a dictionary into a markdown table."""
        if not data:
            return ""

        lines = []

        # Calculate column widths
        col1_width = max(
            len(column_one_header), max((len(k) for k in data.keys()), default=0)
        )
        col2_width = max(
            len(column_two_header), max((len(str(v)) for v in data.values()), default=0)
        )

        # Header
        lines.append(
            f"| {column_one_header.ljust(col1_width)} | {column_two_header.ljust(col2_width)} |"
        )
        lines.append(f"| {'-' * col1_width} | {'-' * col2_width} |")

        # Rows
        for key, value in data.items():
            lines.append(
                f"| {key.ljust(col1_width)} | {str(value).ljust(col2_width)} |"
            )

        return "\n" + "\n".join(lines) + "\n"

    def convert_list_of_dicts(self, data: List[Dict[str, any]]) -> str:
        """Converts a list of dictionaries into a markdown table."""
        if not data:
            return "No data available."

        lines = []

        # Get all column names
        columns = list(data[0].keys()) if data else []
        if not columns:
            return "No data available."

        # Calculate column widths
        column_widths = {}
        for col in columns:
            col_name = col if col else "column"
            column_widths[col] = len(col_name)

        for row in data:
            for col in columns:
                value = self._format_value(row.get(col))
                column_widths[col] = max(column_widths[col], len(value))

        # Header
        header_parts = []
        separator_parts = []
        for col in columns:
            col_name = col if col else "column"
            header_parts.append(col_name.ljust(column_widths[col]))
            separator_parts.append("-" * column_widths[col])

        lines.append("| " + " | ".join(header_parts) + " |")
        lines.append("| " + " | ".join(separator_parts) + " |")

        # Rows
        for row in data:
            row_parts = []
            for col in columns:
                value = self._format_value(row.get(col))
                row_parts.append(value.ljust(column_widths[col]))
            lines.append("| " + " | ".join(row_parts) + " |")

        return "\n" + "\n".join(lines) + "\n"

    def convert_list(self, data: List[str], column_name: str) -> str:
        """Converts a list into a markdown table with a specified column name."""
        if not data:
            return ""

        lines = []

        # Calculate column width
        column_width = max(
            len(column_name), max((len(str(item)) for item in data), default=0)
        )

        # Header
        lines.append(f"| {column_name.ljust(column_width)} |")
        lines.append(f"| {'-' * column_width} |")

        # Rows
        for item in data:
            lines.append(f"| {str(item).ljust(column_width)} |")

        return "\n" + "\n".join(lines) + "\n"
