# mssqlclient_ng/core/utils/formatters/csv.py

# Built-in imports
from typing import Dict, List

# Local library imports
from .base import IOutputFormatter


class CsvFormatter(IOutputFormatter):
    """Formats data into CSV (Comma-Separated Values) format."""

    SEPARATOR = ";"

    @property
    def format_name(self) -> str:
        return "csv"

    @staticmethod
    def _escape_csv_value(value: str) -> str:
        """Escapes CSV values by wrapping in quotes if they contain separators, quotes, or newlines."""
        if not value:
            return ""

        if (
            CsvFormatter.SEPARATOR in value
            or '"' in value
            or "\n" in value
            or "\r" in value
        ):
            escaped = value.replace('"', '""')
            return f'"{escaped}"'

        return value

    @staticmethod
    def _byte_array_to_hex_string(data: bytes) -> str:
        """Converts a byte array to a hexadecimal string representation."""
        if not data:
            return ""
        return "0x" + data.hex().upper()

    @staticmethod
    def _format_value(value: any) -> str:
        """Formats a value for CSV output."""
        if value is None:
            return ""
        if isinstance(value, bytes):
            return CsvFormatter._byte_array_to_hex_string(value)
        if isinstance(value, (list, tuple)):
            return str(value)
        return str(value)

    def convert_dict(
        self, data: Dict[str, str], column_one_header: str, column_two_header: str
    ) -> str:
        """Converts a dictionary into CSV format."""
        if not data:
            return ""

        lines = []

        # Header
        lines.append(
            f"{self._escape_csv_value(column_one_header)}{self.SEPARATOR}{self._escape_csv_value(column_two_header)}"
        )

        # Rows
        for key, value in data.items():
            lines.append(
                f"{self._escape_csv_value(key)}{self.SEPARATOR}{self._escape_csv_value(str(value))}"
            )

        return "\n" + "\n".join(lines) + "\n"

    def convert_list_of_dicts(self, data: List[Dict[str, any]]) -> str:
        """Converts a list of dictionaries into CSV format."""
        if not data:
            return "No data available."

        lines = []

        # Get all column names
        columns = list(data[0].keys()) if data else []
        if not columns:
            return "No data available."

        # Header
        header_parts = [
            self._escape_csv_value(col if col else f"column{i}")
            for i, col in enumerate(columns)
        ]
        lines.append(self.SEPARATOR.join(header_parts))

        # Rows
        for row in data:
            row_parts = [
                self._escape_csv_value(self._format_value(row.get(col)))
                for col in columns
            ]
            lines.append(self.SEPARATOR.join(row_parts))

        return "\n" + "\n".join(lines) + "\n"

    def convert_list(self, data: List[str], column_name: str) -> str:
        """Converts a list into CSV format with a specified column name."""
        if not data:
            return ""

        lines = []

        # Header
        lines.append(self._escape_csv_value(column_name))

        # Rows
        for item in data:
            lines.append(self._escape_csv_value(str(item)))

        return "\n" + "\n".join(lines) + "\n"
