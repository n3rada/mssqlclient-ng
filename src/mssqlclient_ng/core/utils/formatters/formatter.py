# mssqlclient_ng/core/utils/formatters/formatter.py

# Built-in imports
from typing import Dict, List

# Third-party imports
from loguru import logger

# Local library imports
from .base import IOutputFormatter
from .markdown import MarkdownFormatter
from .csv import CsvFormatter


class OutputFormatter:
    """
    Main output formatter that delegates to the appropriate formatter.
    """

    _current_formatter: IOutputFormatter = MarkdownFormatter()

    @classmethod
    def current_format(cls) -> str:
        """Gets the current output format name."""
        return cls._current_formatter.format_name

    @classmethod
    def set_format(cls, format_name: str) -> None:
        """
        Sets the output format based on format name.

        Args:
            format_name: Format name (e.g., 'markdown', 'csv')

        Raises:
            ValueError: If format name is invalid
        """
        if not format_name:
            raise ValueError("Format name cannot be null or empty.")

        format_lower = format_name.lower()

        if format_lower in ("markdown", "md"):
            cls._current_formatter = MarkdownFormatter()
        elif format_lower == "csv":
            cls._current_formatter = CsvFormatter()
        else:
            available = ", ".join(cls.get_available_formats())
            raise ValueError(
                f"Unknown output format: {format_name}. Available formats: {available}"
            )

        logger.debug(f"Output format set to: {cls._current_formatter.format_name}")

    @classmethod
    def get_available_formats(cls) -> List[str]:
        """Gets a list of available format names."""
        return ["markdown", "csv"]

    @classmethod
    def convert_dict(
        cls, data: Dict[str, str], column_one_header: str, column_two_header: str
    ) -> str:
        """Converts a dictionary into the current output format."""
        return cls._current_formatter.convert_dict(
            data, column_one_header, column_two_header
        )

    @classmethod
    def convert_list_of_dicts(cls, data: List[Dict[str, any]]) -> str:
        """Converts a list of dictionaries into the current output format."""
        return cls._current_formatter.convert_list_of_dicts(data)

    @classmethod
    def convert_list(cls, data: List[str], column_name: str) -> str:
        """Converts a list into the current output format with a specified column name."""
        return cls._current_formatter.convert_list(data, column_name)
