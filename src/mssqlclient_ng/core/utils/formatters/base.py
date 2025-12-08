# mssqlclient_ng/core/utils/formatters/base.py

# Built-in imports
from abc import ABC, abstractmethod
from typing import Dict, List


class IOutputFormatter(ABC):
    """
    Interface for output formatters that convert data structures to specific formats.
    """

    @property
    @abstractmethod
    def format_name(self) -> str:
        """Gets the name of the formatter (e.g., 'markdown', 'csv')."""
        pass

    @abstractmethod
    def convert_dict(
        self, data: Dict[str, str], column_one_header: str, column_two_header: str
    ) -> str:
        """Converts a dictionary into a formatted table."""
        pass

    @abstractmethod
    def convert_list_of_dicts(self, data: List[Dict[str, any]]) -> str:
        """Converts a list of dictionaries into a formatted table."""
        pass

    @abstractmethod
    def convert_list(self, data: List[str], column_name: str) -> str:
        """Converts a list into a formatted table with a specified column name."""
        pass
