# mssqlclient_ng/core/utils/formatters/json.py

# Built-in imports
import json
from typing import Any

# Local library imports
from .base import IOutputFormatter


class _SqlEncoder(json.JSONEncoder):
    """Serialize SQL-typed values that json.dumps cannot handle natively."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="replace")
        return str(obj)


class JsonFormatter(IOutputFormatter):
    """Formatter that emits compact, machine-readable JSON."""

    @property
    def format_name(self) -> str:
        return "json"

    def convert_dict(
        self, data: dict[str, str], column_one_header: str, column_two_header: str
    ) -> str:
        return json.dumps(data, indent=2, cls=_SqlEncoder, ensure_ascii=False)

    def convert_list_of_dicts(self, data: list[dict[str, Any]]) -> str:
        if not data:
            return "[]"
        return json.dumps(data, indent=2, cls=_SqlEncoder, ensure_ascii=False)

    def convert_list(self, data: list[str], column_name: str) -> str:
        return json.dumps(data, indent=2, ensure_ascii=False)
