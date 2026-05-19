# mssqlclient_ng/core/utils/formatters/grid.py

# Built-in imports

# Local library imports
from .base import IOutputFormatter

# Box-drawing characters
TL = "\u250c"  # ┌
TR = "\u2510"  # ┐
BL = "\u2514"  # └
BR = "\u2518"  # ┘
H = "\u2500"  # ─
V = "\u2502"  # │
TJ = "\u252c"  # ┬
BJ = "\u2534"  # ┴
LJ = "\u251c"  # ├
RJ = "\u2524"  # ┤
CJ = "\u253c"  # ┼

class GridFormatter(IOutputFormatter):
    """Formats data into box-drawing grid tables for terminal display."""

    @property
    def format_name(self) -> str:
        return "grid"

    @staticmethod
    def _byte_array_to_hex_string(data: bytes) -> str:
        if not data:
            return ""
        return "0x" + data.hex().upper()

    @staticmethod
    def _format_value(value: any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bytes):
            return GridFormatter._byte_array_to_hex_string(value)
        if isinstance(value, (list, tuple)):
            return str(value)
        return str(value)

    @staticmethod
    def _top_border(widths: list[int]) -> str:
        return TL + TJ.join(H * (w + 2) for w in widths) + TR

    @staticmethod
    def _mid_border(widths: list[int]) -> str:
        return LJ + CJ.join(H * (w + 2) for w in widths) + RJ

    @staticmethod
    def _bot_border(widths: list[int]) -> str:
        return BL + BJ.join(H * (w + 2) for w in widths) + BR

    @staticmethod
    def _row(values: list[str], widths: list[int]) -> str:
        cells = [f" {v.ljust(w)} " for v, w in zip(values, widths)]
        return V + V.join(cells) + V

    def convert_dict(
        self, data: dict[str, str], column_one_header: str, column_two_header: str
    ) -> str:
        if not data:
            return ""

        col1_width = max(
            len(column_one_header), max((len(k) for k in data.keys()), default=0)
        )
        col2_width = max(
            len(column_two_header),
            max((len(str(v)) for v in data.values()), default=0),
        )
        widths = [col1_width, col2_width]

        lines = [
            self._top_border(widths),
            self._row([column_one_header, column_two_header], widths),
            self._mid_border(widths),
        ]

        for key, value in data.items():
            lines.append(self._row([key, str(value)], widths))

        lines.append(self._bot_border(widths))
        return "\n" + "\n".join(lines) + "\n"

    def convert_list_of_dicts(self, data: list[dict[str, any]]) -> str:
        if not data:
            return "No data available."

        columns = list(data[0].keys()) if data else []
        if not columns:
            return "No data available."

        column_widths = {}
        for col in columns:
            col_name = col if col else "column"
            column_widths[col] = len(col_name)

        for row in data:
            for col in columns:
                value = self._format_value(row.get(col))
                column_widths[col] = max(column_widths[col], len(value))

        widths = [column_widths[col] for col in columns]
        headers = [col if col else "column" for col in columns]

        lines = [
            self._top_border(widths),
            self._row(headers, widths),
            self._mid_border(widths),
        ]

        for row in data:
            values = [self._format_value(row.get(col)) for col in columns]
            lines.append(self._row(values, widths))

        lines.append(self._bot_border(widths))
        return "\n" + "\n".join(lines) + "\n"

    def convert_list(self, data: list[str], column_name: str) -> str:
        if not data:
            return ""

        column_width = max(
            len(column_name), max((len(str(item)) for item in data), default=0)
        )
        widths = [column_width]

        lines = [
            self._top_border(widths),
            self._row([column_name], widths),
            self._mid_border(widths),
        ]

        for item in data:
            lines.append(self._row([str(item)], widths))

        lines.append(self._bot_border(widths))
        return "\n" + "\n".join(lines) + "\n"
