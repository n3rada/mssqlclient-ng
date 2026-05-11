# tests/test_formatters.py

"""Tests for output formatters (markdown, csv) and the OutputFormatter dispatcher."""

import pytest

from mssqlclient_ng.core.utils.formatters import OutputFormatter
from mssqlclient_ng.core.utils.formatters.markdown import MarkdownFormatter
from mssqlclient_ng.core.utils.formatters.csv import CsvFormatter
from mssqlclient_ng.core.utils.formatter import (
    normalize_value,
    dict_to_markdown_table,
    list_to_markdown_table,
    rows_to_markdown_table,
    table_to_markdown,
    format_table,
)


# ── OutputFormatter dispatcher ──────────────────────────────────────────


class TestOutputFormatterDispatcher:
    """Test the OutputFormatter class-level format switching."""

    def setup_method(self):
        """Reset to markdown before each test."""
        OutputFormatter.set_format("markdown")

    def test_default_format_is_markdown(self):
        assert OutputFormatter.current_format() == "markdown"

    def test_set_csv(self):
        OutputFormatter.set_format("csv")
        assert OutputFormatter.current_format() == "csv"

    def test_set_markdown_alias(self):
        OutputFormatter.set_format("csv")
        OutputFormatter.set_format("md")
        assert OutputFormatter.current_format() == "markdown"

    def test_set_invalid_format_raises(self):
        with pytest.raises(ValueError, match="Unknown output format"):
            OutputFormatter.set_format("xml")

    def test_set_empty_format_raises(self):
        with pytest.raises(ValueError, match="cannot be null or empty"):
            OutputFormatter.set_format("")

    def test_available_formats(self):
        formats = OutputFormatter.get_available_formats()
        assert "markdown" in formats
        assert "csv" in formats


# ── MarkdownFormatter ────────────────────────────────────────────────────


class TestMarkdownFormatter:
    """Test MarkdownFormatter output."""

    def setup_method(self):
        self.fmt = MarkdownFormatter()

    def test_format_name(self):
        assert self.fmt.format_name == "markdown"

    def test_convert_dict(self):
        data = {"key1": "val1", "key2": "val2"}
        result = self.fmt.convert_dict(data, "Key", "Value")
        assert "| Key" in result
        assert "| key1" in result
        assert "| val1" in result
        assert "---" in result

    def test_convert_dict_empty(self):
        assert self.fmt.convert_dict({}, "K", "V") == ""

    def test_convert_list(self):
        data = ["alpha", "beta", "gamma"]
        result = self.fmt.convert_list(data, "Items")
        assert "| Items" in result
        assert "| alpha" in result

    def test_convert_list_empty(self):
        assert self.fmt.convert_list([], "Col") == ""

    def test_convert_list_of_dicts(self):
        data = [
            {"Name": "Alice", "Age": "30"},
            {"Name": "Bob", "Age": "25"},
        ]
        result = self.fmt.convert_list_of_dicts(data)
        assert "| Name" in result
        assert "| Alice" in result
        assert "| Bob" in result

    def test_convert_list_of_dicts_empty(self):
        assert self.fmt.convert_list_of_dicts([]) == "No data available."

    def test_format_value_none(self):
        assert MarkdownFormatter._format_value(None) == "NULL"

    def test_format_value_bytes(self):
        result = MarkdownFormatter._format_value(b"\xde\xad")
        assert result == "0xDEAD"

    def test_format_value_string(self):
        assert MarkdownFormatter._format_value("hello") == "hello"

    def test_byte_array_to_hex_empty(self):
        assert MarkdownFormatter._byte_array_to_hex_string(b"") == ""


# ── CsvFormatter ─────────────────────────────────────────────────────────


class TestCsvFormatter:
    """Test CsvFormatter output."""

    def setup_method(self):
        self.fmt = CsvFormatter()

    def test_format_name(self):
        assert self.fmt.format_name == "csv"

    def test_convert_dict(self):
        data = {"host": "SQL01", "port": "1433"}
        result = self.fmt.convert_dict(data, "Key", "Value")
        assert "Key;Value" in result
        assert "host;SQL01" in result

    def test_convert_dict_empty(self):
        assert self.fmt.convert_dict({}, "K", "V") == ""

    def test_convert_list(self):
        data = ["a", "b", "c"]
        result = self.fmt.convert_list(data, "Col")
        lines = result.strip().splitlines()
        assert lines[0] == "Col"
        assert lines[1] == "a"

    def test_convert_list_empty(self):
        assert self.fmt.convert_list([], "Col") == ""

    def test_convert_list_of_dicts(self):
        data = [{"X": "1", "Y": "2"}, {"X": "3", "Y": "4"}]
        result = self.fmt.convert_list_of_dicts(data)
        assert "X;Y" in result
        assert "1;2" in result

    def test_convert_list_of_dicts_empty(self):
        assert self.fmt.convert_list_of_dicts([]) == "No data available."

    def test_escape_csv_separator(self):
        result = CsvFormatter._escape_csv_value("hello;world")
        assert result == '"hello;world"'

    def test_escape_csv_quotes(self):
        result = CsvFormatter._escape_csv_value('say "hi"')
        assert result == '"say ""hi"""'

    def test_escape_csv_newline(self):
        result = CsvFormatter._escape_csv_value("line1\nline2")
        assert result == '"line1\nline2"'

    def test_escape_csv_plain(self):
        assert CsvFormatter._escape_csv_value("plain") == "plain"

    def test_escape_csv_empty(self):
        assert CsvFormatter._escape_csv_value("") == ""

    def test_format_value_none(self):
        assert CsvFormatter._format_value(None) == ""

    def test_format_value_bytes(self):
        assert CsvFormatter._format_value(b"\xca\xfe") == "0xCAFE"


# ── Formatter utilities (formatter.py) ──────────────────────────────────


class TestFormatterUtilities:
    """Test the convenience functions in formatter.py."""

    def setup_method(self):
        OutputFormatter.set_format("markdown")

    def test_normalize_value_bytes(self):
        assert normalize_value(b"hello") == "hello"

    def test_normalize_value_none(self):
        assert normalize_value(None) == ""

    def test_normalize_value_int(self):
        assert normalize_value(42) == "42"

    def test_dict_to_markdown_table(self):
        result = dict_to_markdown_table({"a": "1"}, "Key", "Val")
        assert "| a" in result

    def test_list_to_markdown_table(self):
        result = list_to_markdown_table(["x", "y"], "Items")
        assert "| x" in result

    def test_rows_to_markdown_table(self):
        rows = [{"Name": "Alice"}]
        result = rows_to_markdown_table(rows)
        assert "Alice" in result

    def test_table_to_markdown_with_headers(self):
        result = table_to_markdown([["a", "b"]], headers=["C1", "C2"])
        assert "C1" in result
        assert "a" in result

    def test_table_to_markdown_no_headers(self):
        result = table_to_markdown([["val1", "val2"]])
        assert "val1" in result

    def test_table_to_markdown_empty(self):
        assert table_to_markdown([]) == ""

    def test_format_table_alias(self):
        result = format_table(["H1"], [["v1"]])
        assert "H1" in result
        assert "v1" in result
