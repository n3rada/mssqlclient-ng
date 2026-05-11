# tests/test_utils.py

"""Tests for utility functions in core/utils/common.py."""

import pytest
import gzip
from base64 import b64encode

from mssqlclient_ng.core.utils.common import (
    generate_random_string,
    get_random_number,
    get_hex_char,
    decode_and_decompress,
    hex_string_to_bytes,
    bytes_to_hex_string,
    compute_sha256,
    normalize_windows_path,
    convert_table_to_dicts,
    bracket_identifier,
)


class TestGenerateRandomString:
    def test_correct_length(self):
        assert len(generate_random_string(10)) == 10

    def test_zero_length(self):
        assert generate_random_string(0) == ""

    def test_alphanumeric_only(self):
        result = generate_random_string(100)
        assert result.isalnum()
        assert result.islower() or result.replace("0123456789", "").islower()

    def test_randomness(self):
        a = generate_random_string(20)
        b = generate_random_string(20)
        # Extremely unlikely to be equal
        assert a != b


class TestGetRandomNumber:
    def test_in_range(self):
        for _ in range(50):
            val = get_random_number(5, 10)
            assert 5 <= val < 10

    def test_single_value_range(self):
        assert get_random_number(7, 8) == 7


class TestGetHexChar:
    def test_digit(self):
        assert get_hex_char(0) == "0"
        assert get_hex_char(9) == "9"

    def test_lower_hex(self):
        assert get_hex_char(10) == "a"
        assert get_hex_char(15) == "f"

    def test_upper_hex(self):
        assert get_hex_char(10, upper=True) == "A"
        assert get_hex_char(15, upper=True) == "F"


class TestDecodeAndDecompress:
    def test_roundtrip(self):
        original = b"Hello, mssqlclient-ng!"
        compressed = gzip.compress(original)
        encoded = b64encode(compressed).decode()
        assert decode_and_decompress(encoded) == original


class TestHexConversions:
    def test_hex_string_to_bytes(self):
        assert hex_string_to_bytes("48656c6c6f") == b"Hello"

    def test_bytes_to_hex_string(self):
        assert bytes_to_hex_string(b"Hello") == "48656c6c6f"

    def test_roundtrip(self):
        original = b"\xde\xad\xbe\xef"
        assert hex_string_to_bytes(bytes_to_hex_string(original)) == original


class TestComputeSha256:
    def test_known_hash(self):
        # SHA-256 of empty string
        result = compute_sha256("")
        assert result == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    def test_deterministic(self):
        assert compute_sha256("test") == compute_sha256("test")

    def test_different_inputs(self):
        assert compute_sha256("a") != compute_sha256("b")


class TestNormalizeWindowsPath:
    def test_single_backslash(self):
        assert normalize_windows_path("C:\\Users") == "C:\\\\Users"

    def test_already_doubled(self):
        assert normalize_windows_path("C:\\\\Users") == "C:\\\\Users"

    def test_forward_slashes_unchanged(self):
        assert normalize_windows_path("C:/Users") == "C:/Users"

    def test_unc_path(self):
        result = normalize_windows_path("\\\\server\\share")
        assert "\\\\" in result


class TestConvertTableToDicts:
    def test_basic_conversion(self):
        headers = ["Name", "Age"]
        rows = [["Alice", 30], ["Bob", 25]]
        result = convert_table_to_dicts(headers, rows)
        assert result == [{"Name": "Alice", "Age": 30}, {"Name": "Bob", "Age": 25}]

    def test_empty_rows(self):
        assert convert_table_to_dicts(["A"], []) == []

    def test_single_column(self):
        result = convert_table_to_dicts(["X"], [["val1"], ["val2"]])
        assert result == [{"X": "val1"}, {"X": "val2"}]


class TestBracketIdentifier:
    def test_plain_name(self):
        assert bracket_identifier("SQL01") == "SQL01"

    def test_name_with_dot(self):
        # Dots are not in the separator list
        assert bracket_identifier("SQL.01") == "SQL.01"

    def test_name_with_colon(self):
        assert bracket_identifier("SQL:01") == "[SQL:01]"

    def test_name_with_slash(self):
        assert bracket_identifier("SQL/01") == "[SQL/01]"

    def test_name_with_at(self):
        assert bracket_identifier("SQL@01") == "[SQL@01]"

    def test_name_with_semicolon(self):
        assert bracket_identifier("SQL;01") == "[SQL;01]"

    def test_name_with_hyphen(self):
        # Hyphens should NOT trigger bracketing
        assert bracket_identifier("SQL-01") == "SQL-01"
