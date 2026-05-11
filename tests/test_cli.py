# tests/test_cli.py

"""Tests for CLI argument parsing via build_parser()."""

import pytest

from mssqlclient_ng.cli import build_parser


@pytest.fixture
def parser():
    return build_parser()


class TestCliTargetArguments:
    """Test required and optional target arguments."""

    def test_host_required(self, parser):
        import argparse
        with pytest.raises((SystemExit, argparse.ArgumentError)):
            parser.parse_args([])

    def test_host_parsed(self, parser):
        args = parser.parse_args(["SQL01"])
        assert args.host == "SQL01"

    def test_host_with_port(self, parser):
        args = parser.parse_args(["SQL01,1434"])
        assert args.host == "SQL01,1434"

    def test_domain_flag(self, parser):
        args = parser.parse_args(["SQL01", "-d", "CORP.LOCAL"])
        assert args.domain == "CORP.LOCAL"

    def test_database_flag(self, parser):
        args = parser.parse_args(["SQL01", "-db", "master"])
        assert args.database == "master"

    def test_links_flag(self, parser):
        args = parser.parse_args(["SQL01", "-l", "SQL02;SQL03"])
        assert args.links == "SQL02;SQL03"


class TestCliCredentials:
    """Test credential-related arguments."""

    def test_username(self, parser):
        args = parser.parse_args(["SQL01", "-u", "admin"])
        assert args.username == "admin"

    def test_password(self, parser):
        args = parser.parse_args(["SQL01", "-p", "secret"])
        assert args.password == "secret"

    def test_no_pass(self, parser):
        args = parser.parse_args(["SQL01", "-no-pass"])
        assert args.no_pass is True

    def test_hashes(self, parser):
        args = parser.parse_args(["SQL01", "-H", "aad3b435b51404ee:abc123"])
        assert args.hashes == "aad3b435b51404ee:abc123"

    def test_windows_auth(self, parser):
        args = parser.parse_args(["SQL01", "-windows-auth"])
        assert args.windows_auth is True

    def test_windows_auth_default(self, parser):
        args = parser.parse_args(["SQL01"])
        assert args.windows_auth is False


class TestCliKerberos:
    """Test Kerberos arguments."""

    def test_kerberos_flag(self, parser):
        args = parser.parse_args(["SQL01", "-k"])
        assert args.kerberos is True

    def test_kdc_host(self, parser):
        args = parser.parse_args(["SQL01", "--kdcHost", "dc01.corp.local"])
        assert args.kdcHost == "dc01.corp.local"

    def test_aes_key(self, parser):
        args = parser.parse_args(["SQL01", "--aesKey", "0123456789abcdef"])
        assert "0123456789abcdef" in args.aesKey


class TestCliActions:
    """Test action-related arguments."""

    def test_query_flag(self, parser):
        args = parser.parse_args(["SQL01", "-q", "SELECT 1"])
        assert args.query == "SELECT 1"

    def test_action_flag(self, parser):
        args = parser.parse_args(["SQL01", "-a", "whoami"])
        assert args.action == ["whoami"]

    def test_action_with_args(self, parser):
        args = parser.parse_args(["SQL01", "-a", "xp-cmd", "dir C:\\"])
        assert args.action == ["xp-cmd", "dir C:\\"]

    def test_output_format_default(self, parser):
        args = parser.parse_args(["SQL01"])
        assert args.output_format == "markdown"

    def test_output_format_csv(self, parser):
        args = parser.parse_args(["SQL01", "-o", "csv"])
        assert args.output_format == "csv"

    def test_output_format_invalid(self, parser):
        import argparse
        with pytest.raises((SystemExit, argparse.ArgumentError)):
            parser.parse_args(["SQL01", "-o", "xml"])


class TestCliAdvancedOptions:
    """Test advanced/debugging options."""

    def test_prefix_default(self, parser):
        args = parser.parse_args(["SQL01"])
        assert args.prefix == "!"

    def test_prefix_custom(self, parser):
        args = parser.parse_args(["SQL01", "--prefix", "#"])
        assert args.prefix == "#"

    def test_history_default(self, parser):
        args = parser.parse_args(["SQL01"])
        assert args.history is False

    def test_history_enabled(self, parser):
        args = parser.parse_args(["SQL01", "--history"])
        assert args.history is True

    def test_multiline_default(self, parser):
        args = parser.parse_args(["SQL01"])
        assert args.multiline is False

    def test_multiline_enabled(self, parser):
        args = parser.parse_args(["SQL01", "--multiline"])
        assert args.multiline is True

    def test_debug_flag(self, parser):
        args = parser.parse_args(["SQL01", "--debug"])
        assert args.debug is True

    def test_trace_flag(self, parser):
        args = parser.parse_args(["SQL01", "--trace"])
        assert args.trace is True

    def test_log_level(self, parser):
        args = parser.parse_args(["SQL01", "--log-level", "DEBUG"])
        assert args.log_level == "DEBUG"

    def test_log_level_invalid(self, parser):
        import argparse
        with pytest.raises((SystemExit, argparse.ArgumentError)):
            parser.parse_args(["SQL01", "--log-level", "VERBOSE"])

    def test_std_default(self, parser):
        args = parser.parse_args(["SQL01"])
        assert args.std == "err"

    def test_std_out(self, parser):
        args = parser.parse_args(["SQL01", "--std", "out"])
        assert args.std == "out"

    def test_no_log_file(self, parser):
        args = parser.parse_args(["SQL01", "--no-log-file"])
        assert args.no_log_file is True


class TestCliNtlmRelay:
    """Test NTLM relay arguments."""

    def test_relay_flag(self, parser):
        args = parser.parse_args(["SQL01", "-r"])
        assert args.ntlm_relay is True

    def test_timeout_default(self, parser):
        args = parser.parse_args(["SQL01"])
        assert args.timeout == 60

    def test_timeout_custom(self, parser):
        args = parser.parse_args(["SQL01", "-t", "120"])
        assert args.timeout == 120

    def test_smb2support(self, parser):
        args = parser.parse_args(["SQL01", "-smb2support"])
        assert args.smb2support is True
