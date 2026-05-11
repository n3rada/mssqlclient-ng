# tests/test_terminal.py

"""Tests for the Terminal class — alias resolution, command dispatch, prompt building."""

import pytest
from unittest.mock import MagicMock, patch

from mssqlclient_ng.core.terminal import Terminal
from mssqlclient_ng.core.models.linked_servers import LinkedServers


class TestTerminalAliases:
    """Test that built-in aliases resolve correctly."""

    def test_alias_imp(self):
        assert Terminal._BUILTIN_ALIASES["imp"] == "impersonate"

    def test_alias_rev(self):
        assert Terminal._BUILTIN_ALIASES["rev"] == "revert"

    def test_alias_ul(self):
        assert Terminal._BUILTIN_ALIASES["ul"] == "unlink"

    def test_alias_ula(self):
        assert Terminal._BUILTIN_ALIASES["ula"] == "unlink-all"

    def test_alias_al(self):
        assert Terminal._BUILTIN_ALIASES["al"] == "add-link"


class TestTerminalInit:
    """Test Terminal initialization stores original state."""

    def test_stores_original_user(self, mock_database_context):
        terminal = Terminal(mock_database_context)
        assert terminal._original_mapped_user == "dbo"
        assert terminal._original_system_user == "sa"

    def test_stores_original_server(self, mock_database_context):
        terminal = Terminal(mock_database_context)
        assert terminal._original_execution_server == "LAB-SQL01"
        assert terminal._original_execution_database == "master"

    def test_command_handlers_registered(self, mock_database_context):
        terminal = Terminal(mock_database_context)
        expected = {
            "debug", "chain", "format", "link",
            "unlink-all", "impersonate", "revert",
            "add-link", "unlink", "help",
        }
        assert set(terminal._command_handlers.keys()) == expected


class TestTerminalPrompt:
    """Test prompt generation."""

    def test_full_prompt(self, mock_database_context):
        terminal = Terminal(mock_database_context)
        prompt = terminal._prompt()
        assert "[LAB-SQL01]" in prompt
        assert "sa" in prompt
        assert "dbo" in prompt
        assert "master" in prompt

    def test_prompt_system_user_only(self, mock_database_context):
        mock_database_context.user_service.mapped_user = None
        terminal = Terminal(mock_database_context)
        prompt = terminal._prompt()
        assert "[LAB-SQL01]/sa@master>" in prompt

    def test_prompt_mapped_user_only(self, mock_database_context):
        mock_database_context.user_service.system_user = None
        terminal = Terminal(mock_database_context)
        prompt = terminal._prompt()
        assert "(dbo)" in prompt

    def test_prompt_no_users(self, mock_database_context):
        mock_database_context.user_service.system_user = None
        mock_database_context.user_service.mapped_user = None
        terminal = Terminal(mock_database_context)
        prompt = terminal._prompt()
        assert prompt == "[LAB-SQL01]@master> "


class TestTerminalCommandMatching:
    """Test _match_command dispatches correctly."""

    def test_exact_match(self, mock_database_context):
        terminal = Terminal(mock_database_context)
        handler = terminal._match_command("debug")
        assert handler is not None
        assert handler == terminal._handle_debug

    def test_match_with_args(self, mock_database_context):
        terminal = Terminal(mock_database_context)
        handler = terminal._match_command("link SQL02")
        assert handler == terminal._handle_link

    def test_no_match(self, mock_database_context):
        terminal = Terminal(mock_database_context)
        handler = terminal._match_command("whoami")
        assert handler is None

    def test_unlink_all_before_unlink(self, mock_database_context):
        """unlink-all must match before unlink (prefix conflict)."""
        terminal = Terminal(mock_database_context)
        handler = terminal._match_command("unlink-all")
        assert handler == terminal._handle_unlink_all


class TestTerminalHandleDebug:
    """Test debug toggle handler."""

    def test_toggle_debug_on(self, mock_database_context):
        terminal = Terminal(mock_database_context, log_level="INFO")
        with patch("mssqlclient_ng.core.terminal.logbook"):
            terminal._handle_debug("debug")
        assert terminal._log_level == "DEBUG"

    def test_toggle_debug_off(self, mock_database_context):
        terminal = Terminal(mock_database_context, log_level="DEBUG")
        with patch("mssqlclient_ng.core.terminal.logbook"):
            terminal._handle_debug("debug")
        assert terminal._log_level == "INFO"


class TestTerminalRestoreToOriginal:
    """Test _restore_to_original helper."""

    def test_restores_server_and_user(self, mock_database_context):
        terminal = Terminal(mock_database_context)

        # Simulate changes
        mock_database_context.server.mapped_user = "changed"
        mock_database_context.server.system_user = "changed"
        mock_database_context.query_service.execution_server = "SQL02"
        mock_database_context.query_service.execution_database = "tempdb"

        terminal._restore_to_original()

        assert mock_database_context.server.mapped_user == "dbo"
        assert mock_database_context.server.system_user == "sa"
        assert mock_database_context.query_service.execution_server == "LAB-SQL01"
        assert mock_database_context.query_service.execution_database == "master"

    def test_clears_linked_servers(self, mock_database_context):
        terminal = Terminal(mock_database_context)
        terminal._restore_to_original()
        # linked_servers is a real LinkedServers instance; verify it's now empty
        assert mock_database_context.query_service.linked_servers.is_empty

    def test_reverts_impersonation(self, mock_database_context):
        terminal = Terminal(mock_database_context)
        terminal._restore_to_original()
        mock_database_context.user_service.revert_impersonation.assert_called_once()


class TestTerminalRefreshUserInfo:
    """Test _refresh_user_info helper."""

    def test_updates_server_model(self, mock_database_context):
        mock_database_context.user_service.get_info.return_value = ("new_mapped", "new_system")
        terminal = Terminal(mock_database_context)
        terminal._refresh_user_info()

        assert mock_database_context.server.mapped_user == "new_mapped"
        assert mock_database_context.server.system_user == "new_system"


class TestTerminalHandleUnlinkAll:
    """Test unlink-all handler."""

    def test_empty_chain_logs_info(self, mock_database_context):
        terminal = Terminal(mock_database_context)
        # linked_servers already empty by default — should not raise
        terminal._handle_unlink_all("unlink-all")
        assert mock_database_context.query_service.linked_servers.is_empty

    def test_non_empty_chain_restores(self, mock_database_context):
        linked = LinkedServers("SQL02;SQL03")
        mock_database_context.query_service.linked_servers = linked
        terminal = Terminal(mock_database_context)

        with patch.object(terminal, "_restore_to_original") as mock_restore:
            terminal._handle_unlink_all("unlink-all")
            mock_restore.assert_called_once()


class TestTerminalHandleUnlink:
    """Test unlink handler."""

    def test_empty_chain(self, mock_database_context):
        terminal = Terminal(mock_database_context)
        # Should not raise, just log info
        terminal._handle_unlink("unlink")

    def test_single_server_chain_restores(self, mock_database_context):
        linked = LinkedServers("SQL02")
        mock_database_context.query_service.linked_servers = linked
        terminal = Terminal(mock_database_context)

        with patch.object(terminal, "_restore_to_original") as mock_restore:
            terminal._handle_unlink("unlink")
            mock_restore.assert_called_once()

    def test_multi_server_chain_pops_last(self, mock_database_context):
        linked = LinkedServers("SQL02;SQL03")
        mock_database_context.query_service.linked_servers = linked

        terminal = Terminal(mock_database_context)

        terminal._handle_unlink("unlink")

        # Should have removed SQL03
        assert len(linked.server_chain) == 1
        assert linked.server_chain[0].hostname == "SQL02"


class TestTerminalHandleImpersonate:
    """Test impersonate handler."""

    def test_no_login_provided(self, mock_database_context):
        terminal = Terminal(mock_database_context)
        # Should not raise, just log error
        terminal._handle_impersonate("impersonate")

    def test_successful_impersonation(self, mock_database_context):
        mock_database_context.user_service.can_impersonate.return_value = True
        mock_database_context.user_service.impersonate_user.return_value = True
        mock_database_context.user_service.get_info.return_value = ("dbo", "sa_impersonated")

        terminal = Terminal(mock_database_context)
        terminal._handle_impersonate("impersonate sa")

        mock_database_context.user_service.can_impersonate.assert_called_with("sa")
        mock_database_context.user_service.impersonate_user.assert_called_with("sa")

    def test_cannot_impersonate(self, mock_database_context):
        mock_database_context.user_service.can_impersonate.return_value = False
        terminal = Terminal(mock_database_context)
        terminal._handle_impersonate("impersonate sa")
        mock_database_context.user_service.impersonate_user.assert_not_called()


class TestTerminalHandleRevert:
    """Test revert handler."""

    def test_reverts_and_refreshes(self, mock_database_context):
        terminal = Terminal(mock_database_context)
        terminal._handle_revert("revert")
        mock_database_context.user_service.revert_impersonation.assert_called_once()
        mock_database_context.user_service.get_info.assert_called()


class TestTerminalHandleFormat:
    """Test format handler."""

    def test_show_current_format(self, mock_database_context):
        terminal = Terminal(mock_database_context)
        # Should not raise when no format specified
        terminal._handle_format("format")

    def test_change_format(self, mock_database_context):
        from mssqlclient_ng.core.utils.formatters import OutputFormatter

        OutputFormatter.set_format("markdown")  # reset
        terminal = Terminal(mock_database_context)
        terminal._handle_format("format csv")
        assert OutputFormatter.current_format() == "csv"
        OutputFormatter.set_format("markdown")  # cleanup
