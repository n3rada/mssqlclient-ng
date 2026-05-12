# tests/test_terminal.py

"""Tests for the Terminal class — alias resolution, command dispatch, prompt building."""

import pytest
from unittest.mock import MagicMock, patch

from mssqlclient_ng.core.terminal import Terminal
from mssqlclient_ng.core.models.linked_servers import LinkedServers
from mssqlclient_ng.core.models.server_execution_state import ServerExecutionState


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
            "debug",
            "chain",
            "format",
            "link",
            "unlink-all",
            "impersonate",
            "revert",
            "add-link",
            "unlink",
            "help",
            "flush",
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
        mock_database_context.user_service.get_info.return_value = (
            "new_mapped",
            "new_system",
        )
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
        mock_database_context.user_service.get_info.return_value = (
            "dbo",
            "sa_impersonated",
        )

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


class TestTerminalHistorySwitching:
    """Test that _switch_history creates per-(server, identity) history files."""

    def _make_terminal_with_history(self, mock_database_context, tmp_path):
        """Return a Terminal whose history is rooted in tmp_path."""
        terminal = Terminal(mock_database_context)
        terminal._history_dir = tmp_path
        # Provide non-empty _session_kwargs so the guard passes; patch
        # _make_session so it returns a fresh MagicMock (avoids constructing a
        # real PromptSession which would need a real terminal).
        terminal._session_kwargs = {"_test": True}
        terminal._prompt_session = MagicMock()
        terminal._make_session = lambda history_backend: MagicMock()
        return terminal

    def _expected_file(
        self, tmp_path, server, system_user, mapped_user, is_sysadmin=False
    ):
        state = ServerExecutionState(
            hostname=server,
            system_user=system_user,
            mapped_user=mapped_user,
            is_sysadmin=is_sysadmin,
        )
        return tmp_path / f"{server}_{state.short_hash}_history"

    # ------------------------------------------------------------------
    # Basic mechanics
    # ------------------------------------------------------------------

    def test_switch_creates_file(self, mock_database_context, tmp_path):
        terminal = self._make_terminal_with_history(mock_database_context, tmp_path)
        terminal._switch_history("SQL02")
        expected = self._expected_file(tmp_path, "SQL02", "sa", "dbo")
        assert expected.exists()

    def test_switch_updates_history_file_attr(self, mock_database_context, tmp_path):
        terminal = self._make_terminal_with_history(mock_database_context, tmp_path)
        terminal._switch_history("SQL02")
        expected = self._expected_file(tmp_path, "SQL02", "sa", "dbo")
        assert terminal._history_file == expected

    def test_switch_assigns_new_history_to_session(
        self, mock_database_context, tmp_path
    ):
        terminal = self._make_terminal_with_history(mock_database_context, tmp_path)
        terminal._switch_history("SQL02")
        assert terminal._prompt_session.history is not None

    # ------------------------------------------------------------------
    # Different servers, different identities
    # ------------------------------------------------------------------

    def test_switch_different_servers_different_files(
        self, mock_database_context, tmp_path
    ):
        terminal = self._make_terminal_with_history(mock_database_context, tmp_path)
        terminal._switch_history("SQL02")
        file_sql02 = terminal._history_file
        terminal._switch_history("SQL03")
        file_sql03 = terminal._history_file
        assert file_sql02 != file_sql03
        assert file_sql02 == self._expected_file(tmp_path, "SQL02", "sa", "dbo")
        assert file_sql03 == self._expected_file(tmp_path, "SQL03", "sa", "dbo")

    def test_same_server_different_identity_different_file(
        self, mock_database_context, tmp_path
    ):
        """Same server but different login must yield a distinct history file."""
        terminal = self._make_terminal_with_history(mock_database_context, tmp_path)
        terminal._switch_history("SQL02")
        file_as_sa = terminal._history_file

        # Simulate a privilege escalation — identity changes
        mock_database_context.user_service.system_user = "svc-deploy"
        mock_database_context.user_service.mapped_user = "svc-deploy"
        terminal._switch_history("SQL02")
        file_as_svc = terminal._history_file

        assert file_as_sa != file_as_svc
        assert file_as_svc == self._expected_file(
            tmp_path, "SQL02", "svc-deploy", "svc-deploy"
        )

    def test_switch_same_server_same_identity_idempotent(
        self, mock_database_context, tmp_path
    ):
        terminal = self._make_terminal_with_history(mock_database_context, tmp_path)
        terminal._switch_history("SQL02")
        first = terminal._history_file
        terminal._switch_history("SQL02")
        assert terminal._history_file == first

    # ------------------------------------------------------------------
    # Edge cases
    # ------------------------------------------------------------------

    def test_switch_no_history_dir_is_noop(self, mock_database_context):
        """If history is disabled (_history_dir is None), _switch_history must not raise."""
        terminal = Terminal(mock_database_context)
        terminal._switch_history("SQL02")  # no-op, no exception

    def test_log_server_context_switches_history(self, mock_database_context, tmp_path):
        terminal = self._make_terminal_with_history(mock_database_context, tmp_path)
        mock_database_context.query_service.execution_server = "SQL02"
        terminal._log_server_context()
        expected = self._expected_file(tmp_path, "SQL02", "sa", "dbo")
        assert terminal._history_file == expected

    def test_history_file_permissions(self, mock_database_context, tmp_path):
        import stat

        terminal = self._make_terminal_with_history(mock_database_context, tmp_path)
        terminal._switch_history("SQL04")
        expected = self._expected_file(tmp_path, "SQL04", "sa", "dbo")
        mode = expected.stat().st_mode
        assert stat.S_IMODE(mode) == 0o600

    def test_context_hash_is_deterministic(self):
        state = ServerExecutionState(
            hostname="SQL02", system_user="sa", mapped_user="dbo", is_sysadmin=False
        )
        assert state.short_hash == state.short_hash

    def test_context_hash_differs_on_user_change(self):
        h1 = ServerExecutionState("SQL02", "sa", "dbo", False).short_hash
        h2 = ServerExecutionState("SQL02", "svc-deploy", "svc-deploy", False).short_hash
        assert h1 != h2

    def test_context_hash_differs_on_server_change(self):
        h1 = ServerExecutionState("SQL02", "sa", "dbo", False).short_hash
        h2 = ServerExecutionState("SQL03", "sa", "dbo", False).short_hash
        assert h1 != h2


class TestTerminalHandleLink:
    """Test _handle_link: sets chain, updates execution server, refreshes user."""

    def test_link_single_server_updates_execution_server(self, mock_database_context):
        terminal = Terminal(mock_database_context)
        terminal._handle_link("link SQL02")
        assert mock_database_context.query_service.execution_server == "SQL02"

    def test_link_updates_chain(self, mock_database_context):
        terminal = Terminal(mock_database_context)
        terminal._handle_link("link SQL02")
        assert not mock_database_context.query_service.linked_servers.is_empty
        assert (
            mock_database_context.query_service.linked_servers.server_chain[0].hostname
            == "SQL02"
        )

    def test_link_calls_refresh_user_info(self, mock_database_context):
        terminal = Terminal(mock_database_context)
        with patch.object(terminal, "_refresh_user_info") as mock_refresh:
            terminal._handle_link("link SQL02")
            mock_refresh.assert_called()

    def test_link_multi_hop_sets_last_server(self, mock_database_context):
        terminal = Terminal(mock_database_context)
        terminal._handle_link("link SQL02;SQL03")
        assert mock_database_context.query_service.execution_server == "SQL03"

    def test_link_with_impersonation_stored_in_chain(self, mock_database_context):
        terminal = Terminal(mock_database_context)
        terminal._handle_link("link SQL02/john")
        chain = mock_database_context.query_service.linked_servers.server_chain
        assert chain[0].impersonation_users == ["john"]

    def test_link_no_server_shows_empty_message(self, mock_database_context):
        terminal = Terminal(mock_database_context)
        # No args — just shows chain status, does not modify linked_servers
        terminal._handle_link("link")
        assert mock_database_context.query_service.linked_servers.is_empty


class TestTerminalChainAndUnchain:
    """Integration-style tests: chain up then walk back, verifying state at each step."""

    def _make_ctx(self, mock_database_context, system_user="sa", mapped_user="dbo"):
        """Convenience: set user_service identity and get_info return value."""
        mock_database_context.user_service.system_user = system_user
        mock_database_context.user_service.mapped_user = mapped_user
        mock_database_context.user_service.get_info.return_value = (
            mapped_user,
            system_user,
        )

    def test_unlink_all_clears_chain_and_reverts_impersonation(
        self, mock_database_context
    ):
        """Unlink-all must call revert_impersonation and land at the original server."""
        # Simulate: after revert_impersonation the user_service reports original identity
        mock_database_context.user_service.get_info.return_value = ("dbo", "sa")
        terminal = Terminal(mock_database_context)

        # Apply a chain AFTER creating the terminal so _original_execution_server = LAB-SQL01
        linked = LinkedServers("SQL02/john;SQL03")
        mock_database_context.query_service.linked_servers = linked
        mock_database_context.query_service.execution_server = "SQL03"

        terminal._handle_unlink_all("unlink-all")

        mock_database_context.user_service.revert_impersonation.assert_called_once()
        assert mock_database_context.query_service.linked_servers.is_empty
        assert mock_database_context.query_service.execution_server == "LAB-SQL01"

    def test_unlink_all_live_identity_used_not_stale_cache(self, mock_database_context):
        """_restore_to_original must call get_info() after revert, not use stale cached values."""
        linked = LinkedServers("SQL02/john")
        mock_database_context.query_service.linked_servers = linked

        # After revert the live user_service reports a different user than stored original
        mock_database_context.user_service.get_info.return_value = (
            "domain_user",
            "DOMAIN\\user",
        )

        terminal = Terminal(mock_database_context)
        terminal._handle_unlink_all("unlink-all")

        # server.* fields must reflect the live get_info() result, not _original_*
        assert mock_database_context.server.mapped_user == "domain_user"
        assert mock_database_context.server.system_user == "DOMAIN\\user"

    def test_unlink_single_hop_calls_restore(self, mock_database_context):
        """!unlink with exactly one hop left triggers full restore."""
        linked = LinkedServers("SQL02")
        mock_database_context.query_service.linked_servers = linked

        terminal = Terminal(mock_database_context)
        with patch.object(terminal, "_restore_to_original") as mock_restore:
            terminal._handle_unlink("unlink")
            mock_restore.assert_called_once()

    def test_unlink_multi_hop_pops_last_and_updates_server(self, mock_database_context):
        """!unlink on a 2-hop chain must remove the last hop and update execution_server."""
        linked = LinkedServers("SQL02;SQL03")
        mock_database_context.query_service.linked_servers = linked
        mock_database_context.query_service.execution_server = "SQL03"

        terminal = Terminal(mock_database_context)
        terminal._handle_unlink("unlink")

        assert len(linked.server_chain) == 1
        assert linked.server_chain[0].hostname == "SQL02"
        assert mock_database_context.query_service.execution_server == "SQL02"

    def test_unlink_refreshes_user_info_after_pop(self, mock_database_context):
        """After popping back to SQL02, get_info() must be called to refresh identity."""
        linked = LinkedServers("SQL02;SQL03")
        mock_database_context.query_service.linked_servers = linked

        terminal = Terminal(mock_database_context)
        get_info_call_count_before = (
            mock_database_context.user_service.get_info.call_count
        )
        terminal._handle_unlink("unlink")
        assert (
            mock_database_context.user_service.get_info.call_count
            > get_info_call_count_before
        )

    def test_chain_then_unlink_all_restores_execution_server(
        self, mock_database_context
    ):
        """Simulates: link SQL02;SQL03 then !unlink-all -> back to LAB-SQL01."""
        terminal = Terminal(mock_database_context)

        mock_database_context.user_service.get_info.return_value = ("dbo", "sa")
        terminal._handle_link("link SQL02;SQL03")

        assert mock_database_context.query_service.execution_server == "SQL03"

        mock_database_context.user_service.get_info.return_value = ("dbo", "sa")
        terminal._handle_unlink_all("unlink-all")

        assert mock_database_context.query_service.execution_server == "LAB-SQL01"
        assert mock_database_context.query_service.linked_servers.is_empty

    def test_chain_with_impersonation_then_unlink_reverts(self, mock_database_context):
        """Impersonation encoded in chain spec is stored; unlink-all reverts it."""
        mock_database_context.user_service.get_info.return_value = ("john-a", "john-a")
        terminal = Terminal(mock_database_context)
        terminal._handle_link("link SQL02/john/john-a")

        chain = mock_database_context.query_service.linked_servers.server_chain
        assert chain[0].impersonation_users == ["john", "john-a"]

        # Now unlink-all: revert_impersonation + live get_info
        mock_database_context.user_service.get_info.return_value = ("dbo", "sa")
        terminal._handle_unlink_all("unlink-all")

        mock_database_context.user_service.revert_impersonation.assert_called()
        assert mock_database_context.server.system_user == "sa"
        assert mock_database_context.server.mapped_user == "dbo"

    def test_restore_to_original_uses_live_get_info(self, mock_database_context):
        """_restore_to_original must call get_info() not rely on _original_* for server fields."""
        linked = LinkedServers("SQL02")
        mock_database_context.query_service.linked_servers = linked

        # Live get_info returns a different identity than the stored original
        mock_database_context.user_service.get_info.return_value = (
            "live_mapped",
            "live_system",
        )

        terminal = Terminal(mock_database_context)
        terminal._restore_to_original()

        assert mock_database_context.server.mapped_user == "live_mapped"
        assert mock_database_context.server.system_user == "live_system"
