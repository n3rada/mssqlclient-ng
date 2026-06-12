# tests/test_relay.py

"""Unit tests for RelayMSSQL — attack registration, capture logic, cleanup."""

import threading

import pytest
from unittest.mock import MagicMock, patch

from impacket.examples.ntlmrelayx.attacks import PROTOCOL_ATTACKS

from mssqlclient_ng.core.services.ntlmrelay import RelayMSSQL
from mssqlclient_ng.core.models.server import Server


@pytest.fixture
def relay():
    with patch("mssqlclient_ng.core.services.ntlmrelay.TargetsProcessor"):
        return RelayMSSQL(hostname="LAB-SQL01", port=1433)


class TestAttackRegistration:
    def test_custom_attack_registered(self, relay):
        assert "MSSQL" in PROTOCOL_ATTACKS

    def test_plugin_names(self, relay):
        assert PROTOCOL_ATTACKS["MSSQL"].PLUGIN_NAMES == ["MSSQL"]

    def test_attack_is_bound_to_instance(self, relay):
        # Each RelayMSSQL instance registers its own capture reference
        relay2 = RelayMSSQL.__new__(RelayMSSQL)
        relay2._captured_client = None
        relay2._capture_event = threading.Event()
        with patch("mssqlclient_ng.core.services.ntlmrelay.TargetsProcessor"):
            relay2._targets_processor = MagicMock()
        relay2._register_attack()

        attack_cls = PROTOCOL_ATTACKS["MSSQL"]
        attack = attack_cls.__new__(attack_cls)
        attack.client = MagicMock()
        attack.username = "victim"
        attack.domain = "CORP"
        attack.target = None
        attack.run()

        # relay2 captured, relay was overwritten — documents the shared-global limitation
        assert relay2._captured_client is not None
        assert relay2._captured_client["username"] == "victim"


class TestWaitForConnection:
    def _fire_capture(self, relay, delay: float = 0.05):
        """Simulate the attack firing from a background thread."""
        def _fire():
            import time
            time.sleep(delay)
            relay._captured_client = {
                "client": MagicMock(),
                "username": "admin",
                "domain": "CORP",
            }
            relay._capture_event.set()

        t = threading.Thread(target=_fire, daemon=True)
        t.start()
        return t

    def test_success_returns_database_context(self, relay, mock_server):
        self._fire_capture(relay)

        with patch("mssqlclient_ng.core.services.ntlmrelay.DatabaseContext") as MockCtx:
            mock_ctx = MagicMock()
            mock_ctx.server = mock_server
            MockCtx.return_value = mock_ctx

            result = relay.wait_for_connection(server_instance=mock_server, timeout=2)

        assert result is mock_ctx
        MockCtx.assert_called_once()

    def test_success_sets_mapped_user(self, relay, mock_server):
        self._fire_capture(relay)

        with patch("mssqlclient_ng.core.services.ntlmrelay.DatabaseContext") as MockCtx:
            mock_ctx = MagicMock()
            mock_ctx.server = mock_server
            MockCtx.return_value = mock_ctx

            relay.wait_for_connection(server_instance=mock_server, timeout=2)

        assert mock_server.mapped_user == "CORP\\admin"

    def test_success_no_domain_sets_plain_user(self, relay, mock_server):
        def _fire():
            import time
            time.sleep(0.05)
            relay._captured_client = {
                "client": MagicMock(),
                "username": "localadmin",
                "domain": "",
            }
            relay._capture_event.set()

        threading.Thread(target=_fire, daemon=True).start()

        with patch("mssqlclient_ng.core.services.ntlmrelay.DatabaseContext") as MockCtx:
            mock_ctx = MagicMock()
            mock_ctx.server = mock_server
            MockCtx.return_value = mock_ctx

            relay.wait_for_connection(server_instance=mock_server, timeout=2)

        assert mock_server.mapped_user == "localadmin"

    def test_timeout_returns_none(self, relay, mock_server):
        result = relay.wait_for_connection(server_instance=mock_server, timeout=0.1)
        assert result is None

    def test_timeout_without_capture_returns_none(self, relay, mock_server):
        # Event never fired, captured_client stays None
        assert relay._captured_client is None
        result = relay.wait_for_connection(server_instance=mock_server, timeout=0.05)
        assert result is None


class TestStopServers:
    def test_shutdown_called_on_smb_server(self, relay):
        from impacket.examples.ntlmrelayx.servers import SMBRelayServer

        mock_smb = MagicMock(spec=SMBRelayServer)
        mock_smb.server = MagicMock()
        relay._threads.add(mock_smb)

        relay.stop_servers()

        mock_smb.server.shutdown.assert_called_once()
        assert len(relay._threads) == 0

    def test_stop_servers_tolerates_shutdown_error(self, relay):
        from impacket.examples.ntlmrelayx.servers import SMBRelayServer

        # spec= would restrict attributes to those on SMBRelayServer; use plain mock
        # so we can attach .server.shutdown freely
        mock_smb = MagicMock(spec=SMBRelayServer)
        mock_smb.server = MagicMock()
        mock_smb.server.shutdown.side_effect = OSError("socket already closed")
        relay._threads.add(mock_smb)

        relay.stop_servers()  # must not raise

        assert len(relay._threads) == 0

    def test_stop_servers_clears_threads(self, relay):
        from impacket.examples.ntlmrelayx.servers import SMBRelayServer

        for _ in range(3):
            mock_smb = MagicMock(spec=SMBRelayServer)
            relay._threads.add(mock_smb)

        relay.stop_servers()
        assert len(relay._threads) == 0


class TestStart:
    def test_loot_dir_not_cwd(self, relay):
        import os

        captured_config = {}

        def fake_set_loot(path):
            captured_config["loot"] = path

        mock_server = MagicMock()
        mock_server.start = MagicMock()

        with (
            patch("mssqlclient_ng.core.services.ntlmrelay.SMBRelayServer", return_value=mock_server),
            patch("mssqlclient_ng.core.services.ntlmrelay.NTLMRelayxConfig") as MockCfg,
        ):
            cfg_instance = MockCfg.return_value
            cfg_instance.setLootdir.side_effect = fake_set_loot

            relay.start()

        loot = captured_config.get("loot", "")
        assert loot != "."
        assert os.getcwd() not in loot or "relay-loot" in loot

    def test_single_target_mode(self, relay):
        mock_server = MagicMock()

        with (
            patch("mssqlclient_ng.core.services.ntlmrelay.SMBRelayServer", return_value=mock_server),
            patch("mssqlclient_ng.core.services.ntlmrelay.NTLMRelayxConfig") as MockCfg,
        ):
            cfg_instance = MockCfg.return_value
            relay.start()

        cfg_instance.setDisableMulti.assert_called_once_with(True)
        cfg_instance.setKeepRelaying.assert_called_once_with(False)

    def test_smb_server_started(self, relay):
        mock_server = MagicMock()

        with (
            patch("mssqlclient_ng.core.services.ntlmrelay.SMBRelayServer", return_value=mock_server),
            patch("mssqlclient_ng.core.services.ntlmrelay.NTLMRelayxConfig"),
        ):
            relay.start()

        mock_server.start.assert_called_once()
        assert mock_server in relay._threads
