# mssqlclient_ng/core/services/authentication.py

# Built-in imports

# Third party imports
from loguru import logger
from impacket.tds import MSSQL

# Local library imports
from ..models.server import Server

class AuthenticationService:
    """
    Service for authenticating and managing MSSQL connections.
    Stores authentication parameters for reconnection and duplication.
    """

    def __init__(
        self,
        server: Server,
        remote_name: str,
        username: str | None = None,
        password: str | None = None,
        domain: str | None = None,
        use_windows_auth: bool = False,
        hashes: str | None = None,
        aes_key: str | None = None,
        kerberos_auth: bool = False,
        kdc_host: str | None = None,
        workstation_id: str = "SQLServerCEIP",
        application_name: str = "Framework Microsoft SqlClient Da",
        client_interface_name: str = ".Net SqlClient Data Provider",
    ):
        """
        Initialize the authentication service.

        Args:
            server: The Server instance to authenticate against
            remote_name: The remote server name for Kerberos SPN
            username: The username for authentication
            password: The password for authentication
            domain: The domain for Windows authentication
            use_windows_auth: Whether to use Windows authentication
            hashes: NTLM hashes in format "lmhash:nthash"
            aes_key: AES key for Kerberos authentication
            kerberos_auth: Whether to use Kerberos authentication
            kdc_host: KDC hostname for Kerberos authentication
            workstation_id: Workstation ID for the TDS LOGIN packet
            application_name: Application name for the TDS LOGIN packet
            client_interface_name: Client interface name for the TDS LOGIN packet
        """
        self.server = server
        self._database: str | None = self.server.database
        self.mssql_instance: MSSQL | None = None

        # Store authentication parameters
        self._remote_name = remote_name
        self._username = username
        self._password = password
        self._domain = domain
        self._use_windows_auth = use_windows_auth
        self._hashes = hashes
        self._aes_key = aes_key
        self._kerberos_auth = kerberos_auth
        self._kdc_host = kdc_host
        self._workstation_id = workstation_id
        self._application_name = application_name
        self._client_interface_name = client_interface_name

    def connect(self) -> bool:
        """
        Establish connection and authenticate to the SQL Server.

        Returns:
            True if authentication was successful; otherwise False
        """
        return self._authenticate()

    def _authenticate(self) -> bool:
        """
        Internal method to authenticate and establish a connection to the SQL Server.

        Returns:
            True if authentication was successful; otherwise False
        """
        try:
            # Create MSSQL connection
            self.mssql_instance = MSSQL(
                address=self.server.hostname,
                port=self.server.port,
                remoteName=self._remote_name,
                # TODO: uncomment once Impacket PR is merged
                # workstation_id=self._workstation_id,
                # application_name=self._application_name,
                # client_interface_name=self._client_interface_name,
            )

            # Establish TCP connection
            chausette = self.mssql_instance.connect()

            if not chausette:
                logger.error(
                    f"Failed to establish TCP connection to {self.server.hostname}:{self.server.port}"
                )
                return False

            # Perform authentication
            if self._kerberos_auth:
                # Kerberos authentication
                logger.info("Attempting Kerberos authentication")
                success = self.mssql_instance.kerberosLogin(
                    database=self._database,
                    username=self._username,
                    password=self._password or "",
                    domain=self._domain or "",
                    hashes=self._hashes,
                    aesKey=self._aes_key or "",
                    kdcHost=self._kdc_host,
                )
            else:
                # SQL or Windows authentication
                auth_type = "Windows" if self._use_windows_auth else "SQL"
                logger.info(f"Attempting {auth_type} authentication")
                success = self.mssql_instance.login(
                    database=self._database,
                    username=self._username or "",
                    password=self._password or "",
                    domain=self._domain or "",
                    hashes=self._hashes,
                    useWindowsAuth=self._use_windows_auth,
                )

            if not success:
                logger.error("Authentication failed")
                self.disconnect()
                return False

            logger.success(f"Successfully authenticated to {self.server.hostname}")
            logger.info(f"Database: {self._database or 'default'}")

            # Update server version from connection
            if hasattr(self.mssql_instance, "mssql_version"):
                self.server.version = str(self.mssql_instance.mssql_version)
                logger.info(f"Server version: {self.server.version}")

            # TODO: uncomment once Impacket PR is merged
            # Log connection details (matching MSSQLand's connection info block)
            # ms = self.mssql_instance
            # encrypted = "TLS" if ms.tlsSocket else "None"
            # logger.info(f"Encryption: {encrypted}")
            # logger.info(f"Packet size: {ms.packetSize}")
            # logger.info(f"Client workstation ID: {ms.workstation_id}")
            # logger.info(f"Client application name: {ms.application_name}")
            # logger.info(f"Client interface name: {ms.client_interface_name}")

            return True

        except Exception as e:
            logger.error(f"Authentication error: {e}")
            self.disconnect()
            return False

    def disconnect(self) -> None:
        """Close the connection if it exists."""
        if self.mssql_instance:
            try:
                self.mssql_instance.disconnect()
                logger.debug("Connection closed")
            except Exception:
                logger.warning("Error closing connection")
            finally:
                self.mssql_instance = None

    def is_connected(self) -> bool:
        """
        Check if the connection is active.

        Returns:
            True if connected; otherwise False
        """
        return (
            self.mssql_instance is not None and self.mssql_instance.socket is not None
        )

    def __del__(self) -> None:
        """Destructor - ensure connection is closed."""
        self.disconnect()
