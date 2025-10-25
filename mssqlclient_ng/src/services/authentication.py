"""
Authentication service for managing SQL Server connections using impacket's TDS.
"""
from typing import Optional
from loguru import logger
from impacket.tds import MSSQL

from mssqlclient_ng.src.models.server import Server


class AuthenticationService:
    """
    Service for authenticating and managing MSSQL connections.
    Stores authentication parameters for reconnection and duplication.
    """

    def __init__(self, server: Server):
        """
        Initialize the authentication service.

        Args:
            server: The Server instance to authenticate against
        """
        self.server = server

        self._database: Optional[str] = self.server.database

        self.connection: Optional[MSSQL] = None

        self._username: Optional[str] = None
        self._password: Optional[str] = None
        self._domain: Optional[str] = None
        self._use_windows_auth: bool = False
        self._hashes: Optional[str] = None
        self._aes_key: Optional[str] = None
        self._kdc_host: Optional[str] = None

    def authenticate(
        self,
        remote_name: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        domain: Optional[str] = None,
        use_windows_auth: bool = False,
        hashes: Optional[str] = None,
        aes_key: Optional[str] = None,
        kdc_host: Optional[str] = None
    ) -> bool:
        """
        Authenticate and establish a connection to the SQL Server.

        Args:
            database: The target database (default: "master")
            username: The username for authentication
            password: The password for authentication
            domain: The domain for Windows authentication
            use_windows_auth: Whether to use Windows authentication
            hashes: NTLM hashes in format "lmhash:nthash"
            aes_key: AES key for Kerberos authentication
            kdc_host: KDC hostname for Kerberos authentication

        Returns:
            True if authentication was successful; otherwise False
        """
        self._remote_name = remote_name

        self._username = username
        self._password = password
        self._domain = domain
        self._use_windows_auth = use_windows_auth
        self._hashes = hashes
        self._aes_key = aes_key
        self._kdc_host = kdc_host

        try:
            # Create MSSQL connection
            self.connection = MSSQL(
                address=self.server.hostname,
                port=self.server.port,
                remoteName=self._remote_name
            )

            # Establish TCP connection
            self.connection.connect()
            logger.debug(f"TCP connection established to {self.server.hostname}:{self.server.port}")


            # Perform authentication
            if self._kdc_host:
                # Kerberos authentication
                logger.info("Attempting Kerberos authentication")
                success = self.connection.kerberosLogin(
                    database=self._database,
                    username=username,
                    password=password or '',
                    domain=domain or '',
                    hashes=hashes,
                    aesKey=aes_key or '',
                    kdcHost=kdc_host
                )
            else:
                # SQL or Windows authentication
                auth_type = "Windows" if use_windows_auth else "SQL"
                logger.info(f"Attempting {auth_type} authentication")
                success = self.connection.login(
                    database=self._database,
                    username=username or '',
                    password=password or '',
                    domain=domain or '',
                    hashes=hashes,
                    useWindowsAuth=use_windows_auth
                )

            if not success:
                logger.error("Authentication failed")
                self.disconnect()
                return False

            logger.success(f"Successfully authenticated to {self.server.hostname}")

            # Update server version from connection
            if hasattr(self.connection, 'mssql_version'):
                self.server.version = str(self.connection.mssql_version)
                logger.debug(f"Server version: {self.server.version}")

            return True

        except Exception as e:
            logger.error(f"Authentication error: {e}")
            self.disconnect()
            return False


    def disconnect(self) -> None:
        """Close the connection if it exists."""
        if self.connection:
            try:
                self.connection.disconnect()
                logger.debug("Connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            finally:
                self.connection = None

    def is_connected(self) -> bool:
        """
        Check if the connection is active.

        Returns:
            True if connected; otherwise False
        """
        return self.connection is not None and self.connection.socket is not None

    def __enter__(self) -> "AuthenticationService":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - disconnect on exit."""
        self.disconnect()

    def __del__(self) -> None:
        """Destructor - ensure connection is closed."""
        self.disconnect()
