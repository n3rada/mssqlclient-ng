"""
Server model representing a SQL Server instance with connection details.
"""

from typing import Optional
from loguru import logger


class Server:
    """
    Represents a SQL Server with optional impersonation user.

    Attributes:
        hostname: The hostname or IP address of the server
        version: The full version string of the server (e.g., "15.00.2000")
        port: The SQL Server port (default: 1433)
        database: The database to connect to (default: "master")
        impersonation_user: The user to impersonate on this server (optional)
        mapped_user: The mapped user for the connection
        system_user: The system user for the connection
    """

    def __init__(
        self,
        hostname: str,
        port: int = 1433,
        database: str = "master",
        impersonation_user: Optional[str] = None,
    ):
        """
        Initialize a Server instance.

        Args:
            hostname: The hostname or IP address of the server
            port: The SQL Server port (default: 1433)
            database: The database to connect to (default: "master")
            impersonation_user: The user to impersonate on this server (optional)

        Raises:
            ValueError: If hostname is empty or port is invalid
        """
        if not hostname or not hostname.strip():
            raise ValueError("Hostname cannot be null or empty.")

        if not (1 <= port <= 65535):
            raise ValueError(f"Port must be between 1 and 65535, got {port}")

        self.hostname = hostname.strip()
        self._version: Optional[str] = None
        self.port = port
        self.database = database.strip() if database else "master"

        self.impersonation_user = impersonation_user if impersonation_user else ""
        self.mapped_user = ""
        self.system_user = ""

    @property
    def version(self) -> Optional[str]:
        """Get the server version."""
        return self._version

    @version.setter
    def version(self, value: Optional[str]) -> None:
        """
        Set the server version and check if it's a legacy server.
        Logs a warning if major version <= 13 (SQL Server 2016 or older).
        """
        self._version = value

        if value is not None:
            major = self._parse_major_version(value)
            if major <= 13 and major > 0:
                logger.warning(
                    f"Legacy server detected: version {value} (major version {major})"
                )

    @property
    def major_version(self) -> int:
        """
        The major version of the server (e.g., 15 for "15.00.2000").
        Computed from the version string.
        """
        if self._version is None:
            return 0
        return self._parse_major_version(self._version)

    @property
    def legacy(self) -> bool:
        """
        Indicates whether this is a legacy server (SQL Server 2016 or older).
        Returns True if major version <= 13.
        """
        return self.major_version <= 13 and self.major_version > 0

    @staticmethod
    def _parse_major_version(version_string: str) -> int:
        """
        Parses the major version from the full version string.

        Args:
            version_string: The full version string (e.g., "15.00.2000")

        Returns:
            The major version number, or 0 if parsing fails
        """
        if not version_string or not version_string.strip():
            return 0

        version_parts = version_string.split(".")

        try:
            return int(version_parts[0])
        except (ValueError, IndexError):
            return 0

    @classmethod
    def parse_server(
        cls, server_input: str, port: int = 1433, database: str = "master"
    ) -> "Server":
        """
        Parses a server string in the format "hostname[:impersonation_user]".

        Args:
            server_input: Server string (e.g., "192.168.1.100" or "192.168.1.100:sa")
            port: The SQL Server port (default: 1433)
            database: The database to connect to (default: "master")

        Returns:
            A Server instance

        Raises:
            ValueError: If the server input format is invalid

        Examples:
            >>> server = Server.parse_server("192.168.1.100")
            >>> server.hostname
            '192.168.1.100'
            >>> server = Server.parse_server("192.168.1.100:sa")
            >>> server.impersonation_user
            'sa'
        """
        parts = server_input.split(":")

        if len(parts) < 1 or len(parts) > 2:
            raise ValueError(
                f"Invalid target format: {server_input}. Expected 'hostname[:impersonation_user]'"
            )

        return cls(
            hostname=parts[0],
            port=port,
            database=database,
            impersonation_user=parts[1] if len(parts) > 1 else None,
        )

    def __str__(self) -> str:
        """String representation of the server."""
        base = f"{self.hostname}:{self.port}/{self.database}"
        if self.impersonation_user:
            base += f" (impersonating: {self.impersonation_user})"
        return base

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return (
            f"Server(hostname='{self.hostname}', port={self.port}, "
            f"database='{self.database}', version='{self.version}', "
            f"legacy={self.legacy})"
        )
