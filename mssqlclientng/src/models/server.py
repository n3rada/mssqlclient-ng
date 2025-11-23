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
        self.port = port or 1433
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
        Parses a server string in the format "server[,port][:user][@database]".
        
        Format supports any combination:
        - server (required) - hostname or IP
        - ,port (optional) - port number
        - :user (optional) - user to impersonate
        - @database (optional) - database context

        Args:
            server_input: Server string (e.g., "SQL01", "SQL01,1434", "SQL01:sa", 
                         "SQL01@mydb", "SQL01,1434:sa@mydb")
            port: Default port if not specified in server_input (default: 1433)
            database: Default database if not specified in server_input (default: "master")

        Returns:
            A Server instance

        Raises:
            ValueError: If the server input format is invalid

        Examples:
            >>> server = Server.parse_server("SQL01")
            >>> server.hostname, server.port, server.database
            ('SQL01', 1433, 'master')
            >>> server = Server.parse_server("SQL01,1434")
            >>> server.port
            1434
            >>> server = Server.parse_server("SQL01:webapp01")
            >>> server.impersonation_user
            'webapp01'
            >>> server = Server.parse_server("SQL01@myapp")
            >>> server.database
            'myapp'
            >>> server = Server.parse_server("SQL01,1434:webapp01@myapp")
            >>> (server.hostname, server.port, server.impersonation_user, server.database)
            ('SQL01', 1434, 'webapp01', 'myapp')
        """
        import re
        
        # Pattern: server[,port][:user][@database]
        # server is required, everything else is optional
        pattern = r'^([^,:\s@]+)(?:,(\d+))?(?::([^@\s]+))?(?:@([^\s]+))?$'
        match = re.match(pattern, server_input.strip())
        
        if not match:
            raise ValueError(
                f"Invalid target format: '{server_input}'. "
                f"Expected format: server[,port][:user][@database]"
            )
        
        hostname = match.group(1)
        port_str = match.group(2)
        impersonation_user = match.group(3)
        database_name = match.group(4)
        
        # Use parsed port if provided, otherwise use the default
        parsed_port = int(port_str) if port_str else port
        
        # Use parsed database if provided, otherwise use the default
        parsed_database = database_name if database_name else database
        
        return cls(
            hostname=hostname,
            port=parsed_port,
            database=parsed_database,
            impersonation_user=impersonation_user,
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
