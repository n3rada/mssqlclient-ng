# mssqlclient_ng/core/models/server.py

# Built-in imports
from typing import Optional

# Third party imports
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
        is_azure_sql: Whether this is an Azure SQL Database instance
    """

    def __init__(
        self,
        hostname: str,
        port: int = 1433,
        database: Optional[str] = None,
        impersonation_user: Optional[str] = None,
    ):
        """
        Initialize a Server instance.

        Args:
            hostname: The hostname or IP address of the server
            port: The SQL Server port (default: 1433)
            database: The database to connect to (default: None, will use server default)
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
        self.database = database.strip() if database else None

        self.impersonation_user = impersonation_user if impersonation_user else ""
        self.mapped_user = ""
        self.system_user = ""
        self.is_azure_sql = False

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
        cls, server_input: str, port: int = 1433, database: Optional[str] = None
    ) -> "Server":
        """
        Parses a server string in the format "server[,port][:user][@database]".

        Order is completely flexible - components can appear in any order after the hostname.
        The hostname is always the part before any delimiter (,, :, @).

        Format supports any combination in any order:
        - server (required) - hostname or IP
        - ,port (optional) - port number
        - :user (optional) - user to impersonate
        - @database (optional) - database context

        Args:
            server_input: Server string. Examples:
                         "SQL01", "SQL01,1434", "SQL01:sa", "SQL01@mydb",
                         "SQL01,1434:sa@mydb", "SQL01:sa@mydb,1434", "SQL01@mydb,1434:sa"
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
            >>> server = Server.parse_server("SQL01:webapp01@myapp,1434")
            >>> (server.port, server.impersonation_user, server.database)
            (1434, 'webapp01', 'myapp')
        """
        if not server_input or not server_input.strip():
            raise ValueError("Server input cannot be null or empty.")

        remaining = server_input.strip()

        # Find the first delimiter
        delimiters = {
            ",": remaining.find(","),
            ":": remaining.find(":"),
            "@": remaining.find("@"),
        }
        valid_delimiters = {k: v for k, v in delimiters.items() if v >= 0}

        if not valid_delimiters:
            # No delimiters, just hostname
            return cls(
                hostname=remaining,
                port=port,
                database=database,
                impersonation_user=None,
            )

        # Find first delimiter
        first_delimiter_pos = min(valid_delimiters.values())
        first_delimiter = next(
            k for k, v in valid_delimiters.items() if v == first_delimiter_pos
        )

        # Extract hostname
        hostname = remaining[:first_delimiter_pos]
        if not hostname or not hostname.strip():
            raise ValueError("Server hostname cannot be empty")

        remaining = remaining[first_delimiter_pos + 1 :]

        # Initialize with defaults
        parsed_port = port
        parsed_database = database
        impersonation_user = None

        # Parse all components
        while remaining:
            # Find next delimiter
            delimiters = {
                ",": remaining.find(","),
                ":": remaining.find(":"),
                "@": remaining.find("@"),
            }
            valid_delimiters = {k: v for k, v in delimiters.items() if v >= 0}

            if not valid_delimiters:
                # Last component
                next_delimiter_pos = len(remaining)
                next_delimiter = None
            else:
                next_delimiter_pos = min(valid_delimiters.values())
                next_delimiter = next(
                    k for k, v in valid_delimiters.items() if v == next_delimiter_pos
                )

            component = remaining[:next_delimiter_pos]

            if not component or not component.strip():
                if first_delimiter == ",":
                    raise ValueError("Port cannot be empty after ,")
                elif first_delimiter == ":":
                    raise ValueError("Impersonation user cannot be empty after :")
                else:
                    raise ValueError("Database cannot be empty after @")

            # Determine what component this is based on the delimiter that preceded it
            if first_delimiter == ",":
                # This is a port
                try:
                    parsed_port = int(component)
                    if not (1 <= parsed_port <= 65535):
                        raise ValueError(
                            f"Port must be between 1 and 65535, got {parsed_port}"
                        )
                except ValueError as e:
                    if "invalid literal" in str(e):
                        raise ValueError(
                            f"Invalid port number: {component}. Port must be between 1 and 65535."
                        )
                    raise
            elif first_delimiter == ":":
                # This is an impersonation user
                impersonation_user = component
            elif first_delimiter == "@":
                # This is a database
                parsed_database = component

            if next_delimiter is None:
                break

            first_delimiter = next_delimiter
            remaining = remaining[next_delimiter_pos + 1 :]

        return cls(
            hostname=hostname.strip(),
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
