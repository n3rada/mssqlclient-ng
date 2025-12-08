# mssqlclient_ng/core/models/server_execution_state.py

# Built-in imports
import hashlib


class ServerExecutionState:
    """
    Represents the runtime execution state of a SQL Server connection.
    Used for loop detection in linked server chains by tracking the exact execution context.

    This is separate from Server (which represents connection configuration) to maintain
    clean separation of concerns: Server = static config, ServerExecutionState = runtime state.

    Attributes:
        hostname: The hostname or IP address of the server
        mapped_user: The mapped database user (from USER_NAME())
        system_user: The system user (from SYSTEM_USER)
        is_sysadmin: Whether the current user has sysadmin privileges
    """

    def __init__(
        self,
        hostname: str,
        mapped_user: str,
        system_user: str,
        is_sysadmin: bool,
    ):
        """
        Initialize a ServerExecutionState instance.

        Args:
            hostname: The server hostname
            mapped_user: The mapped database user
            system_user: The system user
            is_sysadmin: Whether the user has sysadmin privileges
        """
        self.hostname = hostname
        self.mapped_user = mapped_user
        self.system_user = system_user
        self.is_sysadmin = is_sysadmin

    @classmethod
    def from_context(cls, hostname: str, user_service) -> "ServerExecutionState":
        """
        Factory method to create a ServerExecutionState from a UserService.
        Automatically queries the current user info and admin status.

        Args:
            hostname: The server hostname
            user_service: The UserService to query current execution state

        Returns:
            A new ServerExecutionState representing the current execution context
        """
        mapped_user, system_user = user_service.get_info()

        return cls(
            hostname=hostname,
            mapped_user=mapped_user,
            system_user=system_user,
            is_sysadmin=user_service.is_admin(),
        )

    def get_state_hash(self) -> str:
        """
        Computes a unique state hash for loop detection.
        Hash is based on: Hostname, MappedUser, SystemUser, and IsSysadmin.

        Returns:
            SHA-256 hash representing the execution state
        """
        state_string = (
            f"{(self.hostname or '').upper()}:"
            f"{(self.mapped_user or '').upper()}:"
            f"{(self.system_user or '').upper()}:"
            f"{self.is_sysadmin}"
        )

        return hashlib.sha256(state_string.encode("utf-8")).hexdigest()

    def __eq__(self, other) -> bool:
        """
        Checks if two ServerExecutionState instances represent the same execution state.
        Used for loop detection in linked server chains.

        Args:
            other: Another ServerExecutionState instance

        Returns:
            True if both states represent the same execution context
        """
        if not isinstance(other, ServerExecutionState):
            return False

        return (
            (self.hostname or "").upper() == (other.hostname or "").upper()
            and (self.mapped_user or "").upper() == (other.mapped_user or "").upper()
            and (self.system_user or "").upper() == (other.system_user or "").upper()
            and self.is_sysadmin == other.is_sysadmin
        )

    def __hash__(self) -> int:
        """
        Override GetHashCode to support set and dict operations.
        Two states with the same execution context will have the same hash code.

        Returns:
            Hash code based on execution state
        """
        return hash(
            (
                (self.hostname or "").upper(),
                (self.mapped_user or "").upper(),
                (self.system_user or "").upper(),
                self.is_sysadmin,
            )
        )

    def __str__(self) -> str:
        """
        Returns a string representation of the execution state for debugging.

        Returns:
            Human-readable representation
        """
        return f"{self.hostname} (System: {self.system_user}, Mapped: {self.mapped_user}, Sysadmin: {self.is_sysadmin})"

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return (
            f"ServerExecutionState(hostname='{self.hostname}', "
            f"mapped_user='{self.mapped_user}', system_user='{self.system_user}', "
            f"is_sysadmin={self.is_sysadmin})"
        )
