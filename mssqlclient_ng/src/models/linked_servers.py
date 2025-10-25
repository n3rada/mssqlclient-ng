"""
Linked servers model for managing SQL Server linked server chains.
"""
from typing import List, Optional
from loguru import logger

from mssqlclient_ng.src.models.server import Server


class LinkedServers:
    """
    Manages linked server chains for executing queries across multiple SQL Server instances.

    Supports both OPENQUERY and EXEC AT (RPC) methods for chaining.
    """

    def __init__(self, chain_input: Optional[str | List[Server]] = None):
        """
        Initialize the linked server chain.

        Args:
            chain_input: Either a comma-separated string of servers, a list of Server objects,
                        or None for an empty chain
        """
        if chain_input is None:
            self.server_chain: List[Server] = []
        elif isinstance(chain_input, str):
            self.server_chain = self._parse_server_chain(chain_input) if chain_input.strip() else []
        elif isinstance(chain_input, list):
            self.server_chain = chain_input
        else:
            raise TypeError("chain_input must be a string, list of Server objects, or None")

        # Recompute internal arrays
        self._recompute_chain()

        # Remote Procedure Call (RPC) usage flag
        self.use_remote_procedure_call: bool = True

    @property
    def is_empty(self) -> bool:
        """Returns True if the linked server chain is empty."""
        return len(self.server_chain) == 0

    @property
    def server_names(self) -> List[str]:
        """Public array of server names extracted from the server chain."""
        return self._server_names

    def _recompute_chain(self) -> None:
        """Recompute internal arrays (server names, impersonation users)."""
        # Computable server names starts with "0" as convention
        self._computable_server_names: List[str] = ["0"] + [server.hostname for server in self.server_chain]

        # Extract impersonation users
        self._computable_impersonation_names: List[str] = [
            server.impersonation_user if server.impersonation_user else ""
            for server in self.server_chain
        ]

        # Public server names (without "0" prefix)
        self._server_names: List[str] = [server.hostname for server in self.server_chain]

    def add_to_chain(self, new_server: str, impersonation_user: Optional[str] = None) -> None:
        """
        Add a new server to the linked server chain.

        Args:
            new_server: The hostname of the new linked server
            impersonation_user: Optional impersonation user

        Raises:
            ValueError: If server name is empty
        """
        logger.debug(f"Adding server {new_server} to the linked server chain.")

        if not new_server or not new_server.strip():
            raise ValueError("Server name cannot be null or empty.")

        self.server_chain.append(Server(
            hostname=new_server,
            impersonation_user=impersonation_user
        ))

        self._recompute_chain()

    def get_chain_parts(self) -> List[str]:
        """
        Returns a properly formatted linked server chain parts.

        Returns:
            List of server strings with optional impersonation (e.g., ["SQL02:user", "SQL03"])
        """
        chain_parts = []

        for server in self.server_chain:
            if server.impersonation_user:
                chain_parts.append(f"{server.hostname}:{server.impersonation_user}")
            else:
                chain_parts.append(server.hostname)

        return chain_parts

    def get_chain_arguments(self) -> str:
        """
        Returns a comma-separated string of the chain parts.

        Returns:
            Comma-separated chain string (e.g., "SQL02:user,SQL03,SQL04")
        """
        return ",".join(self.get_chain_parts())

    @staticmethod
    def _parse_server_chain(chain_input: str) -> List[Server]:
        """
        Parse a comma-separated list of servers into a list of Server objects.

        Args:
            chain_input: Comma-separated list (e.g., "SQL27:user01,SQL53:user02")

        Returns:
            List of Server objects

        Raises:
            ValueError: If chain_input is empty
        """
        if not chain_input or not chain_input.strip():
            raise ValueError("Server list cannot be null or empty.")

        return [
            Server.parse_server(server_string.strip())
            for server_string in chain_input.split(',')
        ]

    def build_select_openquery_chain(self, query: str) -> str:
        """
        Construct a nested OPENQUERY statement for querying linked SQL servers in a chain.

        OPENQUERY passes the query string as-is to the linked server without attempting
        to parse or validate it as T-SQL on the local server.

        Args:
            query: The SQL query to execute at the final server

        Returns:
            Nested OPENQUERY statement string
        """
        return self._build_select_openquery_chain_recursive(
            linked_servers=self._computable_server_names,
            query=query,
            linked_impersonation=self._computable_impersonation_names
        )

    def _build_select_openquery_chain_recursive(
        self,
        linked_servers: List[str],
        query: str,
        ticks_counter: int = 0,
        linked_impersonation: Optional[List[str]] = None
    ) -> str:
        """
        Recursively construct a nested OPENQUERY statement for querying linked SQL servers.

        Args:
            linked_servers: Array of server names (with "0" prefix)
            query: SQL query to execute at the final server
            ticks_counter: Counter for quote doubling at each nesting level
            linked_impersonation: Array of impersonation users

        Returns:
            Nested OPENQUERY statement

        Raises:
            ValueError: If linked_servers is empty
        """
        if not linked_servers:
            raise ValueError("linked_servers cannot be null or empty.")

        current_query = query

        # Prepare the impersonation login, if any
        login = None
        if linked_impersonation and len(linked_impersonation) > 0:
            login = linked_impersonation[0]
            linked_impersonation = linked_impersonation[1:]

        ticks_repr = "'" * (2 ** ticks_counter)

        # Base case: if this is the last server in the chain
        if len(linked_servers) == 1:
            if login:
                current_query = f"EXECUTE AS LOGIN = '{login}'; {current_query.rstrip(';')}; REVERT;"

            current_query = current_query.replace("'", ticks_repr)
            return current_query

        # Construct the OPENQUERY statement for the next server in the chain
        result = []
        result.append("SELECT * FROM OPENQUERY(")
        result.append(f"[{linked_servers[1]}], ")
        result.append(ticks_repr)

        # Add impersonation if applicable
        if login:
            impersonation_ticks = "'" * (2 ** (ticks_counter + 1))
            impersonation_query = f"EXECUTE AS LOGIN = '{login}'; "
            result.append(impersonation_query.replace("'", impersonation_ticks))

        # Recursive call for the remaining servers
        recursive_call = self._build_select_openquery_chain_recursive(
            linked_servers=linked_servers[1:],
            linked_impersonation=linked_impersonation,
            query=current_query,
            ticks_counter=ticks_counter + 1
        )
        result.append(recursive_call)

        # Add REVERT if impersonation was applied
        if login:
            result.append(" REVERT;")

        # Closing the remote request
        result.append(ticks_repr)
        result.append(")")

        return "".join(result)

    def build_remote_procedure_call_chain(self, query: str) -> str:
        """
        Construct a nested EXEC AT statement for querying linked SQL servers in a chain.

        When using EXEC to run a query on a linked server, SQL Server expects
        the query to be valid T-SQL.

        Args:
            query: The SQL query to execute

        Returns:
            Nested EXEC AT statement string
        """
        return self._build_remote_procedure_call_recursive(
            linked_servers=self._computable_server_names,
            query=query,
            linked_impersonation=self._computable_impersonation_names
        )

    @staticmethod
    def _build_remote_procedure_call_recursive(
        linked_servers: List[str],
        query: str,
        linked_impersonation: Optional[List[str]] = None
    ) -> str:
        """
        Recursively construct a nested EXEC AT statement for querying linked SQL servers.

        Args:
            linked_servers: Array of server names (with "0" prefix)
            query: SQL query to execute
            linked_impersonation: Array of impersonation users

        Returns:
            Nested EXEC AT statement
        """
        current_query = query

        # Start from the end of the array and skip the first element ("0")
        for i in range(len(linked_servers) - 1, 0, -1):
            server = linked_servers[i]

            # Add impersonation if applicable
            if linked_impersonation and len(linked_impersonation) > 0:
                login = linked_impersonation[i - 1]
                if login:
                    current_query = f"EXECUTE AS LOGIN = '{login}'; {current_query.rstrip(';')}; REVERT;"

            # Double single quotes to escape them in the SQL string
            escaped_query = current_query.replace("'", "''")
            current_query = f"EXEC ('{escaped_query} ') AT [{server}]"

        return current_query

    def copy(self) -> "LinkedServers":
        """
        Create a deep copy of the LinkedServers instance.

        Returns:
            A new LinkedServers instance with copied server chain
        """
        copied_servers = [
            Server(
                hostname=server.hostname,
                impersonation_user=server.impersonation_user,
                port=server.port,
                database=server.database
            )
            for server in self.server_chain
        ]

        new_instance = LinkedServers(copied_servers)
        new_instance.use_remote_procedure_call = self.use_remote_procedure_call
        return new_instance

    def __str__(self) -> str:
        """String representation of the linked server chain."""
        if self.is_empty:
            return "LinkedServers(empty)"
        return f"LinkedServers({self.get_chain_arguments()})"

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return f"LinkedServers(chain={self.get_chain_parts()}, rpc={self.use_remote_procedure_call})"
