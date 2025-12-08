# mssqlclient_ng/core/models/linked_servers.py

# Built-in imports
from typing import List, Optional

# Third party imports
from loguru import logger

# Local library imports
from .server import Server


class LinkedServers:
    """
    Manages linked server chains for executing queries across multiple SQL Server instances.

    Supports both OPENQUERY and EXEC AT (RPC) methods for chaining.
    """

    def __init__(
        self, chain_input: Optional[str | List[Server] | "LinkedServers"] = None
    ):
        """
        Initialize the linked server chain.

        Args:
            chain_input: Either a comma-separated string of servers, a list of Server objects,
                        another LinkedServers instance (copy constructor), or None for an empty chain
        """
        if chain_input is None:
            self.server_chain: List[Server] = []
        elif isinstance(chain_input, str):
            self.server_chain = (
                self._parse_server_chain(chain_input) if chain_input.strip() else []
            )
        elif isinstance(chain_input, list):
            self.server_chain = chain_input
        elif isinstance(chain_input, LinkedServers):
            # Copy constructor
            self.server_chain = [
                Server(
                    hostname=server.hostname,
                    port=server.port,
                    database=server.database,
                    impersonation_user=server.impersonation_user,
                )
                for server in chain_input.server_chain
            ]
        else:
            raise TypeError(
                "chain_input must be a string, list of Server objects, LinkedServers instance, or None"
            )

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
        """Recompute internal arrays (server names, impersonation users, databases)."""
        # Computable server names starts with "0" as convention
        self._computable_server_names: List[str] = ["0"] + [
            server.hostname for server in self.server_chain
        ]

        # Extract impersonation users
        self._computable_impersonation_names: List[str] = [
            server.impersonation_user if server.impersonation_user else ""
            for server in self.server_chain
        ]

        # Extract database contexts
        self._computable_database_names: List[str] = [
            server.database if server.database else "" for server in self.server_chain
        ]

        # Public server names (without "0" prefix)
        self._server_names: List[str] = [
            server.hostname for server in self.server_chain
        ]

    def add_to_chain(
        self, new_server: str, impersonation_user: Optional[str] = None
    ) -> None:
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

        self.server_chain.append(
            Server(hostname=new_server, impersonation_user=impersonation_user)
        )

        self._recompute_chain()

    def clear(self) -> None:
        """
        Clear the linked server chain, removing all servers.
        """
        logger.debug("Clearing linked server chain.")
        self.server_chain = []
        self._recompute_chain()

    def get_chain_parts(self) -> List[str]:
        """
        Returns a properly formatted linked server chain parts.

        Returns:
            List of server strings with optional impersonation and database
            (e.g., ["SQL02:user@db", "SQL03", "SQL04@analytics"])
        """
        chain_parts = []

        for server in self.server_chain:
            part = server.hostname

            # Add user@database or just :user or just @database
            if server.impersonation_user and server.database:
                part += f":{server.impersonation_user}@{server.database}"
            elif server.impersonation_user:
                part += f":{server.impersonation_user}"
            elif server.database:
                part += f"@{server.database}"

            chain_parts.append(part)

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
            for server_string in chain_input.split(",")
        ]

    def build_select_openquery_chain(self, query: str) -> str:
        """
        Construct a nested OPENQUERY statement for querying linked SQL servers in a chain.

        OPENQUERY passes the query string as-is to the linked server without attempting
        to parse or validate it as T-SQL on the local server.
        https://learn.microsoft.com/en-us/sql/t-sql/functions/openquery-transact-sql

        Args:
            query: The SQL query to execute at the final server

        Returns:
            Nested OPENQUERY statement string
        """
        return self._build_select_openquery_chain_recursive(
            linked_servers=self._computable_server_names,
            query=query,
            linked_impersonation=self._computable_impersonation_names,
            linked_databases=self._computable_database_names,
        )

    def _build_select_openquery_chain_recursive(
        self,
        linked_servers: List[str],
        query: str,
        ticks_counter: int = 0,
        linked_impersonation: Optional[List[str]] = None,
        linked_databases: Optional[List[str]] = None,
    ) -> str:
        """
        Recursively construct a nested OPENQUERY statement for querying linked SQL servers.
        Executes as a remote SELECT engine on the linked server.
        Each level doubles the single quotes to escape them properly.

        Args:
            linked_servers: Array of server names (with "0" prefix). '0' in front of them is mandatory to make the query work properly.
            query: SQL query to execute at the final server
            ticks_counter: Counter for quote doubling at each nesting level (used to double the single quotes for each level of nesting)
            linked_impersonation: Array of impersonation users
            linked_databases: Array of database contexts

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

        # Prepare the database context, if any
        database = None
        if linked_databases and len(linked_databases) > 0:
            database = linked_databases[0]
            linked_databases = linked_databases[1:]

        ticks_repr = "'" * (1 << ticks_counter)

        # Base case: if this is the last server in the chain
        if len(linked_servers) == 1:
            base_query = []

            if login:
                base_query.append(f"EXECUTE AS LOGIN = '{login}';")

            if database:
                base_query.append(f"USE [{database}];")

            base_query.append(current_query.rstrip(";"))
            base_query.append(";")

            current_query = "".join(base_query).replace("'", ticks_repr)
            return current_query

        # Construct the OPENQUERY statement for the next server in the chain
        result = []
        result.append("SELECT * FROM OPENQUERY(")
        result.append(f"[{linked_servers[1]}],")
        result.append(ticks_repr)

        # We are now inside the query, on the linked server

        # Add impersonation if applicable
        if login:
            impersonation_ticks = "'" * (1 << (ticks_counter + 1))
            impersonation_query = f"EXECUTE AS LOGIN = '{login}';"
            result.append(impersonation_query.replace("'", impersonation_ticks))

        # Add database context if applicable
        if database:
            database_ticks = "'" * (1 << (ticks_counter + 1))
            use_query = f"USE [{database}];"
            result.append(use_query.replace("'", database_ticks))

        # Recursive call for the remaining servers
        recursive_call = self._build_select_openquery_chain_recursive(
            linked_servers=linked_servers[1:],
            linked_impersonation=linked_impersonation,
            linked_databases=linked_databases,
            query=current_query,
            ticks_counter=ticks_counter + 1,
        )
        result.append(recursive_call)

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
            linked_impersonation=self._computable_impersonation_names,
            linked_databases=self._computable_database_names,
        )

    @staticmethod
    def _build_remote_procedure_call_recursive(
        linked_servers: List[str],
        query: str,
        linked_impersonation: Optional[List[str]] = None,
        linked_databases: Optional[List[str]] = None,
    ) -> str:
        """
        Recursively construct a nested EXEC AT statement for querying linked SQL servers.
        It loops from innermost server to outermost server.
        Each iteration adds impersonation and database context if provided. Then, appends prior query escaped.
        And finally wraps everything in EXEC ('...') AT [server].

        Big-O time complexity of O(n * L) where:
            n = number of linked servers
            L = final query string length
        This is expected and optimal: you must touch the whole string each time because SQL must be re-encoded at each hop.

        Args:
            linked_servers: Array of server names (with "0" prefix)
            query: SQL query to execute
            linked_impersonation: Array of impersonation users
            linked_databases: Array of database contexts

        Returns:
            Nested EXEC AT statement
        """
        current_query = query

        # Start from the end of the array and skip the first element ("0")
        for i in range(len(linked_servers) - 1, 0, -1):
            server = linked_servers[i]
            query_builder = []

            # Add impersonation if applicable
            if linked_impersonation and len(linked_impersonation) > 0:
                login = linked_impersonation[i - 1]
                if login:
                    query_builder.append(f"EXECUTE AS LOGIN = '{login}'; ")

            if linked_databases and len(linked_databases) > 0:
                database = linked_databases[i - 1]
                if database and database != "master":
                    query_builder.append(f"USE [{database}]; ")

            query_builder.append(current_query.rstrip(";"))
            query_builder.append(";")

            # Double single quotes to escape them in the SQL string
            escaped_query = "".join(query_builder).replace("'", "''")
            current_query = f"EXEC ('{escaped_query}') AT [{server}]"

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
                database=server.database,
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
