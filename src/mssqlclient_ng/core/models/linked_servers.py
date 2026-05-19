# mssqlclient_ng/core/models/linked_servers.py

from __future__ import annotations

# Built-in imports

# Third party imports
from loguru import logger

# Local library imports
from .server import Server
from ..utils.common import bracket_identifier


class LinkedServers:
    """
    Manages linked server chains for executing queries across multiple SQL Server instances.

    Supports both OPENQUERY and EXEC AT (RPC) methods for chaining.
    """

    def __init__(self, chain_input: str | list[Server] | "LinkedServers" | None = None):
        """
        Initialize the linked server chain.

        Args:
            chain_input: Either a comma-separated string of servers, a list of Server objects,
                        another LinkedServers instance (copy constructor), or None for an empty chain
        """
        if chain_input is None:
            self.server_chain: list[Server] = []
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
                    impersonation_users=list(server.impersonation_users),
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

        # Per-server RPC tracking: set of server names known to lack RPC
        self._non_rpc_servers: set = set()

    @property
    def is_empty(self) -> bool:
        """Returns True if the linked server chain is empty."""
        return len(self.server_chain) == 0

    @property
    def has_non_rpc_servers(self) -> bool:
        """Returns True if any servers in the chain are known to lack RPC."""
        return len(self._non_rpc_servers) > 0

    @property
    def all_servers_non_rpc(self) -> bool:
        """Returns True if ALL servers in the chain lack RPC."""
        if not self._non_rpc_servers or not self._server_names:
            return False
        return all(
            name.lower() in {s.lower() for s in self._non_rpc_servers}
            for name in self._server_names
        )

    def mark_server_as_non_rpc(self, server_name: str) -> None:
        """
        Mark a specific server as not supporting RPC.

        Args:
            server_name: The hostname of the server that doesn't support RPC
        """
        self._non_rpc_servers.add(server_name.lower())
        logger.debug(f"Marked server '{server_name}' as non-RPC")

    @property
    def server_names(self) -> list[str]:
        """Public array of server names extracted from the server chain."""
        return self._server_names

    def _recompute_chain(self) -> None:
        """Recompute internal arrays (server names, impersonation users, databases)."""
        # Computable server names starts with "0" as convention
        self._computable_server_names: list[str] = ["0"] + [
            server.hostname for server in self.server_chain
        ]

        # Extract impersonation users (list[list[str]] for cascading EXECUTE AS support)
        self._computable_impersonation_names: list[list[str]] = [
            list(server.impersonation_users) for server in self.server_chain
        ]

        # Extract database contexts
        self._computable_database_names: list[str] = [
            server.database if server.database else "" for server in self.server_chain
        ]

        # Public server names (without "0" prefix)
        self._server_names: list[str] = [
            server.hostname for server in self.server_chain
        ]

    def add_to_chain(
        self,
        new_server: str,
        impersonation_users: list[str] | None = None,
        database: str | None = None,
    ) -> None:
        """
        Add a new server to the linked server chain.

        Args:
            new_server: The hostname of the new linked server
            impersonation_users: Optional list of cascading impersonation users
            database: Optional database context

        Raises:
            ValueError: If server name is empty
        """
        logger.debug(f"Adding server {new_server} to the linked server chain.")

        if not new_server or not new_server.strip():
            raise ValueError("Server name cannot be null or empty.")

        users = impersonation_users if impersonation_users is not None else []
        self.server_chain.append(
            Server(hostname=new_server, impersonation_users=users, database=database)
        )

        self._recompute_chain()

    def remove_last_from_chain(self) -> None:
        """
        Remove the last server from the linked server chain.
        Used during recursive exploration to pop the current server after processing.
        """
        if self.server_chain:
            removed = self.server_chain.pop()
            logger.debug(
                f"Removed server {removed.hostname} from the linked server chain."
            )
            self._recompute_chain()

    def clear(self) -> None:
        """
        Clear the linked server chain, removing all servers.
        """
        logger.debug("Clearing linked server chain.")
        self.server_chain = []
        self._recompute_chain()

    def get_chain_parts(self) -> list[str]:
        """
        Returns a properly formatted linked server chain parts.
        Server names are wrapped in brackets if they contain special characters.

        Returns:
            list of server strings with optional impersonation and database
            (e.g., ["[SQL-02]/user@db", "SQL03", "[SQL.04]@analytics"])
        """
        chain_parts = []

        for server in self.server_chain:
            # Wrap server name in brackets if it contains special characters
            part = bracket_identifier(server.hostname)

            # Add user@database or just /user or just @database
            if server.impersonation_users and server.database:
                part += (
                    "/" + "/".join(server.impersonation_users) + "@" + server.database
                )
            elif server.impersonation_users:
                part += "/" + "/".join(server.impersonation_users)
            elif server.database:
                part += f"@{server.database}"

            chain_parts.append(part)

        return chain_parts

    def get_chain_arguments(self) -> str:
        """
        Returns a semicolon-separated string of the chain parts.

        Returns:
            Semicolon-separated chain string (e.g., "SQL02:user;SQL03;SQL04")
        """
        return ";".join(self.get_chain_parts())

    def format_chain_display(
        self,
        initial_host: str,
        initial_login: str | None = None,
        initial_impersonation: list[str] | None = None,
    ) -> str:
        """
        Format a human-readable chain display with impersonation context.

        Mimics MSSQLand's FormatChainDisplay:
          LAB-SQL01 (operator) ─(sa)─> LAB-SQL02 ──> LAB-SQL03 (as admin)

        Args:
            initial_host: The initial server hostname
            initial_login: The login used on the initial server
            initial_impersonation: Impersonation users on the initial server

        Returns:
            Formatted chain string
        """
        result = initial_host
        if initial_login:
            result += f" ({initial_login})"

        # Initial impersonation becomes the connector to the first linked server
        result += self._format_connector(initial_impersonation)

        for i, server in enumerate(self.server_chain):
            result += server.hostname
            is_last = i == len(self.server_chain) - 1
            users = server.impersonation_users

            if is_last:
                # Last server: impersonation is the execution context
                if users:
                    cascade = " → ".join(users)
                    result += f" (as {cascade})"
            else:
                # Intermediate: impersonation shown in connector
                result += self._format_connector(users)

        return result

    @staticmethod
    def _format_connector(impersonation_users: list[str] | None = None) -> str:
        """Format a connector arrow, optionally with impersonation cascade."""
        if impersonation_users:
            cascade = " → ".join(impersonation_users)
            return f" ─({cascade})─> "
        return " ──> "

    @staticmethod
    def _parse_server_chain(chain_input: str) -> list[Server]:
        """
        Parse a semicolon-separated list of servers into a list of Server objects.
        Handles bracketed SQL Server identifiers correctly.

        Server names with colons need brackets: [SERVER:001]
        Linked servers don't have ports, so colons in brackets are part of the name.

        Syntax:
        - Colon (:) = port separator (main host only: host:port)
        - Forward slash (/) = impersonation ("execute as user")
        - Semicolon (;) = separator between servers in a chain

        Args:
            chain_input: Semicolon-separated list (e.g., "[SQL-27]/user01;[SQL.53]/user02")

        Returns:
            list of Server objects

        Raises:
            ValueError: If chain_input is empty
        """
        if not chain_input or not chain_input.strip():
            raise ValueError("Server list cannot be null or empty.")

        servers = []
        current = chain_input.strip()

        while current:
            # Find the next semicolon, accounting for bracketed names
            in_brackets = False
            semicolon_pos = -1

            for i, char in enumerate(current):
                if char == "[":
                    in_brackets = True
                elif char == "]":
                    in_brackets = False
                elif char == ";" and not in_brackets:
                    semicolon_pos = i
                    break

            if semicolon_pos == -1:
                # Last server in chain
                servers.append(Server.parse_server(current.strip()))
                break
            else:
                # Extract this server and continue
                server_string = current[:semicolon_pos].strip()
                if server_string:
                    servers.append(Server.parse_server(server_string))
                current = current[semicolon_pos + 1 :].strip()

        return servers

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
        linked_servers: list[str],
        query: str,
        ticks_counter: int = 0,
        linked_impersonation: list[list[str]] | None = None,
        linked_databases: list[str] | None = None,
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

        # Prepare the impersonation login list, if any
        login_list: list[str] = []
        if linked_impersonation and len(linked_impersonation) > 0:
            login_list = linked_impersonation[0]
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

            for login in login_list:
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

        # Add impersonation if applicable (cascading EXECUTE AS for each user)
        for login in login_list:
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
        linked_servers: list[str],
        query: str,
        linked_impersonation: list[list[str]] | None = None,
        linked_databases: list[str] | None = None,
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

            # Add impersonation if applicable (cascading EXECUTE AS)
            if linked_impersonation and len(linked_impersonation) > 0:
                login_list = linked_impersonation[i - 1]
                for login in login_list:
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

    def build_hybrid_chain(self, query: str) -> str:
        """
        Construct a hybrid chain mixing EXEC AT (RPC) and OPENQUERY per-hop.
        Each hop uses RPC unless that server has been marked as non-RPC.

        Iterates from innermost server to outermost (like the RPC builder) but
        decides per-hop whether to use EXEC AT or OPENQUERY.

        Args:
            query: The SQL query to execute at the final server

        Returns:
            Nested hybrid statement string
        """
        linked_servers = self._computable_server_names
        linked_impersonation = self._computable_impersonation_names
        linked_databases = self._computable_database_names

        current_query = query

        # Start from the end of the array and skip the first element ("0")
        for i in range(len(linked_servers) - 1, 0, -1):
            server = linked_servers[i]
            is_rpc = server.lower() not in {s.lower() for s in self._non_rpc_servers}

            if is_rpc:
                # EXEC AT path (same as full RPC builder per-hop)
                query_builder = []

                if linked_impersonation and i - 1 < len(linked_impersonation):
                    login_list = linked_impersonation[i - 1]
                    for login in login_list:
                        query_builder.append(f"EXECUTE AS LOGIN = '{login}'; ")

                if linked_databases and i - 1 < len(linked_databases):
                    database = linked_databases[i - 1]
                    if database and database != "master":
                        query_builder.append(f"USE [{database}]; ")

                query_builder.append(current_query.rstrip(";"))
                query_builder.append(";")

                escaped_query = "".join(query_builder).replace("'", "''")
                current_query = f"EXEC ('{escaped_query}') AT [{server}]"
            else:
                # OPENQUERY path — impersonation not supported on OPENQUERY hops
                if linked_impersonation and i - 1 < len(linked_impersonation):
                    login_list = linked_impersonation[i - 1]
                    if login_list:
                        logger.warning(
                            f"Impersonation skipped on OPENQUERY hop [{server}] "
                            f"(users: {login_list})"
                        )

                query_builder = []

                if linked_databases and i - 1 < len(linked_databases):
                    database = linked_databases[i - 1]
                    if database and database != "master":
                        query_builder.append(f"USE [{database}]; ")

                query_builder.append(current_query.rstrip(";"))
                query_builder.append(";")

                escaped_inner = "".join(query_builder).replace("'", "''")
                current_query = (
                    f"SELECT * FROM OPENQUERY([{server}], '{escaped_inner}')"
                )

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
                impersonation_users=list(server.impersonation_users),
                port=server.port,
                database=server.database,
            )
            for server in self.server_chain
        ]

        new_instance = LinkedServers(copied_servers)
        new_instance.use_remote_procedure_call = self.use_remote_procedure_call
        new_instance._non_rpc_servers = set(self._non_rpc_servers)
        return new_instance

    def __str__(self) -> str:
        """String representation of the linked server chain."""
        if self.is_empty:
            return "LinkedServers(empty)"
        return f"LinkedServers({self.get_chain_arguments()})"

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return f"LinkedServers(chain={self.get_chain_parts()}, rpc={self.use_remote_procedure_call})"
