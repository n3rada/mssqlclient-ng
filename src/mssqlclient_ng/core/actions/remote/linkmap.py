# Built-in imports
from typing import Optional
import uuid

# Third party imports
from loguru import logger

# Local imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...models.linked_servers import LinkedServers
from ...models.server_execution_state import ServerExecutionState
from ...utils.formatters import OutputFormatter


@ActionFactory.register(
    "linkmap",
    "Recursively explore all accessible linked server chains with loop detection",
)
class LinkMap(BaseAction):
    """
    Recursively explores all accessible linked server chains, mapping execution paths.

    This action:
    - Enumerates all directly linked servers
    - Recursively explores each linked server's own linked servers
    - Handles user impersonation with proper stack management
    - Detects and prevents infinite loops using hash-based state tracking
    - Maps complete chains showing: Server -> User -> LinkedServer -> User -> ...
    - Respects maximum recursion depth to prevent runaway exploration
    - Handles slow/unresponsive servers with timeout mechanism
    - Properly restores execution context after recursion

    Key Features:
    - Loop detection: Uses ServerExecutionState hashing (hostname + users + sysadmin)
    - Impersonation stack: Tracks and properly reverts all impersonations in LIFO order
    - Depth limiting: Configurable maximum depth (default: 10 levels)
    - Timeout handling: Leverages QueryService's built-in timeout
    - State restoration: Restores LinkedServers chain and ExecutionServer after recursion
    - Graceful degradation: Continues mapping accessible paths when servers unreachable
    """

    DEFAULT_MAX_DEPTH = 10

    def __init__(self):
        super().__init__()
        self._max_depth: int = self.DEFAULT_MAX_DEPTH
        self._server_mapping: dict[uuid.UUID, list[dict[str, str]]] = {}
        self._visited_states: dict[uuid.UUID, set[str]] = {}
        self._impersonation_stack: dict[uuid.UUID, list[str]] = {}
        self._original_log_level = None

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validates the arguments for the linkmap action.

        Args:
            additional_arguments: Optional maximum recursion depth (1-50)

        Raises:
            ValueError: If depth is invalid
        """
        if not additional_arguments or not additional_arguments.strip():
            return

        try:
            depth = int(additional_arguments.strip())
            if not (1 <= depth <= 50):
                raise ValueError("Maximum depth must be between 1 and 50")
            self._max_depth = depth
        except ValueError as e:
            if "invalid literal" in str(e):
                raise ValueError(
                    "Invalid depth value. Must be an integer between 1 and 50"
                )
            raise

    def execute(self, database_context: DatabaseContext) -> Optional[dict]:
        """
        Executes the linked server chain exploration.

        Args:
            database_context: The DatabaseContext instance

        Returns:
            Dictionary mapping chain IDs to their exploration results
        """
        logger.info("Enumerating linked servers")
        logger.info(f"Maximum recursion depth: {self._max_depth}")

        linked_servers = self._get_linked_servers(database_context)

        if not linked_servers:
            logger.warning("No linked servers found.")
            return None

        logger.info("Exploring all possible linked server chains")

        # Suppress Info/Task/Success logs during exploration to reduce noise
        self._original_log_level = logger._core.min_level
        logger.remove()
        logger.add(lambda msg: print(msg, end=""), level="WARNING", format="{message}")

        for server_info in linked_servers:
            remote_server = server_info["Link"]
            local_login = (
                server_info["Local Login"]
                if server_info["Local Login"]
                else "<Current Context>"
            )

            chain_id = uuid.uuid4()
            self._server_mapping[chain_id] = []
            self._visited_states[chain_id] = set()
            self._impersonation_stack[chain_id] = []

            # Create a copy of the database context for this chain
            from copy import deepcopy

            temp_database_context = deepcopy(database_context)

            # Start exploration with depth 0
            self._explore_server(
                temp_database_context,
                remote_server,
                local_login,
                chain_id,
                current_depth=0,
            )

            # Revert all impersonations in LIFO order
            self._revert_all_impersonations(
                temp_database_context.user_service, chain_id
            )

        # Restore original log level
        logger.remove()
        logger.add(
            lambda msg: print(msg, end=""),
            level=self._original_log_level,
            format="{message}",
        )

        initial_server_entry = f"{database_context.server.hostname} ({database_context.server.system_user} [{database_context.server.mapped_user}])"

        logger.debug(f"Initial server entry: {initial_server_entry}")

        if not database_context.query_service.linked_servers.is_empty:
            chain_parts = (
                database_context.query_service.linked_servers.get_chain_parts()
            )
            initial_server_entry += (
                f" -> {' -> '.join(chain_parts)} "
                f"({database_context.user_service.system_user} [{database_context.user_service.mapped_user}])"
            )
            logger.debug(f"Chain added: {initial_server_entry}")

        logger.success("Accessible linked servers chain")

        for chain_id, chain_mapping in self._server_mapping.items():
            formatted_lines = [initial_server_entry]
            chain_parts = []

            for entry in chain_mapping:
                server_name = entry["ServerName"]
                logged_in = entry["LoggedIn"]
                mapped = entry["Mapped"]
                impersonated_user = entry["ImpersonatedUser"].strip()

                formatted_lines.append(
                    f"-{impersonated_user}-> {server_name} ({logged_in} [{mapped}])"
                )

                # Build chain command
                if impersonated_user != "-":
                    chain_parts.append(f"{server_name}:{impersonated_user}")
                else:
                    chain_parts.append(server_name)

            print()
            print(" ".join(formatted_lines))

            # Show command to reproduce this chain
            if chain_parts:
                chain_command = f"-l {','.join(chain_parts)}"
                logger.info(f"To use this chain: {chain_command}")

        return self._server_mapping

    def _explore_server(
        self,
        database_context: DatabaseContext,
        target_server: str,
        expected_local_login: str,
        chain_id: uuid.UUID,
        current_depth: int,
    ) -> None:
        """
        Recursively explores linked servers with proper state management.

        Args:
            database_context: Current database context
            target_server: Target linked server to explore
            expected_local_login: Expected login for accessing the linked server
            chain_id: Unique identifier for the current exploration chain
            current_depth: Current recursion depth (0-based)
        """
        # Check maximum depth limit
        if current_depth >= self._max_depth:
            logger.warning(
                f"Maximum recursion depth ({self._max_depth}) reached at {target_server}"
            )
            logger.warning(
                "Stopping exploration to prevent excessive recursion. Use argument to increase depth."
            )
            return

        logger.debug(
            f"Accessing linked server: {target_server} (depth: {current_depth})"
        )

        # Save current state for restoration
        previous_linked_servers = LinkedServers(
            database_context.query_service.linked_servers
        )
        previous_execution_server = database_context.query_service.execution_server

        try:
            # Check if we are already logged in with the correct user
            current_user, system_user = database_context.user_service.get_info()
            logger.debug(
                f"[{database_context.query_service.execution_server}] LoggedIn: {system_user}, Mapped: {current_user}"
            )

            impersonated_user = None

            # Only attempt impersonation if expected login is not current context
            if (
                expected_local_login != "<Current Context>"
                and system_user != expected_local_login
            ):
                logger.debug(
                    f"Current user '{system_user}' does not match expected local login '{expected_local_login}'"
                )
                logger.debug("Attempting impersonation")

                if database_context.user_service.can_impersonate(expected_local_login):
                    database_context.user_service.impersonate_user(expected_local_login)
                    impersonated_user = expected_local_login

                    # Track impersonation in stack for proper LIFO reversion
                    self._impersonation_stack[chain_id].append(expected_local_login)

                    logger.debug(
                        f"[{database_context.query_service.execution_server}] Impersonated '{expected_local_login}' to access {target_server}."
                    )
                else:
                    logger.warning(
                        f"[{database_context.query_service.execution_server}] Cannot impersonate {expected_local_login} on {target_server}. Skipping."
                    )
                    return
            elif expected_local_login == "<Current Context>":
                logger.debug(
                    "Linked server uses current security context (no explicit login mapping)"
                )

            # Update the linked server chain
            database_context.query_service.linked_servers.add_to_chain(target_server)
            database_context.query_service.execution_server = target_server

            # Query user info THROUGH the linked server chain
            mapped_user, remote_logged_in_user = (
                database_context.user_service.get_info()
            )

            # Create ServerExecutionState for loop detection - this now queries through the chain
            current_state = ServerExecutionState.from_context(
                target_server, database_context.user_service
            )

            state_hash = current_state.get_state_hash()

            # Check for loops
            if state_hash in self._visited_states[chain_id]:
                logger.warning(
                    f"Detected loop at {target_server} with same execution state: {current_state}"
                )
                logger.warning("Skipping to prevent infinite recursion.")
                return

            # Mark this state as visited
            self._visited_states[chain_id].add(state_hash)

            logger.debug(f"Adding mapping for {target_server}")
            logger.debug(f"LoggedIn User: {current_state.system_user}")
            logger.debug(f"Mapped User: {current_state.mapped_user}")
            logger.debug(f"Is Sysadmin: {current_state.is_sysadmin}")
            logger.debug(f"Impersonated User: {impersonated_user}")
            logger.debug(f"State Hash: {state_hash}")

            self._server_mapping[chain_id].append(
                {
                    "ServerName": target_server,
                    "LoggedIn": current_state.system_user,
                    "Mapped": current_state.mapped_user,
                    "ImpersonatedUser": (
                        f" {impersonated_user} " if impersonated_user else "-"
                    ),
                }
            )

            logger.debug(
                f"[{database_context.query_service.execution_server}] LoggedIn: {remote_logged_in_user}, Mapped: {mapped_user}"
            )

            # Retrieve linked servers from remote server
            remote_linked_servers = self._get_linked_servers_with_timeout(
                database_context, target_server
            )

            if not remote_linked_servers:
                logger.debug(f"No further linked servers found on {target_server}")
                return

            # Explore each linked server recursively
            for server_info in remote_linked_servers:
                next_server = server_info["Link"]
                next_local_login = server_info["Local Login"] or "<Current Context>"

                # Create a new context copy for each branch to avoid state pollution
                from copy import deepcopy

                branch_context = deepcopy(database_context)

                # Copy current linked servers state
                branch_context.query_service.linked_servers = LinkedServers(
                    database_context.query_service.linked_servers
                )
                branch_context.query_service.execution_server = (
                    database_context.query_service.execution_server
                )

                self._explore_server(
                    branch_context,
                    next_server,
                    next_local_login,
                    chain_id,
                    current_depth + 1,
                )

        except Exception as ex:
            logger.error(f"Error exploring {target_server}: {ex}")
            logger.error("Continuing with next server...")

        finally:
            # Restore execution context
            database_context.query_service.linked_servers = previous_linked_servers
            database_context.query_service.execution_server = previous_execution_server

    def _get_linked_servers_with_timeout(
        self, database_context: DatabaseContext, server_name: str
    ) -> Optional[list[dict]]:
        """
        Retrieves linked servers with timeout handling.

        Args:
            database_context: Current database context
            server_name: Name of the server being queried

        Returns:
            List of linked server dictionaries or None on timeout/error
        """
        try:
            return self._get_linked_servers(database_context)
        except Exception as ex:
            if "timeout" in str(ex).lower() or "Timeout" in str(ex):
                logger.warning(f"Timeout querying linked servers on {server_name}")
                logger.warning(
                    "Server may be slow or unresponsive. Skipping further exploration."
                )
            else:
                logger.warning(f"Error querying linked servers on {server_name}: {ex}")
            return None

    def _revert_all_impersonations(self, user_service, chain_id: uuid.UUID) -> None:
        """
        Reverts all impersonations in LIFO (Last In, First Out) order.

        Args:
            user_service: UserService instance
            chain_id: Chain identifier
        """
        if (
            chain_id not in self._impersonation_stack
            or not self._impersonation_stack[chain_id]
        ):
            return

        count = len(self._impersonation_stack[chain_id])
        logger.debug(f"Reverting {count} impersonation(s) in LIFO order")

        while self._impersonation_stack[chain_id]:
            impersonated_user = self._impersonation_stack[chain_id].pop()
            try:
                user_service.revert_impersonation()
                logger.debug(f"Reverted impersonation of '{impersonated_user}'")
            except Exception as ex:
                logger.warning(
                    f"Failed to revert impersonation of '{impersonated_user}': {ex}"
                )

    def _get_linked_servers(self, database_context: DatabaseContext) -> list[dict]:
        """
        Retrieves all linked servers with their login mappings.

        Args:
            database_context: Current database context

        Returns:
            List of linked server dictionaries
        """
        query = """
        SELECT
            srv.name AS [Link],
            prin.name AS [Local Login],
            ll.remote_name AS [Remote Login]
        FROM master.sys.servers srv
        LEFT JOIN master.sys.linked_logins ll
            ON srv.server_id = ll.server_id
        LEFT JOIN master.sys.server_principals prin
            ON ll.local_principal_id = prin.principal_id
        WHERE srv.is_linked = 1
        ORDER BY srv.name;
        """

        results = database_context.query_service.execute_table(query)
        logger.debug(OutputFormatter.convert_list_of_dicts(results))
        return results

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            List containing the optional max depth argument
        """
        return ["[max_depth]"]
