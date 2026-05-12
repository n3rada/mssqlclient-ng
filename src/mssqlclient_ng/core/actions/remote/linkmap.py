# mssqlclient_ng/core/actions/remote/linkmap.py

# Built-in imports
import io
from contextlib import redirect_stdout
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Set, Tuple
import time

# Third party imports
from loguru import logger

# Local imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...models.linked_servers import LinkedServers
from ...models.server import Server
from ...models.server_execution_state import ServerExecutionState
from ...utils.formatters import OutputFormatter
from ...utils.common import bracket_identifier
from ...utils.storage import ChainStore

# Server roles that grant significant privileges beyond standard access.
ELEVATED_ROLES: Set[str] = {
    "securityadmin",  # Can grant permissions: near-sysadmin
    "serveradmin",  # Can change server configuration
    "setupadmin",  # Can add/remove linked servers
    "processadmin",  # Can kill processes
    "dbcreator",  # Can create/alter/drop databases
    "diskadmin",  # Can manage disk files
    "bulkadmin",  # Can run BULK INSERT
}


@dataclass
class ImpersonationStep:
    """Represents a single impersonation step with the login name and its server roles."""

    login: str
    roles: List[str] = field(default_factory=list)

    @property
    def is_sysadmin(self) -> bool:
        return any(r.lower() == "sysadmin" for r in self.roles)

    @property
    def is_elevated(self) -> bool:
        return not self.is_sysadmin and any(
            r.lower() in ELEVATED_ROLES for r in self.roles
        )

    @property
    def privilege_marker(self) -> str:
        if self.is_sysadmin:
            return " \u2605"
        non_sa_roles = [r for r in self.roles if r.lower() != "sysadmin"]
        if not non_sa_roles:
            return ""
        if self.is_elevated:
            return f" \u25c6 [{', '.join(non_sa_roles)}]"
        return f" [{', '.join(non_sa_roles)}]"


@dataclass
class ServerNode:
    """Represents a node in the linked server tree."""

    alias: str
    actual_name: str
    logged_in_user: str
    mapped_user: str
    impersonation_chain: List[ImpersonationStep] = field(default_factory=list)
    is_sysadmin: bool = False
    server_roles: List[str] = field(default_factory=list)
    children: List["ServerNode"] = field(default_factory=list)
    escalation_paths: List[List[ImpersonationStep]] = field(default_factory=list)

    @property
    def is_elevated(self) -> bool:
        return not self.is_sysadmin and any(
            r.lower() in ELEVATED_ROLES for r in self.server_roles
        )

    @property
    def privilege_marker(self) -> str:
        if self.is_sysadmin:
            return " \u2605"
        roles = [r for r in self.server_roles if r.lower() != "sysadmin"]
        if not roles:
            return ""
        if self.is_elevated:
            return f" \u25c6 [{', '.join(roles)}]"
        return f" [{', '.join(roles)}]"


def _context_key(server: str, login: str) -> str:
    """Canonical key for the global 'already explored' set: (server, login)."""
    return f"{server}|{login}".upper()


def _link_attempt_key(source_server: str, target_server: str, caller_login: str) -> str:
    """Canonical key for the negative link-attempt cache."""
    return f"{source_server}|{target_server}|{caller_login}".upper()


def _get_row_string(row: Dict[str, Any], column: str) -> str:
    """Read a column from a row dict, returning empty string for None/NULL."""
    val = row.get(column)
    if val is None:
        return ""
    s = str(val)
    if s.upper() == "NULL":
        return ""
    return s


def _format_impersonation_context(
    chain: List[ImpersonationStep], fallback_login: str = "current login"
) -> str:
    """Format a parent impersonation chain for log messages."""
    if chain:
        return " -> ".join(s.login for s in chain)
    return fallback_login


def _build_impersonation_steps(logins: Optional[List[str]]) -> List[ImpersonationStep]:
    """Build ImpersonationStep list from login names (roles left empty)."""
    if not logins:
        return []
    return [ImpersonationStep(login=login) for login in logins]


def _is_system_account(login: str) -> bool:
    """Check if login is a system account (NT AUTHORITY\\*, NT SERVICE\\*)."""
    if not login:
        return False
    upper = login.upper()
    return upper.startswith("NT ") or upper.startswith("NT\\")


@ActionFactory.register(
    "linkmap",
    "Recursively explore all accessible linked server chains with impersonation discovery",
    aliases=["linksmap", "chains", "tunnel"],
)
class LinkMap(BaseAction):
    """
    Recursively explores all accessible linked server chains, mapping execution paths.
    Uses a tree structure for efficient storage and cleaner output.

    Features:
    - Tree-based visualization with ASCII art and privilege markers
    - Impersonation chain discovery to gain additional link visibility
    - Negative caching of failed link attempts
    - Role-based context caching
    - Provider-based filtering (SQL Server vs other linked servers)
    - Privilege escalation path detection
    - Chain command generation for reproduction
    """

    DEFAULT_MAX_DEPTH = 7
    MAX_ALLOWED_DEPTH = 15

    def __init__(self):
        super().__init__()
        self._limit: int = self.DEFAULT_MAX_DEPTH
        self._root_node: Optional[ServerNode] = None
        self._globally_explored_contexts: Set[str] = set()
        self._failed_link_attempts: Set[str] = set()
        self._context_role_cache: Dict[str, Tuple[List[str], bool]] = {}
        self._all_chains: List[List[ServerNode]] = []
        self._seen_chain_displays: Set[str] = set()
        self._starting_impersonation: List[str] = []
        self._chain_store = ChainStore()

    def validate_arguments(self, additional_arguments: str = "") -> None:
        if not additional_arguments or not additional_arguments.strip():
            return

        parts = additional_arguments.strip().split()
        for part in parts:
            try:
                depth = int(part)
                if not (1 <= depth <= self.MAX_ALLOWED_DEPTH):
                    raise ValueError(
                        f"Limit must be between 1 and {self.MAX_ALLOWED_DEPTH}."
                    )
                self._limit = depth
            except ValueError as e:
                if "invalid literal" in str(e):
                    raise ValueError(
                        f"Invalid argument '{part}'. Expected integer depth (1-{self.MAX_ALLOWED_DEPTH})"
                    )
                raise

    def execute(self, database_context: DatabaseContext) -> Optional[Any]:
        server_name = database_context.server.hostname

        logger.info(f"Maximum recursion depth: {self._limit}")

        # Capture starting impersonation for command generation
        self._starting_impersonation = list(
            database_context.server.impersonation_users or []
        )

        # Get initial linked servers
        try:
            all_linked_servers = self._get_linked_servers_with_access(database_context)
        except Exception as ex:
            logger.warning(f"Failed to query initial linked servers: {ex}")
            return None

        if not all_linked_servers:
            logger.warning("No linked servers found.")
            return None

        # Separate SQL Server links (chainable) from non-chainable
        sql_server_links: List[Dict[str, Any]] = []
        no_visibility_links: List[Dict[str, Any]] = []

        for row in all_linked_servers:
            access = _get_row_string(row, "Access")
            if access == "No visibility":
                no_visibility_links.append(row)
                continue

            provider = _get_row_string(row, "Provider")
            if provider.startswith("SQLNCLI") or provider.startswith("MSOLEDBSQL"):
                sql_server_links.append(row)

        if no_visibility_links:
            logger.debug(
                f"Linked servers with no visibility (current user): {len(no_visibility_links)}"
            )
            for row in no_visibility_links:
                logger.debug(
                    f"  {row.get('Link')} ({row.get('Provider')}) - will retry under impersonation"
                )

        logger.debug(f"SQL Server linked servers (chainable): {len(sql_server_links)}")
        for row in sql_server_links:
            link = _get_row_string(row, "Link")
            local_login = _get_row_string(row, "Local Login")
            remote_login = _get_row_string(row, "Remote Login")
            access = _get_row_string(row, "Access")

            desc = f"{local_login} [{access}]" if local_login else access
            if remote_login:
                desc += f" \u2192 {remote_login}"
            logger.debug(f"  {link} [{desc}]")

        # Create root node
        fixed_root_roles, custom_root_roles = (
            database_context.user_service.get_server_roles()
        )
        root_roles = fixed_root_roles + custom_root_roles
        self._root_node = ServerNode(
            alias=database_context.server.hostname,
            actual_name=database_context.server.hostname,
            logged_in_user=database_context.server.system_user
            or database_context.user_service.system_user
            or "",
            mapped_user=database_context.server.mapped_user
            or database_context.user_service.mapped_user
            or "",
            is_sysadmin=database_context.user_service.is_admin(),
            server_roles=root_roles,
        )

        if not sql_server_links and not no_visibility_links:
            logger.warning("No SQL Server linked servers to explore.")
            return None

        if not sql_server_links and no_visibility_links:
            logger.warning(
                "Current user has no visibility into any linked server mappings. "
                "Will attempt impersonation to gain visibility."
            )

        # Mark starting server+user as explored
        self._globally_explored_contexts.add(
            _context_key(
                database_context.server.hostname,
                database_context.user_service.system_user or "",
            )
        )

        # Compute starting state hash for loop detection
        starting_hash = ServerExecutionState(
            hostname=database_context.server.hostname,
            mapped_user=database_context.user_service.mapped_user or "",
            system_user=database_context.user_service.system_user or "",
            is_sysadmin=self._root_node.is_sysadmin,
        ).get_state_hash()

        # Build complete map of reachable SQL links across all transitive impersonation chains
        force_impersonation_discovery = (
            len(sql_server_links) == 0 and len(no_visibility_links) > 0
        )
        reachable_chains: List[List[str]] = (
            []
            if self._root_node.is_sysadmin
            else self._get_reachable_login_chains(database_context)
        )

        if reachable_chains:
            logger.debug(
                f"Reachable login chains from current user: {len(reachable_chains)}"
            )
            for chain in reachable_chains:
                logger.debug(f"  [{' -> '.join(chain)}]")
        elif force_impersonation_discovery:
            logger.warning(
                "No impersonable logins found. Cannot gain visibility into linked server mappings."
            )
            return None

        # Build unified map: (server, local_login) -> (row, chain)
        all_sql_links: Dict[
            Tuple[str, str], Tuple[Dict[str, Any], Optional[List[str]]]
        ] = {}

        # Current user's links
        for row in sql_server_links:
            link = _get_row_string(row, "Link").upper()
            local_login = _get_row_string(row, "Local Login").upper()
            key = (link, local_login)
            if key not in all_sql_links:
                all_sql_links[key] = (row, None)

        # Additional links visible from each transitive impersonation chain
        for chain in reachable_chains:
            if _is_system_account(chain[-1]):
                continue
            if not self._try_apply_impersonation_chain(database_context, chain):
                continue
            try:
                chain_links = self._get_linked_servers_with_access(database_context)
                gained = 0
                if chain_links:
                    for row in chain_links:
                        if _get_row_string(row, "Access") == "No visibility":
                            continue
                        provider = _get_row_string(row, "Provider")
                        if not (
                            provider.startswith("SQLNCLI")
                            or provider.startswith("MSOLEDBSQL")
                        ):
                            continue
                        link = _get_row_string(row, "Link").upper()
                        local_login = _get_row_string(row, "Local Login").upper()
                        key = (link, local_login)
                        if key not in all_sql_links:
                            all_sql_links[key] = (row, chain)
                            gained += 1
                if gained > 0:
                    logger.debug(
                        f"  Gained visibility into {gained} link mapping(s) via [{' -> '.join(chain)}]"
                    )
            except Exception as ex:
                logger.debug(
                    f"Failed to query linked servers via chain [{' -> '.join(chain)}]: {ex}"
                )
            finally:
                self._revert_chain(database_context, len(chain))

        # For servers with explicit remote login, also create per-chain entries
        existing_keys = list(all_sql_links.keys())
        for chain in reachable_chains:
            if _is_system_account(chain[-1]):
                continue
            chain_end_login = chain[-1]
            for key in existing_keys:
                if all_sql_links[key][1] is not None:
                    continue
                existing_row = all_sql_links[key][0]
                row_remote_login = _get_row_string(existing_row, "Remote Login")
                if not row_remote_login:
                    continue  # Windows pass-through
                chain_key = (key[0], chain_end_login.upper())
                if chain_key not in all_sql_links:
                    all_sql_links[chain_key] = (existing_row, chain)

        logger.info(f"Total reachable SQL Server linked servers: {len(all_sql_links)}")

        start_time = time.time()

        # Explore each discovered SQL link
        for (_, _), (
            link_row,
            chain_to_reach,
        ) in all_sql_links.items():
            remote_server = _get_row_string(link_row, "Link")
            required_login = _get_row_string(link_row, "Local Login")

            if (
                _context_key(remote_server, required_login)
                in self._globally_explored_contexts
            ):
                logger.debug(
                    f"Server '{remote_server}' already explored as '{required_login}'. Skipping."
                )
                continue

            # Negative cache check
            root_caller_login = (
                chain_to_reach[-1]
                if chain_to_reach
                else (
                    required_login
                    if required_login
                    else (database_context.user_service.system_user or "")
                )
            )
            if (
                _link_attempt_key(
                    self._root_node.alias, remote_server, root_caller_login
                )
                in self._failed_link_attempts
            ):
                logger.debug(
                    f"Skipping '{remote_server}': previous attempt from '{self._root_node.alias}' "
                    f"as '{root_caller_login}' was rejected."
                )
                continue

            # Skip self-mapping entries when no impersonation chain available
            if not required_login and chain_to_reach is None:
                logger.debug(
                    f"Skipping '{remote_server}': no explicit local login mapping "
                    "and no impersonation chain available."
                )
                continue

            visited_in_chain: Set[str] = {starting_hash}
            current_path: List[ServerNode] = []

            # Apply session-level impersonation
            session_hops = 0
            if chain_to_reach:
                if not self._try_apply_impersonation_chain(
                    database_context, chain_to_reach
                ):
                    logger.debug(
                        f"Cannot apply impersonation chain for link to '{remote_server}'. Skipping."
                    )
                    continue
                session_hops = len(chain_to_reach)
            elif (
                required_login
                and required_login.lower()
                != (database_context.user_service.system_user or "").lower()
            ):
                if not self._try_apply_impersonation_chain(
                    database_context, [required_login]
                ):
                    logger.debug(
                        f"Cannot impersonate '{required_login}' on starting server. "
                        f"Skipping link to '{remote_server}'."
                    )
                    continue
                session_hops = 1

            try:
                # Determine impersonation chain used on the starting server
                starting_imp_logins = (
                    chain_to_reach
                    if chain_to_reach
                    else (
                        [required_login]
                        if required_login
                        and required_login.lower()
                        != (database_context.user_service.system_user or "").lower()
                        else []
                    )
                )
                starting_imp = _build_impersonation_steps(starting_imp_logins)

                self._explore_linked_server(
                    database_context,
                    remote_server,
                    self._root_node,
                    current_path,
                    visited_in_chain,
                    starting_imp,
                    current_depth=0,
                )
            finally:
                if session_hops > 0:
                    self._revert_chain(database_context, session_hops)

        elapsed = time.time() - start_time

        # Count results
        total_chains = (
            0
            if not self._root_node.children
            else self._count_leaf_nodes(self._root_node)
        )
        total_escalations = self._count_escalation_paths(self._root_node)

        if total_chains == 0 and total_escalations == 0:
            logger.warning("No accessible linked server chains found.")
            return None

        summary = f"Found {total_chains} accessible chain(s)"
        if total_escalations > 0:
            summary += f" and {total_escalations} privilege escalation path(s)"
        summary += f" in {elapsed:.2f}s"
        logger.success(summary)

        # Display tree view
        print()
        self._display_tree()

        # Display chain commands summary
        print()
        chain_rows = self._display_chain_commands()

        # Persist discovered chains for future quick access
        if chain_rows:
            self._chain_store.save(server_name, chain_rows)
            logger.info("Use !chain <id> to apply a chain from the table above")

        return self._all_chains

    # ------------------------------------------------------------------
    # Recursive exploration
    # ------------------------------------------------------------------

    def _explore_linked_server(
        self,
        database_context: DatabaseContext,
        target_server: str,
        parent_node: ServerNode,
        current_path: List[ServerNode],
        visited_in_chain: Set[str],
        parent_impersonation_chain: List[ImpersonationStep],
        current_depth: int,
    ) -> None:
        """Recursively explores linked servers, building a tree structure."""
        if current_depth >= self._limit:
            logger.debug(
                f"Limit {self._limit} reached at server '{target_server}'. Backtracking."
            )
            return

        # Push this server onto the linked chain
        database_context.query_service.linked_servers.add_to_chain(target_server)

        try:
            # Clear stale caches
            database_context.user_service.clear_caches()

            # Silently probe connectivity: avoids noisy error output for inaccessible servers
            actual_server_name = target_server
            try:
                server_name_result = database_context.query_service.execute_scalar(
                    "SELECT @@SERVERNAME", silent=True
                )
                if server_name_result:
                    actual_server_name = str(server_name_result)
            except Exception:
                pass

            try:
                mapped_user, remote_logged_in_user = (
                    database_context.user_service.get_info()
                )
            except Exception as ex:
                as_who = _format_impersonation_context(
                    parent_impersonation_chain, parent_node.logged_in_user
                )
                logger.debug(f"Failed to explore {target_server} as [{as_who}]: {ex}")
                # Record in negative cache
                parent_caller_login = (
                    parent_impersonation_chain[-1].login
                    if parent_impersonation_chain
                    else parent_node.logged_in_user
                )
                self._failed_link_attempts.add(
                    _link_attempt_key(
                        parent_node.alias, target_server, parent_caller_login
                    )
                )
                return

            logger.debug(
                f"Logged in to server '{target_server}' (actual: {actual_server_name}) "
                f"as: '{remote_logged_in_user}' [{mapped_user}]"
            )

            # Early re-entry check
            context_key = _context_key(target_server, remote_logged_in_user)
            already_explored = context_key in self._globally_explored_contexts

            if already_explored and context_key in self._context_role_cache:
                node_roles, is_sysadmin = self._context_role_cache[context_key]
                logger.debug(
                    f"Reusing cached roles for '{target_server}' as '{remote_logged_in_user}' "
                    "(subtree already mapped)."
                )
            else:
                fixed_roles, custom_roles = (
                    database_context.user_service.get_server_roles()
                )
                node_roles = fixed_roles + custom_roles
                is_sysadmin = database_context.user_service.is_admin()
                self._context_role_cache[context_key] = (node_roles, is_sysadmin)

            state_hash = ServerExecutionState(
                hostname=target_server,
                mapped_user=mapped_user,
                system_user=remote_logged_in_user,
                is_sysadmin=is_sysadmin,
            ).get_state_hash()

            # Check for loop in THIS chain path
            if state_hash in visited_in_chain:
                logger.debug(
                    f"Loop detected at server '{target_server}' with user "
                    f"'{remote_logged_in_user}'. Skipping."
                )
                return

            visited_in_chain.add(state_hash)

            # Create node
            current_node = ServerNode(
                alias=target_server,
                actual_name=actual_server_name,
                logged_in_user=remote_logged_in_user,
                mapped_user=mapped_user,
                impersonation_chain=parent_impersonation_chain or [],
                is_sysadmin=is_sysadmin,
                server_roles=node_roles,
            )

            new_path = current_path + [current_node]

            # Deduplicate: skip if this exact chain was already discovered
            chain_display = self._format_chain_progress(new_path)
            if chain_display in self._seen_chain_displays:
                return
            self._seen_chain_displays.add(chain_display)

            parent_node.children.append(current_node)
            self._all_chains.append(new_path)
            logger.info(f"Chain #{len(self._all_chains)}: {chain_display}")

            # If already explored, leaf is enough
            if already_explored:
                return

            self._globally_explored_contexts.add(context_key)

            # Discover impersonation chains on this server
            remote_reachable_chains: List[List[str]] = (
                []
                if is_sysadmin
                else self._get_reachable_login_chains(database_context)
            )
            logger.debug(
                f"Reachable login chains on '{target_server}': {len(remote_reachable_chains)}"
            )

            # Build map of all links on this server
            all_links_on_server: Dict[
                Tuple[str, str], Tuple[Dict[str, Any], Optional[List[str]]]
            ] = {}

            # Current user's links
            try:
                current_user_links = self._get_linked_servers_with_access(
                    database_context
                )
                if current_user_links:
                    for row in current_user_links:
                        if _get_row_string(row, "Access") == "No visibility":
                            continue
                        server_link = _get_row_string(row, "Link").upper()
                        local_login = _get_row_string(row, "Local Login").upper()
                        key = (server_link, local_login)
                        if key not in all_links_on_server:
                            all_links_on_server[key] = (row, None)
            except Exception as ex:
                logger.debug(
                    f"Failed to query linked servers on '{target_server}' as current user: {ex}"
                )

            # Additional links from impersonation chains
            for chain in remote_reachable_chains:
                if _is_system_account(chain[-1]):
                    continue
                if not self._try_apply_impersonation_chain(database_context, chain):
                    continue
                try:
                    # Discover end-of-chain roles for escalation detection
                    fixed_chain_roles, custom_chain_roles = (
                        database_context.user_service.get_server_roles()
                    )
                    chain_end_roles = fixed_chain_roles + custom_chain_roles
                    chain_end_is_sysadmin = database_context.user_service.is_admin()

                    # Record as escalation path if privileged
                    if chain_end_is_sysadmin or any(
                        r.lower() in ELEVATED_ROLES for r in chain_end_roles
                    ):
                        steps = [ImpersonationStep(login=login) for login in chain]
                        steps[-1].roles = chain_end_roles
                        current_node.escalation_paths.append(steps)

                    chain_links = self._get_linked_servers_with_access(database_context)
                    if chain_links:
                        for row in chain_links:
                            server_link = _get_row_string(row, "Link").upper()
                            local_login = _get_row_string(row, "Local Login").upper()
                            key = (server_link, local_login)
                            if key not in all_links_on_server:
                                all_links_on_server[key] = (row, chain)
                except Exception as ex:
                    logger.debug(
                        f"Failed to query linked servers via chain "
                        f"[{' -> '.join(chain)}]: {ex}"
                    )
                finally:
                    self._revert_chain(database_context, len(chain))

            # For servers with explicit remote login, create per-chain entries
            existing_remote_keys = list(all_links_on_server.keys())
            for chain in remote_reachable_chains:
                if _is_system_account(chain[-1]):
                    continue
                chain_end = chain[-1]
                for key in existing_remote_keys:
                    if all_links_on_server[key][1] is not None:
                        continue
                    existing_row = all_links_on_server[key][0]
                    row_remote_login = _get_row_string(existing_row, "Remote Login")
                    if not row_remote_login:
                        continue
                    chain_key = (key[0], chain_end.upper())
                    if chain_key not in all_links_on_server:
                        all_links_on_server[chain_key] = (existing_row, chain)

            # Classify discovered links (only SQL Server providers are chainable)
            remote_sql_links: List[
                Tuple[str, str, Dict[str, Any], Optional[List[str]]]
            ] = []

            for (server_link_upper, _), (row, chain) in all_links_on_server.items():
                provider = _get_row_string(row, "Provider")
                local_login = _get_row_string(row, "Local Login")
                server_link = _get_row_string(row, "Link")

                if provider.startswith("SQLNCLI") or provider.startswith("MSOLEDBSQL"):
                    if not _is_system_account(local_login):
                        remote_sql_links.append((server_link, local_login, row, chain))

            logger.debug(
                f"Exploring SQL Server links on '{target_server}' (found {len(remote_sql_links)})"
            )

            # Explore each SQL Server link
            for (
                next_server,
                next_local_login,
                row,
                chain_to_reach,
            ) in remote_sql_links:
                # Skip if already explored
                if (
                    next_local_login
                    and _context_key(next_server, next_local_login)
                    in self._globally_explored_contexts
                ):
                    logger.debug(
                        f"Server '{next_server}' already explored as '{next_local_login}'. Skipping."
                    )
                    continue

                # Negative cache check
                planned_caller = (
                    (next_local_login if next_local_login else chain_to_reach[-1])
                    if chain_to_reach
                    else (
                        next_local_login if next_local_login else remote_logged_in_user
                    )
                )
                if (
                    _link_attempt_key(target_server, next_server, planned_caller)
                    in self._failed_link_attempts
                ):
                    logger.debug(
                        f"Skipping '{next_server}' from '{target_server}' as "
                        f"'{planned_caller}': previous attempt was rejected."
                    )
                    continue

                branch_visited = set(visited_in_chain)

                # Apply impersonation chain on this linked server for the child link
                impersonation_hops = 0
                if chain_to_reach:
                    if not self._try_apply_impersonation_chain(
                        database_context, chain_to_reach
                    ):
                        logger.debug(
                            f"Cannot apply impersonation chain on '{target_server}' "
                            f"for link to '{next_server}'. Skipping."
                        )
                        continue
                    impersonation_hops = len(chain_to_reach)

                    # If mapping requires a specific local login not at chain end
                    chain_end_login = chain_to_reach[-1]
                    if (
                        next_local_login
                        and next_local_login.lower() != chain_end_login.lower()
                        and next_local_login.lower() != remote_logged_in_user.lower()
                    ):
                        if self._try_apply_impersonation_chain(
                            database_context, [next_local_login]
                        ):
                            impersonation_hops += 1
                        else:
                            logger.debug(
                                f"Cannot impersonate required local login '{next_local_login}' "
                                f"on '{target_server}' for link to '{next_server}'. Skipping."
                            )
                            self._revert_chain(database_context, impersonation_hops)
                            continue
                elif (
                    next_local_login
                    and next_local_login.lower() != remote_logged_in_user.lower()
                ):
                    if self._try_apply_impersonation_chain(
                        database_context, [next_local_login]
                    ):
                        impersonation_hops = 1
                    else:
                        logger.debug(
                            f"Cannot impersonate required local login '{next_local_login}' "
                            f"on '{target_server}' for link to '{next_server}'. Skipping."
                        )
                        continue

                try:
                    # Determine impersonation chain for the next hop
                    if chain_to_reach:
                        next_imp_logins = list(chain_to_reach)
                        chain_end_login = chain_to_reach[-1]
                        if (
                            next_local_login
                            and next_local_login.lower() != chain_end_login.lower()
                            and next_local_login.lower()
                            != remote_logged_in_user.lower()
                            and impersonation_hops > len(chain_to_reach)
                        ):
                            next_imp_logins.append(next_local_login)
                    elif (
                        next_local_login
                        and next_local_login.lower() != remote_logged_in_user.lower()
                    ):
                        next_imp_logins = [next_local_login]
                    else:
                        next_imp_logins = []

                    next_impersonation = _build_impersonation_steps(next_imp_logins)

                    # Recurse
                    self._explore_linked_server(
                        database_context,
                        next_server,
                        current_node,
                        new_path,
                        branch_visited,
                        next_impersonation,
                        current_depth + 1,
                    )
                finally:
                    if impersonation_hops > 0:
                        self._revert_chain(database_context, impersonation_hops)

        except Exception as ex:
            as_who = _format_impersonation_context(
                parent_impersonation_chain, parent_node.logged_in_user
            )
            logger.debug(f"Failed to explore {target_server} as [{as_who}]: {ex}")
        finally:
            # Pop this server from the linked chain
            database_context.query_service.linked_servers.remove_last_from_chain()
            database_context.user_service.clear_caches()

    # ------------------------------------------------------------------
    # Impersonation chain discovery
    # ------------------------------------------------------------------

    def _get_reachable_login_chains(
        self, database_context: DatabaseContext
    ) -> List[List[str]]:
        """
        Returns all transitively reachable login chains from the current context,
        using the ImpersonationMap action. Each entry is an ordered list of
        logins to EXECUTE AS in sequence to reach the final login.
        """
        chains: List[List[str]] = []
        try:
            from ..database.impersonation_map import ImpersonationMap

            action = ImpersonationMap()

            logger.disable("")
            try:
                with redirect_stdout(io.StringIO()):
                    raw = action.execute(database_context)
            finally:
                logger.enable("")

            if not raw or not isinstance(raw, list):
                return chains

            result: List[Dict[str, Any]] = raw

            # Only keep the shortest chain to each unique end login
            shortest_by_end: Dict[str, List[str]] = {}
            for row in result:
                chain: List[str] = []
                middle_logins = row.get("Middle Logins", "")
                end_login = row.get("End Login", "")

                if middle_logins:
                    for login in middle_logins.split(", "):
                        login = login.strip()
                        if login:
                            chain.append(login)
                if end_login:
                    chain.append(end_login)

                if chain:
                    end_key = end_login.lower()
                    if end_key not in shortest_by_end or len(chain) < len(
                        shortest_by_end[end_key]
                    ):
                        shortest_by_end[end_key] = chain

            chains = list(shortest_by_end.values())
        except Exception as ex:
            logger.debug(f"Failed to build impersonation chains: {ex}")
        return chains

    # ------------------------------------------------------------------
    # Impersonation helpers
    # ------------------------------------------------------------------

    def _try_apply_impersonation_chain(
        self, database_context: DatabaseContext, chain: List[str]
    ) -> bool:
        """Apply a multi-hop impersonation chain. Returns False on failure."""
        applied = 0
        for login in chain:
            if database_context.user_service.impersonate_user(login):
                applied += 1
            else:
                self._revert_chain(database_context, applied)
                return False
        return True

    @staticmethod
    def _revert_chain(database_context: DatabaseContext, hops: int) -> None:
        """Revert a multi-hop impersonation chain."""
        for _ in range(hops):
            try:
                database_context.user_service.revert_impersonation()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Linked server queries
    # ------------------------------------------------------------------

    @staticmethod
    def _get_linked_servers_with_access(
        database_context: DatabaseContext,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieve linked servers with computed Access column.
        Matches the C# Links.GetLinkedServers which adds Access classification.
        """
        query = """
SELECT
    srv.name AS [Link],
    srv.product AS [Product],
    srv.provider AS [Provider],
    srv.data_source AS [Data Source],
    prin.name AS [Local Login],
    ll.remote_name AS [Remote Login],
    ll.uses_self_credential AS [Uses Self],
    CASE WHEN ll.server_id IS NOT NULL AND ll.local_principal_id = 0 THEN 1 ELSE 0 END AS [Is Default]
FROM master.sys.servers srv
LEFT JOIN master.sys.linked_logins ll ON srv.server_id = ll.server_id
LEFT JOIN master.sys.server_principals prin ON ll.local_principal_id = prin.principal_id
WHERE srv.is_linked = 1
ORDER BY srv.provider, srv.modify_date DESC;"""

        raw_rows = database_context.query_service.execute_table(query, silent=True)
        if not raw_rows:
            return None

        # Compute Access column
        # Note: impacket returns SQL NULL as the string 'NULL', not Python None
        def _is_null(val) -> bool:
            return val is None or (isinstance(val, str) and val.upper() == "NULL")

        def _to_int(val) -> int:
            if _is_null(val):
                return 0
            return int(val)

        result = []
        for row in raw_rows:
            uses_self_val = row.get("Uses Self")
            is_default_val = row.get("Is Default")
            remote_login = row.get("Remote Login")

            has_row = not _is_null(uses_self_val)
            uses_self = has_row and _to_int(uses_self_val) == 1
            is_default = _to_int(is_default_val) == 1
            remote_login_str = str(remote_login) if not _is_null(remote_login) else None

            if not has_row:
                access = "No visibility"
            elif is_default and uses_self:
                access = "Pass-through (catch-all)"
            elif is_default and remote_login_str:
                access = "Mapped (catch-all)"
            elif is_default:
                access = "Denied (catch-all)"
            elif uses_self:
                access = "Pass-through"
            elif remote_login_str:
                access = "Mapped"
            else:
                access = "Denied"

            row["Access"] = access
            result.append(row)

        return result

    # ------------------------------------------------------------------
    # Counting helpers
    # ------------------------------------------------------------------

    def _count_leaf_nodes(self, node: ServerNode) -> int:
        if not node.children:
            return 1
        count = 0
        for child in node.children:
            count += self._count_leaf_nodes(child)
        return count

    def _count_escalation_paths(self, node: ServerNode) -> int:
        count = len(node.escalation_paths)
        for child in node.children:
            count += self._count_escalation_paths(child)
        return count

    # ------------------------------------------------------------------
    # Display: Real-time progress
    # ------------------------------------------------------------------

    def _format_chain_progress(self, path: List[ServerNode]) -> str:
        """
        Format a discovered chain for real-time display using LinkedServers.format_chain_display().

        Example:
            LAB-SQL01 (operator) ─(operator)─> LAB-SQL02 ─(john → john-a)─> LAB-SQL03 (john-a) ★
        """
        # Build Server objects from the path (same logic as _build_chain_row)
        server_list: List[Server] = []
        for i, node in enumerate(path):
            if i > 0 and node.impersonation_chain:
                server_list[-1].impersonation_users = [
                    s.login for s in node.impersonation_chain
                ]
            server_list.append(Server(hostname=node.alias, impersonation_users=[]))

        linked_servers = LinkedServers(server_list)

        # Initial impersonation: starting_impersonation + first node's impersonation
        initial_imp = list(self._starting_impersonation)
        if path and path[0].impersonation_chain:
            initial_imp.extend(s.login for s in path[0].impersonation_chain)

        result = linked_servers.format_chain_display(
            initial_host=self._root_node.alias,
            initial_login=self._root_node.logged_in_user,
            initial_impersonation=initial_imp or None,
        )

        # Append endpoint login and privilege marker
        last_node = path[-1]
        result += f" ({last_node.logged_in_user}){last_node.privilege_marker}"

        return result

    # ------------------------------------------------------------------
    # Display: Tree view
    # ------------------------------------------------------------------

    def _display_tree(self) -> None:
        """Display the linked server tree with ASCII art."""
        print(
            f"{self._root_node.alias} ({self._root_node.logged_in_user} "
            f"[{self._root_node.mapped_user}]){self._root_node.privilege_marker}"
        )

        for i, child in enumerate(self._root_node.children):
            is_last = i == len(self._root_node.children) - 1
            self._display_tree_node(child, "", is_last, [])

    def _display_tree_node(
        self,
        node: ServerNode,
        indent: str,
        is_last: bool,
        parent_path: List[str],
    ) -> None:
        """Display a single tree node with proper indentation."""
        current_path = list(parent_path)
        chain_part = node.alias
        imp_logins = [s.login for s in node.impersonation_chain]

        if imp_logins:
            if current_path:
                last_part = current_path[-1]
                current_path[-1] = last_part + "/" + "/".join(imp_logins)
            else:
                chain_part = f"({' \u2192 '.join(imp_logins)}) {node.alias}"
        current_path.append(chain_part)

        # Format display name
        display_name = node.alias
        if node.alias.lower() != node.actual_name.lower():
            display_name = f"{node.alias} [{node.actual_name}]"

        if node.impersonation_chain:
            # Render impersonation steps as intermediate tree nodes
            current_indent = indent
            for s in range(len(node.impersonation_chain)):
                step = node.impersonation_chain[s]
                if s == 0:
                    step_connector = (
                        "\u2514\u2500\u2500 " if is_last else "\u251c\u2500\u2500 "
                    )
                    step_child_indent = current_indent + (
                        "    " if is_last else "\u2502   "
                    )
                else:
                    step_connector = "\u2514\u2500\u2500 "
                    step_child_indent = current_indent + "    "

                print(
                    f"{current_indent}{step_connector}{step.login}{step.privilege_marker}"
                )
                current_indent = step_child_indent

            # Render the linked server node after impersonation steps
            server_child_indent = current_indent + "    "
            print(
                f"{current_indent}\u255a\u2550\u2550 {display_name} "
                f"({node.logged_in_user} [{node.mapped_user}]){node.privilege_marker}"
            )

            for i, child in enumerate(node.children):
                child_is_last = (
                    i == len(node.children) - 1 and not node.escalation_paths
                )
                self._display_tree_node(
                    child, server_child_indent, child_is_last, current_path
                )

            self._render_escalation_paths(node, server_child_indent)
        else:
            # No impersonation: render directly
            connector = "\u255a\u2550\u2550 " if is_last else "\u2560\u2550\u2550 "
            child_indent = indent + ("    " if is_last else "\u2551   ")

            print(
                f"{indent}{connector}{display_name} "
                f"({node.logged_in_user} [{node.mapped_user}]){node.privilege_marker}"
            )

            for i, child in enumerate(node.children):
                child_is_last = (
                    i == len(node.children) - 1 and not node.escalation_paths
                )
                self._display_tree_node(
                    child, child_indent, child_is_last, current_path
                )

            self._render_escalation_paths(node, child_indent)

    @staticmethod
    def _render_escalation_paths(node: ServerNode, indent: str) -> None:
        """Render privilege escalation paths discovered at a server node."""
        if not node.escalation_paths:
            return

        for p, path in enumerate(node.escalation_paths):
            is_last = p == len(node.escalation_paths) - 1
            connector = "\u2514\u2500\u2500 " if is_last else "\u251c\u2500\u2500 "
            chain_display = " \u2192 ".join(
                f"{s.login}{s.privilege_marker}" for s in path
            )
            print(f"{indent}{connector}{chain_display}")

    # ------------------------------------------------------------------
    # Display: Chain commands summary
    # ------------------------------------------------------------------

    def _display_chain_commands(self) -> List[Dict[str, Any]]:
        """
        Build a summary table of all discovered chains.
        Sorted by privilege level (sysadmin first), then by hop count.
        """
        rows: List[Dict[str, Any]] = []

        # Sort: privileged first, then shortest hop count
        ordered = sorted(
            (c for c in self._all_chains if c),
            key=lambda c: (-self._get_chain_priority(c), self._get_total_hops(c)),
        )

        chain_id = 0
        for chain in ordered:
            chain_id += 1
            hops = self._get_total_hops(chain)
            row = self._build_chain_row(chain, hops, chain_id)
            rows.append(row)

            # Add escalation path rows for the last node
            last_node = chain[-1]
            for escalation in last_node.escalation_paths:
                chain_id += 1
                esc_hops = hops + len(escalation)
                esc_row = self._build_escalation_row(
                    chain, escalation, esc_hops, chain_id
                )
                rows.append(esc_row)

        if rows:
            print(OutputFormatter.convert_list_of_dicts(rows))

        return rows

    @staticmethod
    def _get_total_hops(chain: List[ServerNode]) -> int:
        """Total number of hops (linked servers + impersonation steps) in a chain."""
        hops = len(chain)
        for node in chain:
            hops += len(node.impersonation_chain)
        return hops

    @staticmethod
    def _get_chain_priority(chain: List[ServerNode]) -> int:
        """Sort priority: 2 = sysadmin, 1 = elevated, 0 = standard."""
        if not chain:
            return 0
        last_node = chain[-1]
        if last_node.is_sysadmin:
            return 2
        if last_node.is_elevated:
            return 1
        return 0

    def _build_chain_row(
        self, chain: List[ServerNode], hops: int, chain_id: int
    ) -> Dict[str, Any]:
        """Build a row for the chain summary table."""
        last_node = chain[-1]

        # Build linked server list for -l argument
        server_list: List[Server] = []
        for i, node in enumerate(chain):
            if i > 0 and node.impersonation_chain:
                server_list[-1].impersonation_users = [
                    s.login for s in node.impersonation_chain
                ]
            server_list.append(Server(hostname=node.alias, impersonation_users=[]))

        linked_servers = LinkedServers(server_list)
        chain_arg = linked_servers.get_chain_arguments()

        # Build host argument
        host_arg = bracket_identifier(self._root_node.alias)
        host_impersonation = list(self._starting_impersonation)
        if chain and chain[0].impersonation_chain:
            host_impersonation.extend(s.login for s in chain[0].impersonation_chain)
        if host_impersonation:
            host_arg += "/" + "/".join(host_impersonation)

        # Full command
        command = f"{host_arg} -l {chain_arg}"

        # Endpoint display
        endpoint = last_node.alias
        if last_node.alias.lower() != last_node.actual_name.lower():
            endpoint = f"{last_node.alias} [{last_node.actual_name}]"

        # Privilege level
        if last_node.is_sysadmin:
            privilege = "sysadmin"
        elif last_node.is_elevated:
            privilege = ", ".join(
                r for r in last_node.server_roles if r.lower() in ELEVATED_ROLES
            )
        else:
            privilege = ""

        return {
            "#": chain_id,
            "Endpoint": endpoint,
            "Login": last_node.logged_in_user,
            "Mapped To": last_node.mapped_user,
            "Hops": hops,
            "Server Roles": privilege,
            "Command": command,
        }

    def _build_escalation_row(
        self,
        chain: List[ServerNode],
        escalation: List[ImpersonationStep],
        hops: int,
        chain_id: int,
    ) -> Dict[str, Any]:
        """Build a row for a privilege escalation path."""
        last_node = chain[-1]

        # Build linked server list with escalation on the last server
        server_list: List[Server] = []
        for i, node in enumerate(chain):
            if i > 0 and node.impersonation_chain:
                server_list[-1].impersonation_users = [
                    s.login for s in node.impersonation_chain
                ]
            server_list.append(Server(hostname=node.alias, impersonation_users=[]))

        # Add escalation impersonation on the last server
        server_list[-1].impersonation_users = [s.login for s in escalation]

        linked_servers = LinkedServers(server_list)
        chain_arg = linked_servers.get_chain_arguments()

        # Build host argument
        host_arg = bracket_identifier(self._root_node.alias)
        host_impersonation = list(self._starting_impersonation)
        if chain and chain[0].impersonation_chain:
            host_impersonation.extend(s.login for s in chain[0].impersonation_chain)
        if host_impersonation:
            host_arg += "/" + "/".join(host_impersonation)

        # Full command
        command = f"{host_arg} -l {chain_arg}"

        # Endpoint
        endpoint = last_node.alias
        if last_node.alias.lower() != last_node.actual_name.lower():
            endpoint = f"{last_node.alias} [{last_node.actual_name}]"

        # Privilege from escalation endpoint
        last_step = escalation[-1]
        if last_step.is_sysadmin:
            privilege = "sysadmin"
        elif any(r.lower() in ELEVATED_ROLES for r in last_step.roles):
            privilege = ", ".join(
                r for r in last_step.roles if r.lower() in ELEVATED_ROLES
            )
        else:
            privilege = ", ".join(last_step.roles)

        return {
            "#": chain_id,
            "Endpoint": endpoint,
            "Login": last_step.login,
            "Mapped To": "",
            "Hops": hops,
            "Server Roles": privilege,
            "Command": command,
        }

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def get_arguments(self) -> List[str]:
        return ["[max_depth]"]
