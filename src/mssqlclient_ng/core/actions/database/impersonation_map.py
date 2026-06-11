# mssqlclient_ng/core/actions/database/impersonation_map.py

# Built-in imports
from typing import Any

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter

_MAX_DEPTH = 10

# NT AUTHORITY\*, NT SERVICE\* — system accounts that are valid IMPERSONATE targets
# on paper but cannot be used as linked-server callers; stop recursion here.
_SYSTEM_PREFIXES = ("nt authority\\", "nt service\\")

_IMPERSONATABLE_QUERY = """
SELECT sp.name
FROM sys.server_principals sp
WHERE HAS_PERMS_BY_NAME(sp.name, 'LOGIN', 'IMPERSONATE') = 1
  AND sp.type_desc IN ('SQL_LOGIN', 'WINDOWS_LOGIN')
  AND sp.name NOT LIKE '##%';"""

def _is_system_account(login: str) -> bool:
    return login.lower().startswith(_SYSTEM_PREFIXES)

@ActionFactory.register(
    "impersonation-map",
    "Map multi-hop EXECUTE AS impersonation chains reachable from the current login. Records system accounts as endpoints without recursing. No-op if the current user is already sysadmin. Output lists each chain with starting login, intermediate hops, and end login.",
    aliases=["impersonate-chains", "impmap", "impchains"],
)
class ImpersonationMap(BaseAction):
    """
    Discover all transitive impersonation chains available to the current login.

    Performs a recursive DFS: for each impersonatable login, impersonates it
    and checks which further logins it can in turn impersonate, up to a maximum
    depth of 10 hops.  Loop detection prevents infinite recursion.

    Output columns:
      Hops          — number of EXECUTE AS steps in the chain
      Starting Login — the login we started from (current user)
      Middle Logins  — intermediate logins (empty for direct 1-hop chains)
      End Login      — the final reachable login
    """

    def execute(
        self, database_context: DatabaseContext
    ) -> list[dict[str, Any]] | None:
        logger.info("Starting impersonation chain mapping")

        starting_login = database_context.user_service.system_user or ""

        if database_context.user_service.is_admin():
            logger.success("Current user is sysadmin; can impersonate any login directly")
            rows = database_context.query_service.execute_table(_IMPERSONATABLE_QUERY)
            if not rows:
                logger.warning("No impersonatable logins found")
                return []
            logins = sorted(
                (r.get("name", "") for r in rows if r.get("name")),
                key=str.lower,
            )
            result = [{"#": i + 1, "Login": login} for i, login in enumerate(logins)]
            print(OutputFormatter.convert_list_of_dicts(result))
            logger.success(f"Found {len(result)} impersonatable login(s)")
            return result

        all_chains: list[dict[str, Any]] = []
        self._build_map(
            database_context,
            starting_login,
            [],
            all_chains,
            set(),
            depth=0,
        )

        if not all_chains:
            logger.warning("No impersonation chains found from current user")
            return []

        # Sort by hop count descending, then end login ascending
        all_chains.sort(key=lambda c: (-c["Hops"], c["End Login"]))

        # Prepend # index for selection via !impmap <id>
        numbered = [{"#": i + 1, **row} for i, row in enumerate(all_chains)]

        print(OutputFormatter.convert_list_of_dicts(numbered))
        logger.success(f"Found {len(all_chains)} impersonation chain(s)")
        return numbered

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_map(
        self,
        db: DatabaseContext,
        starting_login: str,
        current_path: list[str],
        all_chains: list[dict[str, Any]],
        visited: set[str],
        depth: int,
    ) -> None:
        if depth >= _MAX_DEPTH:
            return

        try:
            rows = db.query_service.execute_table(_IMPERSONATABLE_QUERY, silent=True)
        except Exception as ex:
            # Handle error 916: impersonated user cannot access current database
            if "is not able to access the database" in str(ex):
                logger.debug(
                    f"Impersonated user cannot access current DB; switching to master"
                )
                try:
                    db.query_service.execute_non_processing("USE master;", silent=True)
                    rows = db.query_service.execute_table(
                        _IMPERSONATABLE_QUERY, silent=True
                    )
                except Exception as retry_ex:
                    logger.debug(
                        f"Still could not query after USE master at depth {depth}: {retry_ex}"
                    )
                    return
            else:
                logger.debug(
                    f"Could not query impersonatable logins at depth {depth}: {ex}"
                )
                return

        if not rows:
            return

        logins = [r["name"] for r in rows if r.get("name")]
        logger.debug(
            f"depth={depth} found {len(logins)} impersonatable login(s): {logins}"
        )

        for login in logins:
            if login.lower() == starting_login.lower():
                continue
            if login in visited:
                continue

            new_path = current_path + [login]

            middle = ", ".join(new_path[:-1]) if len(new_path) > 1 else ""
            all_chains.append(
                {
                    "Hops": len(new_path),
                    "Starting Login": starting_login,
                    "Middle Logins": middle,
                    "End Login": login,
                }
            )

            # System accounts are recorded as endpoints but not recursed into
            if _is_system_account(login):
                logger.debug(
                    f"System account '{login}' recorded as endpoint; not recursing"
                )
                continue

            new_visited = visited | {login}

            try:
                logger.debug(f"Impersonating '{login}' to explore deeper chains")
                db.user_service.impersonate_user(login)

                if not db.user_service.is_admin():
                    self._build_map(
                        db,
                        starting_login,
                        new_path,
                        all_chains,
                        new_visited,
                        depth + 1,
                    )
            except Exception as ex:
                logger.debug(f"Could not explore deeper chains from '{login}': {ex}")
            finally:
                try:
                    db.user_service.revert_impersonation()
                except Exception as ex:
                    logger.debug(f"Failed to revert from '{login}': {ex}")
