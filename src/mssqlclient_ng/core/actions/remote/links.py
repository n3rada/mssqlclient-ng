# mssqlclient_ng/core/actions/remote/links.py

# Built-in imports
from typing import Optional, List, Dict, Any

# Third-party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


@ActionFactory.register(
    "links",
    "Enumerate linked SQL servers and configurations",
    aliases=["linkedservers"],
)
class Links(BaseAction):
    """
    Enumerate linked SQL servers and their configurations.

    Lists all linked servers configured on the SQL Server instance along with
    their authentication mappings, access settings (RPC Out, OPENQUERY), and
    collation compatibility.
    """

    def execute(
        self, database_context: DatabaseContext
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Execute the linked servers enumeration.

        Args:
            database_context: The database context

        Returns:
            List of linked servers with their configurations
        """
        logger.info("Retrieving linked SQL servers")

        try:
            result_rows = self._get_linked_servers(database_context)

            if not result_rows:
                logger.warning("No linked servers found")
                return None

            print(OutputFormatter.convert_list_of_dicts(result_rows))

            logger.success(f"Found {len(result_rows)} linked server(s)")

            # Check for pass-through entries
            has_pass_through = any(
                "Pass-through" in str(r.get("Access", "")) for r in result_rows
            )
            if has_pass_through:
                logger.warning(
                    "Pass-through entries use the caller's Windows identity (Kerberos delegation required for network hops)"
                )

            logger.warning(
                "Only returns the linked servers that user has visibility into"
            )
            logger.info("Use !link <server> to hop to a linked server")
            logger.info(
                "Use !linkmap to discover full chains, then !chain <id> to apply one"
            )

            return result_rows

        except Exception as e:
            logger.error(f"Failed to retrieve linked servers: {e}")
            return None

    @staticmethod
    def _get_linked_servers(
        database_context: DatabaseContext,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieve linked servers and login mappings with computed Access column.

        The raw columns from SQL Server include Uses Self and Is Default which are
        interpreted into a human-readable Access column:
          - "No visibility"            : no linked_logins row visible
          - "Denied (catch-all)"       : catch-all rule explicitly blocks unmapped logins
          - "Pass-through (catch-all)" : catch-all rule passes caller's Windows identity
          - "Mapped (catch-all)"       : catch-all rule maps to a fixed remote login
          - "Pass-through"             : specific login passes through as itself
          - "Mapped"                   : specific login is mapped to a remote credential
          - "Denied"                   : specific login is denied

        Args:
            database_context: The database context

        Returns:
            List of linked server dictionaries with Access column
        """
        query = """
            SELECT
                srv.modify_date AS [Last Modified],
                srv.name AS [Link],
                srv.provider AS [Provider],
                srv.data_source AS [Data Source],
                prin.name AS [Local Login],
                ll.remote_name AS [Remote Login],
                ll.uses_self_credential AS [Uses Self],
                CASE WHEN ll.server_id IS NOT NULL AND ll.local_principal_id = 0 THEN 1 ELSE 0 END AS [Is Default],
                srv.is_rpc_out_enabled AS [RPC Out],
                srv.is_data_access_enabled AS [OPENQUERY],
                srv.is_collation_compatible AS [Collation]
            FROM master.sys.servers srv
            LEFT JOIN master.sys.linked_logins ll ON srv.server_id = ll.server_id
            LEFT JOIN master.sys.server_principals prin ON ll.local_principal_id = prin.principal_id
            WHERE srv.is_linked = 1
            ORDER BY srv.provider, srv.modify_date DESC;
        """

        raw = database_context.query_service.execute_table(query)
        if not raw:
            return raw

        # Compute Access column
        enriched = []
        for row in raw:
            uses_self_raw = row.get("Uses Self")
            has_row = uses_self_raw is not None
            uses_self = int(uses_self_raw) == 1 if has_row else False
            is_default = int(row.get("Is Default") or 0) == 1
            remote_login = row.get("Remote Login")

            if not has_row:
                access = "No visibility"
            elif is_default and uses_self:
                access = "Pass-through (catch-all)"
            elif is_default and remote_login is not None:
                access = "Mapped (catch-all)"
            elif is_default:
                access = "Denied (catch-all)"
            elif uses_self:
                access = "Pass-through"
            elif remote_login is not None:
                access = "Mapped"
            else:
                access = "Denied"

            enriched.append(
                {
                    "Last Modified": row["Last Modified"],
                    "Link": row["Link"],
                    "Provider": row["Provider"],
                    "Data Source": row["Data Source"],
                    "Local Login": row.get("Local Login") or "N/A",
                    "Remote Login": remote_login,
                    "Access": access,
                    "RPC Out": row["RPC Out"],
                    "OPENQUERY": row["OPENQUERY"],
                    "Collation": row["Collation"],
                }
            )

        # Remove "Denied (catch-all)" rows when specific mappings exist for the same link
        links_with_mappings = set()
        for row in enriched:
            if row["Access"] not in ("Denied (catch-all)", "No visibility"):
                links_with_mappings.add(row["Link"])

        enriched = [
            row
            for row in enriched
            if not (
                row["Access"] == "Denied (catch-all)"
                and row["Link"] in links_with_mappings
            )
        ]

        return enriched

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            Empty list (no arguments required)
        """
        return []
