# mssqlclient_ng/core/actions/cloud/azure/azure_firewall.py

# Built-in imports
from typing import Any

# Third-party imports
from loguru import logger

# Local imports
from ...base import BaseAction
from ...factory import ActionFactory
from ....services.database import DatabaseContext
from ....utils.formatters import OutputFormatter
from .guard import requires_azure_sql


@ActionFactory.register(
    "azure-firewall",
    "Enumerate Azure SQL server-level and database-level firewall rules. "
    "Reveals allowed IP ranges and overly permissive rules (0.0.0.0).",
    aliases=["azure-fw"],
)
class AzureFirewall(BaseAction):
    """
    Enumerate Azure SQL Database firewall rules.

    Server-level rules (sys.firewall_rules) are readable from the master
    database; database-level rules (sys.database_firewall_rules) are
    per-database. A start_ip_address of 0.0.0.0 with end_ip_address of
    0.0.0.0 means "Allow Azure services", and 0.0.0.0 to
    255.255.255.255 means open to the entire internet.
    """

    def execute(
        self, database_context: DatabaseContext
    ) -> list[list[dict[str, Any]]] | None:
        if not requires_azure_sql(database_context):
            return None

        qs = database_context.query_service

        tables: list[list[dict[str, Any]]] = []

        server_rules = self._server_firewall_rules(qs)
        if server_rules:
            tables.append(server_rules)

        db_rules = self._database_firewall_rules(qs)
        if db_rules:
            tables.append(db_rules)

        if not tables:
            logger.warning("No firewall rules found (or no permission to view them)")

        return tables or None

    @staticmethod
    def _server_firewall_rules(qs) -> list[dict[str, Any]]:
        """Enumerate server-level firewall rules (requires master context)."""
        query = (
            "SELECT name AS [Rule Name], "
            "start_ip_address AS [Start IP], "
            "end_ip_address AS [End IP], "
            "create_date AS [Created], "
            "modify_date AS [Modified] "
            "FROM sys.firewall_rules "
            "ORDER BY start_ip_address;"
        )
        try:
            rows = qs.execute_table(query)
        except Exception as ex:
            message = str(ex).lower()
            if "invalid object" in message or "does not exist" in message:
                logger.debug(
                    "sys.firewall_rules not accessible "
                    "(only available in master database)"
                )
            else:
                logger.warning(f"Could not query server firewall rules: {ex}")
            return []

        if not rows:
            logger.debug("No server-level firewall rules found")
            return []

        # Flag dangerous rules
        for row in rows:
            start = row.get("Start IP", "")
            end = row.get("End IP", "")
            if start == "0.0.0.0" and end == "0.0.0.0":
                row["Note"] = "ALLOWS AZURE SERVICES"
            elif start == "0.0.0.0" and end == "255.255.255.255":
                row["Note"] = "OPEN TO INTERNET"
            else:
                row["Note"] = ""

        logger.info(f"Server-level firewall rules ({len(rows)}):")
        print(OutputFormatter.convert_list_of_dicts(rows))
        return rows

    @staticmethod
    def _database_firewall_rules(qs) -> list[dict[str, Any]]:
        """Enumerate database-level firewall rules."""
        query = (
            "SELECT name AS [Rule Name], "
            "start_ip_address AS [Start IP], "
            "end_ip_address AS [End IP], "
            "create_date AS [Created], "
            "modify_date AS [Modified] "
            "FROM sys.database_firewall_rules "
            "ORDER BY start_ip_address;"
        )
        try:
            rows = qs.execute_table(query)
        except Exception as ex:
            message = str(ex).lower()
            if "invalid object" in message or "does not exist" in message:
                logger.debug("sys.database_firewall_rules not accessible")
            else:
                logger.warning(f"Could not query database firewall rules: {ex}")
            return []

        if not rows:
            logger.debug("No database-level firewall rules found")
            return []

        for row in rows:
            start = row.get("Start IP", "")
            end = row.get("End IP", "")
            if start == "0.0.0.0" and end == "255.255.255.255":
                row["Note"] = "OPEN TO INTERNET"
            else:
                row["Note"] = ""

        logger.info(f"Database-level firewall rules ({len(rows)}):")
        print(OutputFormatter.convert_list_of_dicts(rows))
        return rows
