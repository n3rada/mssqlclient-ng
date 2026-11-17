# mssqlclient_ng/core/actions/cloud/azure/azure_users.py

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
    "azure-users",
    "Enumerate contained database users, Entra ID (Azure AD) principals, "
    "and their database role memberships in Azure SQL Database.",
)
class AzureUsers(BaseAction):
    """
    Enumerate Azure SQL Database contained users and external principals.

    Azure SQL Database has no server-level logins visible to normal users.
    Authentication is through contained database users (SQL auth or Entra ID).
    This action enumerates all principals, highlights external (Entra ID)
    accounts, and shows database role memberships for each.
    """

    def execute(
        self, database_context: DatabaseContext
    ) -> list[list[dict[str, Any]]] | None:
        if not requires_azure_sql(database_context):
            return None

        qs = database_context.query_service

        tables: list[list[dict[str, Any]]] = []

        principals = self._contained_principals(qs)
        if principals:
            tables.append(principals)

        role_members = self._role_memberships(qs)
        if role_members:
            tables.append(role_members)

        return tables or None

    @staticmethod
    def _contained_principals(qs) -> list[dict[str, Any]]:
        """Enumerate all contained database principals with auth type."""
        query = """
            SELECT
                dp.name AS [Principal],
                dp.type_desc AS [Type],
                dp.authentication_type_desc AS [Auth Type],
                dp.default_schema_name AS [Default Schema],
                dp.create_date AS [Created],
                dp.modify_date AS [Modified],
                CASE
                    WHEN dp.type IN ('E', 'X') THEN 'Entra ID'
                    WHEN dp.authentication_type_desc = 'DATABASE' THEN 'Contained SQL'
                    WHEN dp.authentication_type_desc = 'WINDOWS' THEN 'Windows'
                    ELSE dp.authentication_type_desc
                END AS [Auth Source]
            FROM sys.database_principals dp
            WHERE dp.type IN ('S', 'U', 'G', 'E', 'X')
              AND dp.sid IS NOT NULL
              AND dp.name NOT LIKE '##%'
              AND dp.name NOT IN ('dbo', 'guest', 'INFORMATION_SCHEMA', 'sys')
            ORDER BY dp.type_desc, dp.name;
        """
        try:
            rows = qs.execute_table(query)
        except Exception as ex:
            logger.error(f"Failed to enumerate contained principals: {ex}")
            return []

        if not rows:
            logger.warning("No contained database principals found")
            return []

        entra_count = sum(
            1 for r in rows if r.get("Auth Source") == "Entra ID"
        )
        sql_count = sum(
            1 for r in rows if r.get("Auth Source") == "Contained SQL"
        )

        logger.success(
            f"Found {len(rows)} principal(s): "
            f"{entra_count} Entra ID, {sql_count} contained SQL, "
            f"{len(rows) - entra_count - sql_count} other"
        )
        print(OutputFormatter.convert_list_of_dicts(rows))
        return rows

    @staticmethod
    def _role_memberships(qs) -> list[dict[str, Any]]:
        """Show database role memberships for all non-system principals."""
        query = """
            SELECT
                dp.name AS [Principal],
                dp.type_desc AS [Principal Type],
                STRING_AGG(r.name, ', ') WITHIN GROUP (ORDER BY r.name) AS [Database Roles]
            FROM sys.database_principals dp
            INNER JOIN sys.database_role_members drm
                ON dp.principal_id = drm.member_principal_id
            INNER JOIN sys.database_principals r
                ON drm.role_principal_id = r.principal_id
            WHERE dp.type IN ('S', 'U', 'G', 'E', 'X')
              AND dp.name NOT LIKE '##%'
              AND dp.name NOT IN ('dbo', 'guest', 'INFORMATION_SCHEMA', 'sys')
            GROUP BY dp.name, dp.type_desc
            ORDER BY dp.name;
        """
        try:
            rows = qs.execute_table(query)
        except Exception as ex:
            logger.warning(f"Could not enumerate role memberships: {ex}")
            return []

        if not rows:
            logger.debug("No explicit database role memberships found")
            return []

        # Highlight privileged roles
        privileged = {"db_owner", "db_securityadmin", "db_accessadmin"}
        for row in rows:
            roles_str = row.get("Database Roles", "")
            assigned = {r.strip() for r in roles_str.split(",")}
            if assigned & privileged:
                row["Privileged"] = "Yes"
            else:
                row["Privileged"] = ""

        logger.info(f"Database role memberships ({len(rows)} principal(s)):")
        print(OutputFormatter.convert_list_of_dicts(rows))
        return rows
