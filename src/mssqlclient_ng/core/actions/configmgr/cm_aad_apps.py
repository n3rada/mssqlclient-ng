# mssqlclient_ng/core/actions/configmgr/cm_aad_apps.py

"""Enumerate Azure AD applications stored in ConfigMgr."""

from typing import Optional

from loguru import logger

from .cm_base import CMBaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter


@ActionFactory.register(
    "cm-aadapps",
    "Enumerate Azure AD applications in ConfigMgr",
    aliases=["cm-aad-apps", "cm-aad"],
)
class CMAadApps(CMBaseAction):
    """
    Enumerate Azure AD application registrations stored in ConfigMgr for CMG and co-management.
    Shows AAD tenant IDs, application (client) IDs, and encrypted secret key blobs.
    """


    def __init__(self):
        super().__init__()
        self._filter: str = ""

    def validate_arguments(self, additional_arguments: str = "", argument_list=None) -> None:
        named, positional = self._parse_action_arguments(additional_arguments)
        self._filter = named.get("filter", named.get("f", "")) or self.get_positional_argument(positional, 0, "")

    def execute(self, database_context: DatabaseContext) -> Optional[list]:
        filter_msg = f" (filter: {self._filter})" if self._filter else ""
        logger.info(f"Enumerating Azure AD applications{filter_msg}")

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(f"ConfigMgr database: {db} (Site Code: {site_code})")

            where = ""
            if self._filter:
                where = f"WHERE a.Name LIKE '%{self._filter}%'"

            query = f"""
SELECT
    a.ID,
    t.TenantID,
    t.Name AS TenantName,
    a.ClientID,
    a.Name AS ApplicationName,
    a.LastUpdateTime,
    CONVERT(VARCHAR(MAX), a.SecretKey, 1) AS SecretKey,
    CONVERT(VARCHAR(MAX), a.SecretKeyForSCP, 1) AS SecretKeyForSCP
FROM [{db}].dbo.AAD_Application_Ex a
LEFT JOIN [{db}].dbo.AAD_Tenant_Ex t ON t.ID = a.TenantDB_ID
{where}
ORDER BY a.LastUpdateTime DESC;"""

            try:
                results = database_context.query_service.execute(query)
                if results:
                    logger.success(f"Found {len(results)} Azure AD application(s)")
                    print(OutputFormatter.convert_list_of_dicts(results))
                else:
                    logger.warning("No Azure AD applications found")
            except Exception as ex:
                logger.error(f"Failed to enumerate AAD apps: {ex}")

        return None
