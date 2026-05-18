# mssqlclient_ng/core/actions/configmgr/cm_servers.py

"""Enumerate ConfigMgr servers in site hierarchy."""

from typing import Optional

from loguru import logger

from .cm_base import CMBaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter


@ActionFactory.register("cm-servers", "Enumerate ConfigMgr servers in site hierarchy")
class CMServers(CMBaseAction):
    """Enumerate ConfigMgr servers including site servers, management points, and distribution points."""

    def execute(self, database_context: DatabaseContext) -> Optional[list]:
        logger.info("Enumerating ConfigMgr servers in hierarchy")

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(f"ConfigMgr database: {db} (Site Code: {site_code})")

            query = f"""
SELECT DISTINCT
    sd.SiteCode,
    sd.SiteServerName,
    sd.SiteDatabaseName,
    sd.SiteDatabaseServer,
    CASE sd.SiteType
        WHEN 1 THEN 'Secondary Site'
        WHEN 2 THEN 'Primary Site'
        WHEN 4 THEN 'Central Administration Site'
        ELSE 'Unknown'
    END AS SiteType,
    s.NALPath,
    s.RoleName
FROM [{db}].dbo.SC_SiteDefinition sd
LEFT JOIN [{db}].dbo.ServerData s ON sd.SiteCode = s.SiteCode
WHERE sd.SiteServerName IS NOT NULL
ORDER BY sd.SiteCode, sd.SiteServerName;"""

            try:
                results = database_context.query_service.execute(query)
                if results:
                    logger.success(f"Found {len(results)} server(s)")
                    print(OutputFormatter.convert_list_of_dicts(results))
                else:
                    logger.warning("No servers found")
            except Exception as ex:
                logger.error(f"Failed to enumerate servers: {ex}")

        return results
