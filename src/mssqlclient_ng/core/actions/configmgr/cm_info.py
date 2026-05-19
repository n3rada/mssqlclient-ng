# mssqlclient_ng/core/actions/configmgr/cm_info.py

"""ConfigMgr site information reconnaissance."""

from loguru import logger

from .cm_base import CMBaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter

@ActionFactory.register(
    "cm-info", "Display ConfigMgr site information and infrastructure"
)
class CMInfo(CMBaseAction):
    """
    Display ConfigMgr site information including site code, version, build,
    database server, and management point details.
    """

    def execute(self, database_context: DatabaseContext) -> list | None:
        logger.info("Detecting ConfigMgr databases")

        databases = self._get_databases(database_context)
        if not databases:
            return None

        tables = []

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(
                f"Enumerating ConfigMgr database: {db} (Site Code: {site_code})"
            )

            # Site information
            query = f"""
SELECT SiteCode, SiteName, Version, SiteServer, InstallDir, DefaultMP
FROM [{db}].dbo.Sites;"""
            try:
                results = database_context.query_service.execute(query)
                if results:
                    logger.success("Site Information")
                    print(OutputFormatter.convert_list_of_dicts(results))
                    tables.append(results)
            except Exception as ex:
                logger.debug(f"Failed to query site info: {ex}")

            # Component status
            try:
                comp_query = f"SELECT * FROM [{db}].dbo.vSMS_SC_Component_Status ORDER BY ComponentName;"
                results = database_context.query_service.execute(comp_query)
                if results:
                    logger.success("ConfigMgr Components")
                    print(OutputFormatter.convert_list_of_dicts(results))
                    tables.append(results)
            except Exception:
                try:
                    comp_query = f"""
SELECT ComponentName, Name AS MachineName,
    CASE Flags WHEN 2 THEN 'OK' WHEN 5 THEN 'Warning' WHEN 6 THEN 'Error' ELSE 'Unknown' END AS Status
FROM [{db}].dbo.SC_Component ORDER BY ComponentName;"""
                    results = database_context.query_service.execute(comp_query)
                    if results:
                        logger.success("ConfigMgr Components")
                        print(OutputFormatter.convert_list_of_dicts(results))
                        tables.append(results)
                except Exception as ex:
                    logger.debug(f"Could not query components: {ex}")

            # Site system roles
            try:
                roles_query = f"SELECT * FROM [{db}].dbo.vSMS_SC_SiteSystemRole ORDER BY ServerName, RoleName;"
                results = database_context.query_service.execute(roles_query)
                if results:
                    logger.success("Site System Roles")
                    print(OutputFormatter.convert_list_of_dicts(results))
                    tables.append(results)
            except Exception:
                try:
                    roles_query = f"""
SELECT sr.NALPath, sr.RoleTypeID,
    CASE sr.RoleTypeID
        WHEN 2 THEN 'SMS Provider' WHEN 3 THEN 'Distribution Point'
        WHEN 4 THEN 'Management Point' WHEN 6 THEN 'Site Server'
        WHEN 11 THEN 'Software Update Point'
        ELSE CAST(sr.RoleTypeID AS VARCHAR(10))
    END AS RoleName
FROM [{db}].dbo.SC_SysResUse sr ORDER BY sr.NALPath;"""
                    results = database_context.query_service.execute(roles_query)
                    if results:
                        logger.success("Site System Roles")
                        print(OutputFormatter.convert_list_of_dicts(results))
                        tables.append(results)
                except Exception as ex:
                    logger.debug(f"Could not query roles: {ex}")

        return tables if tables else None
