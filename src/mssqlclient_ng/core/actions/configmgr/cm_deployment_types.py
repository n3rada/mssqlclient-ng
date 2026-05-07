# mssqlclient_ng/core/actions/configmgr/cm_deployment_types.py

"""Enumerate ConfigMgr deployment types with technical details."""

from typing import Optional

from loguru import logger

from .cm_base import CMBaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatter import OutputFormatter


@ActionFactory.register("cm-dts", "Enumerate ConfigMgr deployment types")
class CMDeploymentTypes(CMBaseAction):
    """
    Display an overview of all ConfigMgr deployment types with searchable technical details.
    Shows technology type, install commands, content paths, and detection methods.
    """

    def __init__(self):
        super().__init__()
        self._technology: str = ""
        self._content_path: str = ""
        self._install_command: str = ""
        self._application: str = ""
        self._limit: int = 25

    def validate_arguments(self, additional_arguments: str = "") -> None:
        named, positional = self._parse_action_arguments(additional_arguments)
        self._technology = named.get("tech", "")
        self._content_path = named.get("content", "")
        self._install_command = named.get("install", "")
        self._application = named.get("app", "")
        self._limit = int(named.get("limit", "25"))

    def execute(self, database_context: DatabaseContext) -> Optional[list]:
        filters = []
        if self._technology:
            filters.append(f"tech: {self._technology}")
        if self._content_path:
            filters.append(f"content: {self._content_path}")
        if self._application:
            filters.append(f"app: {self._application}")
        logger.info(f"Retrieving deployment types{' (' + ', '.join(filters) + ')' if filters else ''}")

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(f"ConfigMgr database: {db} (Site Code: {site_code})")

            where = "WHERE ci.CIType_ID = 21"  # Deployment Type CI
            if self._application:
                where += f" AND lp.DisplayName LIKE '%{self._application}%'"

            top = self._build_top_clause(self._limit)

            query = f"""
SELECT {top}
    ci.CI_ID,
    COALESCE(lp.DisplayName, ci.ModelName) AS Title,
    ci.ModelName,
    ci.CIVersion,
    ci.IsEnabled,
    ci.IsExpired,
    ci.ContentSourcePath,
    ci.CreatedBy,
    ci.DateCreated,
    ci.LastModifiedBy,
    ci.DateLastModified,
    CAST(ci.SDMPackageDigest AS NVARCHAR(MAX)) AS SDMPackageDigest
FROM [{db}].dbo.v_ConfigurationItems ci
LEFT JOIN (
    SELECT CI_ID, MIN(DisplayName) AS DisplayName
    FROM [{db}].dbo.v_LocalizedCIProperties
    WHERE DisplayName IS NOT NULL AND DisplayName != ''
    GROUP BY CI_ID
) lp ON ci.CI_ID = lp.CI_ID
{where}
ORDER BY ci.DateCreated DESC;"""

            try:
                results = database_context.query_service.execute(query)
                if not results:
                    logger.warning("No deployment types found")
                    continue

                # Parse SDM XML for additional info and filter
                display_results = []
                for row in results:
                    xml_content = row.pop("SDMPackageDigest", "")
                    if xml_content:
                        info = CMService.parse_sdm_package_digest(str(xml_content))
                        row["Technology"] = info.get("Technology", "")
                        row["InstallCommand"] = info.get("InstallCommand", "")
                        row["ContentLocation"] = info.get("ContentLocation", "")
                        row["DetectionType"] = info.get("DetectionType", "")
                    else:
                        row["Technology"] = ""
                        row["InstallCommand"] = ""
                        row["ContentLocation"] = ""
                        row["DetectionType"] = ""

                    # Apply post-query filters
                    if self._technology and self._technology.lower() not in row["Technology"].lower():
                        continue
                    if self._content_path and self._content_path.lower() not in row.get("ContentLocation", "").lower():
                        continue
                    if self._install_command and self._install_command.lower() not in row.get("InstallCommand", "").lower():
                        continue

                    display_results.append(row)

                if display_results:
                    logger.success(f"Found {len(display_results)} deployment type(s)")
                    print(OutputFormatter.convert_list_of_dicts(display_results))
                else:
                    logger.warning("No deployment types match filters")

            except Exception as ex:
                logger.error(f"Failed to query deployment types: {ex}")

        return None
