# mssqlclient_ng/core/actions/configmgr/cm_deployment_type.py

"""Display detailed info about a specific ConfigMgr deployment type."""

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter

@ActionFactory.register(
    "cm-dt",
    "Display detailed technical information about a deployment type (detection method, install commands, requirements, XML).",
    aliases=["cm-deploymenttype"],
)
class CMDeploymentType(CMBaseAction):

    _ci_id = Arg(position=0, required=True, description="Deployment type CI_ID integer")

    def __init__(self):
        super().__init__()
        self._ci_id: str = ""

    def validate_arguments(self, additional_arguments: str = "") -> None:
        named, positional = self._parse_action_arguments(additional_arguments)
        self._ci_id = self.get_positional_argument(positional, 0, "")
        if not self._ci_id:
            raise ValueError("CI_ID is required. Usage: cm-dt <CI_ID>")

    def execute(self, database_context: DatabaseContext) -> list | None:
        logger.info(f"Retrieving deployment type details for CI_ID: {self._ci_id}")

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            query = f"""
SELECT
    ci.CI_ID,
    COALESCE(lp.DisplayName, ci.ModelName) AS Title,
    ci.ModelName,
    ci.CI_UniqueID,
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
WHERE ci.CI_ID = {self._ci_id};"""

            try:
                results = database_context.query_service.execute(query)
                if not results:
                    continue

                row = results[0]
                xml_content = row.pop("SDMPackageDigest", "")
                if xml_content:
                    info = CMService.parse_sdm_package_digest(
                        str(xml_content), detailed=True
                    )
                    for k, v in info.items():
                        row[k] = v

                logger.success(f"Deployment type: {row.get('Title', 'Unknown')}")
                print(OutputFormatter.convert_list_of_dicts([row]))
                return results

            except Exception as ex:
                logger.debug(f"Query failed on {db}: {ex}")

        logger.warning(f"Deployment type CI_ID {self._ci_id} not found")
        return None
