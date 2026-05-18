# mssqlclient_ng/core/actions/configmgr/cm_applications.py

"""Enumerate ConfigMgr applications."""

from typing import Optional

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter


@ActionFactory.register(
    "cm-apps",
    "Enumerate ConfigMgr applications with deployment types",
    aliases=["cm-applications"],
)
class CMApplications(CMBaseAction):
    """
    Enumerate ConfigMgr applications with deployment types, installation commands, and detection methods.
    Applications are the modern deployment model (since ConfigMgr 2012).
    """

    _display_name = Arg(short_name="n", long_name="displayname", default="", description="Filter by display name")
    _model_name = Arg(short_name="m", long_name="modelname", default="", description="Filter by model name")
    _limit = Arg(long_name="limit", default=25, description="Cap result count")

    def __init__(self):
        super().__init__()
        self._display_name: str = ""
        self._model_name: str = ""
        self._limit: int = 25

    def validate_arguments(self, additional_arguments: str = "") -> None:
        named, positional = self._parse_action_arguments(additional_arguments)
        self._display_name = named.get("displayname", named.get("n", ""))
        self._model_name = named.get("modelname", named.get("m", ""))
        self._limit = int(named.get("limit", "25"))

    def execute(self, database_context: DatabaseContext) -> Optional[list]:
        filters = []
        if self._display_name:
            filters.append(f"displayname: {self._display_name}")
        if self._model_name:
            filters.append(f"modelname: {self._model_name}")
        logger.info(
            f"Enumerating ConfigMgr applications{' (' + ', '.join(filters) + ')' if filters else ''}"
        )

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(f"ConfigMgr database: {db} (Site Code: {site_code})")

            where = "WHERE ci.CIType_ID = 10"  # Application CI
            if self._display_name:
                where += f" AND lp.DisplayName LIKE '%{self._display_name}%'"
            if self._model_name:
                where += f" AND ci.ModelName LIKE '%{self._model_name}%'"

            top = self._build_top_clause(self._limit)

            query = f"""
SELECT {top}
    ci.CI_ID,
    COALESCE(lp.DisplayName, ci.ModelName) AS DisplayName,
    ci.ModelName,
    ci.CI_UniqueID,
    ci.CIVersion,
    ci.IsDeployed,
    ci.IsEnabled,
    ci.IsExpired,
    ci.IsSuperseded,
    ci.ContentSourcePath,
    ci.CreatedBy,
    ci.DateCreated,
    ci.LastModifiedBy,
    ci.DateLastModified
FROM [{db}].dbo.v_ConfigurationItems ci
LEFT JOIN (
    SELECT CI_ID, MIN(DisplayName) AS DisplayName
    FROM [{db}].dbo.v_LocalizedCIProperties
    WHERE DisplayName IS NOT NULL AND DisplayName != ''
    GROUP BY CI_ID
) lp ON ci.CI_ID = lp.CI_ID
{where}
ORDER BY ci.IsDeployed DESC, ci.DateCreated DESC;"""

            try:
                results = database_context.query_service.execute(query)
                if results:
                    logger.success(f"Found {len(results)} application(s)")
                    print(OutputFormatter.convert_list_of_dicts(results))
                else:
                    logger.warning("No applications found")
            except Exception as ex:
                logger.error(f"Failed to enumerate applications: {ex}")

        return None
