# mssqlclient_ng/core/actions/configmgr/cm_health.py

"""ConfigMgr client health diagnostics."""

from typing import Optional

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter


@ActionFactory.register("cm-health", "Display ConfigMgr client health diagnostics")
class CMHealth(CMBaseAction):
    """
    Display ConfigMgr client health diagnostics and communication status.
    Shows check-in times, inventory cycles, health evaluation results.
    """

    _filter = Arg(position=0, short_name="f", long_name="filter", default="", description="Filter by device name")
    _limit = Arg(long_name="limit", default=25, description="Cap result count")

    def validate_arguments(self, additional_arguments: str = "") -> None:
        super().validate_arguments(additional_arguments)
        self._limit = int(self._limit)

    def execute(self, database_context: DatabaseContext) -> Optional[list]:
        filter_msg = f" (filter: {self._filter})" if self._filter else ""
        logger.info(f"Enumerating ConfigMgr client health{filter_msg}")

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(f"ConfigMgr database: {db} (Site Code: {site_code})")

            where = "WHERE 1=1"
            if self._filter:
                where += f" AND sys.Name0 LIKE '%{self._filter}%'"

            top = self._build_top_clause(self._limit)

            query = f"""
SELECT {top}
    sys.ResourceID,
    sys.Name0 AS DeviceName,
    sys.Resource_Domain_OR_Workgr0 AS Domain,
    ch.ClientActiveStatus,
    ch.ClientStateDescription,
    ch.LastActiveTime,
    ch.LastOnline,
    ch.LastDDR,
    ch.LastHW AS LastHardwareScan,
    ch.LastSW AS LastSoftwareScan,
    ch.LastPolicyRequest,
    ch.LastHealthEvaluation,
    ch.LastHealthEvaluationResult,
    ch.LastEvaluationHealthy
FROM [{db}].dbo.v_R_System sys
LEFT JOIN [{db}].dbo.v_CH_ClientSummary ch ON sys.ResourceID = ch.ResourceID
{where}
ORDER BY ch.LastActiveTime DESC;"""

            try:
                results = database_context.query_service.execute(query)
                if results:
                    logger.success(f"Found {len(results)} device(s)")
                    print(OutputFormatter.convert_list_of_dicts(results))
                else:
                    logger.warning("No health data found")
            except Exception as ex:
                logger.error(f"Failed to query health data: {ex}")

        return None
