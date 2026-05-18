# mssqlclient_ng/core/actions/configmgr/cm_distribution_points.py

"""Enumerate ConfigMgr distribution points."""

from typing import Optional

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter


@ActionFactory.register(
    "cm-dps",
    "Enumerate ConfigMgr distribution points",
    aliases=["cm-distribution-points"],
)
class CMDistributionPoints(CMBaseAction):
    """
    Enumerate ConfigMgr distribution points with content library paths and network shares.
    Distribution points store all deployed content.
    """

    _server = Arg(short_name="s", long_name="server", default="", description="Filter by server name")
    _active_only = Arg(short_name="a", long_name="active", toggle=True, description="Show only active distribution points")
    _limit = Arg(long_name="limit", default=25, description="Cap result count")

    def __init__(self):
        super().__init__()
        self._server: str = ""
        self._active_only: bool = False
        self._limit: int = 25

    def validate_arguments(self, additional_arguments: str = "") -> None:
        named, positional = self._parse_action_arguments(additional_arguments)
        self._server = named.get("server", named.get("s", ""))
        self._active_only = "active" in named or "a" in named
        self._limit = int(named.get("limit", "25"))

    def execute(self, database_context: DatabaseContext) -> Optional[list]:
        logger.info("Enumerating ConfigMgr distribution points")

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(f"ConfigMgr database: {db} (Site Code: {site_code})")

            where = "WHERE 1=1"
            if self._server:
                where += f" AND (ServerName LIKE '%{self._server}%' OR NALPath LIKE '%{self._server}%')"
            if self._active_only:
                where += " AND IsActive = 1"

            top = self._build_top_clause(self._limit)

            # Try views first
            query = f"""
SELECT {top}
    DPID, ServerName, NALPath, ShareName, SMSSiteCode,
    State,
    CASE State
        WHEN 0 THEN 'Not Installed' WHEN 1 THEN 'Installed'
        WHEN 2 THEN 'Installation Failed' WHEN 3 THEN 'Installation Pending'
        ELSE CAST(State AS VARCHAR)
    END AS StateDescription,
    IsActive, IsPXE, IsPullDP, IsMulticast
FROM [{db}].dbo.DistributionPoints
{where}
ORDER BY ServerName;"""

            try:
                results = database_context.query_service.execute(query)
                if results:
                    logger.success(f"Found {len(results)} distribution point(s)")
                    print(OutputFormatter.convert_list_of_dicts(results))
                else:
                    logger.warning("No distribution points found")
            except Exception as ex:
                logger.error(f"Failed to enumerate distribution points: {ex}")

        return results
