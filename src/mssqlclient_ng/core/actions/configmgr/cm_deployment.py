# mssqlclient_ng/core/actions/configmgr/cm_deployment.py

"""Display detailed info about a specific ConfigMgr deployment."""

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter

@ActionFactory.register(
    "cm-deployment",
    "Display detailed information about a specific deployment including rerun behavior and device status.",
    aliases=["cm-assignment"],
)
class CMDeployment(CMBaseAction):
    """
    Display detailed information about a specific ConfigMgr deployment/assignment.
    Shows deployment settings, targeted collection, schedule, and execution behavior.
    """

    _assignment_id = Arg(position=0, required=True, description="Assignment ID")

    def execute(self, database_context: DatabaseContext) -> list | None:
        logger.info(f"Retrieving assignment details for: {self._assignment_id}")

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)

            query = f"""
SELECT
    a.AssignmentID,
    a.AssignmentName,
    a.CollectionID,
    c.Name AS CollectionName,
    c.MemberCount,
    CASE a.AssignmentType
        WHEN 1 THEN 'Application'
        WHEN 2 THEN 'Configuration Item'
        WHEN 5 THEN 'Software Update'
        WHEN 6 THEN 'Baseline'
        ELSE 'Other (' + CAST(a.AssignmentType AS VARCHAR) + ')'
    END AS AssignmentType,
    CASE a.DesiredConfigType
        WHEN 1 THEN 'Install'
        WHEN 2 THEN 'Uninstall'
        ELSE CAST(a.DesiredConfigType AS VARCHAR)
    END AS Intent,
    a.EnforcementDeadline,
    a.StartTime,
    a.ExpirationTime,
    a.CreationTime,
    a.LastModificationTime,
    a.LastModifiedBy,
    CASE a.OverrideServiceWindows
        WHEN 0 THEN 'No' WHEN 1 THEN 'Yes'
        ELSE CAST(a.OverrideServiceWindows AS VARCHAR)
    END AS OverrideMaintenanceWindow,
    a.AssignmentEnabled,
    a.SourceSite
FROM [{db}].dbo.v_CIAssignment a
LEFT JOIN [{db}].dbo.v_Collection c ON a.CollectionID = c.CollectionID
WHERE a.AssignmentID = {self._assignment_id};"""

            try:
                results = database_context.query_service.execute(query)
                if results:
                    logger.success(
                        f"Deployment found: {results[0].get('AssignmentName', 'Unknown')}"
                    )
                    print(OutputFormatter.convert_list_of_dicts(results))
                    return results
            except Exception as ex:
                logger.debug(f"Query failed on {db}: {ex}")

        logger.warning(f"Assignment ID {self._assignment_id} not found")
        return None
