# mssqlclient_ng/core/actions/configmgr/cm_log_trace.py

"""Trace a deployment type GUID to its assignments and collections."""

import re
from typing import Optional

from loguru import logger

from .cm_base import CMBaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatter import OutputFormatter


@ActionFactory.register("cm-trace", "Trace a deployment type GUID to assignments and collections")
class CMLogTrace(CMBaseAction):
    """
    Trace a deployment type GUID from ConfigMgr client logs back to its assignments,
    collections, and deployment settings.

    Accepted GUID formats:
    - Full: "ScopeId_xxx/DeploymentType_xxx"
    - Partial: "DeploymentType_xxx"
    - GUID only: "xxx-xxx-xxx-xxx-xxx"
    """


    def __init__(self):
        super().__init__()
        self._guid: str = ""

    def validate_arguments(self, additional_arguments: str = "", argument_list=None) -> None:
        named, positional = self._parse_action_arguments(additional_arguments)
        self._guid = self.get_positional_argument(positional, 0, "")
        if not self._guid:
            raise ValueError(
                "Deployment Type GUID is required. "
                "Usage: cm-trace <GUID>"
            )

        self._guid = self._guid.strip()

        # Normalize GUID format
        if "ScopeId_" in self._guid:
            pass  # Full identifier, keep as-is
        elif self._guid.startswith("DeploymentType_"):
            pass  # Already has prefix
        elif re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", self._guid, re.IGNORECASE):
            self._guid = f"DeploymentType_{self._guid}"

    def execute(self, database_context: DatabaseContext) -> Optional[list]:
        logger.info(f"Tracing deployment type: {self._guid}")

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(f"ConfigMgr database: {db} (Site Code: {site_code})")

            # Step 1: Find the deployment type CI
            dt_query = f"""
SELECT
    ci.CI_ID,
    ci.ModelName,
    ci.CI_UniqueID,
    COALESCE(lp.DisplayName, ci.ModelName) AS Title,
    ci.IsEnabled,
    ci.CIType_ID
FROM [{db}].dbo.v_ConfigurationItems ci
LEFT JOIN (
    SELECT CI_ID, MIN(DisplayName) AS DisplayName
    FROM [{db}].dbo.v_LocalizedCIProperties
    WHERE DisplayName IS NOT NULL AND DisplayName != ''
    GROUP BY CI_ID
) lp ON ci.CI_ID = lp.CI_ID
WHERE ci.CI_UniqueID LIKE '%{self._guid}%';"""

            try:
                dt_results = database_context.query_service.execute(dt_query)
                if not dt_results:
                    logger.warning(f"Deployment type not found for GUID: {self._guid}")
                    continue

                dt = dt_results[0]
                dt_ci_id = dt["CI_ID"]
                logger.success(f"Deployment Type: {dt.get('Title', 'Unknown')} (CI_ID: {dt_ci_id})")

                # Step 2: Find parent application via CI relations
                parent_query = f"""
SELECT
    ci.CI_ID AS AppCI_ID,
    COALESCE(lp.DisplayName, ci.ModelName) AS ApplicationName,
    ci.ModelName
FROM [{db}].dbo.CI_ConfigurationItemRelations rel
JOIN [{db}].dbo.v_ConfigurationItems ci ON rel.FromCI_ID = ci.CI_ID
LEFT JOIN (
    SELECT CI_ID, MIN(DisplayName) AS DisplayName
    FROM [{db}].dbo.v_LocalizedCIProperties
    WHERE DisplayName IS NOT NULL AND DisplayName != ''
    GROUP BY CI_ID
) lp ON ci.CI_ID = lp.CI_ID
WHERE rel.ToCI_ID = {dt_ci_id} AND ci.CIType_ID = 10;"""

                parent_results = database_context.query_service.execute(parent_query)
                if parent_results:
                    app = parent_results[0]
                    app_ci_id = app["AppCI_ID"]
                    logger.success(f"Parent Application: {app.get('ApplicationName', 'Unknown')} (CI_ID: {app_ci_id})")

                    # Step 3: Find assignments for this application
                    assign_query = f"""
SELECT
    a.AssignmentID,
    a.AssignmentName,
    a.CollectionID,
    c.Name AS CollectionName,
    c.MemberCount,
    a.EnforcementDeadline,
    a.StartTime,
    a.CreationTime
FROM [{db}].dbo.v_CIAssignmentToCI atc
JOIN [{db}].dbo.v_CIAssignment a ON atc.AssignmentID = a.AssignmentID
LEFT JOIN [{db}].dbo.v_Collection c ON a.CollectionID = c.CollectionID
WHERE atc.CI_ID = {app_ci_id}
ORDER BY a.CreationTime DESC;"""

                    assignments = database_context.query_service.execute(assign_query)
                    if assignments:
                        logger.success(f"Assignments ({len(assignments)})")
                        print(OutputFormatter.convert_list_of_dicts(assignments))
                    else:
                        logger.warning("No assignments found for this application")
                else:
                    logger.warning("Could not find parent application")

                return dt_results

            except Exception as ex:
                logger.error(f"Failed to trace deployment type: {ex}")

        return None
