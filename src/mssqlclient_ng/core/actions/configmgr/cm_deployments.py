# mssqlclient_ng/core/actions/configmgr/cm_deployments.py

"""Enumerate ConfigMgr deployments."""

from typing import Optional

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter


@ActionFactory.register(
    "cm-deployments",
    "Enumerate ConfigMgr deployments",
    aliases=["cm-assignments"],
)
class CMDeployments(CMBaseAction):
    """
    Enumerate ConfigMgr deployments showing what content is being pushed to which collections.
    Filter by name, collection, type, or intent.
    """

    _name = Arg(short_name="n", long_name="name", default="", description="Filter by software name")
    _collection = Arg(short_name="c", long_name="collection", default="", description="Filter by collection name or ID")
    _feature_type = Arg(short_name="t", long_name="type", default="", description="Filter by feature type (app/package/ts/...)")
    _intent = Arg(short_name="i", long_name="intent", default="", description="Filter by intent (required/available)")
    _limit = Arg(long_name="limit", default=25, description="Cap result count")

    def __init__(self):
        super().__init__()
        self._name: str = ""
        self._collection: str = ""
        self._feature_type: str = ""
        self._intent: str = ""
        self._limit: int = 25

    def validate_arguments(self, additional_arguments: str = "") -> None:
        named, positional = self._parse_action_arguments(additional_arguments)
        self._name = named.get("name", named.get("n", ""))
        self._collection = named.get("collection", named.get("c", ""))
        self._feature_type = named.get("type", named.get("t", ""))
        self._intent = named.get("intent", named.get("i", ""))
        self._limit = int(named.get("limit", "25"))

    def execute(self, database_context: DatabaseContext) -> Optional[list]:
        filters = []
        if self._name:
            filters.append(f"name: {self._name}")
        if self._collection:
            filters.append(f"collection: {self._collection}")
        if self._feature_type:
            filters.append(f"type: {self._feature_type}")
        if self._intent:
            filters.append(f"intent: {self._intent}")
        logger.info(
            f"Enumerating ConfigMgr deployments{' (' + ', '.join(filters) + ')' if filters else ''}"
        )

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(f"ConfigMgr database: {db} (Site Code: {site_code})")

            where = "WHERE 1=1"
            if self._name:
                where += f" AND ds.SoftwareName LIKE '%{self._name}%'"
            if self._collection:
                where += f" AND (c.Name LIKE '%{self._collection}%' OR ds.CollectionID LIKE '%{self._collection}%')"
            if self._feature_type:
                type_map = {
                    "application": 1,
                    "app": 1,
                    "program": 2,
                    "package": 2,
                    "script": 4,
                    "task-sequence": 7,
                    "ts": 7,
                }
                ft_val = type_map.get(self._feature_type.lower())
                if ft_val:
                    where += f" AND ds.FeatureType = {ft_val}"
            if self._intent:
                intent_map = {"required": 1, "available": 2}
                i_val = intent_map.get(self._intent.lower())
                if i_val:
                    where += f" AND ds.DeploymentIntent = {i_val}"

            top = self._build_top_clause(self._limit)

            query = f"""
SELECT {top}
    ds.AssignmentID,
    ds.SoftwareName,
    ds.CollectionID,
    c.Name AS CollectionName,
    c.MemberCount,
    ds.FeatureType,
    ds.DeploymentIntent,
    ds.StartTime,
    ds.EnforcementDeadline,
    ds.NumberSuccess,
    ds.NumberInProgress,
    ds.NumberErrors
FROM [{db}].dbo.v_DeploymentSummary ds
LEFT JOIN [{db}].dbo.v_Collection c ON ds.CollectionID = c.CollectionID
{where}
ORDER BY ds.StartTime DESC;"""

            try:
                results = database_context.query_service.execute(query)
                if results:
                    # Decode feature types and intents
                    for row in results:
                        if "FeatureType" in row:
                            row["FeatureType"] = CMService.decode_feature_type(
                                row["FeatureType"]
                            )
                        if "DeploymentIntent" in row:
                            row["DeploymentIntent"] = (
                                CMService.decode_deployment_intent(
                                    row["DeploymentIntent"]
                                )
                            )
                    logger.success(f"Found {len(results)} deployment(s)")
                    print(OutputFormatter.convert_list_of_dicts(results))
                else:
                    logger.warning("No deployments found")
            except Exception as ex:
                logger.error(f"Failed to enumerate deployments: {ex}")

        return None
