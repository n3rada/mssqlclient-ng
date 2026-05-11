# mssqlclient_ng/core/actions/configmgr/cm_collections.py

"""Enumerate ConfigMgr collections."""

from typing import Optional

from loguru import logger

from .cm_base import CMBaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter


@ActionFactory.register(
    "cm-collections", "Enumerate ConfigMgr collections with member counts"
)
class CMCollections(CMBaseAction):
    """
    Enumerate ConfigMgr collections with member counts, types, and properties.
    Filter by collection ID, name, or type (user/device).
    """


    def __init__(self):
        super().__init__()
        self._collection_id: str = ""
        self._name_filter: str = ""
        self._collection_type: str = ""
        self._limit: int = 25
        self._with_members: bool = False

    def validate_arguments(self, additional_arguments: str = "", argument_list=None) -> None:
        named, positional = self._parse_action_arguments(additional_arguments)
        self._collection_id = self.get_positional_argument(
            positional, 0, ""
        ) or named.get("collection-id", "")
        self._name_filter = named.get("name", named.get("n", ""))
        self._collection_type = named.get("type", named.get("t", ""))
        self._limit = int(named.get("limit", "25"))
        self._with_members = "with-members" in named

    def execute(self, database_context: DatabaseContext) -> Optional[list]:
        filters = []
        if self._collection_id:
            filters.append(f"ID: {self._collection_id}")
        if self._name_filter:
            filters.append(f"name: {self._name_filter}")
        if self._collection_type:
            filters.append(f"type: {self._collection_type}")
        logger.info(
            f"Enumerating ConfigMgr collections{' (' + ', '.join(filters) + ')' if filters else ''}"
        )

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(f"ConfigMgr database: {db} (Site Code: {site_code})")

            where = "WHERE 1=1"
            if self._collection_id:
                where += f" AND CollectionID = '{self._collection_id}'"
            if self._name_filter:
                where += f" AND (Name LIKE '%{self._name_filter}%' OR Comment LIKE '%{self._name_filter}%')"
            if self._collection_type:
                type_map = {
                    "other": "0",
                    "user": "1",
                    "device": "2",
                    "0": "0",
                    "1": "1",
                    "2": "2",
                }
                ct = type_map.get(self._collection_type.lower(), "")
                if ct:
                    where += f" AND CollectionType = {ct}"
            if self._with_members:
                where += " AND MemberCount > 0"

            top = self._build_top_clause(self._limit)

            query = f"""
SELECT {top}
    CollectionID, Name, Comment,
    CASE CollectionType WHEN 0 THEN 'Other' WHEN 1 THEN 'User' WHEN 2 THEN 'Device' ELSE 'Unknown' END AS CollectionType,
    MemberCount, LastChangeTime
FROM [{db}].dbo.v_Collection
{where}
ORDER BY MemberCount DESC;"""

            try:
                results = database_context.query_service.execute(query)
                if results:
                    logger.success(f"Found {len(results)} collection(s)")
                    print(OutputFormatter.convert_list_of_dicts(results))
                else:
                    logger.warning("No collections found")
            except Exception as ex:
                logger.error(f"Failed to enumerate collections: {ex}")

        return None
