# mssqlclient_ng/core/actions/configmgr/cm_collection.py

"""Display comprehensive info about a specific ConfigMgr collection."""

from typing import Optional

from loguru import logger

from .cm_base import CMBaseAction
from ..base import Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter


@ActionFactory.register(
    "cm-collection", "Display details of a specific ConfigMgr collection"
)
class CMCollection(CMBaseAction):
    """
    Display comprehensive information about a specific ConfigMgr collection including all members.
    Supports lookup by Collection ID or name pattern.
    """

    _collection_id = Arg(position=0, default="", description="Collection ID")
    _collection_name = Arg(short_name="n", long_name="name", default="", description="Collection name pattern")

    def validate_arguments(self, additional_arguments: str = "") -> None:
        super().validate_arguments(additional_arguments)
        if not self._collection_id and not self._collection_name:
            raise ValueError("Collection ID or --name is required.")

    def execute(self, database_context: DatabaseContext) -> Optional[list]:
        search_msg = (
            f"ID: {self._collection_id}"
            if self._collection_id
            else f"name: {self._collection_name}"
        )
        logger.info(f"Retrieving collection information for {search_msg}")

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)

            if self._collection_id:
                where = f"c.CollectionID = '{self._collection_id}'"
            else:
                where = f"c.Name LIKE '%{self._collection_name}%'"

            query = f"""
SELECT
    c.CollectionID, c.Name, c.Comment, c.CollectionType,
    CASE c.CollectionType WHEN 0 THEN 'Other' WHEN 1 THEN 'User' WHEN 2 THEN 'Device' ELSE 'Unknown' END AS TypeName,
    c.MemberCount, c.LastRefreshTime, c.LastMemberChangeTime
FROM [{db}].dbo.v_Collection c
WHERE {where};"""

            try:
                results = database_context.query_service.execute(query)
                if not results:
                    continue

                if len(results) > 1:
                    logger.warning(f"Multiple collections match:")
                    print(OutputFormatter.convert_list_of_dicts(results))
                    logger.info("Use the exact CollectionID to view details.")
                    return None

                row = results[0]
                collection_id = row["CollectionID"]
                logger.info(f"Collection: {row['Name']} ({collection_id})")

                # Get members
                members_query = f"""
SELECT Name, Domain, ResourceID, ResourceType, SiteCode
FROM [{db}].dbo.v_CM_RES_COLL_{collection_id}
ORDER BY Name;"""

                try:
                    members = database_context.query_service.execute(members_query)
                    if members:
                        logger.success(f"Members ({len(members)})")
                        print(OutputFormatter.convert_list_of_dicts(members))
                    else:
                        logger.info("No members in this collection")
                except Exception as ex:
                    logger.debug(f"Could not query members: {ex}")

                return results

            except Exception as ex:
                logger.error(f"Failed to query collection: {ex}")

        logger.warning("Collection not found")
        return None
