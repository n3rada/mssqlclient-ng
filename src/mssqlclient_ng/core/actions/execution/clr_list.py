# mssqlclient_ng/core/actions/execution/clr_list.py

from loguru import logger

from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter
from ..base import BaseAction
from ..factory import ActionFactory

@ActionFactory.register(
    "clr-list",
    "Enumerate user-defined CLR assemblies in the current database.",
    aliases=["assemblies"],
)
class ClrList(BaseAction):
    """
    Enumerate all user-defined CLR assemblies registered in the current database.

    Shows name, CLR name, permission set (SAFE/EXTERNAL/UNSAFE), creation date,
    modification date, and the number of stored procedures each assembly exports.
    """

    def execute(self, database_context: DatabaseContext) -> object:
        logger.info("Enumerating user-defined CLR assemblies")

        query = """
SELECT
    a.name                  AS [Name],
    a.clr_name              AS [CLR Name],
    a.permission_set_desc   AS [Permission set],
    a.create_date           AS [Created],
    a.modify_date           AS [Modified],
    COUNT(am.object_id)     AS [Procedures]
FROM sys.assemblies a
LEFT JOIN sys.assembly_modules am ON a.assembly_id = am.assembly_id
WHERE a.is_user_defined = 1
GROUP BY a.name, a.clr_name, a.permission_set_desc, a.create_date, a.modify_date
ORDER BY a.create_date DESC
"""

        rows = database_context.query_service.execute_table(query)

        if not rows:
            logger.warning(
                "No user-defined CLR assemblies found in the current database"
            )
            return rows

        print(OutputFormatter.convert_list_of_dicts(rows))
        count = len(rows)
        logger.success(
            f"Found {count} user-defined {'assembly' if count == 1 else 'assemblies'}"
        )

        return rows
