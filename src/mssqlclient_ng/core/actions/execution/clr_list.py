# mssqlclient_ng/core/actions/execution/clr_list.py

from typing import Optional

from loguru import logger

from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter
from ..base import BaseAction, Arg
from ..factory import ActionFactory


@ActionFactory.register(
    "clr-list",
    "List user-defined CLR assemblies and their exported procedures",
    aliases=["assemblies"],
)
class ClrList(BaseAction):
    """
    List user-defined CLR assemblies registered in the current database.

    Without arguments, enumerates all non-system assemblies with their
    permission set, creation date, and number of exported procedures.

    With an assembly name, shows the stored procedures and functions
    it exports along with their mapped .NET class and method.
    """

    _assembly_name: Optional[str] = Arg(position=0, default=None, description="Assembly name to inspect (omit to list all)")  # type: ignore[assignment]

    def execute(self, database_context: DatabaseContext) -> object:
        if not self._assembly_name:
            return self._list_assemblies(database_context)
        return self._show_modules(database_context)

    def _list_assemblies(self, database_context: DatabaseContext) -> object:
        logger.info("Enumerating user-defined CLR assemblies")

        query = """
SELECT
    a.name                  AS [Name],
    a.clr_name              AS [CLR Name],
    a.permission_set_desc   AS [Permission Set],
    a.create_date           AS [Created],
    a.modify_date           AS [Modified],
    COUNT(am.object_id)     AS [Procedures]
FROM sys.assemblies a
LEFT JOIN sys.assembly_modules am ON a.assembly_id = am.assembly_id
WHERE a.is_user_defined = 1
GROUP BY a.name, a.clr_name, a.permission_set_desc, a.create_date, a.modify_date
ORDER BY a.create_date DESC
"""

        rows = database_context.query_service.execute_query(query)

        if not rows:
            logger.warning(
                "No user-defined CLR assemblies found in the current database"
            )
            return rows

        print(OutputFormatter.convert_list_of_dicts(rows))
        count = len(rows)
        label = "assembly" if count == 1 else "assemblies"
        logger.success(f"Found {count} user-defined {label}")

        return rows

    def _show_modules(self, database_context: DatabaseContext) -> object:
        logger.info(f"Inspecting assembly '{self._assembly_name}'")

        safe_name = self._assembly_name.replace("'", "''")

        check_query = f"""
SELECT name, clr_name, permission_set_desc, create_date, modify_date
FROM sys.assemblies
WHERE is_user_defined = 1 AND name = '{safe_name}'
"""

        meta = database_context.query_service.execute_query(check_query)

        if not meta:
            logger.error(
                f"Assembly '{self._assembly_name}' not found or is not user-defined"
            )
            return None

        row = meta[0]
        logger.info(f"CLR name    : {row.get('clr_name', '')}")
        logger.info(f"Permission  : {row.get('permission_set_desc', '')}")
        logger.info(f"Created     : {row.get('create_date', '')}")
        logger.info(f"Modified    : {row.get('modify_date', '')}")

        modules_query = f"""
SELECT
    o.name              AS [Object],
    o.type_desc         AS [Type],
    am.assembly_class   AS [Class],
    am.assembly_method  AS [Method]
FROM sys.assembly_modules am
JOIN sys.objects o ON am.object_id = o.object_id
JOIN sys.assemblies a ON am.assembly_id = a.assembly_id
WHERE a.name = '{safe_name}'
"""

        modules = database_context.query_service.execute_query(modules_query)

        if not modules:
            logger.warning(
                "No stored procedures or functions registered for this assembly"
            )
            return meta

        print(OutputFormatter.convert_list_of_dicts(modules))

        return modules
