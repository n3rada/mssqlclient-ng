# mssqlclient_ng/core/actions/execution/clr_inspect.py

from loguru import logger

from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter
from ..base import BaseAction, Arg
from ..factory import ActionFactory

@ActionFactory.register(
    "clr-inspect",
    "Show exported procedures and metadata for a named CLR assembly.",
    aliases=["assembly"],
)
class ClrInspect(BaseAction):
    """
    Inspect a specific user-defined CLR assembly in the current database.

    Displays the assembly metadata (CLR name, permission set, dates) and
    the full list of stored procedures or functions it exports, including
    the mapped .NET class and method name for each.
    """

    _assembly_name = Arg(position=0, required=True, description="Assembly name to inspect")

    def execute(self, database_context: DatabaseContext) -> object:
        logger.info(f"Inspecting assembly '{self._assembly_name}'")

        safe_name = self._assembly_name.replace("'", "''")

        meta = database_context.query_service.execute_table(f"""
SELECT name, clr_name, permission_set_desc, create_date, modify_date
FROM sys.assemblies
WHERE is_user_defined = 1 AND name = '{safe_name}'
""")

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

        modules = database_context.query_service.execute_table(f"""
SELECT
    o.name              AS [Object],
    o.type_desc         AS [type],
    am.assembly_class   AS [Class],
    am.assembly_method  AS [Method]
FROM sys.assembly_modules am
JOIN sys.objects o ON am.object_id = o.object_id
JOIN sys.assemblies a ON am.assembly_id = a.assembly_id
WHERE a.name = '{safe_name}'
""")

        if not modules:
            logger.warning(
                "No stored procedures or functions registered for this assembly"
            )
            return meta

        print(OutputFormatter.convert_list_of_dicts(modules))

        return modules
