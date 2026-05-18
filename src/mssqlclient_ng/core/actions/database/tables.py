# /mssqlclient_ng/core/actions/database/tables.py

# Built-in imports
from typing import Optional, List, Dict

# Third party imports
from loguru import logger

# Local library imports
from ..base import Arg, BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


@ActionFactory.register(
    "tables",
    "List tables in a database with schemas and permissions",
)
class Tables(BaseAction):
    """
    Retrieves all tables and views from a database with row counts.

    Supports filtering by name pattern, column name, row count, and
    optional permission/column display.

    Usage:
        tables [database] [-n name] [-C] [-c column] [-r] [-p]

    Options:
        database           Target database (default: current)
        -n, --name         Filter tables by name pattern (supports %)
        -C, --columns      Show column names with types
        -c, --column        Filter tables containing a column name pattern
        -r, --rows         Filter out tables with 0 rows
        -p, --permissions  Show permissions (slower)
    """

    _database = Arg(position=0, long_name="database", default="", description="Target database (default: current)")
    _name_filter = Arg(short_name="n", long_name="name", default="", description="Filter tables by name pattern")
    _show_columns = Arg(short_name="C", long_name="columns", toggle=True, description="Show column names with types")
    _column_filter = Arg(short_name="c", long_name="column", default="", description="Filter tables containing a column name pattern")
    _with_rows = Arg(short_name="r", long_name="rows", toggle=True, description="Filter out tables with 0 rows")
    _show_permissions = Arg(short_name="p", long_name="permissions", toggle=True, description="Show permissions (slower)")

    def __init__(self):
        super().__init__()
        self._database: Optional[str] = None
        self._name_filter: str = ""
        self._show_columns: bool = False
        self._column_filter: str = ""
        self._with_rows: bool = False
        self._show_permissions: bool = False

    def validate_arguments(self, additional_arguments: str = "") -> None:
        if not additional_arguments or not additional_arguments.strip():
            return

        named_args, positional_args = self._parse_action_arguments(
            additional_arguments.strip()
        )

        # Positional: database name
        if positional_args:
            self._database = positional_args[0]

        # Named arguments
        self._database = named_args.get(
            "database", named_args.get("db", self._database)
        )
        self._name_filter = named_args.get("name", named_args.get("n", ""))
        self._column_filter = named_args.get("column", named_args.get("c", ""))
        self._with_rows = "rows" in named_args or "r" in named_args
        self._show_permissions = "permissions" in named_args or "p" in named_args
        self._show_columns = "columns" in named_args or "C" in named_args

        # If a column filter is provided, automatically show columns
        if self._column_filter:
            self._show_columns = True

    def execute(self, database_context: DatabaseContext) -> Optional[List[Dict]]:
        target_database = (
            self._database
            if self._database
            else database_context.query_service.execution_database
        )

        parts = []
        if self._name_filter:
            parts.append(f"name: {self._name_filter}")
        if self._show_columns:
            parts.append("with columns")
        if self._column_filter:
            parts.append(f"column containing '{self._column_filter}'")
        if self._with_rows:
            parts.append("rows > 0")
        if self._show_permissions:
            parts.append("with permissions")
        filter_msg = f" ({', '.join(parts)})" if parts else ""
        logger.info(f"Retrieving tables from [{target_database}]{filter_msg}")

        use_statement = f"USE [{self._database}];" if self._database else ""

        # Build WHERE clause
        where_parts = ["t.type IN ('U', 'V')"]
        if self._name_filter:
            safe_name = self._name_filter.replace("'", "''")
            where_parts.append(f"t.name LIKE '%{safe_name}%'")
        if self._column_filter:
            safe_col = self._column_filter.replace("'", "''")
            where_parts.append(
                f"EXISTS (SELECT 1 FROM sys.columns c WHERE c.object_id = t.object_id AND c.name LIKE '%{safe_col}%')"
            )
        if self._with_rows:
            where_parts.append(
                "(t.type = 'V' OR EXISTS (SELECT 1 FROM sys.partitions p2 WHERE p2.object_id = t.object_id AND p2.index_id IN (0, 1) AND p2.rows > 0))"
            )
        where_clause = " AND ".join(where_parts)

        query = f"""
            {use_statement}
            SELECT
                t.object_id AS ObjectId,
                s.name AS SchemaName,
                t.name AS TableName,
                t.type_desc AS TableType,
                CASE
                    WHEN t.type = 'U' THEN CAST(COALESCE(pr.Rows, 0) AS VARCHAR(20))
                    ELSE 'N/A'
                END AS Rows
            FROM
                sys.objects t
            JOIN
                sys.schemas s ON t.schema_id = s.schema_id
            OUTER APPLY (
                SELECT SUM(p.rows) AS Rows
                FROM sys.partitions p
                WHERE p.object_id = t.object_id AND p.index_id IN (0, 1)
            ) pr
            WHERE {where_clause}
            ORDER BY
                CASE WHEN t.type = 'U' THEN COALESCE(pr.Rows, 0) ELSE -1 END DESC,
                SchemaName, TableName;
        """

        tables = database_context.query_service.execute_table(query)

        if not tables:
            logger.warning("No tables found.")
            return tables

        # Collect object IDs for batch queries
        object_ids = [str(t["ObjectId"]) for t in tables]
        object_id_filter = ",".join(object_ids)

        # Optionally get columns
        if self._show_columns:
            columns_query = f"""
                {use_statement}
                SELECT
                    o.object_id,
                    c.name AS column_name,
                    TYPE_NAME(c.user_type_id) AS data_type
                FROM sys.columns c
                INNER JOIN sys.objects o ON c.object_id = o.object_id
                WHERE o.object_id IN ({object_id_filter})
                ORDER BY o.object_id, c.column_id;
            """
            columns_result = database_context.query_service.execute_table(columns_query)
            columns_dict: Dict[str, List[str]] = {}
            for col_row in columns_result:
                key = str(col_row["object_id"])
                col_info = f"{col_row['column_name']} ({col_row['data_type']})"
                columns_dict.setdefault(key, []).append(col_info)

        # Optionally get permissions
        if self._show_permissions:
            perms_query = f"""
                {use_statement}
                SELECT
                    o.object_id,
                    p.permission_name
                FROM sys.objects o
                CROSS APPLY fn_my_permissions(
                    QUOTENAME(SCHEMA_NAME(o.schema_id)) + '.' + QUOTENAME(o.name), 'OBJECT'
                ) p
                WHERE o.object_id IN ({object_id_filter});
            """
            perms_result = database_context.query_service.execute_table(perms_query)
            perms_dict: Dict[str, set] = {}
            for perm_row in perms_result:
                key = str(perm_row["object_id"])
                perms_dict.setdefault(key, set()).add(perm_row["permission_name"])

        # Build enriched output (remove ObjectId)
        enriched = []
        for table in tables:
            obj_id = str(table["ObjectId"])
            row = {
                "SchemaName": table["SchemaName"],
                "TableName": table["TableName"],
                "TableType": table["TableType"],
                "Rows": table["Rows"],
            }
            if self._show_columns:
                row["Columns"] = ", ".join(columns_dict.get(obj_id, []))
            if self._show_permissions:
                row["Permissions"] = ", ".join(sorted(perms_dict.get(obj_id, set())))
            enriched.append(row)

        print(OutputFormatter.convert_list_of_dicts(enriched))
        logger.success(f"Retrieved {len(enriched)} table(s) from [{target_database}]")

        if not self._show_permissions:
            logger.info("Use -p to show permissions")

        return enriched

    def get_arguments(self) -> List[str]:
        return [
            "[database] [-n name] [-C] [-c column] [-r] [-p]",
        ]
