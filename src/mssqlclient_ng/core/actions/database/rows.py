# mssqlclient_ng/core/actions/database/rows.py

# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from ..base import Arg, BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


@ActionFactory.register("rows", "Retrieve all rows from a specified table")
class Rows(BaseAction):
    """
    Retrieves all rows from a specified table.

    Supports multiple formats:
    - table: Uses current database and dbo schema (default)
    - schema.table: Uses current database with specified schema
    - database.schema.table: Fully qualified table name

    Optional arguments:
    - -l/--limit: Maximum number of rows to retrieve (default: 25, 0 = unlimited)
    """

    _fqtn = Arg(position=0, required=True, description="Table name or FQTN (database.schema.table)")
    _limit = Arg(short_name="l", long_name="limit", default=25, description="Max rows to retrieve (0 = unlimited)")

    def __init__(self):
        super().__init__()
        self._fqtn: str = ""
        self._database: Optional[str] = None
        self._schema: str = "dbo"  # Default to dbo schema
        self._table: str = ""
        self._limit: int = 25  # Default: 25 rows (0 = unlimited)

    def validate_arguments(self, additional_arguments: str = "") -> None:
        """
        Validates the table name argument and optional flags.

        Args:
            additional_arguments: Table name or FQTN with optional --top/-t flags

        Raises:
            ValueError: If the table name format is invalid.
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError(
                "Rows action requires at least a Table Name as an argument or a "
                "Fully Qualified Table Name (FQTN) in the format 'database.schema.table'."
            )

        # Parse named and positional arguments
        named_args, positional_args = self._parse_action_arguments(
            additional_arguments.strip()
        )

        # Get the FQTN from the first positional argument
        if not positional_args:
            raise ValueError(
                "Rows action requires at least a Table Name as an argument or a "
                "Fully Qualified Table Name (FQTN) in the format 'database.schema.table'."
            )

        self._fqtn = positional_args[0]
        parts = self._fqtn.split(".")

        if len(parts) == 3:  # Format: database.schema.table
            self._database = parts[0]
            self._schema = parts[1]
            self._table = parts[2]
        elif len(parts) == 2:  # Format: schema.table
            self._database = None  # Use the current database
            self._schema = parts[0]
            self._table = parts[1]
        elif len(parts) == 1:  # Format: table
            self._database = None  # Use the current database
            self._schema = "dbo"  # Default to dbo schema
            self._table = parts[0]
        else:
            raise ValueError(
                "Invalid format. Use: [table], [schema.table], or [database.schema.table]."
            )

        if not self._table:
            raise ValueError("Table name cannot be empty.")

        # Parse limit argument (supports --limit, -l, --top, -t for backward compat)
        limit_str = named_args.get(
            "limit", named_args.get("l", named_args.get("top", named_args.get("t")))
        )
        if limit_str is not None:
            try:
                self._limit = int(limit_str)
                if self._limit < 0:
                    raise ValueError(
                        f"Invalid limit value: {self._limit}. Limit must be a non-negative integer."
                    )
            except ValueError as e:
                if "invalid literal" in str(e).lower():
                    raise ValueError(
                        f"Invalid limit value: '{limit_str}'. Must be an integer."
                    )
                raise

    def execute(self, database_context: DatabaseContext) -> Optional[list[dict]]:
        """
        Executes the rows retrieval query.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            List of rows from the table.
        """
        # Use the execution database if no database is specified
        if not self._database:
            self._database = database_context.query_service.execution_database

        # Build the target table name with all three parts
        target_table = f"[{self._database}].[{self._schema}].[{self._table}]"

        logger.info(f"Retrieving rows from {target_table}")

        # Get approximate row count from sys.partitions (fast metadata lookup)
        schema_filter = self._schema if self._schema else "dbo"
        count_query = f"""
SELECT SUM(p.rows)
FROM [{self._database}].sys.partitions p
JOIN [{self._database}].sys.objects o ON p.object_id = o.object_id
JOIN [{self._database}].sys.schemas s ON o.schema_id = s.schema_id
WHERE o.name = '{self._table.replace("'", "''")}'
  AND s.name = '{schema_filter.replace("'", "''")}'
  AND p.index_id IN (0, 1);"""

        total_rows = 0
        try:
            count_result = database_context.query_service.execute_scalar(count_query)
            if count_result is not None:
                total_rows = int(count_result)
        except Exception:
            logger.warning("Could not retrieve row count metadata")

        # Intelligently decide whether to use TOP
        use_top = self._limit > 0 and self._limit < total_rows

        if self._limit == 0:
            if total_rows > 0:
                logger.info(f"Retrieving all {total_rows:,} rows")
        elif total_rows == 0:
            logger.info(f"Limiting to {self._limit} row(s)")
            logger.info("Use --limit 0 to retrieve all rows")
        elif use_top:
            logger.info(f"Limiting to {self._limit} row(s) over {total_rows:,}")
            logger.info("Use --limit 0 to retrieve all rows")
        else:
            logger.info(
                f"Retrieving all {total_rows:,} rows (limit {self._limit} exceeds total)"
            )

        top_clause = f"TOP ({self._limit}) " if use_top else ""

        try:
            # Optimistic: try SELECT * first
            query = f"SELECT {top_clause}* FROM {target_table};"
            rows = database_context.query_service.execute_table(query)
        except Exception as e:
            # Check for error 9514: XML data type not supported in distributed queries
            if "9514" in str(e):
                logger.warning(
                    "XML columns detected - retrying with CAST to NVARCHAR(MAX)"
                )
                column_list = self._build_column_list_with_xml_cast(
                    database_context, self._database, schema_filter, self._table
                )
                query = f"SELECT {top_clause}{column_list} FROM {target_table};"
                rows = database_context.query_service.execute_table(query)
            else:
                raise

        if not rows:
            logger.warning("No rows found in the table")
            return []

        print(OutputFormatter.convert_list_of_dicts(rows))
        logger.success(f"Extracted {len(rows)} row(s)")

        return rows

    @staticmethod
    def _build_column_list_with_xml_cast(
        database_context, database: str, schema: str, table: str
    ) -> str:
        """Build column list casting XML columns to NVARCHAR(MAX) for distributed query compat."""
        column_query = f"""
SELECT c.name AS ColumnName, t.name AS TypeName
FROM [{database}].sys.columns c
JOIN [{database}].sys.types t ON c.user_type_id = t.user_type_id
JOIN [{database}].sys.objects o ON c.object_id = o.object_id
JOIN [{database}].sys.schemas s ON o.schema_id = s.schema_id
WHERE o.name = '{table.replace("'", "''")}'
  AND s.name = '{schema.replace("'", "''")}'
ORDER BY c.column_id;"""

        columns = database_context.query_service.execute_table(column_query)
        if not columns:
            raise RuntimeError(
                f"Could not retrieve column information for {database}.{schema}.{table}"
            )

        expressions = []
        for col in columns:
            col_name = col["ColumnName"]
            type_name = str(col["TypeName"]).lower()
            if type_name == "xml":
                expressions.append(
                    f"CAST([{col_name}] AS NVARCHAR(MAX)) AS [{col_name}]"
                )
            else:
                expressions.append(f"[{col_name}]")
        return ", ".join(expressions)

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            List containing the table name argument description.
        """
        return ["table|schema.table|database.schema.table [-l/--limit N]"]
