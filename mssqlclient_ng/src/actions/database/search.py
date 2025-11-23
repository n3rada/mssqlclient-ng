# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from mssqlclient_ng.src.actions.base import BaseAction
from mssqlclient_ng.src.actions.factory import ActionFactory
from mssqlclient_ng.src.services.database import DatabaseContext
from mssqlclient_ng.src.utils import formatter


@ActionFactory.register("search", "Search for a keyword in database columns and rows")
class Search(BaseAction):
    """
    Searches for a keyword in column names and table data.

    Can search a specific database or all accessible databases.
    """

    def __init__(self):
        super().__init__()
        self._database: Optional[str] = None
        self._keyword: str = ""
        self._all_databases: bool = False

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validates the search arguments.

        Args:
            additional_arguments: [database|*] keyword

        Raises:
            ValueError: If arguments are invalid.
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError("Invalid arguments. Search usage: [database|*] keyword")

        parts = additional_arguments.strip().split(maxsplit=1)

        if len(parts) == 1:
            # Only the keyword is provided
            self._keyword = parts[0].strip()
            self._database = None  # Use the default database
        elif len(parts) == 2:
            # Check if first argument is "*" for all databases
            if parts[0].strip() == "*":
                self._all_databases = True
                self._keyword = parts[1].strip()
            else:
                # Both database and keyword are provided
                self._database = parts[0].strip()
                self._keyword = parts[1].strip()
        else:
            raise ValueError("Invalid arguments. Search usage: [database|*] keyword")

        if not self._keyword:
            raise ValueError("The keyword cannot be empty")

    def execute(self, database_context: DatabaseContext) -> Optional[dict]:
        """
        Executes the search operation.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            Dictionary with search statistics.
        """
        databases_to_search = []

        logger.info(
            f"Lurking for '{self._keyword}' accross accessible user tables only (excluding Microsoft system tables)"
        )

        if self._all_databases:
            logger.info("Searching across ALL accessible databases")
            # Get all accessible databases
            accessible_databases = database_context.query_service.execute_table(
                "SELECT name FROM sys.databases WHERE HAS_DBACCESS(name) = 1 AND state = 0 ORDER BY name;"
            )

            for db in accessible_databases:
                databases_to_search.append(db["name"])

            logger.info(
                f"Found {len(databases_to_search)} accessible databases to search"
            )
        else:
            # Use specified database or default
            if not self._database:
                self._database = database_context.server.database
            databases_to_search.append(self._database)

        total_header_matches = 0
        total_row_matches = 0
        total_tables_searched = 0

        for db_name in databases_to_search:
            logger.info(f"Searching database: {db_name}")
            header_matches, row_matches, tables_searched = self._search_database(
                database_context, db_name
            )
            total_header_matches += header_matches
            total_row_matches += row_matches
            total_tables_searched += tables_searched

        logger.success(
            f"Search completed across {len(databases_to_search)} database(s) and {total_tables_searched} table(s)"
        )
        logger.info(f"Column header matches: {total_header_matches}")
        logger.info(f"Row matches: {total_row_matches}")

        return {
            "databases_searched": len(databases_to_search),
            "tables_searched": total_tables_searched,
            "header_matches": total_header_matches,
            "row_matches": total_row_matches,
        }

    def _search_database(
        self, database_context: DatabaseContext, database: str
    ) -> tuple[int, int, int]:
        """
        Searches a specific database for the keyword.

        Args:
            database_context: The DatabaseContext instance.
            database: Database name to search.

        Returns:
            Tuple of (header_matches, row_matches, tables_searched).
        """
        # Escape single quotes in keyword for SQL
        escaped_keyword = self._keyword.replace("'", "''")

        # Query to get all columns in all tables of the specified database
        metadata_query = f"""
            SELECT
                s.name AS TABLE_SCHEMA,
                t.name AS TABLE_NAME,
                c.name AS COLUMN_NAME,
                ty.name AS DATA_TYPE,
                c.column_id AS ORDINAL_POSITION
            FROM [{database}].sys.tables t
            INNER JOIN [{database}].sys.schemas s ON t.schema_id = s.schema_id
            INNER JOIN [{database}].sys.columns c ON t.object_id = c.object_id
            INNER JOIN [{database}].sys.types ty ON c.user_type_id = ty.user_type_id
            WHERE t.is_ms_shipped = 0
            ORDER BY s.name, t.name, c.column_id;
        """

        try:
            columns_table = database_context.query_service.execute_table(metadata_query)
        except Exception as ex:
            logger.error(f"Failed to query metadata for database '{database}': {ex}")
            return (0, 0, 0)

        if not columns_table:
            logger.warning(f"No user tables found in database '{database}'")
            return (0, 0, 0)

        # Group columns by table
        table_columns = {}
        header_matches = []

        for row in columns_table:
            schema = row["TABLE_SCHEMA"]
            table = row["TABLE_NAME"]
            column = row["COLUMN_NAME"]
            data_type = row["DATA_TYPE"]
            position = row["ORDINAL_POSITION"]

            table_key = f"{schema}.{table}"
            if table_key not in table_columns:
                table_columns[table_key] = []

            table_columns[table_key].append((column, data_type, position))

            # Search for the keyword in column name
            if self._keyword.lower() in column.lower():
                header_matches.append(
                    {
                        "FQTN": f"[{database}].[{schema}].[{table}]",
                        "Header": column,
                        "Ordinal Position": position,
                    }
                )

        header_match_count = len(header_matches)
        row_match_count = 0
        tables_searched = 0

        # Log header matches
        if header_match_count > 0:
            logger.success(
                f"Found {header_match_count} column header match(es) containing '{self._keyword}'"
            )
            print(formatter.rows_to_markdown_table(header_matches))

        # Search for the keyword in each table's rows
        for table_key, columns in table_columns.items():
            schema, table = table_key.split(".")

            # Build WHERE clause - only search text-based columns and convert others to string
            conditions = []

            for column_name, data_type, _ in columns:
                # Escape column names with square brackets
                escaped_column = f"[{column_name}]"

                # Handle different data types
                if self._is_text_type(data_type):
                    # Direct string comparison for text types
                    conditions.append(f"{escaped_column} LIKE '%{escaped_keyword}%'")
                elif self._is_convertible_type(data_type):
                    # Convert numeric/date types to string for comparison
                    conditions.append(
                        f"CAST({escaped_column} AS NVARCHAR(MAX)) LIKE '%{escaped_keyword}%'"
                    )
                # Skip binary, image, and other non-searchable types

            if not conditions:
                continue  # Skip tables with no searchable columns

            where_clause = " OR ".join(conditions)

            # Get ALL matching rows
            search_query = f"""
                SELECT *
                FROM [{database}].[{schema}].[{table}]
                WHERE {where_clause};
            """

            try:
                tables_searched += 1

                result_table = database_context.query_service.execute_table(
                    search_query
                )

                if result_table:
                    row_match_count += len(result_table)
                    logger.success(
                        f"Found {len(result_table)} row(s) containing '{self._keyword}' in [{database}].[{schema}].[{table}]"
                    )
                    print(formatter.rows_to_markdown_table(result_table))

            except Exception as ex:
                logger.debug(f"Failed to search table [{schema}].[{table}]: {ex}")

        return (header_match_count, row_match_count, tables_searched)

    def _is_text_type(self, data_type: str) -> bool:
        """
        Checks if a data type is text-based and can be searched directly.

        Args:
            data_type: SQL Server data type name.

        Returns:
            True if text-based type.
        """
        text_types = ["char", "varchar", "nchar", "nvarchar", "text", "ntext"]
        return any(t in data_type for t in text_types)

    def _is_convertible_type(self, data_type: str) -> bool:
        """
        Checks if a data type can be converted to string for searching.

        Args:
            data_type: SQL Server data type name.

        Returns:
            True if convertible type.
        """
        convertible_types = [
            "int",
            "bigint",
            "smallint",
            "tinyint",
            "bit",
            "decimal",
            "numeric",
            "float",
            "real",
            "money",
            "smallmoney",
            "date",
            "datetime",
            "datetime2",
            "smalldatetime",
            "time",
            "datetimeoffset",
            "uniqueidentifier",
            "xml",
        ]
        return any(t in data_type for t in convertible_types)

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            List of argument descriptions.
        """
        return ["[database|*] keyword"]
