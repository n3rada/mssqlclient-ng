# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from mssqlclient_ng.src.actions.base import BaseAction
from mssqlclient_ng.src.actions.factory import ActionFactory
from mssqlclient_ng.src.services.database import DatabaseContext
from mssqlclient_ng.src.utils.formatters import OutputFormatter


@ActionFactory.register("search", "Search for keywords in column names and data across databases")
class Search(BaseAction):
    """
    Search for keywords in column names and data across databases.

    Usage:
    - search <keyword>: Search current database for keyword in column names and row data
    - search <keyword> -a: Search all accessible databases
    - search <keyword> -t schema.table: Search specific table only
    - search <keyword> -t database.schema.table: Search specific table in specific database
    - search <keyword> -c: Search column names only (no row data)

    Examples:
    - search password: Search for 'password' in current database
    - search password -a: Search for 'password' in all databases
    - search admin -t dbo.users: Search only in dbo.users table
    - search email -c: Find columns containing 'email' (fast)
    """

    def __init__(self):
        super().__init__()
        self._keyword: str = ""
        self._columns_only: bool = False
        self._all_databases: bool = False
        self._target_table: Optional[str] = None
        self._target_database: Optional[str] = None
        self._target_schema: Optional[str] = None

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validates the search arguments.

        Args:
            additional_arguments: keyword [-c] [-a] [-t table]

        Raises:
            ValueError: If arguments are invalid.
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError(
                "Keyword is required. Usage: search <keyword> [-c] [-a] [-t table]"
            )

        # Parse both positional and named arguments
        named_args, positional_args = self._parse_action_arguments(
            additional_arguments.strip()
        )

        # Get keyword from position 0 or -k/--keyword
        self._keyword = (
            named_args.get("k")
            or named_args.get("keyword")
            or (positional_args[0] if positional_args else None)
        )

        if not self._keyword:
            raise ValueError(
                "Keyword is required. Usage: search <keyword> [-c] [-a] [-t table]"
            )

        # Check for columns-only flag
        if "c" in named_args or "columns" in named_args:
            self._columns_only = True

        # Check for all databases flag
        if "a" in named_args or "all" in named_args:
            self._all_databases = True

        # Get target table if specified
        self._target_table = named_args.get("t") or named_args.get("table")

    def execute(self, database_context: DatabaseContext) -> Optional[dict]:
        """
        Executes the search operation.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            Dictionary with search statistics or None.
        """
        # Parse table argument if provided
        if self._target_table:
            table_parts = self._target_table.split(".")

            if len(table_parts) == 3:  # database.schema.table
                self._target_database = table_parts[0]
                self._target_schema = table_parts[1]
                self._target_table = table_parts[2]
            elif len(table_parts) == 2:  # schema.table (current database)
                self._target_schema = table_parts[0]
                self._target_table = table_parts[1]
            elif len(table_parts) == 1:  # just table name (use dbo schema)
                self._target_schema = "dbo"
                self._target_table = table_parts[0]
            else:
                raise ValueError(
                    "Invalid table format. Use: schema.table or database.schema.table"
                )

        # Handle column-only search
        if self._columns_only:
            return self._search_columns_only(database_context)

        # Handle specific table search
        if self._target_table:
            db_name = (
                self._target_database
                or database_context.query_service.execution_database
            )
            logger.info(
                f"Searching for '{self._keyword}' in [{db_name}].[{self._target_schema}].[{self._target_table}]"
            )
            header_matches, row_matches, _ = self._search_database(
                database_context, db_name, self._target_schema, self._target_table
            )

            print()
            logger.success("Search completed:")
            logger.info(f"  Column header matches: {header_matches}")
            logger.info(f"  Row matches: {row_matches}")
            return None

        # Handle database-wide or all databases search
        databases_to_search = []

        if self._all_databases:
            logger.info("Searching for keyword across ALL accessible databases")
            logger.info(
                "  Searching in accessible user tables only (excluding Microsoft system tables)"
            )

            # Get all accessible databases
            accessible_databases = database_context.query_service.execute_table(
                "SELECT name FROM master.sys.databases WHERE HAS_DBACCESS(name) = 1 AND state = 0 ORDER BY name;"
            )

            for db in accessible_databases:
                databases_to_search.append(db["name"])

            logger.info(
                f"  Found {len(databases_to_search)} accessible databases to search"
            )
        else:
            logger.info(
                f"Lurking for '{self._keyword}' in user tables only (excluding Microsoft system tables)"
            )
            # Use the execution database from QueryService
            database = database_context.query_service.execution_database
            databases_to_search.append(database)

        total_header_matches = 0
        total_row_matches = 0
        total_tables_searched = 0

        for db_name in databases_to_search:
            logger.info(f"  Searching database: {db_name}")
            header_matches, row_matches, tables_searched = self._search_database(
                database_context, db_name
            )
            total_header_matches += header_matches
            total_row_matches += row_matches
            total_tables_searched += tables_searched

        print()
        logger.success(
            f"Search completed across {len(databases_to_search)} database(s) and {total_tables_searched} table(s):"
        )
        logger.info(f"  Column header matches: {total_header_matches}")
        logger.info(f"  Row matches: {total_row_matches}")

        return None

    def _search_columns_only(
        self, database_context: DatabaseContext
    ) -> Optional[dict]:
        """
        Search only column names across all accessible databases (fast, no row data scanning).

        Args:
            database_context: The DatabaseContext instance.

        Returns:
            None
        """
        logger.info(
            f"Searching for '{self._keyword}' in column names only (fast mode)"
        )
        print()

        # Get all accessible databases
        databases_to_search = []

        if self._all_databases:
            accessible_databases = database_context.query_service.execute_table(
                "SELECT name FROM master.sys.databases WHERE HAS_DBACCESS(name) = 1 AND state = 0 ORDER BY name;"
            )

            for db in accessible_databases:
                databases_to_search.append(db["name"])
        else:
            databases_to_search.append(
                database_context.query_service.execution_database
            )

        all_matches = []
        total_matches = 0

        for db_name in databases_to_search:
            # Escape single quotes in keyword
            escaped_keyword = self._keyword.replace("'", "''")

            metadata_query = f"""
                SELECT
                    '{db_name}' AS [Database],
                    s.name AS [Schema],
                    t.name AS [Table],
                    c.name AS [Column],
                    ty.name AS [Data Type],
                    c.column_id AS [Position]
                FROM [{db_name}].sys.tables t
                INNER JOIN [{db_name}].sys.schemas s ON t.schema_id = s.schema_id
                INNER JOIN [{db_name}].sys.columns c ON t.object_id = c.object_id
                INNER JOIN [{db_name}].sys.types ty ON c.user_type_id = ty.user_type_id
                WHERE t.is_ms_shipped = 0
                AND c.name LIKE '%{escaped_keyword}%'
                ORDER BY s.name, t.name, c.column_id;
            """

            try:
                matches = database_context.query_service.execute_table(metadata_query)

                for row in matches:
                    all_matches.append(
                        {
                            "Database": row["Database"],
                            "Schema": row["Schema"],
                            "Table": row["Table"],
                            "Column": row["Column"],
                            "Data Type": row["Data Type"],
                            "Position": row["Position"],
                        }
                    )

                total_matches += len(matches)
            except Exception as ex:
                logger.error(f"Failed to search database '{db_name}': {ex}")

        if total_matches > 0:
            logger.success(
                f"Found {total_matches} column(s) containing '{self._keyword}':"
            )
            print(OutputFormatter.convert_list_of_dicts(all_matches))
        else:
            logger.warning(f"No columns found containing '{self._keyword}'")

        return None

    def _search_database(
        self,
        database_context: DatabaseContext,
        database: str,
        target_schema: Optional[str] = None,
        target_table: Optional[str] = None,
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

        # Build WHERE clause for specific table if specified
        table_filter = ""
        if target_table:
            escaped_schema = target_schema.replace("'", "''")
            escaped_table = target_table.replace("'", "''")
            table_filter = f" AND s.name = '{escaped_schema}' AND t.name = '{escaped_table}' "

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
            WHERE t.is_ms_shipped = 0 {table_filter}
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
            print(OutputFormatter.convert_list_of_dicts(header_matches))

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
                    print()
                    logger.success(
                        f"Found {len(result_table)} row(s) containing '{self._keyword}' in [{database}].[{schema}].[{table}]:"
                    )
                    print(OutputFormatter.convert_list_of_dicts(result_table))

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
        return ["<keyword> [-c] [-a] [-t table]"]
