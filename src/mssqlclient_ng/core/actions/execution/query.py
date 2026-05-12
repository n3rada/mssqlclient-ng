# mssqlclient_ng/core/actions/execution/query.py


# Built-in imports
from typing import Optional, List, Dict, Any

# Third-party imports
from loguru import logger

from ..base import BaseAction
from utils.formatters import OutputFormatter


# Not registered via ActionFactory - Query is invoked directly by the shell
# when no action prefix is matched (any raw SQL input).
class Query(BaseAction):
    """
    Execute a T-SQL query against the SQL Server.

    Supports both queries that return result sets (SELECT) and
    non-query commands (INSERT, UPDATE, DELETE, etc.).

    Usage:
        query SELECT @@SERVERNAME
        query --all SELECT DB_NAME()
        sql SELECT name FROM sys.databases
    """

    def __init__(self):
        super().__init__()
        self._query: Optional[str] = None
        self._execute_all: bool = False

    def validate_arguments(
        self, additional_arguments: str = "", argument_list=None
    ) -> None:
        """
        Validate that a query is provided.

        Args:
            additional_arguments: The SQL query to execute, optionally prefixed with --all

        Raises:
            ValueError: If no query is provided
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError(
                "Query action requires a valid SQL query as an additional argument."
            )

        args = additional_arguments.strip()

        # Check for --all flag
        if args.startswith("--all ") or args.startswith("--all\t"):
            self._execute_all = True
            args = args[6:].strip()
        elif args == "--all":
            raise ValueError("--all requires a SQL query to execute.")

        self._query = args

    def execute(self, database_context=None) -> Optional[List[Dict[str, Any]]]:
        """
        Execute the T-SQL query.

        Args:
            database_context: The database context containing QueryService

        Returns:
            List of result rows for SELECT queries, None for non-query commands
        """
        if not database_context or not hasattr(database_context, "query_service"):
            logger.error("Database context with query_service is required")
            return None

        query_service = database_context.query_service
        execution_server = query_service.execution_server

        logger.info(f"Executing T-SQL query against {execution_server}: {self._query}")

        if self._execute_all:
            return self._execute_across_all_databases(database_context)

        try:
            # Detect if it's a non-query command
            if self._is_non_query(self._query):
                logger.debug("Executing as a non-query command")
                rows_affected = query_service.execute_non_processing(self._query)

                if rows_affected >= 0:
                    logger.success(
                        f"Query executed successfully. Rows affected: {rows_affected}"
                    )
                else:
                    logger.warning(
                        "Query executed but could not determine rows affected"
                    )

                return None

            # Execute as a query that returns results
            result_rows = query_service.execute_table(self._query)

            rows = len(result_rows)

            logger.success(f"Rows returned: {rows}")
            if rows == 0:
                return result_rows

            # Check if it's a scalar result (single row with empty string key)
            if rows == 1 and len(result_rows[0]) == 1 and "" in result_rows[0]:
                scalar_value = result_rows[0][""]
                print()
                print(scalar_value)
                print()
                return result_rows

            # Format and print results as table
            print()
            print(OutputFormatter.convert_list_of_dicts(result_rows))
            return result_rows

        except Exception as e:
            error_message = str(e)
            logger.error(f"Error executing query: {error_message}")

            # Log additional details if available
            if hasattr(e, "number"):
                logger.debug(f"Error Number: {e.number}")
            if hasattr(e, "line_number"):
                logger.debug(f"Line Number: {e.line_number}")
            if hasattr(e, "procedure"):
                logger.debug(f"Procedure: {e.procedure}")
            if hasattr(e, "server"):
                logger.debug(f"Server: {e.server}")

            return None

    def _is_non_query(self, query: str) -> bool:
        """
        Determine if a query is a non-query command (doesn't return result set).

        Args:
            query: The SQL query to check

        Returns:
            True if it's a non-query command, False otherwise
        """
        if not query or not query.strip():
            return False

        # Keywords that indicate non-query commands
        non_query_keywords = [
            "INSERT",
            "UPDATE",
            "DELETE",
            "DROP",
            "ALTER",
            "CREATE",
            "TRUNCATE",
            "EXEC",
            "EXECUTE",
        ]

        # Normalize query for comparison
        normalized = query.strip().upper()

        # Check if query starts with any non-query keyword
        # or contains it as a standalone word
        for keyword in non_query_keywords:
            if normalized.startswith(keyword + " ") or normalized.startswith(
                keyword + ";"
            ):
                return True
            if f" {keyword} " in normalized or f" {keyword};" in normalized:
                return True

        return False

    def _execute_across_all_databases(
        self, database_context
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Execute the query across all accessible databases.

        Args:
            database_context: The database context

        Returns:
            Combined result rows from all databases
        """
        logger.info(
            f"Executing across ALL accessible databases on "
            f"{database_context.query_service.execution_server}"
        )

        # Get list of accessible databases
        databases = database_context.query_service.execute_table(
            "SELECT name FROM master.sys.databases WHERE HAS_DBACCESS(name) = 1 AND state = 0 ORDER BY name"
        )

        if not databases:
            logger.warning("No accessible databases found")
            return None

        db_names = [db["name"] for db in databases]
        logger.info(
            f"Found {len(db_names)} accessible database(s): {', '.join(db_names)}"
        )

        combined_results = []
        total_rows = 0

        for db_name in db_names:
            logger.info(f"Querying: {db_name}")
            try:
                db_query = f"USE [{db_name}]; {self._query}"
                db_results = database_context.query_service.execute_table(db_query)

                if db_results:
                    for row in db_results:
                        combined_row = {"Database": db_name}
                        combined_row.update(row)
                        combined_results.append(combined_row)
                    total_rows += len(db_results)
            except Exception as ex:
                logger.warning(f"Error on {db_name}: {ex}")
                continue

        logger.success(f"Total rows from all databases: {total_rows}")

        if combined_results:
            print()
            print(OutputFormatter.convert_list_of_dicts(combined_results))

        return combined_results

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return ["query: T-SQL query to execute (required)"]
