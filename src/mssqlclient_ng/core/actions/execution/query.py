"""
Query action for executing T-SQL queries.
"""

from typing import Optional, List, Dict, Any
from loguru import logger

from ..base import BaseAction
from ..factory import ActionFactory
from ...utils.formatters import OutputFormatter


# We do not register this action automatically to avoid it appearing in help
class Query(BaseAction):
    """
    Execute a T-SQL query against the SQL Server.

    Supports both queries that return result sets (SELECT) and
    non-query commands (INSERT, UPDATE, DELETE, etc.).
    """

    def __init__(self):
        super().__init__()
        self._query: Optional[str] = None

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate that a query is provided.

        Args:
            additional_arguments: The SQL query to execute

        Raises:
            ValueError: If no query is provided
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError(
                "Query action requires a valid SQL query as an additional argument."
            )

        self._query = additional_arguments.strip()

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

            # If only one row, display the result directly
            if rows == 1:
                result = result_rows[0][""]
                print()
                print(result)
                print()
            else:
                # Format and print results as Markdown table
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

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return ["query: T-SQL query to execute (required)"]
