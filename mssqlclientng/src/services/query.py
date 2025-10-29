"""
Query service for executing SQL queries against MSSQL servers using impacket's TDS.
"""

from typing import Optional, Any, List, Dict

# Third party imports
from loguru import logger
from impacket.tds import MSSQL, SQLErrorException
from impacket.tds import TDS_DONE_TOKEN, TDS_DONEINPROC_TOKEN, TDS_DONEPROC_TOKEN

# Local library imports
from mssqlclientng.src.models.linked_servers import LinkedServers


class QueryService:
    """
    Service for executing SQL queries against MSSQL using impacket's TDS protocol.
    """

    def __init__(self, mssql: MSSQL):
        """
        Initialize the query service with an MSSQL connection.

        Args:
            mssql: An active MSSQL connection instance from impacket
        """
        self.connection = mssql
        self.execution_server: Optional[str] = None
        self._linked_servers = LinkedServers()
        self.command_timeout = 20  # Default timeout in seconds

        # Initialize execution server
        self.execution_server = self._get_server_name()

    @property
    def linked_servers(self) -> LinkedServers:
        """Get the linked servers configuration."""
        return self._linked_servers

    @linked_servers.setter
    def linked_servers(self, value: Optional[LinkedServers]) -> None:
        """
        Set the linked servers configuration.
        Updates the execution server to the last server in the chain.
        """
        self._linked_servers = value if value is not None else LinkedServers()

        if not self._linked_servers.is_empty:
            self.execution_server = self._linked_servers.server_names[-1]
            logger.debug(f"Execution server set to: {self.execution_server}")
        else:
            self.execution_server = self._get_server_name()

    def _get_server_name(self) -> str:
        """
        Retrieve the current server name from the connection.

        Returns:
            The server name, or "Unknown" if retrieval fails
        """
        try:
            result = self.execute_scalar("SELECT @@SERVERNAME")
            if result:
                server_name = str(result)
                # Extract hostname before backslash (instance name)
                return (
                    server_name.split("\\")[0] if "\\" in server_name else server_name
                )
        except Exception as e:
            logger.warning(f"Failed to get server name: {e}")

        return "Unknown"

    def execute(self, query: str, tuple_mode: bool = False) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results as rows.

        Args:
            query: The SQL query to execute
            tuple_mode: If True, return rows as tuples instead of dicts

        Returns:
            List of rows (dicts or tuples based on tuple_mode)

        Raises:
            ValueError: If query is empty
            SQLErrorException: If query execution fails
        """
        return self._execute_with_handling(
            query, tuple_mode=tuple_mode, return_rows=True
        )

    def execute_non_processing(self, query: str) -> int:
        """
        Execute a SQL query without returning results (INSERT, UPDATE, DELETE, etc.).

        Args:
            query: The SQL query to execute

        Returns:
            Number of affected rows, or -1 on error
        """
        try:
            result = self._execute_with_handling(
                query, tuple_mode=False, return_rows=False
            )
            return result if result is not None else -1
        except Exception as error:
            logger.error(error)
            return -1

    def execute_table(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return the results as a list of dictionaries.

        Args:
            query: The SQL query to execute.

        Returns:
            List of row dictionaries, one per result row.
        """
        rows = self.execute(query, tuple_mode=False)
        return rows if rows else []

    def execute_scalar(self, query: str) -> Optional[Any]:
        """
        Execute a SQL query and return a single scalar value (first column of first row).

        Args:
            query: The SQL query to execute

        Returns:
            The scalar value, or None if no rows returned
        """
        rows = self.execute(query, tuple_mode=False)

        if rows and len(rows) > 0:
            # Get first column value of first row
            first_row = rows[0]
            if isinstance(first_row, dict) and first_row:
                # Return first value from dict
                return next(iter(first_row.values()))
            elif isinstance(first_row, (list, tuple)) and first_row:
                return first_row[0]

        return None

    def _execute_with_handling(
        self, query: str, tuple_mode: bool = False, return_rows: bool = True
    ) -> Any:
        """
        Shared execution logic with error handling.

        Args:
            query: The SQL query to execute
            tuple_mode: If True, return rows as tuples
            return_rows: If True, return row data; otherwise return affected count

        Returns:
            Query results or affected row count

        Raises:
            ValueError: If query is empty or connection is invalid
            SQLErrorException: If query execution fails
        """
        if not query or not query.strip():
            raise ValueError("Query cannot be null or empty.")

        if not self.connection or not self.connection.socket:
            logger.error("Database connection is not initialized or not open.")
            raise ValueError("Database connection is not open.")

        # Prepare the final query with linked server logic
        final_query = self._prepare_query(query)

        try:
            # Execute the query using impacket's batch method
            self.connection.batch(final_query, tuplemode=tuple_mode)

            # Print replies to capture any errors
            self.connection.printReplies()

            # Check for errors
            if self.connection.lastError:
                raise self.connection.lastError

            # Return results based on request
            if return_rows:
                return self.connection.rows
            else:
                # For non-query operations, return affected row count
                # This information is in the DONE token replies
                return self._get_affected_rows()

        except SQLErrorException as e:
            error_message = str(e)
            logger.debug(f"Query execution returned an error: {error_message}")

            # Handle RPC configuration error
            if "not configured for RPC" in error_message:
                logger.warning(
                    "The targeted server is not configured for Remote Procedure Call (RPC)"
                )
                logger.warning("Trying again with OPENQUERY")
                self._linked_servers.use_remote_procedure_call = False
                return self._execute_with_handling(query, tuple_mode, return_rows)

            # Handle metadata errors
            if "metadata could not be determined" in error_message:
                logger.error(
                    "When you wrap a remote procedure in OPENQUERY, SQL Server wants a single, consistent set of columns."
                )
                logger.error(
                    "Since sp_configure does not provide that, the metadata parser chokes."
                )
                logger.info("Enable RPC OUT option to allow the use of sp_configure.")
                logger.info(f"Command: /a:rpc add {self.execution_server}")

            raise

        except Exception as e:
            error_message = str(e).strip()

            # Some stored procedures (like OLE Automation) may raise exceptions
            # with just "0" as the message, which actually indicates success
            if error_message == "0":
                logger.debug("Query returned status code 0 (success)")
                if return_rows:
                    return (
                        self.connection.rows if hasattr(self.connection, "rows") else []
                    )
                else:
                    return 0

            logger.error(f"Unexpected error during query execution: {e}")
            raise

    def _prepare_query(self, query: str) -> str:
        """
        Prepare the final query by adding linked server logic if needed.

        Args:
            query: The initial SQL query

        Returns:
            The modified query with linked server chaining if applicable
        """
        logger.debug(f"Query to execute: {query}")
        final_query = query

        if not self._linked_servers.is_empty:
            logger.debug("Linked server detected")

            if self._linked_servers.use_remote_procedure_call:
                final_query = self._linked_servers.build_remote_procedure_call_chain(
                    query
                )
            else:
                final_query = self._linked_servers.build_select_openquery_chain(query)

            logger.debug(f"Linked query: {final_query}")

        return final_query

    def _get_affected_rows(self) -> int:
        """
        Extract the number of affected rows from TDS replies.

        Returns:
            Number of affected rows, or 0 if not available
        """

        affected = 0

        # Check for DONE tokens in replies
        for token_type in [TDS_DONE_TOKEN, TDS_DONEINPROC_TOKEN, TDS_DONEPROC_TOKEN]:
            if token_type in self.connection.replies:
                tokens = self.connection.replies[token_type]
                if tokens:
                    # Get the last DONE token's row count
                    last_token = tokens[-1]
                    if "DoneRowCount" in last_token:
                        affected = last_token["DoneRowCount"]

        return affected

    def change_database(self, database: str) -> None:
        """
        Change the current database context.

        Args:
            database: The database name to switch to
        """
        if database != self.connection.currentDB:
            self.connection.changeDB(database)
            self.connection.printReplies()

    def get_current_database(self) -> str:
        """
        Get the current database context.

        Returns:
            The current database name
        """
        return self.connection.currentDB
