# mssqlclient_ng/core/services/query.py

# Built-in imports
from typing import Optional, Any, List, Dict, TYPE_CHECKING

# Third party imports
from loguru import logger
from impacket.tds import SQLErrorException
from impacket.tds import TDS_DONE_TOKEN, TDS_DONEINPROC_TOKEN, TDS_DONEPROC_TOKEN

if TYPE_CHECKING:
    # Import MSSQL only for type checking to avoid a runtime dependency
    from impacket.tds import MSSQL

# Local library imports
from ..models.linked_servers import LinkedServers


class QueryService:
    """
    Service for executing SQL queries against MSSQL using impacket's TDS protocol.
    """

    MAX_RETRIES = 3

    def __init__(self, mssql: "MSSQL"):
        """
        Initialize the query service with an MSSQL connection.

        Args:
            mssql: An active MSSQL connection instance from impacket
        """
        self.mssql_instance = mssql
        self.execution_server: Optional[str] = None
        self.execution_database: Optional[str] = None
        self._linked_servers = LinkedServers()
        self.command_timeout = 120  # Default timeout in seconds

        # Dictionary to cache Azure SQL detection for each execution server
        self._is_azure_sql_cache: Dict[str, bool] = {}

        # Initialize execution server and database
        self.execution_server = self._get_server_name()
        self.execution_database = self.get_current_database()

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
            self.execution_database = self.get_current_database()

    @property
    def is_azure_sql(self) -> bool:
        """
        Checks if the current execution server is Azure SQL Database.
        Results are cached per server for performance.

        Returns:
            True if the server is Azure SQL Database (PaaS), otherwise False.
        """
        # Check if Azure SQL detection is already cached for the current ExecutionServer
        if self.execution_server in self._is_azure_sql_cache:
            return self._is_azure_sql_cache[self.execution_server]

        # If not cached, detect and store the result
        azure_status = self._detect_azure_sql()

        # Cache the result for the current ExecutionServer
        self._is_azure_sql_cache[self.execution_server] = azure_status

        if azure_status:
            logger.debug(f"Detected Azure SQL Database on {self.execution_server}")

        return azure_status

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
        self,
        query: str,
        tuple_mode: bool = False,
        return_rows: bool = True,
        timeout: int = 120,
        retry_count: int = 0,
    ) -> Any:
        """
        Shared execution logic with error handling and retry mechanism.

        Args:
            query: The SQL query to execute
            tuple_mode: If True, return rows as tuples
            return_rows: If True, return row data; otherwise return affected count
            timeout: Timeout in seconds for query execution
            retry_count: Current retry attempt (for exponential backoff)

        Returns:
            Query results or affected row count

        Raises:
            ValueError: If query is empty or connection is invalid
            SQLErrorException: If query execution fails
        """
        if not query or not query.strip():
            raise ValueError("Query cannot be null or empty.")

        # Check if we've exceeded max retries
        if retry_count > self.MAX_RETRIES:
            logger.error(
                f"Maximum retry attempts ({self.MAX_RETRIES}) exceeded. Aborting query execution."
            )
            return None if return_rows else -1

        if not self.mssql_instance or not self.mssql_instance.socket:
            logger.error("Database connection is not initialized or not open.")
            return None if return_rows else -1

        # Prepare the final query with linked server logic
        final_query = self._prepare_query(query)

        try:
            # Execute the query using impacket's batch method
            self.mssql_instance.batch(final_query, tuplemode=tuple_mode)

            # Print replies to capture any errors
            self.mssql_instance.printReplies()

            # Check for errors
            if self.mssql_instance.lastError:
                raise self.mssql_instance.lastError

            # Return results based on request
            if return_rows:
                return self.mssql_instance.rows
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
                return self._execute_with_handling(
                    query, tuple_mode, return_rows, timeout, retry_count + 1
                )

            # Handle metadata errors
            if "metadata could not be determined" in error_message.lower():
                logger.warning(
                    "DDL statement detected - wrapping query to make it OPENQUERY-compatible"
                )

                # Wrap the query to return a result set - use EXEC to avoid metadata issues
                wrapped_query = f"DECLARE @result NVARCHAR(MAX); BEGIN TRY {query.rstrip(';')}; SET @result = 'Success'; END TRY BEGIN CATCH SET @result = ERROR_MESSAGE(); END CATCH; SELECT @result AS Result;"

                logger.warning("Retrying with wrapped query")
                return self._execute_with_handling(
                    wrapped_query,
                    tuple_mode,
                    return_rows,
                    timeout,
                    retry_count + 1,
                )

            # Handle database prefix not supported on remote server
            if (
                "is not supported" in error_message
                and "master." in error_message
                and "master." in query
            ):
                logger.warning(
                    "Database prefix 'master.' not supported on remote server"
                )
                logger.warning("Retrying without database prefix")

                # Remove all master. prefixes from the query
                query_without_prefix = query.replace("master.", "")

                return self._execute_with_handling(
                    query_without_prefix,
                    tuple_mode,
                    return_rows,
                    timeout,
                    retry_count + 1,
                )

            raise

        except Exception as e:
            error_message = str(e).strip()

            # Handle timeout errors with exponential backoff
            if "timeout" in error_message.lower():
                new_timeout = timeout * 2  # Exponential backoff
                logger.warning(
                    f"Query timed out after {timeout} seconds. Retrying with {new_timeout} seconds (attempt {retry_count + 1}/{self.MAX_RETRIES})"
                )
                return self._execute_with_handling(
                    query, tuple_mode, return_rows, new_timeout, retry_count + 1
                )

            # If OPENQUERY is in use and we hit a metadata/no-rowset error,
            # attempt to wrap the query so OPENQUERY returns a rowset.
            if (
                not self._linked_servers.use_remote_procedure_call
                and self._is_openquery_rowset_failure(e)
            ):
                logger.debug("OPENQUERY returned no rowset. Wrapping query.")
                wrapped = self._wrap_for_openquery(query)
                return self._execute_with_handling(
                    wrapped, tuple_mode, return_rows, timeout, retry_count + 1
                )

            # Some stored procedures (like OLE Automation) may raise exceptions
            # with just "0" as the message, which actually indicates success
            if error_message == "0":
                logger.debug("Query returned status code 0 (success)")
                if return_rows:
                    return (
                        self.mssql_instance.rows
                        if hasattr(self.mssql_instance, "rows")
                        else []
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

            # If OPENQUERY is being used, refuse server-scoped commands that
            # require RPC (e.g. login creation, server config changes).
            if (
                not self._linked_servers.use_remote_procedure_call
                and self._requires_rpc(query)
            ):
                logger.warning("Server-level command rejected under OPENQUERY.")
                raise ValueError(
                    "This query requires RPC and cannot be executed via OPENQUERY."
                )

            if self._linked_servers.use_remote_procedure_call:
                final_query = self._linked_servers.build_remote_procedure_call_chain(
                    query
                )
            else:
                final_query = self._linked_servers.build_select_openquery_chain(query)

            logger.debug(f"Linked query: {final_query}")

        return final_query

    def _requires_rpc(self, sql: str) -> bool:
        """
        Determines if a SQL statement requires RPC execution because it modifies
        server-level state. These commands cannot be executed over OPENQUERY.
        """
        if not sql:
            return False

        s = sql.upper()

        return (
            "CREATE LOGIN" in s
            or "ALTER LOGIN" in s
            or "DROP LOGIN" in s
            or "ALTER SERVER" in s
            or "SP_CONFIGURE" in s
            or "RECONFIGURE" in s
            or "XP_" in s
            or "CREATE ENDPOINT" in s
            or "SYS.SERVER_" in s
        )

    def _is_openquery_rowset_failure(self, ex: Exception) -> bool:
        """
        Detects typical OPENQUERY failures where no rowset is returned or
        deferred/metadata errors occur.
        """
        if ex is None:
            return False
        m = str(ex).lower()
        return (
            "metadata" in m
            or "no columns" in m
            or "deferred prepare" in m
            or "no column" in m
        )

    def _wrap_for_openquery(self, query: str) -> str:
        """
        Wrap a non-rowset SQL statement into a SELECT-able result so that
        OPENQUERY can return a resultset instead of failing on metadata.
        """
        # Trim trailing semicolons and create a TRY/CATCH wrapper that returns
        # either rowcount or the error message so OPENQUERY always sees a rowset.
        core = query.rstrip(";")
        wrapped = (
            "DECLARE @result NVARCHAR(MAX); DECLARE @error NVARCHAR(MAX);"
            " BEGIN TRY "
            f" {core}; SET @result = CAST(@@ROWCOUNT AS NVARCHAR(MAX)); SET @error = NULL;"
            " END TRY BEGIN CATCH SET @result = NULL; SET @error = ERROR_MESSAGE(); END CATCH;"
            " SELECT @result AS Result, @error AS Error;"
        )
        return wrapped

    def _get_affected_rows(self) -> int:
        """
        Extract the number of affected rows from TDS replies.

        Returns:
            Number of affected rows, or 0 if not available
        """

        affected = 0

        # Check for DONE tokens in replies
        for token_type in [TDS_DONE_TOKEN, TDS_DONEINPROC_TOKEN, TDS_DONEPROC_TOKEN]:
            if token_type in self.mssql_instance.replies:
                tokens = self.mssql_instance.replies[token_type]
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
        if database != self.mssql_instance.currentDB:
            self.mssql_instance.changeDB(database)
            self.mssql_instance.printReplies()

    def get_current_database(self) -> str:
        """
        Get the current database context.

        Returns:
            The current database name
        """
        return self.mssql_instance.currentDB

    def _detect_azure_sql(self) -> bool:
        """
        Detects if the current execution server is Azure SQL by checking @@VERSION.

        Returns:
            True if Azure SQL Database (PaaS) is detected, otherwise False.
        """
        try:
            version = self.execute_scalar("SELECT @@VERSION")

            if not version or not isinstance(version, str):
                return False

            # Check if it contains "Microsoft SQL Azure" (case-insensitive)
            is_azure = "microsoft sql azure" in version.lower()

            if is_azure:
                # Distinguish between Azure SQL Database and Managed Instance
                # Azure SQL Database (PaaS) contains "SQL Azure" but NOT "Managed Instance"
                # Azure SQL Managed Instance contains both "SQL Azure" and specific MI indicators
                is_managed_instance = (
                    "azure sql managed instance" in version.lower()
                    or "sql azure managed instance" in version.lower()
                )

                if is_managed_instance:
                    logger.info(
                        f"Detected Azure SQL Managed Instance on {self.execution_server}"
                    )
                    return False  # Managed Instance has full features
                else:
                    logger.info(
                        f"Detected Azure SQL Database (PaaS) on {self.execution_server}"
                    )
                    return True  # PaaS has limitations

            return False
        except Exception:
            # If detection fails, assume it's not Azure SQL
            return False

    def compute_execution_database(self) -> None:
        """
        Computes the execution database based on the linked server chain.
        Should be called after the entire linked server setup is complete.
        """
        if not self._linked_servers.is_empty:
            last_server = self._linked_servers.server_chain[-1]
            if last_server.database:
                # Use explicitly specified database from chain
                self.execution_database = last_server.database
                logger.debug(
                    f"Using explicitly specified database: {self.execution_database}"
                )
            else:
                # No explicit database: query to detect actual database where the link landed us
                try:
                    self.execution_database = self.execute_scalar("SELECT DB_NAME();")
                    logger.debug(
                        f"Detected execution database: {self.execution_database}"
                    )
                except Exception as ex:
                    # If detection fails, database remains unknown
                    self.execution_database = None
                    logger.debug(f"Database detection failed: {ex}")
        # If no linked servers, execution_database is already set from get_current_database()
