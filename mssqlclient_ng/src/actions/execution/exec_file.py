"""
ExecFile action for executing remote files on the SQL Server.
"""

from typing import Optional, List
from loguru import logger

from mssqlclient_ng.src.actions.base import BaseAction
from mssqlclient_ng.src.actions.factory import ActionFactory
from mssqlclient_ng.src.services.database import DatabaseContext
from mssqlclient_ng.src.utils.misc import normalize_windows_path


@ActionFactory.register("exec", "Execute a remote file on the SQL Server")
class ExecFile(BaseAction):
    """
    Execute a remote file on the SQL Server filesystem.

    This action runs executables, scripts, or batch files on the SQL Server using
    multiple methods in order of preference:
    1. OLE Automation with WScript.Shell
    2. xp_cmdshell (fallback)

    The action verifies the file exists before attempting execution.
    """

    def __init__(self):
        super().__init__()
        self._file_path: str = ""
        self._arguments: str = ""

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate arguments for the exec action.

        Args:
            additional_arguments: File path and optional arguments
                Format: <file_path> [arguments...]

        Raises:
            ValueError: If no file path is provided
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError("Exec action requires a file path as an argument.")

        parts = additional_arguments.strip().split(maxsplit=1)
        self._file_path = normalize_windows_path(parts[0])
        self._arguments = parts[1] if len(parts) > 1 else ""

        logger.info(f"Target file: {self._file_path}")
        if self._arguments:
            logger.info(f"Arguments: {self._arguments}")

    def execute(self, database_context: DatabaseContext) -> Optional[List[str]]:
        """
        Execute the remote file.

        Args:
            database_context: The database context containing services

        Returns:
            Output lines from execution if available, or None
        """
        # Verify file exists
        if not self._file_exists(database_context):
            logger.error(f"File does not exist: {self._file_path}")
            return None

        logger.info(f"Executing file: {self._file_path}")

        # Check if OLE Automation is available
        ole_available = database_context.config_service.set_configuration_option(
            "Ole Automation Procedures", 1
        )

        # Use OLE if available, otherwise xp_cmdshell
        if ole_available:
            logger.info("OLE Automation is available, using OLE method")
            return self._execute_via_ole(database_context)
        else:
            logger.info("OLE Automation not available, using xp_cmdshell method")
            return self._execute_via_xpcmdshell(database_context)

    def _file_exists(self, database_context: DatabaseContext) -> bool:
        """
        Check if a file exists using xp_fileexist.

        Args:
            database_context: The database context

        Returns:
            True if file exists; otherwise False
        """
        try:
            escaped_path = self._file_path.replace("'", "''")
            query = f"EXEC master..xp_fileexist '{escaped_path}'"
            result = database_context.query_service.execute_table(query)

            if not result:
                return False

            return result[0].get("File Exists", False)

        except Exception as e:
            logger.error(f"Could not check if file exists: {e}")
            return False

    def _execute_via_ole(
        self, database_context: DatabaseContext
    ) -> Optional[List[str]]:
        """
        Execute the file using OLE Automation with WScript.Shell.Run.

        Args:
            database_context: The database context

        Returns:
            None (OLE Run method doesn't capture output directly)
        """
        try:
            # Escape single quotes for SQL
            escaped_path = self._file_path.replace("'", "''")
            escaped_args = self._arguments.replace("'", "''")

            # Build the command string
            if self._arguments:
                command = f"{escaped_path} {escaped_args}"
            else:
                command = escaped_path

            query = f"""
                DECLARE @ObjectToken INT;
                DECLARE @Result INT;
                DECLARE @ErrorSource NVARCHAR(255);
                DECLARE @ErrorDesc NVARCHAR(255);
                DECLARE @ExitCode INT;

                -- Create WScript.Shell object
                EXEC @Result = sp_OACreate 'WScript.Shell', @ObjectToken OUTPUT;
                IF @Result <> 0
                BEGIN
                    EXEC sp_OAGetErrorInfo @ObjectToken, @ErrorSource OUT, @ErrorDesc OUT;
                    RAISERROR('Failed to create WScript.Shell: %s', 16, 1, @ErrorDesc);
                    RETURN;
                END

                -- Execute the command
                -- Run(command, windowStyle, waitOnReturn)
                -- windowStyle: 0 = hidden, 1 = normal
                -- waitOnReturn: true = wait for completion
                EXEC @Result = sp_OAMethod @ObjectToken, 'Run', @ExitCode OUTPUT, '{command}', 0, 1;
                IF @Result <> 0
                BEGIN
                    EXEC sp_OAGetErrorInfo @ObjectToken, @ErrorSource OUT, @ErrorDesc OUT;
                    EXEC sp_OADestroy @ObjectToken;
                    RAISERROR('Failed to execute file: %s', 16, 1, @ErrorDesc);
                    RETURN;
                END

                -- Destroy object
                EXEC sp_OADestroy @ObjectToken;

                -- Return exit code
                SELECT @ExitCode AS ExitCode;
            """

            result = database_context.query_service.execute_table(query)

            if result:
                exit_code = result[0].get("ExitCode", -1)
                logger.success(
                    f"File executed successfully via OLE (Exit Code: {exit_code})"
                )
                print()
                print(f"Process completed with exit code: {exit_code}")
                return [f"Exit code: {exit_code}"]
            else:
                logger.error("OLE execution failed")
                return None

        except Exception as e:
            logger.error(f"Failed to execute via OLE: {e}")
            return None

    def _execute_via_xpcmdshell(
        self, database_context: DatabaseContext
    ) -> Optional[List[str]]:
        """
        Execute the file using xp_cmdshell.

        Args:
            database_context: The database context

        Returns:
            Output lines from the command
        """
        # Enable xp_cmdshell if needed
        if not database_context.config_service.set_configuration_option(
            "xp_cmdshell", 1
        ):
            logger.error("Failed to enable xp_cmdshell")
            return None

        try:
            # Build the command
            if self._arguments:
                command = f'"{self._file_path}" {self._arguments}'
            else:
                command = f'"{self._file_path}"'

            # Escape single quotes for SQL
            escaped_command = command.replace("'", "''")

            query = f"EXEC master..xp_cmdshell '{escaped_command}'"

            logger.info("Executing via xp_cmdshell")
            result = database_context.query_service.execute(query, tuple_mode=True)

            output_lines: List[str] = []

            if result:
                print()
                for row in result:
                    # Handle NULL values and extract first column
                    output = row[0] if row and row[0] is not None else ""
                    if output == "NULL":
                        output = ""

                    print(output)
                    output_lines.append(output)

                logger.success("File executed successfully via xp_cmdshell")
                return output_lines

            logger.warning("The command executed but returned no results.")
            return output_lines

        except Exception as e:
            logger.error(f"Failed to execute via xp_cmdshell: {e}")
            return None

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return ["Remote file path to execute", "Optional: command-line arguments"]
