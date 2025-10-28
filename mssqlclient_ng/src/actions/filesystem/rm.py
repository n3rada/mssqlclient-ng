"""
Remove (delete) action for deleting files from the SQL Server filesystem.
"""

from typing import Optional
from loguru import logger

from mssqlclient_ng.src.actions.base import BaseAction
from mssqlclient_ng.src.actions.factory import ActionFactory
from mssqlclient_ng.src.services.database import DatabaseContext
from mssqlclient_ng.src.utils.misc import normalize_windows_path


@ActionFactory.register("rm", "Delete a file from the SQL Server filesystem")
class Remove(BaseAction):
    """
    Delete a file from the SQL Server filesystem.

    This action removes a file from the SQL Server using OLE Automation
    (Scripting.FileSystemObject) or xp_cmdshell as a fallback. It verifies
    the file exists before deletion and confirms successful removal afterwards.

    Methods used (in order of preference):
    1. OLE Automation with FileSystemObject (most compatible)
    2. xp_cmdshell with 'del' command (if OLE is disabled)
    """

    def __init__(self):
        super().__init__()
        self._file_path: str = ""

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate arguments for the remove action.

        Args:
            additional_arguments: The remote file path to delete

        Raises:
            ValueError: If the file path is empty
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError("Remove action requires a file path as an argument.")

        # Normalize Windows path to handle single backslashes
        self._file_path = normalize_windows_path(additional_arguments.strip())

        logger.info(f"Target file: {self._file_path}")

    def execute(self, database_context: DatabaseContext) -> bool:
        """
        Execute the remove action.

        Args:
            database_context: The database context containing services

        Returns:
            True if file was successfully deleted; otherwise False
        """
        # First, verify the file exists
        if not self._file_exists(database_context):
            logger.error(f"File does not exist: {self._file_path}")
            return False

        logger.info(f"Deleting file: {self._file_path}")

        # Check if OLE Automation is available
        ole_available = database_context.config_service.set_configuration_option(
            "Ole Automation Procedures", 1
        )

        # Use OLE if available, otherwise xp_cmdshell
        if ole_available:
            logger.info("OLE Automation is available, using OLE method")
            success = self._delete_via_ole(database_context)
        else:
            logger.info("OLE Automation not available, using xp_cmdshell method")
            success = self._delete_via_xpcmdshell(database_context)

        if not success:
            logger.error("Failed to delete file")
            return False

        # Verify deletion
        if self._file_exists(database_context):
            logger.error("File still exists after deletion attempt")
            return False

        logger.success(f"File deleted successfully: {self._file_path}")
        return True

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

    def _delete_via_ole(self, database_context: DatabaseContext) -> bool:
        """
        Delete the file using OLE Automation with Scripting.FileSystemObject.

        Args:
            database_context: The database context

        Returns:
            True if deletion succeeded; otherwise False
        """
        try:
            # Escape single quotes for SQL
            escaped_path = self._file_path.replace("'", "''")

            query = f"""
                DECLARE @ObjectToken INT;
                DECLARE @Result INT;
                DECLARE @ErrorSource NVARCHAR(255);
                DECLARE @ErrorDesc NVARCHAR(255);

                -- Create FileSystemObject
                EXEC @Result = sp_OACreate 'Scripting.FileSystemObject', @ObjectToken OUTPUT;
                IF @Result <> 0
                BEGIN
                    EXEC sp_OAGetErrorInfo @ObjectToken, @ErrorSource OUT, @ErrorDesc OUT;
                    RAISERROR('Failed to create FileSystemObject: %s', 16, 1, @ErrorDesc);
                    RETURN;
                END

                -- Delete the file
                EXEC @Result = sp_OAMethod @ObjectToken, 'DeleteFile', NULL, '{escaped_path}';
                IF @Result <> 0
                BEGIN
                    EXEC sp_OAGetErrorInfo @ObjectToken, @ErrorSource OUT, @ErrorDesc OUT;
                    EXEC sp_OADestroy @ObjectToken;
                    RAISERROR('Failed to delete file {escaped_path}: %s', 16, 1, @ErrorDesc);
                    RETURN;
                END

                -- Destroy object
                EXEC sp_OADestroy @ObjectToken;
            """

            result = database_context.query_service.execute_non_processing(query)
            if result == -1:
                logger.error("OLE delete failed")
                return False

            logger.info("OLE delete command executed successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to execute OLE delete: {e}")
            return False

    def _delete_via_xpcmdshell(self, database_context: DatabaseContext) -> bool:
        """
        Delete the file using xp_cmdshell with 'del' command.

        Args:
            database_context: The database context

        Returns:
            True if deletion command executed successfully; otherwise False
        """
        # Enable xp_cmdshell if needed
        if not database_context.config_service.set_configuration_option(
            "xp_cmdshell", 1
        ):
            logger.error("Failed to enable xp_cmdshell")
            return False

        try:
            # Escape single quotes for SQL
            escaped_path = self._file_path.replace("'", "''")

            # Use del command to remove the file
            # /Q = quiet mode (don't ask for confirmation)
            # /F = force deletion of read-only files
            query = f"EXEC master..xp_cmdshell 'del /Q /F \"{escaped_path}\"'"

            result = database_context.query_service.execute_non_processing(query)

            if result == -1:
                return False

            logger.info("xp_cmdshell delete command executed successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to execute delete command: {e}")
            return False

    def get_arguments(self) -> list[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return ["Remote file path to delete"]
