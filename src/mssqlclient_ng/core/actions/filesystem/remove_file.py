# mssqlclient_ng/core/actions/filesystem/remove_file.py

# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction, Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.common import normalize_windows_path


@ActionFactory.register(
    "rm",
    "Delete a file on the SQL Server filesystem via OLE Automation (Scripting.FileSystemObject)",
    aliases=["del", "delete"],
)
class RemoveFile(BaseAction):
    """
    Delete a remote file using OLE Automation Procedures.

    Uses Scripting.FileSystemObject.DeleteFile via sp_OACreate/sp_OAMethod.
    If OLE Automation is disabled, attempts to enable it once before retrying.

    Requires OLE Automation Procedures to be enabled (or ALTER SETTINGS to enable them).
    """

    _file_path = Arg(position=0, required=True, description="Remote file path to delete")

    def validate_arguments(self, additional_arguments: str = "") -> None:
        self._bind_arguments(additional_arguments)
        self._file_path = normalize_windows_path(self._file_path).replace("/", "\\")

    def execute(self, database_context: DatabaseContext) -> Optional[bool]:
        logger.info(f"Deleting remote file: {self._file_path}")

        # Ensure OLE Automation is enabled
        if not database_context.config_service.set_configuration_option(
            "Ole Automation Procedures", 1
        ):
            logger.error(
                "Cannot enable OLE Automation Procedures — "
                "ALTER SETTINGS permission required"
            )
            return False

        success = self._delete_via_ole(database_context)

        if success:
            logger.success("File deleted")
        return success

    def _delete_via_ole(self, database_context: DatabaseContext) -> bool:
        escaped_path = self._file_path.replace("'", "''")

        query = f"""
DECLARE @ObjectToken INT;
DECLARE @Result INT;
DECLARE @ErrorSource NVARCHAR(255);
DECLARE @ErrorDesc NVARCHAR(255);

EXEC @Result = sp_OACreate 'Scripting.FileSystemObject', @ObjectToken OUTPUT;
IF @Result <> 0
BEGIN
    EXEC sp_OAGetErrorInfo @ObjectToken, @ErrorSource OUT, @ErrorDesc OUT;
    RAISERROR('Failed to create FileSystemObject: %s', 16, 1, @ErrorDesc);
    RETURN;
END

EXEC @Result = sp_OAMethod @ObjectToken, 'DeleteFile', NULL, '{escaped_path}';
IF @Result <> 0
BEGIN
    EXEC sp_OAGetErrorInfo @ObjectToken, @ErrorSource OUT, @ErrorDesc OUT;
    EXEC sp_OADestroy @ObjectToken;
    RAISERROR('Failed to delete file: %s', 16, 1, @ErrorDesc);
    RETURN;
END

EXEC sp_OADestroy @ObjectToken;"""

        try:
            database_context.query_service.execute_non_processing(query)
            return True
        except Exception as ex:
            logger.error(f"Deletion failed: {ex}")
            return False

    def get_arguments(self) -> list:
        return ["<remote_file_path>"]
