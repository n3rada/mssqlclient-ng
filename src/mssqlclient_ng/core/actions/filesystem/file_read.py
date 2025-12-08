# mssqlclient_ng/core/actions/filesystem/file_read.py

# Built-in imports
from typing import Optional, List

# Third-party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.common import normalize_windows_path


@ActionFactory.register(
    "read",
    "Read file content from the target server using OPENROWSET",
)
class FileRead(BaseAction):
    """
    Reads file content from the target SQL Server using OPENROWSET BULK.
    Requires ADMINISTER BULK OPERATIONS or ADMINISTER DATABASE BULK OPERATIONS permission.
    """

    def __init__(self):
        super().__init__()
        self._file_path: str = ""

    def validate_arguments(self, args: List[str]) -> bool:
        """
        Validate and bind the arguments passed to the FileRead action.

        Args:
            args: List of command line arguments

        Returns:
            bool: True if validation succeeds

        Raises:
            ValueError: If the file path is empty
        """
        named_args, positional_args = self._parse_action_arguments(args)

        if len(positional_args) >= 1:
            self._file_path = positional_args[0]
        else:
            self._file_path = ""

        if not self._file_path:
            raise ValueError(
                "File path is required. Example: fileread C:\\\\temp\\\\data.txt"
            )

        # Normalize Windows path to handle single backslashes
        self._file_path = normalize_windows_path(self._file_path)

        return True

    def execute(self, database_context: DatabaseContext) -> Optional[str]:
        """
        Execute the Read action to fetch the content of a file using OPENROWSET BULK.

        Args:
            database_context: The DatabaseContext instance to execute the query

        Returns:
            The file content as a string, or None on error
        """
        logger.info(f"Reading file: {self._file_path}")

        try:
            # Escape single quotes in file path for SQL
            escaped_path = self._file_path.replace("'", "''")

            # Use OPENROWSET BULK to read file content
            query = (
                f"SELECT A FROM OPENROWSET(BULK '{escaped_path}', SINGLE_CLOB) AS R(A);"
            )

            file_content = database_context.query_service.execute_scalar(query)

            if file_content is None:
                return None

            # Convert to string if needed
            file_content_str = str(file_content) if file_content else ""

            print(file_content_str)

            return file_content_str

        except Exception as ex:
            logger.error(f"Failed to read file: {ex}")
            return None

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return ["file_path"]
