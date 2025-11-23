# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from mssqlclient_ng.src.actions.base import BaseAction
from mssqlclient_ng.src.actions.factory import ActionFactory
from mssqlclient_ng.src.services.database import DatabaseContext
from mssqlclient_ng.src.utils.common import normalize_windows_path


@ActionFactory.register(
    "read", "Read file content from the target server using OPENROWSET"
)
class FileRead(BaseAction):
    """
    Reads file content from the target SQL Server using OPENROWSET BULK.
    Requires BULK INSERT permissions or similar privileges.
    """

    def __init__(self):
        super().__init__()
        self._file_path: str = ""

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validates that a file path has been provided.

        Args:
            additional_arguments: The file path to read.

        Raises:
            ValueError: If the file path is empty.
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError("Read action requires a file path as an argument.")

        # Normalize Windows path to handle single backslashes
        self._file_path = normalize_windows_path(additional_arguments.strip())

    def execute(self, database_context: DatabaseContext) -> Optional[str]:
        """
        Executes the Read action to fetch the content of a file using OPENROWSET.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            The file content as a string.
        """
        logger.info(f"Reading file: {self._file_path}")

        # Escape single quotes in file path for SQL
        escaped_path = self._file_path.replace("'", "''")

        # Use OPENROWSET BULK to read file content
        query = f"""
            SELECT A FROM OPENROWSET(BULK '{escaped_path}', SINGLE_CLOB) AS R(A);
        """

        try:
            file_content = database_context.query_service.execute_scalar(query)

            if file_content is None:
                logger.warning("File is empty or query returned NULL")
                return None

            file_content_str = file_content.decode("utf-8", errors="replace")

            logger.success(f"Successfully read {len(file_content_str)} bytes")

            # Print file content directly to stdout
            print()
            print(file_content_str)

            return file_content_str

        except Exception as e:
            logger.error(f"Failed to read file '{self._file_path}': {e}")
            raise

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            List containing the expected argument name.
        """
        return ["file_path"]
