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
    aliases=["cat"],
)
class FileRead(BaseAction):
    """
    Reads file content from the target SQL Server using OPENROWSET BULK.
    Requires ADMINISTER BULK OPERATIONS or ADMINISTER DATABASE BULK OPERATIONS permission.

    Flags:
      --base64 / -b   Return file content as a Base64-encoded string instead of plain text.
                      Useful for binary files or files with characters that break the console.
    """

    def __init__(self):
        super().__init__()
        self._file_path: str = ""
        self._base64: bool = False

    def validate_arguments(self, additional_arguments: str = "") -> None:
        """
        Validate and bind the arguments passed to the FileRead action.

        Args:
            additional_arguments: The argument string to parse

        Raises:
            ValueError: If the file path is empty
        """
        named_args, positional_args = self._parse_action_arguments(additional_arguments)

        if len(positional_args) >= 1:
            self._file_path = positional_args[0]
        else:
            self._file_path = ""

        if not self._file_path:
            raise ValueError(
                "File path is required. Example: read C:\\\\temp\\\\data.txt"
            )

        # Normalize Windows path to handle single backslashes
        self._file_path = normalize_windows_path(self._file_path)

        # Base64 flag
        if "base64" in named_args or "b" in named_args:
            self._base64 = True

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
            escaped_path = self._file_path.replace("'", "''")

            if self._base64:
                # Return raw bytes encoded as Base64 (works for any file type)
                query = f"""
SELECT CAST('' AS XML).value(
    'xs:base64Binary(sql:column("B"))', 'VARCHAR(MAX)'
) AS B64
FROM (
    SELECT A AS B
    FROM OPENROWSET(BULK '{escaped_path}', SINGLE_BLOB) AS R(A)
) AS T;"""
                file_content = database_context.query_service.execute_scalar(query)
            else:
                # SINGLE_NCLOB handles both ANSI and Unicode text files
                query = f"SELECT A FROM OPENROWSET(BULK '{escaped_path}', SINGLE_NCLOB) AS R(A);"
                file_content = database_context.query_service.execute_scalar(query)

            if file_content is None:
                return None

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
        return ["<file_path>", "[-b|--base64]"]
