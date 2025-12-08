"""
PowerShell action for executing PowerShell scripts via xp_cmdshell.
"""

import base64
from typing import Optional, List
from loguru import logger

from ..execution.xpcmd import XpCmd
from ..factory import ActionFactory
from ...services.database import DatabaseContext


@ActionFactory.register("pwsh", "Execute PowerShell scripts via xp_cmdshell")
class PowerShell(XpCmd):
    """
    Execute PowerShell scripts on the SQL Server using xp_cmdshell.

    This action encodes the PowerShell script in Base64 and executes it using
    the -EncodedCommand parameter, which bypasses execution policy restrictions
    and handles special characters properly.
    """

    def __init__(self):
        super().__init__()
        self._script: Optional[str] = None

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate that a PowerShell script is provided.

        Args:
            additional_arguments: The PowerShell script or command to execute

        Raises:
            ValueError: If no script is provided
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError("PowerShell action requires a script to execute.")

        self._script = additional_arguments.strip()

    def execute(self, database_context: DatabaseContext) -> Optional[List[str]]:
        """
        Execute the provided PowerShell script on the SQL Server using xp_cmdshell.

        The script is Base64-encoded and executed using PowerShell's -EncodedCommand
        parameter to avoid issues with special characters and execution policies.

        Args:
            database_context: The database context containing QueryService and ConfigService

        Returns:
            A list of strings containing the command output, or None on error
        """
        logger.info(f"Executing PowerShell script: {self._script}")

        # Convert the PowerShell script to Base64 encoding
        base64_encoded_script = self._convert_to_base64(self._script)

        # Craft the PowerShell command to execute the Base64-encoded script
        powershell_command = f"powershell.exe -noni -NoLogo -e {base64_encoded_script}"

        # Set the crafted PowerShell command as the _command in the parent class
        super().validate_arguments(powershell_command)

        # Call the parent's execute method to execute the command
        return super().execute(database_context)

    def _convert_to_base64(self, input_string: str) -> str:
        """
        Convert a string to Base64 encoding for PowerShell.

        PowerShell's -EncodedCommand expects UTF-16LE (Unicode) encoding.

        Args:
            input_string: The input string to encode

        Returns:
            The Base64-encoded string
        """
        # PowerShell expects UTF-16LE (Unicode) encoding
        input_bytes = input_string.encode("utf-16le")
        return base64.b64encode(input_bytes).decode("ascii")

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return ["PowerShell script or command to execute"]
