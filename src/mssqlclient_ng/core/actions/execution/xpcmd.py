# mssqlclient_ng/core/actions/execution/xpcmd.py

# Built-in imports
from typing import Optional, List

# Third-party imports
from loguru import logger

# Local imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext


@ActionFactory.register("xpcmd", "Execute operating system commands via xp_cmdshell")
class XpCmd(BaseAction):
    """
    Execute operating system commands on the SQL Server using xp_cmdshell.

    This action automatically enables xp_cmdshell if it's disabled, executes the
    provided command, and returns the output line by line.
    """

    def __init__(self):
        super().__init__()
        self._command: str = ""

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate that a command is provided.

        Args:
            additional_arguments: Command to execute

        Raises:
            ValueError: If no command is provided
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError("xpcmd action requires a command to execute.")

        self._command = additional_arguments.strip()

    def execute(self, database_context: DatabaseContext) -> Optional[List[str]]:
        """
        Execute the provided shell command on the SQL Server using xp_cmdshell.

        Args:
            database_context: The database context containing QueryService and ConfigService

        Returns:
            A list of strings containing the command output, or None on error
        """
        logger.info(f"Executing command: {self._command}")

        # Ensure 'xp_cmdshell' is enabled
        if not database_context.config_service.set_configuration_option(
            "xp_cmdshell", 1
        ):
            logger.error("Failed to enable 'xp_cmdshell'.")
            return None

        # Escape single quotes in command
        escaped_command = self._command.replace("'", "''")
        query = f"EXEC master..xp_cmdshell '{escaped_command}'"

        output_lines: List[str] = []

        try:
            result = database_context.query_service.execute(query, tuple_mode=True)

            if result:
                print()
                for row in result:
                    output = row.pop(0).strip()
                    if output and output.upper() != "NULL":
                        print(output)
                        output_lines.append(output)

                return output_lines

            logger.warning("The command executed but returned no results.")
            return output_lines

        except Exception as ex:
            # Handle specific xp_cmdshell proxy account error
            error_message = str(ex)
            if (
                "xp_cmdshell_proxy_account" in error_message
                or "proxy account" in error_message
            ):
                logger.error("xp_cmdshell proxy account is not configured or invalid.")
                logger.error(
                    "  1. SQL Server service account lacks permissions to execute the command"
                )
                logger.error("  2. No xp_cmdshell proxy credential is configured")
            else:
                logger.error(f"Error executing xp_cmdshell: {ex}")

            return None

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return ["Operating system command to execute via xp_cmdshell"]
