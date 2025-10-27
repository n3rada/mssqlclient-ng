# Standard library imports
from typing import Optional, List

# Third-party imports
from loguru import logger

# Local library imports
from mssqlclient_ng.src.actions.base import BaseAction
from mssqlclient_ng.src.actions.factory import ActionFactory


@ActionFactory.register("configure", "Configure SQL Server options (xp_cmdshell, etc.)")
class Configure(BaseAction):
    """
    Configure SQL Server options using sp_configure.

    Allows enabling or disabling advanced options like xp_cmdshell, OLE Automation, etc.

    Usage:
        configure xp_cmdshell 1      # Enable xp_cmdshell
        configure xp_cmdshell 0      # Disable xp_cmdshell
    """

    def __init__(self):
        super().__init__()
        self._option_name: Optional[str] = None
        self._state: Optional[int] = None

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate that option name and state are provided.

        Args:
            additional_arguments: Must be in format: "option_name state"
                                 where state is 0 (disable) or 1 (enable)

        Raises:
            ValueError: If arguments are invalid
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError("Configure action requires option name and state.")

        parts = self.split_arguments(additional_arguments)

        if len(parts) != 2:
            raise ValueError(
                "Invalid arguments. Usage: configure <option_name> <state>\n"
                "Example: configure xp_cmdshell 1\n"
                "State: 1 = enable, 0 = disable"
            )

        self._option_name = parts[0]

        # Validate and parse the state
        try:
            self._state = int(parts[1])
            if self._state not in [0, 1]:
                raise ValueError(
                    f"Invalid state value: {self._state}. Use 1 to enable or 0 to disable."
                )
        except ValueError as e:
            raise ValueError(
                f"Invalid state value: {parts[1]}. Must be 0 (disable) or 1 (enable).\n{e}"
            )

    def execute(self, database_context=None) -> Optional[object]:
        """
        Execute the configuration change.

        Args:
            database_context: The database context containing config_service

        Returns:
            None
        """

        config_service = database_context.config_service
        state_str = "enable" if self._state == 1 else "disable"

        logger.info(f"Setting {self._option_name} to {state_str}")

        if config_service.set_configuration_option(self._option_name, self._state):
            logger.success(f"Successfully {state_str}d {self._option_name}")
            return True

        logger.error(f"Failed to configure {self._option_name}")
        return False

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return [
            "option_name: Name of the configuration option (e.g., xp_cmdshell, OLE Automation) (required)",
            "state: 1 to enable, 0 to disable (required)",
        ]
