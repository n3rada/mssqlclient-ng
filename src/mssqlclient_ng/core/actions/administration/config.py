# Standard library imports
from typing import Optional, List

# Third-party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


@ActionFactory.register("config", "Configure SQL Server options or list configurations")
class Config(BaseAction):
    """
    Configure SQL Server options using sp_configure or list configurations.

    Modes:
    1. List all configurations: config
    2. Check specific option: config xp_cmdshell
    3. Set option: config xp_cmdshell 1

    Usage:
        config                      # List all configuration options
        config xp_cmdshell          # Check status of xp_cmdshell
        config xp_cmdshell 1        # Enable xp_cmdshell
        config xp_cmdshell 0        # Disable xp_cmdshell
    """

    def __init__(self):
        super().__init__()
        self._option_name: Optional[str] = None
        self._value: int = -1

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate arguments for configuration action.

        Args:
            additional_arguments: Optional "[option_name] [value]"
                                 where value is 0 (disable) or 1 (enable)

        Raises:
            ValueError: If arguments are invalid
        """
        if not additional_arguments or not additional_arguments.strip():
            # No arguments = list all configurations
            return

        # Parse both positional and named arguments
        named_args, positional_args = self._parse_action_arguments(
            additional_arguments.strip()
        )

        # Parse option name (optional)
        self._option_name = positional_args[0] if positional_args else None
        if not self._option_name:
            self._option_name = named_args.get("option") or named_args.get("o")

        # Parse value (optional)
        value_str = positional_args[1] if len(positional_args) > 1 else "-1"
        if value_str == "-1":
            value_str = named_args.get("value", named_args.get("v", "-1"))

        try:
            self._value = int(value_str)
        except ValueError:
            raise ValueError(f"Invalid value: {value_str}. Must be a number.")

        # Validation
        if self._value < -1:
            raise ValueError("Invalid value for configuration option")

        if self._option_name and self._value >= 0 and self._value not in [0, 1]:
            raise ValueError("Invalid value. Use 1 to enable or 0 to disable.")

    def execute(self, database_context: DatabaseContext) -> Optional[object]:
        """
        Execute the configuration action.

        Args:
            database_context: The database context containing config_service

        Returns:
            Status or list of configurations
        """
        # Mode 1: Set configuration option
        if self._value >= 0 and self._option_name:
            logger.info(f"Setting {self._option_name} to {self._value}")
            database_context.config_service.set_configuration_option(
                self._option_name, self._value
            )
            return None

        # Mode 2: Show specific option status
        if self._option_name and self._value < 0:
            logger.info(f"Checking status of '{self._option_name}'")
            status = database_context.config_service.get_configuration_status(
                self._option_name
            )

            if status < 0:
                logger.warning(
                    f"Configuration '{self._option_name}' not found or inaccessible"
                )
                return None

            logger.info(
                f"{self._option_name}: {'Enabled' if status == 1 else 'Disabled'}"
            )
            return status

        # Mode 3: List all security-sensitive configurations
        logger.info("Listing all configuration options")
        results = self._check_configuration_options(database_context)

        if results:
            print()
            self._display_results(results)
            return results

        logger.warning("No configuration information could be retrieved")
        return None

    def _check_configuration_options(
        self, database_context: DatabaseContext
    ) -> List[dict]:
        """
        Checks all configuration options.

        Args:
            database_context: The database context

        Returns:
            List of configuration dictionaries
        """
        results = []

        try:
            # Fetch all configurations at once
            query = "SELECT name, value_in_use FROM sys.configurations ORDER BY name;"
            configs_table = database_context.query_service.execute_table(query)

            for row in configs_table:
                name = row["name"]
                status = int(row["value_in_use"])

                results.append(
                    {
                        "Option": name,
                        "Value": str(status),
                        "Enabled": "True" if status == 1 else "False",
                    }
                )
        except Exception as ex:
            logger.warning(f"Could not retrieve configuration options: {ex}")

        return results

    def _display_results(self, results: List[dict]) -> None:
        """
        Displays the results in a formatted table.

        Args:
            results: List of configuration dictionaries
        """
        print(OutputFormatter.convert_list_of_dicts(results))

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return ["[option_name] [value]"]
