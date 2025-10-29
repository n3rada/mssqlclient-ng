# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from mssqlclientng.src.actions.base import BaseAction
from mssqlclientng.src.actions.factory import ActionFactory
from mssqlclientng.src.services.database import DatabaseContext
from mssqlclientng.src.utils import formatter
from mssqlclientng.src.utils import common


@ActionFactory.register(
    "domsid", "Retrieve the domain SID using SUSER_SID and DEFAULT_DOMAIN"
)
class DomainSid(BaseAction):
    """
    Retrieves the domain SID using SUSER_SID and DEFAULT_DOMAIN functions.

    Queries a known group (Domain Admins) to obtain the domain SID,
    then strips the trailing RID to get the domain SID prefix.
    """

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        No additional arguments required for this action.

        Args:
            additional_arguments: Ignored.
        """
        pass

    def execute(self, database_context: DatabaseContext) -> Optional[dict]:
        """
        Executes the domain SID retrieval.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            Dictionary containing domain and SID information.
        """
        logger.info("Retrieving domain SID")

        try:
            # 1) Get the default domain
            domain_result = database_context.query_service.execute_table(
                "SELECT DEFAULT_DOMAIN();"
            )

            if not domain_result:
                logger.error(
                    "Could not determine DEFAULT_DOMAIN(). The server may not be domain-joined."
                )
                return None

            domain = next(iter(domain_result[0].values()))
            if not domain:
                logger.error(
                    "DEFAULT_DOMAIN() returned NULL. Server may not be domain-joined."
                )
                return None

            logger.info(f"Domain: {domain}")

            # 2) Obtain the domain SID by querying a known group (Domain Admins)
            sid_result = database_context.query_service.execute(
                f"SELECT SUSER_SID('{domain}\\Domain Admins');"
            )[0][""]

            if not sid_result:
                logger.error(
                    "Could not obtain domain SID via SUSER_SID(). "
                    "Ensure the server has access to the domain."
                )
                return None

            # Parse the binary SID
            domain_sid_string = common.sid_bytes_to_string(sid_result)
            if not domain_sid_string:
                logger.error("Failed to parse domain SID")
                return None

            # Strip the trailing RID to get the domain SID prefix
            last_dash = domain_sid_string.rfind("-")
            if last_dash <= 0:
                logger.error(f"Unexpected SID format: {domain_sid_string}")
                return None

            domain_sid_prefix = domain_sid_string[:last_dash]

            logger.success("Domain SID information retrieved")

            # Create result dictionary
            result = {
                "Domain": domain,
                "Full SID (Domain Admins)": domain_sid_string,
                "Domain SID": domain_sid_prefix,
            }

            # Display as markdown table
            print(formatter.dict_to_markdown_table(result, "Property", "Value"))

            return result

        except Exception as e:
            logger.error(f"Failed to retrieve domain SID: {e}")
            raise

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            Empty list as no arguments are required.
        """
        return []
