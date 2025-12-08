# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter
from ...utils import common


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
            Dictionary containing domain and SID information or None.
        """
        logger.info("Retrieving domain SID")

        try:
            # 1) Get the default domain
            domain_result = database_context.query_service.execute_table(
                "SELECT DEFAULT_DOMAIN();"
            )

            if not domain_result or not domain_result[0][next(iter(domain_result[0]))]:
                logger.error(
                    "Could not determine DEFAULT_DOMAIN(). The server may not be domain-joined."
                )
                return None

            domain = next(iter(domain_result[0].values()))
            logger.info(f"Domain: {domain}")

            # 2) Obtain the domain SID by querying a known group (Domain Admins)
            sid_result_table = database_context.query_service.execute_table(
                f"SELECT SUSER_SID('{domain}\\Domain Admins');"
            )

            if (
                not sid_result_table
                or not sid_result_table[0][next(iter(sid_result_table[0]))]
            ):
                logger.error(
                    "Could not obtain domain SID via SUSER_SID(). "
                    "Ensure the server has access to the domain."
                )
                return None

            # Extract the binary SID from the query result
            raw_sid_obj = next(iter(sid_result_table[0].values()))

            # Parse the binary SID
            ad_domain_string = common.sid_bytes_to_string(raw_sid_obj)

            if not ad_domain_string:
                logger.error("Unable to parse domain SID from SUSER_SID() result.")
                return None

            # Strip the trailing RID to get the domain SID prefix
            last_dash = ad_domain_string.rfind("-")
            if last_dash <= 0:
                logger.error(f"Unexpected SID format: {ad_domain_string}")
                return None

            ad_domain_prefix = ad_domain_string[:last_dash]

            print()
            logger.success("Domain SID information retrieved")

            # Create result dictionary
            result = {
                "Domain": domain,
                "Full SID (Domain Admins)": ad_domain_string,
                "Domain SID": ad_domain_prefix,
            }

            # Display as markdown table
            print(OutputFormatter.convert_dict(result, "Property", "Value"))

            return result

        except Exception as e:
            logger.error(
                f"Failed to retrieve domain SID: {e.message if hasattr(e, 'message') else str(e)}"
            )
            return None

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            Empty list as no arguments are required.
        """
        return []
