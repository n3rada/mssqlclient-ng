"""
Retrieves the current user's SID using SUSER_SID() function.
"""

from typing import Optional, Dict, List
from loguru import logger

from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatter import OutputFormatter
from ...utils.common import sid_bytes_to_string


@ActionFactory.register(
    "adsid",
    "Retrieves the current user's SID using SUSER_SID() function",
)
class AdSid(BaseAction):
    """
    Retrieves the current user's SID using SUSER_SID() function.
    Also extracts domain SID and RID if the user is a domain account.
    """

    def __init__(self):
        """Initialize the AdSid action."""
        super().__init__()

    def validate_arguments(self, args: List[str]) -> bool:
        """
        Validate arguments (no arguments required).

        Args:
            args: List of command line arguments

        Returns:
            bool: True if validation succeeds
        """
        # No additional arguments needed
        return True

    def execute(self, db_context: DatabaseContext) -> Optional[Dict[str, str]]:
        """
        Execute the user SID retrieval action.

        Args:
            db_context: Database context with connection and services

        Returns:
            Optional[Dict[str, str]]: Dictionary with user SID information or None if failed
        """
        logger.info("Retrieving current user's SID")

        try:
            system_user = db_context.user_service.system_user
            logger.info(f"System User: {system_user}")

            # Escape single quotes to prevent SQL injection
            escaped_user = system_user.replace("'", "''")

            # Get the user's SID using SUSER_SID()
            query = f"SELECT SUSER_SID('{escaped_user}');"
            dt_sid = db_context.query_service.execute_table(query)

            if not dt_sid or dt_sid[0].get("") is None:
                logger.error("Could not obtain user SID via SUSER_SID().")
                return None

            # Extract the binary SID from the query result
            raw_sid_obj = dt_sid[0].get("")

            # Parse the binary SID
            if isinstance(raw_sid_obj, bytes):
                ad_sid_string = sid_bytes_to_string(raw_sid_obj)
            else:
                logger.error("Unexpected SID format from SUSER_SID() result.")
                return None

            if not ad_sid_string:
                logger.error("Unable to parse user SID from SUSER_SID() result.")
                return None

            # Create result dictionary
            result = {
                "System User": system_user,
                "User SID": ad_sid_string,
            }

            # Extract domain SID and RID if it's a domain account
            # Domain SIDs have format: S-1-5-21-<domain>-<rid>
            # The domain portion consists of three sub-authorities before the RID
            if ad_sid_string.startswith("S-1-5-21-"):
                parts = ad_sid_string.split("-")
                if len(parts) >= 8:  # S-1-5-21-X-Y-Z-RID
                    # Domain SID is everything except the last component (RID)
                    ad_domain = "-".join(parts[:-1])
                    rid = parts[-1]
                    result["Domain SID"] = ad_domain
                    result["RID"] = rid
                else:
                    result["Type"] = "Local or Built-in Account"
            else:
                result["Type"] = "Local or Built-in Account"

            logger.success("User SID information retrieved")
            print(OutputFormatter.convert_dict(result, "Property", "Value"))

            return result

        except Exception as e:
            logger.error(f"Failed to retrieve user SID: {e}")
            return None
