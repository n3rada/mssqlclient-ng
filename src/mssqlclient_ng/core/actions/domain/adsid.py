# mssqlclient_ng/core/actions/domain/adsid.py

# Built-in imports
from typing import Optional, Dict

# Third-party imports
from loguru import logger


# Local imports
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

    def validate_arguments(self, additional_arguments: str = "") -> None:
        """
        Validate arguments (no arguments required).

        Args:
            additional_arguments: The argument string to parse (unused)
        """
        # No additional arguments needed
        pass

    def execute(self, database_context: DatabaseContext) -> Optional[Dict[str, str]]:
        """
        Execute the user SID retrieval action.

        Args:
            database_context: Database context with connection and services

        Returns:
            Optional[Dict[str, str]]: Dictionary with user SID information or None if failed
        """
        logger.info("Retrieving current user's SID")

        try:
            # Get the user's SID using SUSER_SID()
            # Convert to VARCHAR with style 1 to get hex string format (0x...)
            # This ensures the data isn't corrupted when crossing linked server boundaries
            query = "SELECT CONVERT(VARCHAR(200), SUSER_SID(), 1) AS SID;"
            dt_sid = database_context.query_service.execute_table(query)

            logger.trace(f"SUSER_SID() query result: {dt_sid}")

            if not dt_sid or not dt_sid[0]:
                logger.error("Could not obtain user SID via SUSER_SID().")
                return None

            # Extract the SID from the query result (first column value)
            raw_sid_obj = next(iter(dt_sid[0].values())) if dt_sid[0] else None
            
            if raw_sid_obj is None or raw_sid_obj == "NULL":
                logger.error("SUSER_SID() returned NULL.")
                return None

            logger.trace(f"Raw SID object: {raw_sid_obj} (type: {type(raw_sid_obj)})")

            # Parse the SID - should now be in hex string format like '0x01050000...'
            if isinstance(raw_sid_obj, str):
                # Hex string format from CONVERT (e.g., '0x01050000...')
                try:
                    # Remove '0x' prefix and convert to hex bytes for sid_bytes_to_string
                    hex_str = raw_sid_obj
                    if hex_str.startswith('0x') or hex_str.startswith('0X'):
                        hex_str = hex_str[2:]
                    
                    # Convert to ASCII bytes format that sid_bytes_to_string expects
                    hex_bytes = hex_str.encode('ascii')
                    ad_sid_string = sid_bytes_to_string(hex_bytes)
                except Exception as e:
                    logger.error(f"Failed to parse SID from hex string: {e}")
                    logger.debug(f"Raw SID string: {raw_sid_obj}")
                    return None
            elif isinstance(raw_sid_obj, bytes):
                # Fallback: direct bytes (shouldn't happen with CONVERT, but handle it)
                try:
                    # Assume it's hex ASCII representation
                    ad_sid_string = sid_bytes_to_string(raw_sid_obj)
                except Exception as e:
                    logger.error(f"Failed to parse SID from bytes: {e}")
                    logger.debug(f"Raw SID bytes (hex): {raw_sid_obj.hex()}")
                    return None
            else:
                logger.error(f"Unexpected SID format from SUSER_SID() result: {type(raw_sid_obj)}")
                return None

            if not ad_sid_string:
                logger.error("Unable to parse user SID from SUSER_SID() result.")
                return None

            # Create result dictionary
            result = {
                "System User": database_context.user_service.system_user,
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
