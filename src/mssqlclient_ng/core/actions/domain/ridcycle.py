"""
RID enumeration via cycling through RIDs using SUSER_SNAME(SID_BINARY('S-...-RID')).
"""

from typing import Optional, List, Dict
from loguru import logger

from ..base import BaseAction
from ..domain.addomain import DomainSid
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatter import OutputFormatter


DEFAULT_MAX_RID = 10000
BATCH_SIZE = 1000


@ActionFactory.register(
    "ridcycle",
    "Enumerate domain accounts by cycling through RIDs",
)
class RidCycle(BaseAction):
    """
    RID enumeration via cycling through RIDs using SUSER_SNAME(SID_BINARY('S-...-RID')).
    Enumerates domain objects (users and groups), not group membership.
    """

    def __init__(self):
        super().__init__()
        self._max_rid: int = DEFAULT_MAX_RID
        self._bash_output: bool = False
        self._python_output: bool = False
        self._table_output: bool = False

    def validate_arguments(self, args: List[str]) -> bool:
        """
        Validate arguments for RID cycling.

        Args:
            args: List of command line arguments

        Returns:
            bool: True if validation succeeds

        Raises:
            ValueError: If arguments are invalid
        """
        if not args or len(args) == 0:
            return True

        named_args, positional_args = self._parse_action_arguments(args)

        # Check for --format flag
        if "format" in named_args:
            format_type = named_args["format"].lower()
            if format_type == "bash":
                self._bash_output = True
            elif format_type in ("python", "py"):
                self._python_output = True
            elif format_type == "table":
                self._table_output = True
            else:
                raise ValueError(
                    f"Invalid format: {format_type}. Use 'bash', 'python', or 'table'."
                )

        # First positional argument is max RID
        if len(positional_args) > 0:
            try:
                max_rid = int(positional_args[0])
                if max_rid > 0:
                    self._max_rid = max_rid
                else:
                    raise ValueError(
                        f"Invalid max RID: {positional_args[0]}. Must be a positive integer."
                    )
            except ValueError:
                raise ValueError(
                    f"Invalid max RID: {positional_args[0]}. Must be a positive integer."
                )

        if len(positional_args) > 1:
            raise ValueError(
                "Too many positional arguments. Expected: [maxRid]. Use --format flag for output format."
            )

        return True

    def execute(self, database_context: DatabaseContext) -> Optional[List[Dict]]:
        """
        Execute the RID cycling enumeration.

        Args:
            database_context: Database context with connection and services

        Returns:
            Optional[List[Dict]]: List of discovered domain accounts or None
        """
        logger.info(f"Starting RID cycling (max RID: {self._max_rid})")

        results = []

        try:
            # Use DomainSid action to get domain SID information
            domain_sid_action = DomainSid()
            domain_sid_action.validate_arguments([])

            domain_info = domain_sid_action.execute(database_context)

            if domain_info is None:
                logger.error(
                    "Failed to retrieve domain SID. Cannot proceed with RID cycling."
                )
                return results

            domain = domain_info["Domain"]
            domain_sid_prefix = domain_info["Domain SID"]

            logger.info(f"Target domain: {domain}")
            logger.info(f"Domain SID prefix: {domain_sid_prefix}")

            # Iterate in batches
            found_count = 0
            for start in range(0, self._max_rid + 1, BATCH_SIZE):
                sids_to_check = min(BATCH_SIZE, self._max_rid - start + 1)
                if sids_to_check == 0:
                    break

                # Build semicolon-separated SELECT statements
                queries = []
                for i in range(sids_to_check):
                    rid = start + i
                    queries.append(
                        f"SELECT SUSER_SNAME(SID_BINARY(N'{domain_sid_prefix}-{rid}'))"
                    )

                sql = "; ".join(queries)

                try:
                    # Execute returns data reader which can handle multiple result sets
                    raw_output = database_context.query_service.execute_table(sql)

                    for result_index, item in enumerate(raw_output):
                        # Get the first (and only) column value from the result
                        username = next(iter(item.values())) if item else None

                        # Skip NULL or empty results
                        if not username or username == "NULL":
                            continue

                        found_rid = start + result_index
                        account_name = (
                            username.split("\\")[1] if "\\" in username else username
                        )

                        logger.success(f"RID {found_rid}: {username}")
                        found_count += 1

                        results.append(
                            {
                                "RID": found_rid,
                                "Domain": domain,
                                "Username": account_name,
                                "Full Account": username,
                            }
                        )

                except Exception as ex:
                    logger.warning(
                        f"Batch failed for RIDs {start}-{start + sids_to_check - 1}: {ex}"
                    )
                    continue

            logger.success(
                f"RID cycling completed. Found {found_count} domain accounts."
            )

            # Print results if any found
            if results:
                self._print_results(results)

        except Exception as e:
            logger.error(f"RID enumeration failed: {e}")

        return results

    def _print_results(self, results: List[Dict]) -> None:
        """
        Print the results in the specified format.

        Args:
            results: List of discovered accounts
        """
        if self._bash_output:
            # Output in bash associative array format
            logger.info("Bash associative array format:")
            print()
            print("declare -A rid_users=(")

            for entry in results:
                rid = entry["RID"]
                username = entry["Username"]
                # Escape single quotes in username if present
                username = username.replace("'", "'\\''")
                print(f"  [{rid}]='{username}'")

            print(")")

        elif self._python_output:
            # Output in Python dictionary format
            logger.info("Python dictionary format:")
            print()
            print("rid_users = {")

            for idx, entry in enumerate(results):
                rid = entry["RID"]
                username = entry["Username"]
                # Escape backslashes and single quotes for Python strings
                username = username.replace("\\", "\\\\").replace("'", "\\'")

                comma = "," if idx < len(results) - 1 else ""
                print(f"    {rid}: '{username}'{comma}")

            print("}")

        elif self._table_output:
            # Detailed table output
            print(OutputFormatter.convert_list_of_dicts(results))

        else:
            # Default: simple line-by-line username output (pipe-friendly)
            for entry in results:
                print(entry["Username"])
