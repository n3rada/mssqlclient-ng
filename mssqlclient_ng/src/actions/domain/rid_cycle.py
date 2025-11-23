# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from mssqlclient_ng.src.actions.base import BaseAction
from mssqlclient_ng.src.actions.domain.domsid import DomainSid
from mssqlclient_ng.src.actions.factory import ActionFactory
from mssqlclient_ng.src.services.database import DatabaseContext
from mssqlclient_ng.src.utils import formatter


DEFAULT_MAX_RID = 10000
BATCH_SIZE = 1000


@ActionFactory.register("ridcycle", "Enumerate domain accounts by cycling through RIDs")
class RidCycle(BaseAction):
    """
    RID enumeration via cycling through RIDs using SUSER_SNAME(SID_BINARY('S-...-RID')).

    Enumerates domain objects (users and groups), not group membership.
    Supports output formats: default (table), bash, python
    """

    def __init__(self):
        super().__init__()
        self._max_rid: int = DEFAULT_MAX_RID
        self._bash_output: bool = False
        self._python_output: bool = False

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validates the arguments for RID cycling.

        Args:
            additional_arguments: Optional max RID and/or output format (bash/python/py)

        Raises:
            ValueError: If arguments are invalid.
        """
        if not additional_arguments or not additional_arguments.strip():
            return

        parts = additional_arguments.strip().split()

        for part in parts:
            arg = part.strip().lower()

            if arg == "bash":
                self._bash_output = True
            elif arg in ("python", "py"):
                self._python_output = True
            elif arg.isdigit():
                max_rid = int(arg)
                if max_rid > 0:
                    self._max_rid = max_rid
                else:
                    raise ValueError(f"Max RID must be positive: {arg}")
            else:
                raise ValueError(
                    f"Invalid argument: {arg}. Use a positive integer for max RID, "
                    "'bash' for bash output, or 'python'/'py' for Python output."
                )

        # Both cannot be enabled at the same time
        if self._bash_output and self._python_output:
            raise ValueError(
                "Cannot use both 'bash' and 'python' output formats simultaneously. Choose one."
            )

    def execute(self, database_context: DatabaseContext) -> Optional[list[dict]]:
        """
        Executes the RID cycling enumeration.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            List of discovered domain accounts.
        """
        logger.info(f"Starting RID cycling (max RID: {self._max_rid})")
        logger.info(
            "Note: This enumerates domain objects (users and groups), not group membership"
        )
        logger.info(
            "Use 'groupmembers DOMAIN\\GroupName' to see members of a specific group"
        )

        entries = []

        try:
            # Use DomainSid action to get domain SID information
            domain_sid_action = DomainSid()
            domain_sid_action.validate_arguments(None)

            domain_info = domain_sid_action.execute(database_context)

            if domain_info is None:
                logger.error(
                    "Failed to retrieve domain SID. Cannot proceed with RID cycling"
                )
                return entries

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
                sid_queries = [
                    f"SELECT SUSER_SNAME(SID_BINARY(N'{domain_sid_prefix}-{i:d}'))"
                    for i in range(start, start + sids_to_check)
                ]
                sql = ";".join(sid_queries)

                try:
                    raw_output = database_context.query_service.execute_table(sql)

                    for result_index, item in enumerate(raw_output):
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

                        entries.append(
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
                f"RID cycling completed. Found {found_count} domain accounts"
            )

            # Print results
            if entries:
                self._print_results(entries)

        except Exception as e:
            logger.error(f"RID enumeration failed: {e}")
            raise

        return entries

    def _print_results(self, results: list[dict]) -> None:
        """
        Prints the results in the specified format.

        Args:
            results: List of discovered accounts.
        """
        if self._bash_output:
            # Output in bash associative array format
            logger.info("Bash associative array format")
            print()
            print("declare -A rid_users=(")

            for entry in results:
                rid = entry["RID"]
                username = entry["Username"]
                # Escape single quotes in username
                username = username.replace("'", "'\\''")
                print(f"  [{rid}]='{username}'")

            print(")")
            print()
            print("# Usage example:")
            print('# for rid in "${!rid_users[@]}"; do')
            print('#   echo "RID: $rid - User: ${rid_users[$rid]}"')
            print("# done")

        elif self._python_output:
            # Output in Python dictionary format
            logger.info("Python dictionary format")
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
            print()
            print("# Usage example:")
            print("# for rid, username in rid_users.items():")
            print('#     print(f"RID: {rid} - User: {username}")')
            print("#")
            print("# # Direct lookup:")
            print(
                "# print(f\"User with RID 1001: {rid_users.get(1001, 'Not found')}\")"
            )

        else:
            # Standard markdown table output
            print(formatter.rows_to_markdown_table(results))

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            List of argument descriptions.
        """
        return ["[max_rid] [bash|python|py]"]
