# mssqlclient_ng/core/actions/remote/adsi_query.py

# Built-in imports
from typing import Optional, List
from loguru import logger

# Local library imports
from ...utils.common import generate_random_string
from ...utils.formatters import OutputFormatter

from ...services.database import DatabaseContext
from ...services.adsi import AdsiService

from ..base import BaseAction
from ..factory import ActionFactory


@ActionFactory.register(
    "adsi-query", "Perform LDAP queries against ADSI linked servers"
)
class AdsiQuery(BaseAction):
    """
    Performs LDAP queries against ADSI linked servers.
    Allows querying Active Directory objects through SQL Server's OPENQUERY.
    """

    def __init__(self):
        super().__init__()
        self._adsi_server_name: Optional[str] = None
        self._ldap_query: Optional[str] = None
        self._preset: str = "users"
        self._using_temp_server: bool = False
        self._domain_fqdn: Optional[str] = None

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate arguments for the ADSI query action.

        Args:
            additional_arguments: Query parameters
                Format: <domain_fqdn> [adsi_server_name] [preset|ldap_query]
                Presets: users, computers, groups, admins, ou, all

        Raises:
            ValueError: If arguments are invalid
        """
        if not additional_arguments or not additional_arguments.strip():
            raise ValueError(
                "Domain FQDN is required. Example: /a:adsiquery domain.local users"
            )

        parts = self.split_arguments(additional_arguments)

        # First argument is always the domain FQDN
        self._domain_fqdn = parts[0]

        # Check if there's a second argument
        if len(parts) < 2:
            # Only domain provided, use temp server with default preset
            self._using_temp_server = True
            self._preset = "users"
            return

        # Check if second argument is a preset (means no server name provided)
        if self._is_preset(parts[1].lower()):
            # No server name provided, use temporary server
            self._using_temp_server = True
            self._preset = parts[1].lower()

            # Check if there's a custom query after the preset
            if len(parts) > 2:
                self._preset = "custom"
                self._ldap_query = " ".join(parts[2:])
        else:
            # Second argument is the ADSI server name
            self._adsi_server_name = parts[1]

            # Third argument can be either a preset or a custom LDAP query
            if len(parts) > 2:
                third_arg = parts[2].lower()

                # Check if it's a preset
                if self._is_preset(third_arg):
                    self._preset = third_arg
                else:
                    # Treat as custom LDAP query
                    self._preset = "custom"
                    self._ldap_query = parts[2]

            # If more arguments, combine them as the LDAP query (for multi-word queries)
            if len(parts) > 3 and self._preset == "custom":
                self._ldap_query = " ".join(parts[2:])

    def _is_preset(self, arg: str) -> bool:
        """Check if argument is a valid preset."""
        return arg in ["users", "computers", "groups", "admins", "ou", "all"]

    def execute(self, database_context: DatabaseContext) -> bool:
        """
        Execute the ADSI query action.

        Args:
            database_context: The database context containing services

        Returns:
            True if query succeeded; otherwise False
        """
        adsi_service = AdsiService(database_context)
        cleanup_required = False

        try:
            # Handle temporary ADSI server creation if needed
            if self._using_temp_server:
                self._adsi_server_name = f"ADSI-{generate_random_string(6)}"

                logger.info(
                    f"Creating temporary ADSI server '{self._adsi_server_name}'"
                )

                if not adsi_service.create_adsi_linked_server(self._adsi_server_name):
                    logger.error("Failed to create temporary ADSI server")
                    return False

                logger.success(
                    f"Temporary ADSI server '{self._adsi_server_name}' created"
                )
                cleanup_required = True
            else:
                # Verify the specified ADSI server exists
                if not adsi_service.adsi_server_exists(self._adsi_server_name):
                    logger.error(
                        f"ADSI linked server '{self._adsi_server_name}' not found"
                    )
                    logger.info(
                        "Use '/a:adsi list' to see available ADSI servers or omit the server name to create a temporary one"
                    )
                    return False

            logger.info(f"Querying ADSI server '{self._adsi_server_name}'")

            # Build the LDAP query based on preset or custom input
            ldap_query = (
                self._ldap_query
                if self._preset == "custom"
                else self._build_preset_query(database_context)
            )

            logger.info(f"Preset: {self._preset}")
            logger.info(f"LDAP Query: {ldap_query}")

            # Execute the OPENQUERY against the ADSI server
            escaped_query = self._escape_single_quotes(ldap_query)
            query = f"SELECT * FROM OPENQUERY([{self._adsi_server_name}], '{escaped_query}')"

            result = database_context.query_service.execute_table(query)

            if not result:
                logger.warning("No results found")
                return False

            plural = "s" if len(result) > 1 else ""
            logger.success(f"Retrieved {len(result)} result{plural}")

            # Display the results
            table = OutputFormatter.convert_list_of_dicts(result)
            print(table)

            return True

        except Exception as e:
            logger.error(f"Failed to execute LDAP query: {e}")

            # Provide helpful error messages
            error_msg = str(e).lower()
            if "access denied" in error_msg or "permission" in error_msg:
                logger.info(
                    "The SQL Server service account may not have permissions to query Active Directory"
                )
            elif "provider" in error_msg and "found" in error_msg:
                logger.info(
                    "The ADSDSOObject provider may not be available on this server"
                )
            elif "syntax" in error_msg:
                logger.info(
                    "Check your LDAP query syntax. Example: "
                    "'SELECT * FROM ''LDAP://DC=domain,DC=local'' WHERE objectClass=''user'''"
                )

            return False

        finally:
            # Cleanup temporary ADSI server if it was created
            if cleanup_required and self._adsi_server_name:
                logger.info(
                    f"Cleaning up temporary ADSI server '{self._adsi_server_name}'"
                )
                try:
                    adsi_service.drop_linked_server(self._adsi_server_name)
                except Exception as e:
                    logger.warning(f"Failed to cleanup temporary server: {e}")

    def _build_preset_query(self, database_context: DatabaseContext) -> str:
        """
        Build an LDAP query based on the selected preset.

        Args:
            database_context: The database context

        Returns:
            LDAP query string
        """
        # Build LDAP path from the provided domain FQDN
        domain = self._get_domain_path()
        domain_dn = domain.replace("LDAP://", "")

        preset_queries = {
            "users": f"SELECT cn, sAMAccountName, distinguishedName, whenCreated FROM '{domain}' WHERE objectClass='user' AND objectCategory='person'",
            "computers": f"SELECT cn, dNSHostName, operatingSystem, operatingSystemVersion FROM '{domain}' WHERE objectClass='computer'",
            "groups": f"SELECT cn, sAMAccountName, distinguishedName, groupType FROM '{domain}' WHERE objectClass='group'",
            "admins": f"SELECT cn, sAMAccountName, distinguishedName FROM '{domain}' WHERE objectClass='user' AND (memberOf='CN=Domain Admins,CN=Users,{domain_dn}' OR memberOf='CN=Enterprise Admins,CN=Users,{domain_dn}')",
            "ou": f"SELECT ou, name, distinguishedName FROM '{domain}' WHERE objectClass='organizationalUnit'",
            "all": f"SELECT * FROM '{domain}'",
        }

        return preset_queries.get(
            self._preset,
            f"SELECT cn, distinguishedName FROM '{domain}' WHERE objectClass='user'",
        )

    def _get_domain_path(self) -> str:
        """
        Convert the user-provided domain FQDN to LDAP path.

        Returns:
            LDAP path string
        """
        # Convert domain FQDN to LDAP path (e.g., SIGNED.LOCAL -> LDAP://DC=SIGNED,DC=LOCAL)
        domain_parts = self._domain_fqdn.split(".")
        ldap_path = "LDAP://" + ",".join([f"DC={part}" for part in domain_parts])
        return ldap_path

    def _escape_single_quotes(self, input_str: Optional[str]) -> str:
        """
        Escape single quotes in LDAP queries for SQL Server.

        Args:
            input_str: Input string to escape

        Returns:
            Escaped string
        """
        if not input_str:
            return ""
        return input_str.replace("'", "''")

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return [
            "Domain FQDN (required, e.g., domain.local)",
            "ADSI server name (optional - creates temporary server if omitted)",
            "LDAP query string or preset (users, computers, groups, admins, ou, all - default: users)",
        ]
