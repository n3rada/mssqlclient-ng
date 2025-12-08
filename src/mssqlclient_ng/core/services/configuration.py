# mssqlclient_ng/core/services/configuration.py

# Third-party imports
from loguru import logger

# Local library imports
from ..models.server import Server
from .query import QueryService


class ConfigurationService:
    """
    Service for managing SQL Server configuration options and CLR assemblies.
    Provides methods for configuration, assembly management, and server options.
    """

    def __init__(self, query_service: QueryService, server: Server):
        """
        Initialize the configuration service.

        Args:
            query_service: The query service instance to use for database operations
            server: The Server instance for checking server properties
        """
        self._query_service = query_service
        self._server = server

    def check_assembly(self, assembly_name: str) -> bool:
        """
        Check if a CLR assembly exists in the database.

        Args:
            assembly_name: The name of the assembly to check

        Returns:
            True if the assembly exists; otherwise False
        """
        try:
            query = f"SELECT name FROM sys.assemblies WHERE name='{assembly_name}';"
            result = self._query_service.execute_scalar(query)
            exists = result is not None and str(result) == assembly_name

            if exists:
                logger.success(f"Assembly '{assembly_name}' exists")
            else:
                logger.warning(f"Assembly '{assembly_name}' not found")

            return exists
        except Exception as e:
            logger.error(f"Error checking assembly '{assembly_name}': {e}")
            return False

    def check_assembly_modules(self, assembly_name: str) -> bool:
        """
        Check if a CLR assembly has modules.

        Args:
            assembly_name: The name of the assembly to check

        Returns:
            True if the assembly has modules; otherwise False
        """
        try:
            query = "SELECT * FROM sys.assembly_modules;"
            result_rows = self._query_service.execute_table(query)

            if not result_rows:
                logger.warning("No assembly modules found")
                return False

            # Check if assembly name appears in any module
            for row in result_rows:
                for value in row.values():
                    if value and assembly_name.lower() in str(value).lower():
                        logger.success(f"Assembly '{assembly_name}' has modules")
                        return True

            logger.warning(f"Assembly '{assembly_name}' has no modules")
            return False
        except Exception as e:
            logger.error(f"Error checking assembly modules for '{assembly_name}': {e}")
            return False

    def check_trusted_assembly(self, assembly_name: str) -> bool:
        """
        Check if a CLR assembly is trusted.

        Args:
            assembly_name: The name of the assembly to check

        Returns:
            True if the assembly is trusted; otherwise False
        """
        try:
            query = "SELECT description FROM sys.trusted_assemblies;"
            result_rows = self._query_service.execute_table(query)

            if not result_rows:
                logger.warning("No trusted assemblies found")
                return False

            logger.debug("Trusted assemblies:")
            for row in result_rows:
                description = row.get("description", "")
                logger.debug(f"  {description}")

                # Extract name from description (first part before comma)
                if description:
                    name = description.split(",")[0].strip()
                    if name == assembly_name:
                        logger.success(f"Assembly '{assembly_name}' is trusted")
                        return True

            logger.warning(f"Assembly '{assembly_name}' is not trusted")
            return False
        except Exception as e:
            logger.error(f"Error checking trusted assemblies: {e}")
            return False

    def check_procedure(self, procedure_name: str) -> bool:
        """
        Check if a stored procedure exists.

        Args:
            procedure_name: The name of the procedure to check

        Returns:
            True if the procedure exists; otherwise False
        """
        try:
            query = "SELECT SCHEMA_NAME(schema_id) AS schema_name, name, type FROM sys.procedures;"
            result_rows = self._query_service.execute_table(query)

            if not result_rows:
                logger.warning("No procedures found")
                return False

            logger.debug("Procedures:")
            for row in result_rows:
                name = row.get("name", "")
                logger.debug(f"  {name}")

                if name == procedure_name:
                    logger.success(f"Procedure '{procedure_name}' exists")
                    return True

            logger.warning(f"Procedure '{procedure_name}' does not exist")
            return False
        except Exception as e:
            logger.error(f"Error checking procedures: {e}")
            return False

    def set_configuration_option(self, option_name: str, value: int) -> bool:
        """
        Set a SQL Server configuration option using sp_configure.

        Args:
            option_name: The name of the configuration option to modify
            value: The value to set (e.g., 1 to enable, 0 to disable)

        Returns:
            True if the option was set successfully; otherwise False
        """
        if not self._enable_advanced_options():
            logger.error("Cannot proceed without 'show advanced options' enabled")
            return False

        logger.debug(f"Checking status of '{option_name}'")
        try:
            # Check current configuration value
            # Cast to INT to avoid sql_variant type issues in impacket
            query = f"SELECT CAST(value AS INT) AS value FROM sys.configurations WHERE name = '{option_name}';"
            config_value = self._query_service.execute_scalar(query)

            if config_value is None:
                logger.warning(
                    f"Configuration '{option_name}' not found or inaccessible"
                )
                return False

            if int(config_value) == value:
                logger.info(
                    f"Configuration option '{option_name}' is already set to {value}"
                )
                return True
        except Exception as e:
            logger.error(
                f"Error checking configuration status for '{option_name}': {e}"
            )
            return False

        try:
            logger.info(f"Updating configuration option '{option_name}' to {value}")
            query = f"EXEC master..sp_configure '{option_name}', {value}; RECONFIGURE;"
            self._query_service.execute_non_processing(query)
            logger.success(f"Successfully set '{option_name}' to {value}")
            return True
        except Exception as e:
            logger.warning(f"Failed to set configuration option '{option_name}': {e}")
            return False

    def register_trusted_assembly(
        self, assembly_hash: str, assembly_description: str
    ) -> bool:
        """
        Add a CLR assembly hash to the list of trusted assemblies.

        Args:
            assembly_hash: The SHA-512 hash of the assembly (without '0x' prefix)
            assembly_description: A description of the assembly (e.g., name, version)

        Returns:
            True if the hash was successfully added; otherwise False
        """
        if self._server.version and "2008" in self._server.version:
            logger.warning("CLR hash cannot be added to legacy servers")
            return False

        try:
            # Check if the hash already exists
            check_query = (
                f"SELECT * FROM sys.trusted_assemblies WHERE hash = 0x{assembly_hash};"
            )
            check_result = self._query_service.execute_scalar(check_query)

            if check_result is not None:
                logger.warning("Hash already exists in sys.trusted_assemblies")

                # Attempt to remove the existing hash
                try:
                    delete_query = (
                        f"EXEC master..sp_drop_trusted_assembly 0x{assembly_hash};"
                    )
                    self._query_service.execute_non_processing(delete_query)
                    logger.success("Existing hash removed successfully")
                except Exception as e:
                    if "permission" in str(e).lower():
                        logger.error(
                            "Insufficient privileges to remove existing trusted assembly"
                        )
                    return False

            # Add the new hash to the trusted assemblies
            add_query = f"""
                EXEC master..sp_add_trusted_assembly
                0x{assembly_hash},
                N'{assembly_description}, version=0.0.0.0, culture=neutral, publickeytoken=null, processorarchitecture=msil';
            """
            self._query_service.execute_non_processing(add_query)

            # Verify if the hash was successfully added
            if self.check_trusted_assembly(assembly_description):
                logger.success(f"Added assembly hash '0x{assembly_hash}' as trusted")
                return True

            logger.error("Failed to add hash to sys.trusted_assemblies")
            return False
        except Exception as e:
            logger.error(f"Error adding CLR hash: {e}")
            return False

    def enable_data_access(self, server_name: str) -> bool:
        """
        Enable data access for a SQL Server.

        Args:
            server_name: The name of the server

        Returns:
            True if data access was enabled; otherwise False
        """
        logger.debug(f"Enabling data access on server '{server_name}'")
        try:
            query = (
                f"EXEC master..sp_serveroption '{server_name}', 'DATA ACCESS', TRUE;"
            )
            self._query_service.execute_non_processing(query)

            if self._is_data_access_enabled(server_name):
                logger.success(f"Data access enabled for server '{server_name}'")
                return True

            logger.error(f"Failed to enable data access for server '{server_name}'")
            return False
        except Exception as e:
            logger.error(f"Error enabling data access for server '{server_name}': {e}")
            return False

    def disable_data_access(self, server_name: str) -> bool:
        """
        Disable data access for a SQL Server.

        Args:
            server_name: The name of the server

        Returns:
            True if data access was disabled; otherwise False
        """
        logger.debug(f"Disabling data access on server '{server_name}'")
        try:
            query = (
                f"EXEC master..sp_serveroption '{server_name}', 'DATA ACCESS', FALSE;"
            )
            self._query_service.execute_non_processing(query)

            if not self._is_data_access_enabled(server_name):
                logger.success(f"Data access disabled for server '{server_name}'")
                return True

            logger.error(f"Failed to disable data access for server '{server_name}'")
            return False
        except Exception as e:
            logger.error(f"Error disabling data access for server '{server_name}': {e}")
            return False

    def _is_data_access_enabled(self, server_name: str) -> bool:
        """
        Check if data access is enabled for a server.

        Args:
            server_name: The name of the server

        Returns:
            True if data access is enabled; otherwise False
        """
        try:
            query = f"SELECT CAST(is_data_access_enabled AS INT) AS is_enabled FROM sys.servers WHERE name = '{server_name}';"
            result = self._query_service.execute_scalar(query)

            if result is None:
                logger.warning(f"Server '{server_name}' not found in sys.servers")
                return False

            return int(result) == 1
        except Exception as e:
            logger.error(
                f"Error checking data access status for server '{server_name}': {e}"
            )
            return False

    def drop_dependent_objects(self, assembly_name: str) -> bool:
        """
        Drop all objects dependent on a CLR assembly.

        Args:
            assembly_name: The name of the assembly

        Returns:
            True if all dependent objects were dropped; otherwise False
        """
        try:
            logger.debug(
                f"Identifying dependent objects for assembly '{assembly_name}'"
            )

            query = f"""
                SELECT o.type_desc, o.name
                FROM sys.assembly_modules am
                JOIN sys.objects o ON am.object_id = o.object_id
                WHERE am.assembly_id = (
                    SELECT assembly_id
                    FROM sys.assemblies
                    WHERE name = '{assembly_name}'
                );
            """

            dependencies = self._query_service.execute_table(query)

            if not dependencies:
                logger.info(
                    f"No dependent objects found for assembly '{assembly_name}'"
                )
                return True

            logger.info(
                f"Found {len(dependencies)} dependent objects for assembly '{assembly_name}'"
            )

            for row in dependencies:
                object_type = row.get("type_desc", "")
                object_name = row.get("name", "")

                # Map object types to DROP statements
                drop_command = None
                if object_type in [
                    "CLR_SCALAR_FUNCTION",
                    "SQL_SCALAR_FUNCTION",
                    "CLR_TABLE_VALUED_FUNCTION",
                    "SQL_TABLE_VALUED_FUNCTION",
                ]:
                    drop_command = f"DROP FUNCTION IF EXISTS [{object_name}];"
                elif object_type in ["CLR_STORED_PROCEDURE", "SQL_STORED_PROCEDURE"]:
                    drop_command = f"DROP PROCEDURE IF EXISTS [{object_name}];"
                elif object_type == "VIEW":
                    drop_command = f"DROP VIEW IF EXISTS [{object_name}];"
                else:
                    logger.warning(
                        f"Unsupported object type '{object_type}' for object '{object_name}'"
                    )
                    continue

                logger.debug(
                    f"Dropping dependent object '{object_name}' of type '{object_type}'"
                )
                self._query_service.execute_non_processing(drop_command)

            logger.success(
                f"All dependent objects for assembly '{assembly_name}' dropped successfully"
            )
            return True
        except Exception as e:
            logger.error(
                f"Failed to drop dependent objects for assembly '{assembly_name}': {e}"
            )
            return False

    def _enable_advanced_options(self) -> bool:
        """
        Ensure that 'show advanced options' is enabled.

        Returns:
            True if successfully enabled or already enabled; otherwise False
        """
        logger.debug("Ensuring advanced options are enabled")

        try:
            # Cast to INT to avoid sql_variant type issues in impacket
            query = "SELECT CAST(value_in_use AS INT) AS value_in_use FROM sys.configurations WHERE name = 'show advanced options';"
            advanced_options_enabled = self._query_service.execute_scalar(query)

            if (
                advanced_options_enabled is not None
                and int(advanced_options_enabled) == 1
            ):
                logger.debug("Advanced options already enabled")
                return True

            logger.debug("Enabling advanced options...")
            enable_query = (
                "EXEC master..sp_configure 'show advanced options', 1; RECONFIGURE;"
            )
            self._query_service.execute_non_processing(enable_query)

            # Verify the change
            advanced_options_enabled = self._query_service.execute_scalar(query)
            if (
                advanced_options_enabled is not None
                and int(advanced_options_enabled) == 1
            ):
                logger.success("Advanced options successfully enabled")
                return True

            logger.warning("Failed to verify 'show advanced options' was enabled")
            return False
        except Exception as e:
            logger.error(f"Error enabling advanced options: {e}")
            return False
