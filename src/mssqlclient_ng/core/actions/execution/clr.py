# mssqlclient_ng/core/actions/execution/clr.py

# Built-in imports
import hashlib
import re
from pathlib import Path

# Third-party imports
from loguru import logger

# Local imports
from ...services.database import DatabaseContext
from ...utils.common import (
    generate_random_string,
    get_hex_char,
    normalize_windows_path,
)
from ..base import BaseAction, Arg
from ..factory import ActionFactory

@ActionFactory.register(
    "clr", "Deploy and execute .NET CLR assemblies (DLL) on SQL Server"
)
class ClrExecution(BaseAction):
    """
    Deploy and execute .NET CLR assemblies on SQL Server.

    This action allows loading custom .NET DLLs from the local filesystem
    and executing stored procedures defined within them. The DLL must contain a
    class named 'StoredProcedures' with static methods representing SQL procedures.

    The workflow:
    1. Read the DLL bytes
    2. Compute SHA-512 hash for trusted assembly registration
    3. Enable CLR and register the assembly
    4. Create a stored procedure linked to the assembly method
    5. Execute the procedure
    6. Cleanup (drop procedure, assembly, and hash)
    """

    _dll_path = Arg(position=0, required=True, description="Path to the DLL (local file)")
    _class_name = Arg(position=1, required=True, default="StoredProcedures", description="Class name containing the function")
    _function = Arg(position=2, required=True, default="Main", description="Function name to execute")
    _args = Arg(position=3, remainder=True, default="", description="Function args")

    def validate_arguments(self, additional_arguments: str = "") -> None:
        self._bind_arguments(additional_arguments)
        logger.info(f"DLL path: {Path(self._dll_path).resolve()}")
        logger.info(f"Class: {self._class_name}")
        logger.info(f"Function: {self._function}")
        if self._args:
            logger.info(f"Args: {self._args}")

    def execute(self, database_context: DatabaseContext) -> bool:
        """
        Deploy and execute the CLR assembly.

        Args:
            database_context: The database context containing services

        Returns:
            True if execution succeeded; otherwise False
        """
        # Step 1: Get the SHA-512 hash for the DLL and its bytes
        library_hash, library_hex_bytes = self._convert_dll_to_sql_bytes(self._dll_path)

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
        used_trusted_assembly = False
        set_trustworthy = False

        logger.info("Starting CLR assembly deployment process")

        try:
            # Strategy 1: Try sp_add_trusted_assembly (SQL 2017+)
            # Strategy 2: Fall back to TRUSTWORTHY property
            if not database_context.server.legacy:
                used_trusted_assembly = (
                    database_context.config_service.register_trusted_assembly(
                        library_hash, library_path
                    )
                )

            if not used_trusted_assembly:
                logger.warning(
                    "Trusted assembly registration unavailable, falling back to TRUSTWORTHY"
                )
                trustworthy_result = database_context.query_service.execute_scalar(
                    "SELECT is_trustworthy_on FROM sys.databases WHERE name = DB_NAME();"
                )
                is_trustworthy = trustworthy_result is not None and bool(
                    int(trustworthy_result)
                )

                if not is_trustworthy:
                    logger.warning(
                        "Current database is not TRUSTWORTHY, attempting to enable it"
                    )
                    try:
                        database_context.query_service.execute_non_processing(
                            f"ALTER DATABASE [{database_context.query_service.execution_database}] SET TRUSTWORTHY ON;"
                        )
                        set_trustworthy = True
                        logger.success("TRUSTWORTHY enabled on current database")
                    except Exception as ex:
                        logger.error(f"Failed to enable TRUSTWORTHY: {ex}")
                        return False
                else:
                    logger.info(
                        "Database is already TRUSTWORTHY, using it for CLR deployment"
                    )

            # Drop existing procedure and assembly if they exist
            database_context.query_service.execute_non_processing(drop_procedure)
            database_context.query_service.execute_non_processing(drop_assembly)

            # Step 3: Create the assembly from the DLL bytes, retrying once on MVID conflict
            logger.info("Creating the assembly from DLL bytes")
            try:
                database_context.query_service.execute_non_processing(
                    f"CREATE ASSEMBLY [{assembly_name}] FROM 0x{library_hex_bytes} WITH PERMISSION_SET = UNSAFE;",
                    silent=True,
                )
            except Exception as create_err:
                conflicting = self._extract_mvid_conflict_name(str(create_err))
                if conflicting:
                    logger.warning(
                        f"Dropping conflicting leftover assembly '{conflicting}' (MVID collision)"
                    )
                    database_context.query_service.execute_non_processing(
                        f"DROP ASSEMBLY IF EXISTS [{conflicting}];"
                    )
                    database_context.query_service.execute_non_processing(
                        f"CREATE ASSEMBLY [{assembly_name}] FROM 0x{library_hex_bytes} WITH PERMISSION_SET = UNSAFE;"
                    )
                else:
                    raise

            if not database_context.config_service.check_assembly(assembly_name):
                logger.error("Failed to create a new assembly")
                return False

            logger.success(f"Assembly '{assembly_name}' successfully created")

            # Step 4: Create the stored procedure linked to the assembly
            logger.info("Creating the stored procedure linked to the assembly")
            database_context.query_service.execute_non_processing(
                f"CREATE PROCEDURE [dbo].[{self._function}] @args NVARCHAR(MAX) AS EXTERNAL NAME [{assembly_name}].[{self._class_name}].[{self._function}];"
            )

            if not database_context.config_service.check_procedure(self._function):
                logger.error("Failed to create the stored procedure")
                return False

            logger.success(f"Stored procedure '{self._function}' successfully created")

            # Step 5: Execute the stored procedure
            logger.info(f"Executing the stored procedure '{self._function}'")
            database_context.query_service.execute_non_processing(
                f"EXEC [{self._function}] @args = '{self._args}';"
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

            if used_trusted_assembly:
                database_context.query_service.execute_non_processing(drop_clr_hash)

            if set_trustworthy:
                logger.info("Resetting TRUSTWORTHY property")
                database_context.query_service.execute_non_processing(
                    f"ALTER DATABASE [{database_context.query_service.execution_database}] SET TRUSTWORTHY OFF;"
                )

    @staticmethod
    def _extract_mvid_conflict_name(error_message: str) -> str:
        """
        Extract the conflicting assembly name from an MVID collision error.
        SQL Server message: "CREATE ASSEMBLY failed ... identical to an assembly
        that is already registered under the name 'X'."
        """
        match = re.search(
            r"already registered under the name ['\"]([^'\"]+)['\"]",
            error_message,
            re.IGNORECASE,
        )
        return match.group(1) if match else ""

    def _convert_dll_to_sql_bytes(self, dll: str) -> tuple[str, str]:
        """
        Convert DLL to SQL-compatible bytes from a local file.

        Args:
            dll: Path to local file

        Returns:
            tuple of (sha512_hash_lowercase, dll_hex_bytes_uppercase)
        """
        resolved = str(Path(normalize_windows_path(dll)).resolve())
        return self._convert_dll_to_sql_bytes_file(resolved)

    def _convert_dll_to_sql_bytes_file(self, dll: str) -> tuple[str, str]:
        """
        Read a .NET assembly from local filesystem and convert to SQL format.

        Args:
            dll: Full path to the DLL on disk

        Returns:
            tuple of (sha512_hash_lowercase, dll_hex_bytes_uppercase)
        """
        try:
            path = Path(dll)
            if not path.exists():
                raise FileNotFoundError(f"File not found: {path}")

            file_size = path.stat().st_size
            logger.info(f"{path} is {file_size} bytes")

            # Read all DLL bytes
            with path.open("rb") as f:
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
