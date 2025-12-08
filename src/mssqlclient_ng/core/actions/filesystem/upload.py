# mssqlclient_ng/core/actions/filesystem/upload.py

# Built-in imports
import base64
from pathlib import Path
from typing import Optional, List

# Third-party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.common import normalize_windows_path


@ActionFactory.register("upload", "Upload a local file to the SQL Server filesystem")
class Upload(BaseAction):
    """
    Upload a local file to the SQL Server filesystem.

    This action reads a file from the local filesystem and writes it to a
    remote path on the SQL Server using OLE Automation (ADODB.Stream) or
    xp_cmdshell with PowerShell. After upload, it verifies the file was created.

    Methods used (in order of preference):
    1. OLE Automation with ADODB.Stream (most compatible)
    2. xp_cmdshell with PowerShell -EncodedCommand (if OLE is disabled)
    """

    def __init__(self):
        super().__init__()
        self._local_path: Optional[Path] = None
        self._remote_path: str = ""

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate arguments for the upload action.

        Args:
            additional_arguments: Local file path and optional remote destination path
                Format: <local_path> [remote_path]
                If remote_path is omitted, uses C:\\Windows\\Tasks\\

        Raises:
            ValueError: If arguments are invalid or file doesn't exist
        """
        parts = self.split_arguments(additional_arguments)

        if len(parts) < 1 or len(parts) > 2:
            raise ValueError(
                "Upload action requires one or two arguments: <local_path> [remote_path]"
            )

        local_path_str = parts[0].strip()

        # Validate local file exists using pathlib
        # expanduser() handles ~ expansion, resolve() makes it absolute
        self._local_path = Path(local_path_str).expanduser().resolve()

        if not self._local_path.exists():
            raise ValueError(f"Local file does not exist: {self._local_path}")

        if not self._local_path.is_file():
            raise ValueError(f"Path is not a file: {self._local_path}")

        # If no remote path specified, use world-writable directory
        if len(parts) == 1:
            # C:\Windows\Tasks is writable by everyone and commonly used
            self._remote_path = f"C:\\\\Windows\\\\Tasks\\\\{self._local_path.name}"
            logger.info(f"No remote path specified, using default: {self._remote_path}")
        else:
            self._remote_path = normalize_windows_path(parts[1].strip())

            if not self._remote_path:
                raise ValueError("Remote path cannot be empty")

            # If remote path ends with backslash, it's a directory - append filename
            if self._remote_path.endswith("\\"):
                self._remote_path = self._remote_path + self._local_path.name
                logger.info(f"Remote path is a directory, appending filename")

        logger.info(f"Local file: {self._local_path}")
        logger.info(f"File size: {self._local_path.stat().st_size} bytes")
        logger.info(f"Remote destination: {self._remote_path}")

    def execute(self, database_context: DatabaseContext) -> bool:
        """
        Execute the upload action.

        Args:
            database_context: The database context containing services

        Returns:
            True if upload succeeded; otherwise False
        """
        # Read local file content
        try:
            with open(self._local_path, "rb") as f:
                file_content = f.read()
        except Exception as e:
            logger.error(f"Failed to read local file: {e}")
            return False

        logger.info(f"Read {len(file_content)} bytes from local file")

        # Check if OLE Automation is available
        ole_available = database_context.config_service.set_configuration_option(
            "Ole Automation Procedures", 1
        )

        # Use OLE if available, otherwise PowerShell
        if ole_available:
            logger.info("OLE Automation is available, using OLE method")
            success = self._upload_via_ole(database_context, file_content)
        else:
            logger.info("OLE Automation not available, using PowerShell method")
            success = self._upload_via_xpcmdshell(database_context, file_content)

        if not success:
            logger.error("Upload failed")
            return False

        return self._verify_upload(database_context)

    def _verify_upload(self, database_context: DatabaseContext) -> bool:
        """
        Verify that the file was uploaded successfully using xp_fileexist.

        Args:
            database_context: The database context

        Returns:
            True if file exists; otherwise False
        """
        try:
            escaped_path = self._remote_path.replace("'", "''")

            # Use xp_fileexist to check if file exists
            query = f"EXEC master..xp_fileexist '{escaped_path}'"
            result = database_context.query_service.execute_table(query)

            if not result or not result[0].get("File Exists"):
                logger.error(f"File was not created at: {self._remote_path}")
                return False

            logger.success(f"File uploaded successfully to: {self._remote_path}")
            return True

        except Exception as e:
            logger.error(f"Could not verify upload: {e}")
            return False

    def _upload_via_ole(
        self, database_context: DatabaseContext, file_content: bytes
    ) -> bool:
        """
        Upload file using OLE Automation with ADODB.Stream.

        Args:
            database_context: The database context
            file_content: The file content as bytes

        Returns:
            True if upload succeeded; otherwise False
        """
        logger.info("Uploading file via OLE Automation (ADODB.Stream)")

        # For large files, this might fail due to VARBINARY(MAX) limits
        if len(file_content) > 2000000000:  # ~2GB limit
            logger.warning("File too large for OLE method (>2GB), will try xp_cmdshell")
            return False

        # Convert bytes to hex string for SQL
        hex_content = file_content.hex().upper()

        # Escape single quotes in remote path
        escaped_remote_path = self._remote_path.replace("'", "''")

        # Use ADODB.Stream to write binary data
        query = f"""
            DECLARE @ObjectToken INT;
            DECLARE @FileContent VARBINARY(MAX);
            DECLARE @Result INT;
            DECLARE @ErrorSource NVARCHAR(255);
            DECLARE @ErrorDesc NVARCHAR(255);

            -- Convert hex string to binary
            SET @FileContent = 0x{hex_content};

            -- Create ADODB.Stream object
            EXEC @Result = sp_OACreate 'ADODB.Stream', @ObjectToken OUTPUT;
            IF @Result <> 0
            BEGIN
                EXEC sp_OAGetErrorInfo @ObjectToken, @ErrorSource OUT, @ErrorDesc OUT;
                RAISERROR('Failed to create ADODB.Stream: %s', 16, 1, @ErrorDesc);
                RETURN;
            END

            -- Set stream type to binary
            EXEC @Result = sp_OASetProperty @ObjectToken, 'Type', 1;
            IF @Result <> 0
            BEGIN
                EXEC sp_OAGetErrorInfo @ObjectToken, @ErrorSource OUT, @ErrorDesc OUT;
                EXEC sp_OADestroy @ObjectToken;
                RAISERROR('Failed to set stream type: %s', 16, 1, @ErrorDesc);
                RETURN;
            END

            -- Open the stream
            EXEC @Result = sp_OAMethod @ObjectToken, 'Open';
            IF @Result <> 0
            BEGIN
                EXEC sp_OAGetErrorInfo @ObjectToken, @ErrorSource OUT, @ErrorDesc OUT;
                EXEC sp_OADestroy @ObjectToken;
                RAISERROR('Failed to open stream: %s', 16, 1, @ErrorDesc);
                RETURN;
            END

            -- Write binary data to stream
            EXEC @Result = sp_OAMethod @ObjectToken, 'Write', NULL, @FileContent;
            IF @Result <> 0
            BEGIN
                EXEC sp_OAGetErrorInfo @ObjectToken, @ErrorSource OUT, @ErrorDesc OUT;
                EXEC sp_OAMethod @ObjectToken, 'Close';
                EXEC sp_OADestroy @ObjectToken;
                RAISERROR('Failed to write to stream: %s', 16, 1, @ErrorDesc);
                RETURN;
            END

            -- Save to file (mode 2 = overwrite if exists)
            EXEC @Result = sp_OAMethod @ObjectToken, 'SaveToFile', NULL, '{escaped_remote_path}', 2;
            IF @Result <> 0
            BEGIN
                EXEC sp_OAGetErrorInfo @ObjectToken, @ErrorSource OUT, @ErrorDesc OUT;
                EXEC sp_OAMethod @ObjectToken, 'Close';
                EXEC sp_OADestroy @ObjectToken;
                RAISERROR('Failed to save file to {escaped_remote_path}: %s', 16, 1, @ErrorDesc);
                RETURN;
            END

            -- Close and destroy
            EXEC sp_OAMethod @ObjectToken, 'Close';
            EXEC sp_OADestroy @ObjectToken;
        """

        # Execute the query and check the result
        result = database_context.query_service.execute_non_processing(query)
        if result == -1:
            logger.error("OLE upload failed")
            return False

        logger.info("OLE upload command executed successfully")
        return True

    def _upload_via_xpcmdshell(
        self, database_context: DatabaseContext, file_content: bytes
    ) -> bool:
        """
        Upload file using xp_cmdshell with PowerShell -EncodedCommand.

        Args:
            database_context: The database context
            file_content: The file content as bytes

        Returns:
            True if upload succeeded; otherwise False
        """

        # Enable xp_cmdshell if needed
        if not database_context.config_service.set_configuration_option(
            "xp_cmdshell", 1
        ):
            logger.error("Failed to enable xp_cmdshell")
            return False

        logger.info("Uploading file via xp_cmdshell (PowerShell -EncodedCommand)")

        # Convert file content to base64 for embedding in PowerShell script
        file_base64 = base64.b64encode(file_content).decode("ascii")

        # Escape single quotes in remote path for PowerShell
        escaped_remote_path = self._remote_path.replace("'", "''")

        # Determine chunk size for large files
        # Encoded command has limits, be conservative
        max_chunk_size = 4000  # Conservative limit for base64 data in script

        if len(file_base64) <= max_chunk_size:
            # Small file - single command
            ps_script = (
                f"$d=[Convert]::FromBase64String('{file_base64}');"
                f"[IO.File]::WriteAllBytes('{escaped_remote_path}',$d)"
            )

            # Encode PowerShell script to base64 UTF-16LE for -EncodedCommand
            encoded_command = base64.b64encode(ps_script.encode("utf-16-le")).decode(
                "ascii"
            )

            query = f"EXEC master..xp_cmdshell 'powershell -e {encoded_command}'"
            database_context.query_service.execute_non_processing(query)
            logger.info("PowerShell upload command executed")

        else:
            # Large file - write in chunks
            total_chunks = (len(file_base64) + max_chunk_size - 1) // max_chunk_size
            logger.info(f"Large file detected, uploading in {total_chunks} chunks")

            # First chunk - create new file
            chunk = file_base64[:max_chunk_size]
            ps_script = (
                f"$d=[Convert]::FromBase64String('{chunk}');"
                f"[IO.File]::WriteAllBytes('{escaped_remote_path}',$d)"
            )
            encoded_command = base64.b64encode(ps_script.encode("utf-16-le")).decode(
                "ascii"
            )
            query = f"EXEC master..xp_cmdshell 'powershell -e {encoded_command}'"
            database_context.query_service.execute_non_processing(query)
            logger.info("Chunk 1 uploaded")

            # Remaining chunks - append
            offset = max_chunk_size
            chunk_num = 2
            while offset < len(file_base64):
                chunk = file_base64[offset : offset + max_chunk_size]

                # Use FileStream to append (compatible with all PS versions)
                ps_script = (
                    f"$d=[Convert]::FromBase64String('{chunk}');"
                    f"$f=[IO.File]::Open('{escaped_remote_path}',[IO.FileMode]::Append);"
                    f"$f.Write($d,0,$d.Length);$f.Close()"
                )
                encoded_command = base64.b64encode(
                    ps_script.encode("utf-16-le")
                ).decode("ascii")
                query = f"EXEC master..xp_cmdshell 'powershell -e {encoded_command}'"

                if database_context.query_service.execute_non_processing(query) == -1:
                    logger.error("PowerShell upload failed")
                    return False

                offset += max_chunk_size
                logger.info(f"Chunk {chunk_num}/{total_chunks} uploaded")
                chunk_num += 1

        logger.success("PowerShell upload completed")
        return True

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return [
            "Local file path (must exist)",
            "Optional: Remote destination path (defaults to C:\\Windows\\Tasks\\)",
        ]
