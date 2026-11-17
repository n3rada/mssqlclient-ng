# mssqlclient_ng/core/actions/cloud/rds/rds_s3.py

# Built-in imports
from typing import Any

# Third-party imports
from loguru import logger

# Local imports
from ...base import Arg, BaseAction
from ...factory import ActionFactory
from ....services.database import DatabaseContext
from ....utils.formatters import OutputFormatter
from .guard import requires_rds


@ActionFactory.register(
    "rds-s3-list",
    "List files in the RDS staging directory (D:\\S3\\).",
    aliases=["rds-ls"],
)
class RdsS3List(BaseAction):
    """
    List files in the D:\\S3\\ staging directory on the RDS instance.

    Uses rds_gather_file_details (async) then rds_fn_list_file_details.
    """

    def execute(
        self, database_context: DatabaseContext
    ) -> list[dict[str, Any]] | None:
        if not requires_rds(database_context):
            return None

        qs = database_context.query_service
        logger.info("Listing files in RDS staging directory (D:\\S3\\)")

        try:
            qs.execute_non_processing(
                "EXEC msdb.dbo.rds_gather_file_details;", silent=True
            )
        except Exception as ex:
            logger.error(f"rds_gather_file_details failed: {ex}")
            return None

        try:
            rows = qs.execute_table(
                "SELECT * FROM msdb.dbo.rds_fn_list_file_details();"
            )
        except Exception as ex:
            logger.error(f"Could not list files: {ex}")
            return None

        if not rows:
            logger.warning("No files found in D:\\S3\\")
            return []

        logger.success(f"Found {len(rows)} file(s)")
        print(OutputFormatter.convert_list_of_dicts(rows))
        return rows


@ActionFactory.register(
    "rds-s3-download",
    "Download a file from S3 to the RDS instance staging directory (D:\\S3\\).",
)
class RdsS3Download(BaseAction):
    """
    Download a file from S3 to D:\\S3\\ on the RDS instance using
    rds_download_from_s3. Requires the IAM role to have s3:GetObject.
    """

    _s3_arn = Arg(
        position=0,
        short_name="s",
        long_name="s3-arn",
        required=True,
        description="S3 ARN, e.g. arn:aws:s3:::bucket/file.csv",
    )
    _filename = Arg(
        position=1,
        short_name="f",
        long_name="file",
        default="",
        description="Local filename in D:\\S3\\ (default: derived from ARN)",
    )
    _overwrite = Arg(
        long_name="overwrite",
        toggle=True,
        default=False,
        description="Overwrite if file exists",
    )

    def validate_arguments(self, additional_arguments: str = "") -> None:
        self._bind_arguments(additional_arguments)
        if not self._s3_arn:
            raise ValueError("S3 ARN is required")

    def execute(self, database_context: DatabaseContext) -> str | None:
        if not requires_rds(database_context):
            return None

        qs = database_context.query_service

        filename = self._filename
        if not filename:
            filename = self._s3_arn.rstrip("/").rsplit("/", 1)[-1]

        rds_path = f"D:\\S3\\{filename}"
        overwrite = 1 if self._overwrite else 0
        arn = self._s3_arn.replace("'", "''")
        path = rds_path.replace("'", "''")

        logger.info(f"Downloading {self._s3_arn} -> {rds_path}")

        query = (
            f"EXEC msdb.dbo.rds_download_from_s3 "
            f"@s3_arn_of_file = '{arn}', "
            f"@rds_file_path = '{path}', "
            f"@overwrite_file = {overwrite};"
        )

        try:
            rows = qs.execute_table(query)
        except Exception as ex:
            logger.error(f"Download failed: {ex}")
            return None

        task_id = rows[0].get("task_id") if rows else None
        if task_id:
            logger.success(f"Download task started (task_id: {task_id})")
            logger.info("Monitor with: !rds-tasks")
        return str(task_id) if task_id else None


@ActionFactory.register(
    "rds-s3-upload",
    "Upload a file from the RDS staging directory (D:\\S3\\) to S3.",
)
class RdsS3Upload(BaseAction):
    """
    Upload a file from D:\\S3\\ to S3 using rds_upload_to_s3.
    Requires the IAM role to have s3:PutObject.
    """

    _rds_path = Arg(
        position=0,
        short_name="f",
        long_name="file",
        required=True,
        description="File path on the RDS instance (D:\\S3\\...)",
    )
    _s3_arn = Arg(
        position=1,
        short_name="s",
        long_name="s3-arn",
        required=True,
        description="Target S3 ARN, e.g. arn:aws:s3:::bucket/output.csv",
    )
    _overwrite = Arg(
        long_name="overwrite",
        toggle=True,
        default=False,
        description="Overwrite if S3 object exists",
    )

    def execute(self, database_context: DatabaseContext) -> str | None:
        if not requires_rds(database_context):
            return None

        qs = database_context.query_service

        overwrite = 1 if self._overwrite else 0
        path = self._rds_path.replace("'", "''")
        arn = self._s3_arn.replace("'", "''")

        logger.info(f"Uploading {self._rds_path} -> {self._s3_arn}")

        query = (
            f"EXEC msdb.dbo.rds_upload_to_s3 "
            f"@rds_file_path = '{path}', "
            f"@s3_arn_of_file = '{arn}', "
            f"@overwrite_file = {overwrite};"
        )

        try:
            rows = qs.execute_table(query)
        except Exception as ex:
            logger.error(f"Upload failed: {ex}")
            return None

        task_id = rows[0].get("task_id") if rows else None
        if task_id:
            logger.success(f"Upload task started (task_id: {task_id})")
            logger.info("Monitor with: !rds-tasks")
        return str(task_id) if task_id else None


@ActionFactory.register(
    "rds-s3-delete",
    "Delete a file from the RDS staging directory (D:\\S3\\).",
    aliases=["rds-rm"],
)
class RdsS3Delete(BaseAction):
    """Delete a file from D:\\S3\\ on the RDS instance."""

    _rds_path = Arg(
        position=0,
        short_name="f",
        long_name="file",
        required=True,
        description="File path to delete (D:\\S3\\...)",
    )

    def execute(self, database_context: DatabaseContext) -> bool:
        if not requires_rds(database_context):
            return False

        qs = database_context.query_service
        path = self._rds_path.replace("'", "''")

        logger.info(f"Deleting {self._rds_path}")

        try:
            qs.execute_non_processing(
                f"EXEC msdb.dbo.rds_delete_from_filesystem "
                f"@rds_file_path = '{path}';",
                silent=True,
            )
            logger.success(f"Deleted {self._rds_path}")
            return True
        except Exception as ex:
            logger.error(f"Delete failed: {ex}")
            return False
