# mssqlclient_ng/core/actions/cloud/rds/rds_tasks.py

# Built-in imports
from typing import Any

# Third party imports
from loguru import logger

# Local library imports
from ...base import Arg, BaseAction
from ...factory import ActionFactory
from ....services.database import DatabaseContext
from ....utils.formatters import OutputFormatter
from .guard import requires_rds


@ActionFactory.register(
    "rds-tasks",
    "List RDS task history (backups, restores, S3 transfers). "
    "Reveals S3 bucket ARNs, KMS keys, and operational patterns.",
)
class RdsTasks(BaseAction):
    """
    Query rds_fn_task_status to enumerate past and running RDS tasks.

    History is kept for 36 days. The S3_object_arn and KMS_master_key_arn
    columns reveal bucket names, paths, and encryption keys used by other
    operations — free intelligence about the AWS infrastructure.
    """

    _database = Arg(
        position=0,
        short_name="d",
        long_name="db",
        default="",
        description="Filter tasks by database name",
    )

    def execute(
        self, database_context: DatabaseContext
    ) -> list[dict[str, Any]] | None:
        if not requires_rds(database_context):
            return None

        qs = database_context.query_service

        if self._database:
            logger.info(f"Querying RDS task history for database '{self._database}'")
            db = self._database.replace("'", "''")
            query = f"EXEC msdb.dbo.rds_task_status @db_name = '{db}';"
        else:
            logger.info("Querying full RDS task history")
            query = "SELECT * FROM msdb.dbo.rds_fn_task_status(NULL, 0);"

        try:
            rows = qs.execute_table(query)
        except Exception as ex:
            message = str(ex)
            if "rdsadmin" in message.lower() or "does not exist" in message.lower():
                logger.error("Not an RDS instance or no permission on rds_fn_task_status")
            else:
                logger.error(f"Failed to query RDS task history: {message}")
            return None

        if not rows:
            logger.warning("No RDS tasks found (history kept for 36 days)")
            return []

        s3_arns = {
            r.get("S3_object_arn")
            for r in rows
            if r.get("S3_object_arn")
        }
        kms_keys = {
            r.get("KMS_master_key_arn")
            for r in rows
            if r.get("KMS_master_key_arn")
        }

        logger.success(f"Found {len(rows)} task(s)")
        if s3_arns:
            logger.info(f"S3 buckets seen: {', '.join(sorted(s3_arns))}")
        if kms_keys:
            logger.info(f"KMS keys seen: {', '.join(sorted(kms_keys))}")

        print(OutputFormatter.convert_list_of_dicts(rows))
        return rows
