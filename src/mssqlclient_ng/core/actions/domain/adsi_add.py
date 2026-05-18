# mssqlclient_ng/core/actions/domain/adsi_add.py

# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local library imports
from ..base import BaseAction, Arg
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...services.adsi import AdsiService
from ...utils.common import generate_random_string


@ActionFactory.register("adsi-add", "Create an ADSI linked server")
class AdsiAdd(BaseAction):
    """
    Creates an ADSI linked server for LDAP querying via OPENQUERY.
    Auto-generates the server name if omitted.
    """

    _server_name = Arg(position=0, default=None, description="ADSI linked server name")
    _data_source = Arg(position=1, default="adsdatasource", description="OLE DB data source")

    def execute(self, database_context: DatabaseContext) -> None:
        adsi_service = AdsiService(database_context)

        if not self._server_name:
            self._server_name = f"ADSI_{generate_random_string(8)}"

        if adsi_service.adsi_server_exists(self._server_name):
            logger.error(f"ADSI linked server '{self._server_name}' already exists.")
            return None

        success = adsi_service.create_adsi_linked_server(
            self._server_name, self._data_source
        )

        if not success:
            return None

        logger.success(f"ADSI linked server '{self._server_name}' created successfully")
        logger.info(f"Data source: {self._data_source}")
        return None
