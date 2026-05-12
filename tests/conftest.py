# tests/conftest.py

"""Shared fixtures for mssqlclient-ng test suite."""

import pytest
from unittest.mock import MagicMock

from mssqlclient_ng.core.models.server import Server
from mssqlclient_ng.core.models.linked_servers import LinkedServers
from mssqlclient_ng.core.services.database import DatabaseContext


@pytest.fixture
def mock_server():
    """Create a mock Server with typical test values."""
    server = Server(hostname="LAB-SQL01", port=1433)
    server.mapped_user = "dbo"
    server.system_user = "sa"
    return server


@pytest.fixture
def mock_database_context(mock_server):
    """Create a mock DatabaseContext for terminal tests."""
    ctx = MagicMock(spec=DatabaseContext)
    ctx.server = mock_server

    # Query service
    ctx.query_service.execution_server = "LAB-SQL01"
    ctx.query_service.execution_database = "master"
    ctx.query_service.linked_servers = LinkedServers()

    # User service
    ctx.user_service.system_user = "sa"
    ctx.user_service.mapped_user = "dbo"
    ctx.user_service.get_info.return_value = ("dbo", "sa")
    ctx.user_service.is_admin.return_value = False

    return ctx
