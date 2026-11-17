# tests/test_query_service.py

"""Tests for QueryService database switching."""

# Built-in imports
from unittest.mock import MagicMock, patch

# Third-party imports
import pytest

# Local imports
from mssqlclient_ng.core.models.linked_servers import LinkedServers
from mssqlclient_ng.core.services.query import QueryService


def _make_service(chain: str | None = None) -> QueryService:
    """Build a QueryService without opening a connection."""
    service = QueryService.__new__(QueryService)
    service.mssql_instance = MagicMock()
    service.mssql_instance.currentDB = "master"
    service.execution_server = "LAB-SQL01"
    service.execution_database = "master"
    service.linked_server_alias = None
    service._full_version_string = None
    service._linked_servers = LinkedServers(chain) if chain else LinkedServers()
    service._is_azure_sql_cache = {}
    return service


class TestChangeDatabaseDirect:
    def test_runs_bracketed_use_and_reads_session_database(self):
        service = _make_service()

        def switch(query: str, silent: bool = False) -> int:
            service.mssql_instance.currentDB = "my]db"
            return 0

        with patch.object(
            service, "execute_non_processing", side_effect=switch
        ) as mock_exec:
            result = service.change_database("my]db")

        mock_exec.assert_called_once_with("USE [my]]db];", silent=True)
        assert result == "my]db"
        assert service.execution_database == "my]db"

    def test_failure_keeps_previous_database(self):
        service = _make_service()
        with patch.object(
            service, "execute_non_processing", side_effect=RuntimeError("no access")
        ):
            with pytest.raises(RuntimeError):
                service.change_database("secret")
        assert service.execution_database == "master"

    def test_empty_name_rejected(self):
        service = _make_service()
        with pytest.raises(ValueError):
            service.change_database("   ")


class TestChangeDatabaseLinked:
    def test_sets_last_hop_database_and_confirms(self):
        service = _make_service("SQL02;SQL03@old")
        with patch.object(service, "execute_scalar", return_value="CM_ABC") as mock_q:
            result = service.change_database("CM_ABC")

        mock_q.assert_called_once_with("SELECT DB_NAME();", silent=True)
        assert result == "CM_ABC"
        assert service.execution_database == "CM_ABC"
        chain = service.linked_servers.server_chain
        assert chain[0].database is None
        assert chain[-1].database == "CM_ABC"
        assert service.linked_servers.get_chain_arguments().endswith("SQL03@CM_ABC")

    def test_wrapped_queries_use_new_database(self):
        service = _make_service("SQL02")
        with patch.object(service, "execute_scalar", return_value="CM_ABC"):
            service.change_database("CM_ABC")
        assert "USE [CM_ABC]" in service._prepare_query("SELECT 1")

    def test_failure_restores_previous_database(self):
        service = _make_service("SQL02@old")
        with patch.object(
            service, "execute_scalar", side_effect=RuntimeError("does not exist")
        ):
            with pytest.raises(RuntimeError):
                service.change_database("missing")
        assert service.linked_servers.server_chain[-1].database == "old"
        assert service.execution_database == "master"
        assert "USE [old]" in service._prepare_query("SELECT 1")

    def test_unconfirmed_switch_restores_previous_database(self):
        service = _make_service("SQL02")
        with patch.object(service, "execute_scalar", return_value=None):
            with pytest.raises(ValueError):
                service.change_database("CM_ABC")
        assert service.linked_servers.server_chain[-1].database is None


class TestSyncExecutionDatabase:
    def test_direct_follows_session_database(self):
        service = _make_service()
        service.mssql_instance.currentDB = "tempdb"
        service.sync_execution_database()
        assert service.execution_database == "tempdb"

    def test_linked_keeps_chain_database(self):
        service = _make_service("SQL02@CM_ABC")
        service.execution_database = "CM_ABC"
        service.mssql_instance.currentDB = "tempdb"
        service.sync_execution_database()
        assert service.execution_database == "CM_ABC"


class TestLinkedServersSetLastDatabase:
    def test_empty_chain_raises(self):
        with pytest.raises(ValueError):
            LinkedServers().set_last_database("tempdb")
