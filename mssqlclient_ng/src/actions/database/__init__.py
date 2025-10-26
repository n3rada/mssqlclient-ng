"""
Database actions for SQL Server database management.
"""

# Import actions to trigger registration with ActionFactory
from mssqlclient_ng.src.actions.database.dabases import Databases
from mssqlclient_ng.src.actions.database.xprocs import ExtendedProcs
from mssqlclient_ng.src.actions.database.impersonate import Impersonation
from mssqlclient_ng.src.actions.database.info import Info
from mssqlclient_ng.src.actions.database.oledb_providers import OleDbProviders

__all__ = [
    "Databases",
    "ExtendedProcs",
    "Impersonation",
    "Info",
    "OleDbProviders",
]
