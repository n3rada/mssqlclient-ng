"""
Database actions for SQL Server database management.
"""

# Import actions to trigger registration with ActionFactory
from mssqlclient_ng.src.actions.database.dabases import Databases
from mssqlclient_ng.src.actions.database.xprocs import ExtendedProcs

__all__ = [
    "Databases",
    "ExtendedProcs",
]
