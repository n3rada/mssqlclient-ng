"""
Database actions for SQL Server database management.
"""

# Import actions to trigger registration with ActionFactory
from mssqlclient_ng.src.actions.database.authtoken import AuthToken
from mssqlclient_ng.src.actions.database.dabases import Databases
from mssqlclient_ng.src.actions.database.xprocs import ExtendedProcs
from mssqlclient_ng.src.actions.database.impersonate import Impersonation
from mssqlclient_ng.src.actions.database.info import Info
from mssqlclient_ng.src.actions.database.loginmap import LoginMap
from mssqlclient_ng.src.actions.database.oledb_providers import OleDbProviders
from mssqlclient_ng.src.actions.database.permissions import Permissions
from mssqlclient_ng.src.actions.database.procedures import Procedures
from mssqlclient_ng.src.actions.database.rolemembers import RoleMembers
from mssqlclient_ng.src.actions.database.roles import Roles
from mssqlclient_ng.src.actions.database.rows import Rows
from mssqlclient_ng.src.actions.database.search import Search
from mssqlclient_ng.src.actions.database.tables import Tables
from mssqlclient_ng.src.actions.database.users import Users
from mssqlclient_ng.src.actions.database.whoami import Whoami

__all__ = [
    "AuthToken",
    "Databases",
    "ExtendedProcs",
    "Impersonation",
    "Info",
    "LoginMap",
    "OleDbProviders",
    "Permissions",
    "Procedures",
    "RoleMembers",
    "Roles",
    "Rows",
    "Search",
    "Tables",
    "Users",
    "Whoami",
]
