"""
Execution actions package.
Import all execution actions to ensure they are registered with the factory.
"""

from mssqlclient_ng.src.actions.execution.query import Query
from mssqlclient_ng.src.actions.execution.xpcmd import XpCmd

__all__ = [
    "Query",
    "XpCmd",
]
