"""
Execution actions package.
Import all execution actions to ensure they are registered with the factory.
"""

from mssqlclient_ng.src.actions.execution.query import Query
from mssqlclient_ng.src.actions.execution.xpcmd import XpCmd
from mssqlclient_ng.src.actions.execution.powershell import PowerShell
from mssqlclient_ng.src.actions.execution.remote_powershell import RemotePowerShell
from mssqlclient_ng.src.actions.execution.ole import ObjectLinkingEmbedding
from mssqlclient_ng.src.actions.execution.agents import Agents
from mssqlclient_ng.src.actions.execution.clr import ClrExecution

__all__ = [
    "Query",
    "XpCmd",
    "PowerShell",
    "RemotePowerShell",
    "ObjectLinkingEmbedding",
    "Agents",
    "ClrExecution",
]
