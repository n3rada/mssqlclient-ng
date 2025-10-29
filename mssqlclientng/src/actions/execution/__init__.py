"""
Execution actions package.
Import all execution actions to ensure they are registered with the factory.
"""

from mssqlclientng.src.actions.execution.query import Query
from mssqlclientng.src.actions.execution.xpcmd import XpCmd
from mssqlclientng.src.actions.execution.powershell import PowerShell
from mssqlclientng.src.actions.execution.remote_powershell import RemotePowerShell
from mssqlclientng.src.actions.execution.ole import ObjectLinkingEmbedding
from mssqlclientng.src.actions.execution.agents import Agents
from mssqlclientng.src.actions.execution.clr import ClrExecution
from mssqlclientng.src.actions.execution.exec_file import ExecFile

__all__ = [
    "Query",
    "XpCmd",
    "PowerShell",
    "RemotePowerShell",
    "ObjectLinkingEmbedding",
    "Agents",
    "ClrExecution",
    "ExecFile",
]
