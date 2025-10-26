"""
Actions package.
Import all action subpackages to ensure actions are registered with the factory.
"""

import mssqlclient_ng.src.actions.execution as execution
import mssqlclient_ng.src.actions.administration as administration
import mssqlclient_ng.src.actions.network as network
import mssqlclient_ng.src.actions.database as database

__all__ = [
    "execution",
    "administration",
    "network",
    "database",
]
