# mssqlclient_ng/core/actions/cloud/__init__.py

from . import rds, azure

__all__ = [
    "rds",
    "azure",
]
