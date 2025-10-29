# Local library imports
from mssqlclientng import __version__ as version

def display_banner() -> str:
    return (
        r"""
                          _      _ _            _
                         | |    | (_)          | |
  _ __ ___  ___ ___  __ _| | ___| |_  ___ _ __ | |_
 | '_ ` _ \/ __/ __|/ _` | |/ __| | |/ _ \ '_ \| __|
 | | | | | \__ \__ \ (_| | | (__| | |  __/ | | | |_
 |_| |_| |_|___/___/\__, |_|\___|_|_|\___|_| |_|\__|
                       | |
               @n3rada |_| New Gen - %10s
"""
        % version
    )
