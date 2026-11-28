# mssqlclient_ng/exceptions.py

"""Typed exceptions for programmatic error handling."""


class MssqlError(Exception):
    """Base exception for all mssqlclient-ng library errors."""


class ConnectionError(MssqlError):
    """Raised when a connection or authentication attempt fails."""


class QueryError(MssqlError):
    """Raised when a SQL query fails to execute."""
