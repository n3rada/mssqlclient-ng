# mssqlclient_ng/client.py

"""Thin library facade for programmatic MSSQL access."""

# Built-in imports
from dataclasses import dataclass, field
from typing import Any, Self

# Third-party imports
from loguru import logger

# Local imports
from .core import actions as _actions  # noqa: F401 — register all actions
from .core.actions.base import ActionResult
from .core.actions.factory import ActionFactory
from .core.models.server import Server
from .core.services.authentication import AuthenticationService
from .core.services.database import DatabaseContext
from .exceptions import ConnectionError, QueryError


@dataclass(frozen=True)
class MssqlConfig:
    """Connection parameters for an MSSQL server.

    Args:
        host: Hostname or IP address of the SQL Server.
        port: TCP port. Defaults to 1433.
        username: SQL or Windows login name.
        password: Password for the login.
        domain: Windows domain for NTLM or Kerberos authentication.
        database: Initial database context.
        windows_auth: Use Windows authentication instead of SQL auth.
        hashes: NTLM hashes in ``LMHASH:NTHASH`` format.
        aes_key: AES key for Kerberos authentication.
        kerberos: Use Kerberos authentication.
        kdc_host: Explicit KDC hostname for Kerberos.
        impersonation_chain: Logins to cascade ``EXECUTE AS`` after connecting.
    """

    host: str
    port: int = 1433
    username: str = ""
    password: str = field(default="", repr=False)
    domain: str = ""
    database: str | None = None
    windows_auth: bool = False
    hashes: str | None = field(default=None, repr=False)
    aes_key: str | None = field(default=None, repr=False)
    kerberos: bool = False
    kdc_host: str | None = None
    impersonation_chain: tuple[str, ...] = ()


class MssqlClient:
    """Context-managed MSSQL client backed by AuthenticationService and DatabaseContext.

    Usage::

        config = MssqlConfig(host="sql01", username="sa", password="secret")
        with MssqlClient(config) as client:
            rows = client.query("SELECT name FROM sys.databases")

    Args:
        config: Connection parameters.
    """

    def __init__(self, config: MssqlConfig) -> None:
        self._config = config
        self._auth: AuthenticationService | None = None
        self._context: DatabaseContext | None = None

    def connect(self) -> None:
        """Build the server, authenticate, and establish the database context.

        Raises:
            ConnectionError: If the TCP connection or authentication fails.
        """
        server = Server(
            hostname=self._config.host,
            port=self._config.port,
            database=self._config.database,
            impersonation_users=list(self._config.impersonation_chain) or None,
        )

        self._auth = AuthenticationService(
            server=server,
            remote_name=server.hostname,
            username=self._config.username,
            password=self._config.password,
            domain=self._config.domain,
            use_windows_auth=self._config.windows_auth,
            hashes=self._config.hashes,
            aes_key=self._config.aes_key,
            kerberos_auth=self._config.kerberos,
            kdc_host=self._config.kdc_host,
        )

        if not self._auth.connect():
            self._auth = None
            raise ConnectionError("Authentication failed")

        try:
            assert self._auth.mssql_instance is not None
            self._context = DatabaseContext(
                server=server,
                mssql_instance=self._auth.mssql_instance,
            )
        except Exception as exc:
            logger.debug("Failed to establish database context: {}", exc)
            self.close()
            raise ConnectionError("Failed to establish database context") from exc

    def query(self, sql: str) -> list[dict[str, Any]]:
        """Execute a SQL query and return every row as a dictionary.

        Args:
            sql: The T-SQL statement to execute.

        Returns:
            A list of row dictionaries keyed by column name.

        Raises:
            QueryError: If the query fails.
            ConnectionError: If no connection is established.
        """
        ctx = self._require_context()
        try:
            return ctx.query_service.execute_table(sql)
        except Exception as exc:
            raise QueryError(str(exc)) from exc

    def scalar(self, sql: str) -> Any:
        """Execute a SQL query and return the first column of the first row.

        Args:
            sql: The T-SQL statement to execute.

        Returns:
            The scalar value, or ``None`` when the result set is empty.

        Raises:
            QueryError: If the query fails.
            ConnectionError: If no connection is established.
        """
        ctx = self._require_context()
        try:
            return ctx.query_service.execute_scalar(sql)
        except Exception as exc:
            raise QueryError(str(exc)) from exc

    def execute(self, sql: str) -> bool:
        """Execute a non-query statement such as INSERT, UPDATE, or DELETE.

        Args:
            sql: The T-SQL statement to execute.

        Returns:
            ``True`` when the statement succeeds, ``False`` otherwise.

        Raises:
            QueryError: If the execution fails.
            ConnectionError: If no connection is established.
        """
        ctx = self._require_context()
        try:
            result = ctx.query_service.execute_non_processing(sql)
            return result >= 0
        except Exception as exc:
            raise QueryError(str(exc)) from exc

    @property
    def context(self) -> DatabaseContext:
        """The underlying database context for advanced operations.

        Raises:
            ConnectionError: If no connection is established.
        """
        return self._require_context()

    def action(self, name: str, arguments: str = "") -> ActionResult:
        """Run a registered mssqlclient-ng action by name.

        Args:
            name: Action name or alias (e.g. ``"whoami"``, ``"databases"``).
            arguments: Whitespace-separated argument string as the CLI would receive.

        Returns:
            Tabular rows, multiple tables, or ``None`` for side-effect actions.

        Raises:
            QueryError: If the action fails or is unknown.
            ConnectionError: If no connection is established.
        """
        ctx = self._require_context()
        instance = ActionFactory.get_action(name)
        if instance is None:
            raise QueryError(f"Unknown action: {name}")
        try:
            instance.validate_arguments(arguments)
            return instance.execute(database_context=ctx)
        except Exception as exc:
            raise QueryError(str(exc)) from exc

    @staticmethod
    def available_actions() -> list[tuple[str, str, list[str]]]:
        """List registered actions as ``(name, description, arguments)`` tuples."""
        return ActionFactory.get_available_actions()

    def close(self) -> None:
        """Disconnect from the server and release resources."""
        if self._auth is not None:
            self._auth.disconnect()
            self._auth = None
        self._context = None

    def __enter__(self) -> Self:
        """Connect and return this client for use as a context manager."""
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Disconnect on context-manager exit."""
        self.close()

    def _require_context(self) -> DatabaseContext:
        """Return the active context or raise if disconnected."""
        if self._context is None:
            raise ConnectionError("Not connected")
        return self._context
