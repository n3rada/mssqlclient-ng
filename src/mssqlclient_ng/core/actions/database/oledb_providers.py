# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


@ActionFactory.register(
    "oledb-providers", "Enumerate OLE DB providers and their registry settings"
)
class OleDbProviders(BaseAction):
    """
    Enumerates OLE DB providers installed on the SQL Server instance and retrieves
    their registry configuration settings.

    Reference: https://github.com/NetSPI/PowerUpSQL/blob/7d73373b0751b8648a800fbeef4c00ced66eba58/PowerUpSQL.ps1#L6987
    """

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        No additional arguments required for this action.

        Args:
            additional_arguments: Ignored.
        """
        pass

    def execute(self, database_context: DatabaseContext) -> Optional[list[dict]]:
        """
        Executes the OLE DB providers enumeration query.

        Retrieves all OLE DB providers and their registry settings including:
        - AllowInProcess
        - DisallowAdHocAccess
        - DynamicParameters
        - IndexAsAccessPath
        - LevelZeroOnly
        - NestedQueries
        - NonTransactedUpdates
        - SqlServerLIKE

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            List of dictionaries containing provider information.
        """
        logger.info("Enumerating OLE DB providers")

        query = r"""
            CREATE TABLE #Providers ([ProviderName] varchar(8000),
            [ParseName] varchar(8000),
            [ProviderDescription] varchar(8000))

            INSERT INTO #Providers
            EXEC xp_enum_oledb_providers

            -- Create temp table for provider information
            CREATE TABLE #ProviderInformation ([ProviderName] varchar(8000),
            [ProviderDescription] varchar(8000),
            [ProviderParseName] varchar(8000),
            [AllowInProcess] int,
            [DisallowAdHocAccess] int,
            [DynamicParameters] int,
            [IndexAsAccessPath] int,
            [LevelZeroOnly] int,
            [NestedQueries] int,
            [NonTransactedUpdates] int,
            [SqlServerLIKE] int)

            -- Setup required variables for cursor
            DECLARE @Provider_name varchar(8000);
            DECLARE @Provider_parse_name varchar(8000);
            DECLARE @Provider_description varchar(8000);
            DECLARE @property_name varchar(8000)
            DECLARE @regpath nvarchar(512)

            -- Start cursor
            DECLARE MY_CURSOR1 CURSOR
            FOR
            SELECT * FROM #Providers
            OPEN MY_CURSOR1
            FETCH NEXT FROM MY_CURSOR1 INTO @Provider_name,@Provider_parse_name,@Provider_description
            WHILE @@FETCH_STATUS = 0

	            BEGIN

	            -- Set the registry path
	            SET @regpath = N'SOFTWARE\Microsoft\MSSQLServer\Providers\' + @provider_name

	            -- AllowInProcess
	             DECLARE @AllowInProcess int
	             SET @AllowInProcess = 0
	             exec sys.xp_instance_regread N'HKEY_LOCAL_MACHINE',@regpath,'AllowInProcess',	@AllowInProcess OUTPUT
	             IF @AllowInProcess IS NULL
	             SET @AllowInProcess = 0

	            -- DisallowAdHocAccess
	             DECLARE @DisallowAdHocAccess int
	             SET @DisallowAdHocAccess = 0
	             exec sys.xp_instance_regread N'HKEY_LOCAL_MACHINE',@regpath,'DisallowAdHocAccess',	@DisallowAdHocAccess OUTPUT
	             IF @DisallowAdHocAccess IS NULL
	             SET @DisallowAdHocAccess = 0

	            -- DynamicParameters
	             DECLARE @DynamicParameters  int
	             SET @DynamicParameters  = 0
	             exec sys.xp_instance_regread N'HKEY_LOCAL_MACHINE',@regpath,'DynamicParameters',	@DynamicParameters OUTPUT
	             IF @DynamicParameters  IS NULL
	             SET @DynamicParameters  = 0

	            -- IndexAsAccessPath
	             DECLARE @IndexAsAccessPath int
	             SET @IndexAsAccessPath = 0
	             exec sys.xp_instance_regread N'HKEY_LOCAL_MACHINE',@regpath,'IndexAsAccessPath',	@IndexAsAccessPath OUTPUT
	             IF @IndexAsAccessPath IS NULL
	             SET @IndexAsAccessPath  = 0

	            -- LevelZeroOnly
	             DECLARE @LevelZeroOnly int
	             SET @LevelZeroOnly  = 0
	             exec sys.xp_instance_regread N'HKEY_LOCAL_MACHINE',@regpath,'LevelZeroOnly',	@LevelZeroOnly OUTPUT
	             IF  @LevelZeroOnly IS NULL
	             SET  @LevelZeroOnly  = 0

	            -- NestedQueries
	             DECLARE @NestedQueries int
	             SET @NestedQueries = 0
	             exec sys.xp_instance_regread N'HKEY_LOCAL_MACHINE',@regpath,'NestedQueries',	@NestedQueries OUTPUT
	             IF   @NestedQueries IS NULL
	             SET  @NestedQueries = 0

	            -- NonTransactedUpdates
	             DECLARE @NonTransactedUpdates int
	             SET @NonTransactedUpdates = 0
	             exec sys.xp_instance_regread N'HKEY_LOCAL_MACHINE',@regpath,'NonTransactedUpdates',	@NonTransactedUpdates  OUTPUT
	             IF  @NonTransactedUpdates IS NULL
	             SET @NonTransactedUpdates = 0

	            -- SqlServerLIKE
	             DECLARE @SqlServerLIKE int
	             SET @SqlServerLIKE  = 0
	             exec sys.xp_instance_regread N'HKEY_LOCAL_MACHINE',@regpath,'SqlServerLIKE',	@SqlServerLIKE OUTPUT
	             IF  @SqlServerLIKE IS NULL
	             SET @SqlServerLIKE = 0

	            -- Add the full provider record to the temp table
	            INSERT INTO #ProviderInformation
	            VALUES (@Provider_name,@Provider_description,@Provider_parse_name,@AllowInProcess,@DisallowAdHocAccess,@DynamicParameters,@IndexAsAccessPath,@LevelZeroOnly,@NestedQueries,@NonTransactedUpdates,@SqlServerLIKE);

	            FETCH NEXT FROM MY_CURSOR1 INTO  @Provider_name,@Provider_parse_name,@Provider_description

	            END

            -- Return records
            SELECT * FROM #ProviderInformation

            -- Clean up
            CLOSE MY_CURSOR1
            DEALLOCATE MY_CURSOR1
            DROP TABLE #Providers
            DROP TABLE #ProviderInformation
        """

        try:
            result = database_context.query_service.execute_table(query)

            if not result:
                logger.warning("No OLE DB providers found")
                return []

            logger.success(f"Found {len(result)} OLE DB provider(s)")

            # Display results as markdown table
            print(OutputFormatter.convert_list_of_dicts(result))

            return result

        except Exception as e:
            logger.error(f"Failed to enumerate OLE DB providers: {e}")
            raise

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            Empty list as no arguments are required.
        """
        return []
