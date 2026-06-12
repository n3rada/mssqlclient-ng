# mssqlclient_ng/cli.py

# Built-in imports
import argparse
import os
import shlex
import sys
from getpass import getpass

# Third party imports
from loguru import logger

# Local library imports
from . import __version__, banner
from .core.models import server
from .core.models.linked_servers import LinkedServers
from .core.services.authentication import AuthenticationService
from .core.services.database import DatabaseContext
from .core.terminal import Terminal
from .core.utils import logbook
from .core.utils.formatters import OutputFormatter

# Import actions package to trigger @ActionFactory.register() decorators
from .core import actions  # noqa: F401
from .core.actions.factory import ActionFactory
from .core.actions.execution import query

def _log_identity(server_name: str, system_user: str, mapped_user: str) -> None:
    if mapped_user and mapped_user != system_user:
        logger.info(f"Logged in on {server_name} as {system_user} (mapped to {mapped_user})")
    else:
        logger.info(f"Logged in on {server_name} as {system_user}")


def build_parser() -> argparse.ArgumentParser:

    parser = argparse.ArgumentParser(
        prog="mssqlclient-ng",
        description="Interract with Microsoft SQL Server (MS SQL | MSSQL) servers and their linked instances, without the need for complex T-SQL queries.",
        usage="%(prog)s <host> [options] [-a [action_args]]",
        allow_abbrev=True,
        exit_on_error=False,
        add_help=True,
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="Show version and exit.",
    )

    # Target arguments
    group_target = parser.add_argument_group("Target")
    group_target.add_argument(
        "host",
        type=str,
        help="Target MS SQL Server. Format: server[,port][:user][@database]. Examples: 'SQL01', 'SQL01,1434', 'SQL01:sa@mydb'",
    )

    group_target.add_argument("-d", "--domain", type=str, help="Domain name")

    credentials_group = parser.add_argument_group(
        "Credentials", "Options for credentials"
    )
    credentials_group.add_argument(
        "-u", "--username", type=str, help="Username (either local or Windows)."
    )
    credentials_group.add_argument("-p", "--password", type=str, help="Password")
    credentials_group.add_argument(
        "-no-pass", action="store_true", help="Do not ask for password"
    )
    credentials_group.add_argument(
        "-H", "--hashes", type=str, metavar="[LMHASH:]NTHASH", help="NT/LM hashes."
    )
    credentials_group.add_argument(
        "-windows-auth",
        action="store_true",
        default=False,
        help="whether or not to use Windows " "Authentication (default False)",
    )

    kerberos_group = parser.add_argument_group(
        "Kerberos", "Options for Kerberos authentication"
    )
    kerberos_group.add_argument(
        "-k", "--kerberos", action="store_true", help="Use Kerberos authentication"
    )

    kerberos_group.add_argument(
        "--aesKey",
        metavar="AESKEY",
        nargs="+",
        help="AES key to use for Kerberos Authentication (128 or 256 bits)",
    )
    kerberos_group.add_argument(
        "--kdcHost",
        metavar="KDCHOST",
        help="FQDN of the domain controller. If omitted it will use the domain part (FQDN) specified in the target parameter",
    )

    group_target.add_argument(
        "-db",
        "--database",
        action="store",
        help="MSSQL database instance (default None)",
    )

    group_target.add_argument(
        "-l",
        "--links",
        type=str,
        help="Semicolon-separated linked server chain (e.g., 'SQL02/user;SQL03;SQL04/admin@db'). Use /user1/user2 for cascading impersonation.",
    )

    group_relay = parser.add_argument_group("NTLM Relay")
    group_relay.add_argument(
        "-r",
        "--ntlm-relay",
        action="store_true",
        help="Start a NTLM relay listener to capture and relay incoming authentication attempts.",
    )
    group_relay.add_argument(
        "-smb2support", action="store_true", default=False, help="SMB2 Support"
    )
    group_relay.add_argument(
        "-ntlmchallenge",
        action="store",
        default=None,
        help="Specifies the NTLM server challenge used by the "
        "SMB Server (16 hex bytes long. eg: 1122334455667788)",
    )
    group_relay.add_argument(
        "-t",
        "--timeout",
        type=int,
        default=60,
        help="Timeout in seconds to wait for relayed connection (default: 60)",
    )

    group_conn = parser.add_argument_group("Connection")

    group_conn.add_argument(
        "-dc-ip",
        action="store",
        type=str,
        metavar="ip address",
        help="IP Address of the domain controller. If "
        "ommited it use the domain part (FQDN) specified in the target parameter",
    )
    group_conn.add_argument(
        "-target-ip",
        action="store",
        type=str,
        metavar="ip address",
        help="IP Address of the target machine. If omitted it will use whatever was specified as target. "
        "This is useful when target is the NetBIOS name and you cannot resolve it",
    )
    group_conn.add_argument(
        "--workstation-id",
        type=str,
        default="SQLServerCEIP",
        help="Workstation ID sent in the TDS LOGIN packet (default: SQLServerCEIP).",
    )
    group_conn.add_argument(
        "--app-name",
        type=str,
        default="Framework Microsoft SqlClient Da",
        help="Application name sent in the TDS LOGIN packet (default: 'Framework Microsoft SqlClient Da').",
    )
    group_conn.add_argument(
        "--client-interface-name",
        type=str,
        default=".Net SqlClient Data Provider",
        help="Client interface name sent in the TDS LOGIN packet (default: '.Net SqlClient Data Provider').",
    )

    actions_groups = parser.add_argument_group(
        "Actions", "Actions to perform upon successful connection."
    )

    actions_groups.add_argument(
        "-q",
        "--query",
        type=str,
        default=None,
        help="T-SQL command to execute upon successful connection.",
    )

    actions_groups.add_argument(
        "-a",
        "--action",
        type=str,
        nargs=argparse.REMAINDER,
        default=None,
        help="Action to perform upon successful connection, followed by its arguments.",
    )

    actions_groups.add_argument(
        "-o",
        "--output-format",
        type=str,
        default="markdown",
        choices=["markdown", "md", "csv", "grid", "box", "table", "json"],
        help="Output format for data display (default: markdown). Options: markdown/md, csv, grid/box/table, json.",
    )

    advanced_group = parser.add_argument_group(
        "Advanced Options", "Additional advanced or debugging options."
    )

    advanced_group.add_argument(
        "--prefix",
        action="store",
        required=False,
        type=str,
        default="!",
        help="Command prefix for actions.",
    )

    advanced_group.add_argument(
        "--no-history",
        action="store_true",
        required=False,
        default=False,
        help="Disable persistent command history.",
    )

    advanced_group.add_argument(
        "--multiline",
        action="store_true",
        required=False,
        default=False,
        help="Enable multiline input mode.",
    )

    advanced_group.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging (shortcut for --log-level DEBUG).",
    )

    advanced_group.add_argument(
        "--trace",
        action="store_true",
        help="Enable TRACE logging (shortcut for --log-level TRACE).",
    )

    advanced_group.add_argument(
        "--log-level",
        type=str,
        choices=["TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default=None,
        help="set the logging level explicitly.",
    )

    advanced_group.add_argument(
        "--std",
        type=str,
        choices=["err", "out"],
        default="err",
        help="Output stream for logs: 'err' for stderr (default), 'out' for stdout. Useful in restricted environments.",
    )

    advanced_group.add_argument(
        "--no-log-file",
        action="store_true",
        help="Disable file logging (only output to console).",
    )

    return parser

def main() -> int:
    parser = build_parser()
    try:
        args = parser.parse_args()
    except argparse.ArgumentError as exc:
        print(str(exc))
        parser.print_usage()
        return 2
    except SystemExit:
        # Raised by argparse for things like --version or malformed invocations
        return 2

    # Action help: handle before banner/logging to keep output clean
    if args.action and isinstance(args.action, list) and len(args.action) > 0:
        action_name = args.action[0]
        if "--help" in args.action[1:] or "-h" in args.action[1:]:
            if not ActionFactory.action_exists(action_name):
                print(f"Unknown action: {action_name}")
                return 1
            ActionFactory.display_action_help(action_name)
            return 0

    print(banner.display_banner())

    # Show help if no cli args provided at all
    if len(sys.argv) <= 1:
        parser.print_help()
        return 0

    # Determine log level: --log-level takes precedence, then --trace, then --debug, then INFO
    if args.log_level:
        log_level = args.log_level
    elif args.trace:
        log_level = "TRACE"
    elif args.debug:
        log_level = "DEBUG"
    else:
        log_level = "INFO"

    # Determine output stream
    log_stream = args.std

    # Check if file logging should be disabled
    no_log_file = args.no_log_file

    logbook.setup_logging(
        level=log_level, stream=log_stream, enable_file=not no_log_file
    )

    # set output format based on CLI argument
    try:
        OutputFormatter.set_format(args.output_format)
    except ValueError as e:
        logger.error(f"Invalid output format: {e}")
        return 1

    try:
        server_instance = server.Server.parse_server(server_input=args.host)
        # Apply -db/--database override if provided
        if args.database:
            server_instance.database = args.database
        # If still no database, leave as None so SQL Server uses the login's default database
    except ValueError as e:
        logger.error(f"Invalid host format: {e}")
        return 1

    # Establish connection - either via relay or direct authentication
    auth_service = None
    database_context = None

    use_kerberos = False

    # Require explicit credentials — no silent anonymous SQL auth
    if not args.ntlm_relay and not any(
        [
            args.username,
            args.windows_auth,
            args.kerberos,
            args.hashes,
            getattr(args, "aesKey", None),
            args.no_pass,
        ]
    ):
        logger.error("No credentials provided.")
        logger.error(
            "Use -u/-p for SQL auth, -u/-p -windows-auth for Windows auth, "
            "-k for Kerberos, -H for pass-the-hash, or -no-pass to connect without a password."
        )
        return 1

    if args.ntlm_relay:
        from .core.services.ntlmrelay import RelayMSSQL

        relay = RelayMSSQL(hostname=server_instance.hostname, port=server_instance.port)
        relay.start(smb2support=args.smb2support, ntlmchallenge=args.ntlmchallenge)

        try:
            # Wait for relayed connection and create DatabaseContext
            database_context = relay.wait_for_connection(
                server_instance=server_instance, timeout=args.timeout
            )

            if not database_context:
                return 1

            logger.success("Using relayed connection for interactive session")
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
            return 0
        finally:
            # Always cleanup relay servers
            relay.stop_servers()

    else:
        # Extract credentials
        domain = args.domain if args.domain else ""
        username = args.username if args.username else ""
        password = args.password if args.password else ""

        # Auto-split DOMAIN\user, DOMAIN/user, or user@domain when -d was not provided
        if username and not domain:
            if "\\" in username:
                domain, username = username.split("\\", 1)
            elif "/" in username and not username.startswith("/"):
                domain, username = username.split("/", 1)
            elif "@" in username:
                username, domain = username.split("@", 1)

        logger.debug(
            f"Parsed credentials - domain: {domain!r}, username: {username!r}, password set: {bool(password)}, hashes: {args.hashes!r}, windows_auth: {args.windows_auth}, kerberos: {args.kerberos}"
        )
        logger.debug(
            f"Target - host arg: {args.host!r}, server hostname: {server_instance.hostname!r}, port: {server_instance.port}, dc_ip: {args.dc_ip!r}, target_ip: {getattr(args, 'target_ip', None)!r}"
        )

        # Show resolved identity before attempting connection
        if args.kerberos:
            auth_label = f"{domain}\\{username}" if domain else username
            logger.info(f"Authenticating as {auth_label!r} via Kerberos")
        elif args.windows_auth:
            auth_label = f"{domain}\\{username}" if domain else username
            logger.info(f"Authenticating as {auth_label!r} via Windows auth (NTLM)")
        elif username:
            logger.info(f"Authenticating as {username!r} via SQL auth")

        # Prompt for password if username provided but no password/hashes/aesKey.
        # Skip when Kerberos is requested — ccache or AES key handles auth.
        if (
            username
            and not password
            and args.hashes is None
            and args.aesKey is None
            and not args.no_pass
            and not args.kerberos
        ):
            password = getpass("Password: ")

        # Override hostname with target_ip if provided
        if args.target_ip:
            remote_name = server_instance.hostname  # preserve clean name for Kerberos SPN
            server_instance.hostname = args.target_ip
        else:
            remote_name = server_instance.hostname

        # Enable Kerberos if AES key is provided
        use_kerberos = args.kerberos or (args.aesKey is not None)

        if use_kerberos and not args.aesKey and not os.environ.get("KRB5CCNAME"):
            logger.error("KRB5CCNAME is not set. Point it to your ccache before using Kerberos.")
            return 1

        # Determine KDC host
        kdc_host = args.kdcHost or args.dc_ip

        # Create authentication service and connect (long-lived connection)
        auth_service = AuthenticationService(
            server=server_instance,
            remote_name=remote_name,
            username=username,
            password=password,
            domain=domain,
            use_windows_auth=args.windows_auth,
            hashes=args.hashes,
            aes_key=args.aesKey,
            kerberos_auth=use_kerberos,
            kdc_host=kdc_host,
            workstation_id=args.workstation_id,
            application_name=args.app_name,
            client_interface_name=args.client_interface_name,
        )

        if not auth_service.connect():
            logger.error("Failed to authenticate")
            return 1

        try:
            assert auth_service.mssql_instance is not None
            database_context = DatabaseContext(
                server=server_instance,
                mssql_instance=auth_service.mssql_instance,
            )
        except Exception:
            logger.exception("Failed to establish database context")
            auth_service.disconnect()
            return 1

    # Common execution path for both relay and normal authentication
    try:
        # Display pre-impersonation identity (matches MSSQLand GetInfo() order)
        system_user = database_context.pre_impersonation_system
        user_name = database_context.pre_impersonation_user
        database_context.server.mapped_user = user_name
        database_context.server.system_user = system_user

        _log_identity(database_context.server.hostname, system_user, user_name)

        # Compute effective user and source principal (handles group-based access via AD groups)
        # Only works for Windows authentication (NTLM/Kerberos) on on-premises SQL Server
        # Does NOT work for: SQL auth, Azure AD auth, LocalDB, or linked servers
        # Already computed in DatabaseContext constructor (before impersonation)
        if (
            args.windows_auth or use_kerberos
        ) and database_context.user_service.is_domain_user:
            effective_user = database_context.user_service.effective_user
            source_principal = database_context.user_service.source_principal

            if effective_user and not effective_user.lower() == user_name.lower():
                logger.info(f"Effective database user: {effective_user}")
            if source_principal and source_principal.lower() != system_user.lower():
                logger.info(
                    f"Domain user is mapped via Domain Group '{source_principal}'"
                )

        # Show impersonation chain if impersonation was applied on the initial server
        impersonation_users = database_context.server.impersonation_users
        if impersonation_users:
            chain = " → ".join(impersonation_users)
            logger.info(f"Impersonation chain: {chain}")

            # Refresh identity to reflect the post-impersonation context
            user_name, system_user = database_context.user_service.get_info()
            database_context.server.mapped_user = user_name
            database_context.server.system_user = system_user

            _log_identity(database_context.server.hostname, system_user, user_name)

        # If linked servers are provided, set them up
        if args.links:
            try:
                linked_servers = LinkedServers(args.links)
                database_context.query_service.linked_servers = linked_servers

                chain_display = linked_servers.format_chain_display(
                    initial_host=database_context.server.hostname,
                    initial_login=database_context.server.system_user,
                    initial_impersonation=database_context.server.impersonation_users,
                )
                logger.info(f"Server chain: {chain_display}")

                # Compute execution database after linked server chain is set up
                database_context.query_service.compute_execution_database()

                # Get info from the final server in the chain
                try:
                    user_name, system_user = database_context.user_service.get_info()
                except Exception:
                    logger.exception("Error retrieving user info from linked server")
                    return 1

                _log_identity(database_context.query_service.execution_server, system_user, user_name)

            except Exception:
                logger.exception("Failed to set up linked servers")
                return 1

        # Compute and display the final execution context
        database_context.query_service.compute_execution_database()
        logger.info(
            f"Execution database: {database_context.query_service.execution_database or 'default'}"
        )

        # Detect Azure SQL on the final execution server
        _ = database_context.query_service.is_azure_sql

        if args.query or args.action:
            # Execute single action/query and exit
            if args.action:
                # args.action is a list: [action_name, arg1, arg2, ]
                if isinstance(args.action, list) and len(args.action) > 0:
                    action_name = args.action[0]
                    argument_list = args.action[1:]
                else:
                    logger.error("No action specified")
                    return 1

                # Handle special case: query action
                if action_name == "query":
                    args.query = " ".join(argument_list)
                else:
                    # Get and execute the action directly
                    action_instance = ActionFactory.get_action(action_name)
                    if action_instance is None:
                        logger.error(f"Unknown action: {action_name}")
                        return 1

                    try:
                        # Use shlex.join to preserve arguments with spaces
                        args_str = shlex.join(argument_list)
                        action_instance.validate_arguments(args_str)
                    except ValueError as ve:
                        logger.error(f"Argument validation error: {ve}")
                        return 1

                    server_name = database_context.query_service.execution_server
                    logger.info(
                        f"Executing action '{action_name}' against {server_name}"
                    )

                    try:
                        action_instance.execute(database_context=database_context)
                        return 0
                    except Exception:
                        logger.exception("Action execution failed")
                        return 1

            # Execute query if provided
            if args.query:
                query_action = query.Query()
                try:
                    query_action.validate_arguments(additional_arguments=args.query)
                except ValueError as ve:
                    logger.error(f"Argument validation error: {ve}")
                    return 1

                query_action.execute(database_context)
                return 0

        else:
            # Starting interactive shell - only create Terminal instance here
            terminal_instance = Terminal(database_context)
            terminal_instance.start(
                prefix=args.prefix,
                multiline=args.multiline,
                history=not args.no_history,
            )
            return 0

    except Exception:
        logger.exception("Error in execution")
        return 1
    finally:
        # Clean up authentication service if it was created (non-relay mode)
        if auth_service is not None:
            auth_service.disconnect()
