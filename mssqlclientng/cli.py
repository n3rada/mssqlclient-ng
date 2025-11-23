# Built-in imports
import argparse
import sys
from getpass import getpass

# Third party imports
from loguru import logger

# Local library imports
from mssqlclientng import __version__
from mssqlclientng.src.models import server
from mssqlclientng.src.models.linked_servers import LinkedServers
from mssqlclientng.src.services.authentication import AuthenticationService
from mssqlclientng.src.services.database import DatabaseContext
from mssqlclientng.src.terminal import Terminal
from mssqlclientng.src.utils import logbook
from mssqlclientng.src.utils import banner

# Import actions to register them with the factory
from mssqlclientng.src import actions
from mssqlclientng.src.actions.factory import ActionFactory
from mssqlclientng.src.actions.execution import query


def build_parser() -> argparse.ArgumentParser:

    parser = argparse.ArgumentParser(
        prog="mssqlclient-ng",
        add_help=False,  # We'll handle help manually
        description="Interract with Microsoft SQL Server (MS SQL | MSSQL) servers and their linked instances, without the need for complex T-SQL queries.",
        usage="%(prog)s <host> [options] [action [action-options]]",
        allow_abbrev=True,
        exit_on_error=False,
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="Show version and exit.",
    )

    parser.add_argument(
        "-h",
        "--help",
        nargs="?",
        const="__general__",
        metavar="ACTION",
        help="Show help. Use '-h ACTION' to show help for a specific action (e.g., '-h createuser'). Use '-h search_term' to filter actions.",
    )

    # Target arguments
    group_target = parser.add_argument_group("Target")
    group_target.add_argument(
        "host",
        type=str,
        nargs="?",
        help="Target MS SQL Server. Format: server[,port][:user][@database]. Examples: 'SQL01', 'SQL01,1434', 'SQL01:sa@mydb'",
    )

    credentials_group = parser.add_argument_group(
        "Credentials", "Options for credentials"
    )
    credentials_group.add_argument(
        "-d", "--domain", type=str, help="Domain name for Windows authentication"
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
        "-l",
        "--links",
        type=str,
        help="Comma-separated list of linked servers to chain (e.g., 'SQL02:user,SQL03,SQL04:admin')",
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

    # Positional action argument
    parser.add_argument(
        "action",
        type=str,
        nargs="?",
        help="Action to perform (e.g., 'info', 'createuser', 'links'). Use '-h' to see all actions or '-h ACTION' for specific action help.",
    )

    # Action arguments (everything after action)
    parser.add_argument(
        "action_args",
        nargs=argparse.REMAINDER,
        help="Arguments for the specified action.",
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
        "--history",
        action="store_true",
        required=False,
        default=False,
        help="Enable persistent command history (stored in temporary folder).",
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
        "--log-level",
        type=str,
        choices=["TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default=None,
        help="Set the logging level explicitly.",
    )

    return parser


def display_general_help(parser: argparse.ArgumentParser) -> None:
    """Display general help including all available actions."""
    parser.print_help()
    print("\n" + "=" * 80)
    print("AVAILABLE ACTIONS:")
    print("=" * 80)
    
    actions = ActionFactory.get_available_actions()
    
    # Group actions by category (based on package structure)
    from collections import defaultdict
    categorized = defaultdict(list)
    
    for name, description, arguments in sorted(actions):
        # Try to determine category from action class module
        action_class = ActionFactory.get_action_type(name)
        if action_class:
            module_path = action_class.__module__
            if 'administration' in module_path:
                category = 'Administration'
            elif 'database' in module_path:
                category = 'Database'
            elif 'domain' in module_path:
                category = 'Domain'
            elif 'execution' in module_path:
                category = 'Execution'
            elif 'filesystem' in module_path:
                category = 'Filesystem'
            elif 'network' in module_path:
                category = 'Network'
            else:
                category = 'Other'
        else:
            category = 'Other'
        
        categorized[category].append((name, description))
    
    # Display actions by category
    for category in sorted(categorized.keys()):
        print(f"\n{category}:")
        print("-" * 80)
        for name, description in categorized[category]:
            print(f"  {name:20} - {description}")
    
    print("\n" + "=" * 80)
    print("USAGE EXAMPLES:")
    print("=" * 80)
    print("  mssqlclient-ng SQL01 -u sa -p password info")
    print("  mssqlclient-ng SQL01,1434@mydb -u admin -p pass createuser backup_user P@ssw0rd")
    print("  mssqlclient-ng SQL01:webapp01 -c token links")
    print("  mssqlclient-ng SQL01 -u sa -p password -l SQL02,SQL03 info")
    print("\nFor detailed help on a specific action:")
    print("  mssqlclient-ng SQL01 -u sa -p password createuser -h")
    print("  mssqlclient-ng -h createuser")
    print("\nTo filter actions by keyword:")
    print("  mssqlclient-ng -h adsi")
    print("=" * 80 + "\n")


def display_action_help(action_name: str) -> bool:
    """
    Display help for a specific action.
    
    Returns:
        True if action was found and help displayed, False otherwise.
    """
    action = ActionFactory.get_action(action_name)
    if not action:
        return False
    
    print("=" * 80)
    print(f"ACTION: {action_name}")
    print("=" * 80)
    print(f"\n{action.get_help()}\n")
    
    arguments = action.get_arguments()
    if arguments:
        print("ARGUMENTS:")
        print("-" * 80)
        for i, arg in enumerate(arguments, 1):
            print(f"  {i}. {arg}")
    else:
        print("This action takes no arguments.")
    
    print("\n" + "=" * 80)
    print("USAGE:")
    print("=" * 80)
    print(f"  mssqlclient-ng <host> [options] {action_name} [arguments]")
    print("\nEXAMPLE:")
    print(f"  mssqlclient-ng SQL01 -u sa -p password {action_name}")
    if arguments:
        print(f"  mssqlclient-ng SQL01 -u sa -p password {action_name} arg1 arg2")
    print("=" * 80 + "\n")
    
    return True


def filter_actions_by_keyword(keyword: str) -> None:
    """Display actions matching a keyword filter."""
    actions = ActionFactory.get_available_actions()
    matching = [
        (name, description) 
        for name, description, _ in actions 
        if keyword.lower() in name.lower() or keyword.lower() in description.lower()
    ]
    
    if not matching:
        print(f"No actions found matching '{keyword}'")
        return
    
    print("=" * 80)
    print(f"ACTIONS MATCHING '{keyword}':")
    print("=" * 80)
    for name, description in sorted(matching):
        print(f"  {name:20} - {description}")
    print(f"\nFound {len(matching)} action(s).")
    print("=" * 80)
    print(f"\nFor detailed help: mssqlclient-ng -h <action_name>")
    print("=" * 80 + "\n")


def main() -> int:
    print(banner.display_banner())

    parser = build_parser()
    
    # Parse known args to handle --version and other flags separately from action args
    try:
        args, unknown = parser.parse_known_args()
    except SystemExit:
        return 1
    
    # Handle help specially
    if args.help is not None:
        if args.help == "__general__":
            # General help requested
            display_general_help(parser)
        else:
            # Check if it's an action name or a search keyword
            if ActionFactory.get_action(args.help):
                # Specific action help
                display_action_help(args.help)
            else:
                # Filter actions by keyword
                filter_actions_by_keyword(args.help)
        return 0
    
    # If no host provided, show help
    if not args.host:
        display_general_help(parser)
        return 1

    # Determine log level: --log-level takes precedence, then --debug, then default INFO
    if args.log_level:
        log_level = args.log_level
    elif args.debug:
        log_level = "DEBUG"
    else:
        log_level = "INFO"

    logbook.setup_logging(level=log_level)

    # Parse server string (now supports server[,port][:user][@database])
    try:
        server_instance = server.Server.parse_server(server_input=args.host)
    except ValueError as e:
        logger.error(f"Invalid host format: {e}")
        return 1

    # Establish connection - either via relay or direct authentication
    auth_service = None
    database_context = None

    if args.ntlm_relay:
        from mssqlclientng.src.services.ntlmrelay import RelayMSSQL

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

        # Prompt for password if username provided but no password/hashes/aesKey
        if (
            username
            and not password
            and args.hashes is None
            and args.aesKey is None
            and not args.no_pass
        ):
            password = getpass("Password: ")

        # Override hostname with target_ip if provided
        if args.target_ip:
            remote_name = args.host
            server_instance.hostname = args.target_ip
        else:
            remote_name = server_instance.hostname

        # Enable Kerberos if AES key is provided
        use_kerberos = args.kerberos or (args.aesKey is not None)

        # Determine KDC host
        kdc_host = (
            args.kdcHost if hasattr(args, "kdcHost") and args.kdcHost else args.dc_ip
        )

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
        )

        if not auth_service.connect():
            logger.error("Failed to authenticate")
            return 1

        try:
            database_context = DatabaseContext(
                server=server_instance,
                mssql_instance=auth_service.mssql_instance,
            )
        except Exception as exc:
            logger.error(f"Failed to establish database context: {exc}")
            auth_service.disconnect()
            return 1

    # Common execution path for both relay and normal authentication
    try:
        user_name, system_user = database_context.user_service.get_info()
        database_context.server.mapped_user = user_name
        database_context.server.system_user = system_user

        logger.info(f"Logged in on {database_context.server.hostname} as {system_user}")
        logger.info(f"Mapped to the user: {user_name}")

        # If linked servers are provided, set them up
        if args.links:
            try:
                linked_servers = LinkedServers(args.links)
                database_context.query_service.linked_servers = linked_servers

                chain_display = " -> ".join(linked_servers.server_names)
                logger.info(
                    f"Server chain: {database_context.server.hostname} -> {chain_display}"
                )

                # Get info from the final server in the chain
                try:
                    user_name, system_user = database_context.user_service.get_info()
                except Exception as exc:
                    logger.error(
                        f"Error retrieving user info from linked server: {exc}"
                    )
                    return 1

                logger.info(
                    f"Logged in on {database_context.query_service.execution_server} as {system_user}"
                )
                logger.info(f"Mapped to the user: {user_name}")

            except Exception as exc:
                logger.error(f"Failed to set up linked servers: {exc}")
                return 1

        terminal_instance = Terminal(database_context)

        # Check if an action was specified
        if args.action:
            # Check if user wants help for this specific action
            if args.action_args and args.action_args[0] in ['-h', '--help']:
                display_action_help(args.action)
                return 0
            
            # Execute specified action
            if args.action == "query":
                # Special case: query action takes T-SQL as arguments
                query_sql = " ".join(args.action_args) if args.action_args else ""
                if not query_sql:
                    logger.error("No SQL query provided")
                    return 1
                
                query_action = query.Query()
                try:
                    query_action.validate_arguments(additional_arguments=query_sql)
                except ValueError as ve:
                    logger.error(f"Argument validation error: {ve}")
                    return 1

                query_action.execute(database_context)
                return 0
            else:
                # Execute regular action with its arguments
                terminal_instance.execute_action(
                    action_name=args.action, 
                    argument_list=args.action_args if args.action_args else []
                )
                return 0
        else:
            # No action specified - start interactive terminal
            terminal_instance.start(
                prefix=args.prefix, multiline=args.multiline, history=args.history
            )
            return 0

    except Exception as exc:
        logger.error(f"Error in execution: {exc}")
        return 1
    finally:
        # Clean up authentication service if it was created (non-relay mode)
        if auth_service is not None:
            auth_service.disconnect()
