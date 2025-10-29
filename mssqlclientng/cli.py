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
from mssqlclientng.src.utils import helper
from mssqlclientng.src.utils import banner

# Import actions to register them with the factory
from mssqlclientng.src import actions


def build_parser() -> argparse.ArgumentParser:
    class BannerArgumentParser(argparse.ArgumentParser):
        """Custom ArgumentParser that shows banner before help."""

        def format_help(self):
            banner_text = banner.display_banner()
            return banner_text + "\n" + super().format_help()

    parser = BannerArgumentParser(
        prog="mssqlclientng",
        add_help=True,
        description="Interract with Microsoft SQL Server (MS SQL | MSSQL) servers and their linked instances, without the need for complex T-SQL queries.",
        allow_abbrev=True,
        exit_on_error=True,
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
        help="Target MS SQL Server IP or hostname.",
    )

    group_target.add_argument(
        "-P",
        "--port",
        type=int,
        required=False,
        default=1433,
        help="Target MS SQL Server port (default: 1433).",
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
        "--use-kcache",
        action="store_true",
        help="Use Kerberos authentication from ccache file (KRB5CCNAME)",
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
        help="Comma-separated list of linked servers to chain (e.g., 'SQL02:user,SQL03,SQL04:admin')",
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
        "--list-actions",
        action="store_true",
        help="List all available actions and exit.",
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
        help="Set the logging level explicitly (overrides --debug).",
    )

    return parser


def main() -> int:
    print(banner.display_banner())

    parser = build_parser()
    args = parser.parse_args()

    # Show help if no cli args provided
    if len(sys.argv) <= 1:
        parser.print_help()
        return 1

    # Determine log level: --log-level takes precedence, then --debug, then default INFO
    if args.log_level:
        log_level = args.log_level
    elif args.debug:
        log_level = "DEBUG"
    else:
        log_level = "INFO"

    logbook.setup_logging(level=log_level)

    # Show available actions if requested
    if args.list_actions:
        helper.display_all_commands()
        return 0

    # Extract credentials
    domain = args.domain if args.domain else ""
    username = args.username if args.username else ""
    password = args.password if args.password else ""

    # Prompt for password if username provided but no password/hashes/aesKey
    if username and not password and args.hashes is None and args.aesKey is None:
        password = getpass("Password: ")

    # Parse server string (hostname[:impersonation_user])
    server_instance = server.Server.parse_server(
        server_input=args.host,
        port=args.port,
        database=args.database if args.database else "master",
    )

    # Override hostname with target_ip if provided
    if args.target_ip:
        remote_name = args.host
        server_instance.hostname = args.target_ip
    else:
        remote_name = server_instance.hostname

    # Enable Kerberos if AES key or use-kcache is provided
    use_kerberos = args.kerberos or args.use_kcache or (args.aesKey is not None)

    # Determine KDC host
    kdc_host = args.kdcHost if hasattr(args, "kdcHost") and args.kdcHost else args.dc_ip

    try:
        # Context manager will connect and ensure cleanup
        with AuthenticationService(
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
        ) as auth_service:
            try:
                database_context = DatabaseContext(auth_service=auth_service)
            except Exception as exc:
                logger.error(f"Failed to establish database context: {exc}")
                return 1

            try:
                user_name, system_user = database_context.user_service.get_info()
            except Exception as exc:
                logger.error(f"Error retrieving user info: {exc}")
                return 1

            database_context.server.mapped_user = user_name
            database_context.server.system_user = system_user

            logger.info(
                f"Logged in on {database_context.server.hostname} as {system_user}"
            )
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
                        user_name, system_user = (
                            database_context.user_service.get_info()
                        )
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

            if args.query or args.action:

                # Execute query if provided
                if args.query:
                    action_name = "query"
                    argument_list = [args.query]

                elif args.action:
                    # args.action is now a list: [action_name, arg1, arg2, ...]
                    if isinstance(args.action, list) and len(args.action) > 0:
                        action_name = args.action[0]
                        argument_list = args.action[1:]
                    else:
                        logger.error("No action specified")
                        return 1

                terminal_instance.execute_action(
                    action_name=action_name, argument_list=argument_list
                )
            else:
                # Starting interactive fake-shell
                terminal_instance.start(prefix=args.prefix, multiline=args.multiline)

    except Exception as exc:
        logger.error(f"Unexpected error: {exc}")
        return 1

    return 0
