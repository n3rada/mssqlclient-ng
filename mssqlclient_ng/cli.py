# Built-in imports
import argparse
import sys
import re
from getpass import getpass

# Third party imports
from loguru import logger

# Local library imports
from mssqlclient_ng.src.models import server
from mssqlclient_ng.src.services.authentication import AuthenticationService
from mssqlclient_ng.src.services.database import DatabaseContext
from mssqlclient_ng.src.terminal import Terminal
from mssqlclient_ng.src.utils import logbook
from mssqlclient_ng.src.utils import helper

# Import actions to register them with the factory
from mssqlclient_ng.src import actions


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mssqlclient-ng.py",
        add_help=True,
        description="Interract with Microsoft SQL Server (MS SQL | MSSQL) servers and their linked instances, without the need for complex T-SQL queries.",
    )

    # Target arguments
    parser.add_argument(
        "target",
        action="store",
        help="[[domain/]username[:password]@]<targetName or address>",
    )
    parser.add_argument(
        "-db", action="store", help="MSSQL database instance (default None)"
    )
    parser.add_argument(
        "-windows-auth",
        action="store_true",
        default=False,
        help="whether or not to use Windows " "Authentication (default False)",
    )
    parser.add_argument("-debug", action="store_true", help="Turn DEBUG output ON")
    parser.add_argument(
        "-ts", action="store_true", help="Adds timestamp to every logging output"
    )
    parser.add_argument("-show", action="store_true", help="show the queries")

    # Authentication arguments
    group_auth = parser.add_argument_group("Authentication")

    group_auth.add_argument(
        "-hashes",
        action="store",
        type=str,
        metavar="LMHASH:NTHASH",
        help="NTLM hashes, format is LMHASH:NTHASH",
    )
    group_auth.add_argument(
        "-no-pass", action="store_true", help="don't ask for password (useful for -k)"
    )
    group_auth.add_argument(
        "-k",
        action="store_true",
        help="Use Kerberos authentication. Grabs credentials from ccache file "
        "(KRB5CCNAME) based on target parameters. If valid credentials cannot be found, it will use the "
        "ones specified in the command line",
    )
    group_auth.add_argument(
        "-aesKey",
        action="store",
        type=str,
        metavar="hex key",
        help="AES key to use for Kerberos Authentication " "(128 or 256 bits)",
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
        "-port",
        action="store",
        type=str,
        default="1433",
        help="target MSSQL port (default 1433)",
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
        "--debug",
        action="store_true",
        required=False,
        help="Enable debug logging mode.",
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    # Show help if no cli args provided
    if len(sys.argv) <= 1:
        parser.print_help()
        return 1

    # Set log level to DEBUG if --debug is passed
    if args.debug:
        logbook.setup_logging(level="DEBUG")
    else:
        logbook.setup_logging(level="INFO")

    # Show available actions if requested
    if args.list_actions:
        helper.display_all_commands()
        return 0

    target_regex = re.compile(r"(?:(?:([^/@:]*)/)?([^@:]*)(?::([^@]*))?@)?(.*)")
    domain, username, password, remote_name = target_regex.match(args.target).groups("")

    # In case the password contains '@'
    if "@" in remote_name:
        password = password + "@" + remote_name.rpartition("@")[0]
        remote_name = remote_name.rpartition("@")[2]

    if domain is None:
        domain = ""

    if (
        password == ""
        and username != ""
        and args.hashes is None
        and args.no_pass is False
        and args.aesKey is None
    ):
        password = getpass("Password:")

    if args.target_ip is None:
        host = remote_name
    else:
        host = args.target_ip

    if args.aesKey is not None:
        args.k = True

    server_instance = server.Server(
        hostname=host, port=int(args.port), database=args.db if args.db else "master"
    )

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
            kerberos_auth=args.k,
            kdc_host=args.dc_ip,
        ) as auth_service:
            try:
                database_context = DatabaseContext(auth_service=auth_service)
            except Exception as exc:
                logger.error(f"Failed to establish database context: {exc}")
                return 1

            user_name, system_user = database_context.user_service.get_info()

            database_context.server.mapped_user = user_name
            database_context.server.system_user = system_user

            logger.info(
                f"Logged in on {database_context.server.hostname} as {system_user}"
            )
            logger.info(f"Mapped to the user: {user_name}")

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
                terminal_instance.start()

    except Exception as exc:
        logger.error(f"Unexpected error: {exc}")
        return 1

    return 0
