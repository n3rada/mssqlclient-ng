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

# Import actions to register them with the factory
from mssqlclient_ng.src import actions

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mssqlclient-ng.py",
        add_help=True,
        description="Interract with Microsoft SQL Server (MS SQL | MSSQL) servers and their linked instances, without the need for complex T-SQL queries.",
    )

    # Target arguments
    parser.add_argument('target', action='store', help='[[domain/]username[:password]@]<targetName or address>')
    parser.add_argument('-db', action='store', help='MSSQL database instance (default None)')
    parser.add_argument('-windows-auth', action='store_true', default=False, help='whether or not to use Windows '
                                                                                  'Authentication (default False)')
    parser.add_argument('-debug', action='store_true', help='Turn DEBUG output ON')
    parser.add_argument('-ts', action='store_true', help='Adds timestamp to every logging output')
    parser.add_argument('-show', action='store_true', help='show the queries')
    parser.add_argument('-command', action='extend', nargs='*', help='Commands to execute in the SQL shell. Multiple commands can be passed.')
    parser.add_argument('-file', type=argparse.FileType('r'), help='input file with commands to execute in the SQL shell')


    # Authentication arguments
    group_auth = parser.add_argument_group('Authentication')

    group_auth.add_argument('-hashes', action="store", metavar = "LMHASH:NTHASH", help='NTLM hashes, format is LMHASH:NTHASH')
    group_auth.add_argument('-no-pass', action="store_true", help="don't ask for password (useful for -k)")
    group_auth.add_argument('-k', action="store_true", help='Use Kerberos authentication. Grabs credentials from ccache file '
                       '(KRB5CCNAME) based on target parameters. If valid credentials cannot be found, it will use the '
                       'ones specified in the command line')
    group_auth.add_argument('-aesKey', action="store", metavar = "hex key", help='AES key to use for Kerberos Authentication '
                                                                            '(128 or 256 bits)')

    group_conn = parser.add_argument_group('Connection')

    group_conn.add_argument('-dc-ip', action='store',metavar = "ip address",  help='IP Address of the domain controller. If '
                       'ommited it use the domain part (FQDN) specified in the target parameter')
    group_conn.add_argument('-target-ip', action='store', metavar = "ip address",
                       help='IP Address of the target machine. If omitted it will use whatever was specified as target. '
                            'This is useful when target is the NetBIOS name and you cannot resolve it')
    group_conn.add_argument('-port', action='store', default='1433', help='target MSSQL port (default 1433)')

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

    target_regex = re.compile(r"(?:(?:([^/@:]*)/)?([^@:]*)(?::([^@]*))?@)?(.*)")
    domain, username, password, remote_name = target_regex.match(args.target).groups('')

    # In case the password contains '@'
    if '@' in remote_name:
        password = password + '@' + remote_name.rpartition('@')[0]
        remote_name = remote_name.rpartition('@')[2]

    if domain is None:
        domain = ''

    if password == '' and username != '' and args.hashes is None and args.no_pass is False and args.aesKey is None:
        password = getpass("Password:")

    if args.target_ip is None:
        host = remote_name
    else:
        host = args.target_ip

    if args.aesKey is not None:
        args.k = True

    server_instance = server.Server(
        hostname=host,
        port=int(args.port),
        database=args.db if args.db else 'master'
    )

    with AuthenticationService(server=server_instance) as auth_service:
        if not auth_service.authenticate(
            remote_name=remote_name,
            username=username,
            password=password,
            domain=domain,
            use_windows_auth=args.windows_auth,
            hashes=args.hashes,
            aes_key=args.aesKey,
            kdc_host=args.dc_ip
        ):
            return 1


        try:
            database_context = DatabaseContext(
                auth_service=auth_service
            )
        except Exception as e:
            logger.error(f"Failed to establish database context: {e}")
            return 1

        user_name, system_user = database_context.user_service.get_info()

        database_context.server.mapped_user = user_name
        database_context.server.system_user = system_user

        logger.info(f"Logged in on {database_context.server.hostname} as {system_user}")
        logger.info(f"Mapped to the user: {user_name}")

        Terminal(database_context).start()


