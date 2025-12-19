"""
Test server parsing.

Syntax: host:port/user@database
- : = port separator (standard host:port)
- / = impersonation ("execute as user")
- @ = database context
- ; = chain separator

Run with: python3 -m unittest tests.test_server_parsing
"""

import unittest
from src.mssqlclient_ng.core.models.server import Server
from src.mssqlclient_ng.core.models.linked_servers import LinkedServers


class TestServerParsing(unittest.TestCase):
    """Test Server.parse_server() with various input formats."""

    def test_simple_hostname(self):
        """Test parsing a simple hostname without any delimiters."""
        server = Server.parse_server("SQL01")
        self.assertEqual(server.hostname, "SQL01")
        self.assertEqual(server.port, 1433)
        self.assertEqual(server.impersonation_user, "")
        self.assertIsNone(server.database)

    def test_hostname_with_port(self):
        """Test parsing hostname with port specification."""
        server = Server.parse_server("SQL01:1434")
        self.assertEqual(server.hostname, "SQL01")
        self.assertEqual(server.port, 1434)

    def test_hostname_with_user(self):
        """Test parsing hostname with impersonation user."""
        server = Server.parse_server("SQL01/admin")
        self.assertEqual(server.hostname, "SQL01")
        self.assertEqual(server.impersonation_user, "admin")

    def test_hostname_with_database(self):
        """Test parsing hostname with database context."""
        server = Server.parse_server("SQL01@mydb")
        self.assertEqual(server.hostname, "SQL01")
        self.assertEqual(server.database, "mydb")

    def test_complete_syntax(self):
        """Test parsing with all components in order."""
        server = Server.parse_server("SQL01:1434/admin@mydb")
        self.assertEqual(server.hostname, "SQL01")
        self.assertEqual(server.port, 1434)
        self.assertEqual(server.impersonation_user, "admin")
        self.assertEqual(server.database, "mydb")

    def test_flexible_order_user_db_port(self):
        """Test parsing with flexible component order."""
        server = Server.parse_server("SQL01/admin@mydb:1434")
        self.assertEqual(server.hostname, "SQL01")
        self.assertEqual(server.port, 1434)
        self.assertEqual(server.impersonation_user, "admin")
        self.assertEqual(server.database, "mydb")

    def test_flexible_order_db_port_user(self):
        """Test parsing with database, port, then user."""
        server = Server.parse_server("SQL01@mydb:1434/admin")
        self.assertEqual(server.hostname, "SQL01")
        self.assertEqual(server.port, 1434)
        self.assertEqual(server.impersonation_user, "admin")
        self.assertEqual(server.database, "mydb")


class TestBracketedServerNames(unittest.TestCase):
    """Test bracketed SQL Server identifiers with special characters."""

    def test_bracketed_simple(self):
        """Test parsing a simple bracketed server name."""
        server = Server.parse_server("[SQL-01]")
        self.assertEqual(server.hostname, "SQL-01")
        self.assertEqual(server.port, 1433)

    def test_bracketed_with_colon(self):
        """Test parsing bracketed server name containing colon."""
        server = Server.parse_server("[SERVER:001]")
        self.assertEqual(server.hostname, "SERVER:001")
        self.assertEqual(server.port, 1433)

    def test_bracketed_with_colon_and_port(self):
        """Test parsing bracketed server with colon in name AND explicit port."""
        server = Server.parse_server("[SERVER:001]:1434")
        self.assertEqual(server.hostname, "SERVER:001")
        self.assertEqual(server.port, 1434)

    def test_bracketed_with_semicolon(self):
        """Test parsing bracketed server name containing semicolon."""
        server = Server.parse_server("[SERVER;PROD]")
        self.assertEqual(server.hostname, "SERVER;PROD")
        self.assertEqual(server.port, 1433)

    def test_bracketed_complete_syntax(self):
        """Test parsing bracketed server with all components."""
        server = Server.parse_server("[SERVER:001]:1434/admin@mydb")
        self.assertEqual(server.hostname, "SERVER:001")
        self.assertEqual(server.port, 1434)
        self.assertEqual(server.impersonation_user, "admin")
        self.assertEqual(server.database, "mydb")

    def test_bracketed_with_dots(self):
        """Test parsing bracketed server name with dots."""
        server = Server.parse_server("[SQL.PROD.COM]")
        self.assertEqual(server.hostname, "SQL.PROD.COM")

    def test_bracketed_with_slash(self):
        """Test parsing bracketed server name containing forward slash."""
        server = Server.parse_server("[SERVER/TEST]")
        self.assertEqual(server.hostname, "SERVER/TEST")


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and error handling."""

    def test_empty_string(self):
        """Test parsing empty string raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            Server.parse_server("")
        self.assertIn("cannot be null or empty", str(ctx.exception))

    def test_whitespace_only(self):
        """Test parsing whitespace-only string raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            Server.parse_server("   ")
        self.assertIn("cannot be null or empty", str(ctx.exception))

    def test_unclosed_bracket(self):
        """Test parsing unclosed bracket raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            Server.parse_server("[SERVER")
        self.assertIn("Unclosed bracket", str(ctx.exception))

    def test_empty_port(self):
        """Test parsing empty port uses default port."""
        result = Server.parse_server("SQL01:")
        self.assertEqual(result.hostname, "SQL01")
        self.assertEqual(result.port, 1433)  # Falls back to default

    def test_invalid_port_number(self):
        """Test parsing invalid port number raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            Server.parse_server("SQL01:abc")
        self.assertIn("Invalid port number", str(ctx.exception))

    def test_port_out_of_range_high(self):
        """Test parsing port number too high raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            Server.parse_server("SQL01:99999")
        self.assertIn("Port must be between 1 and 65535", str(ctx.exception))

    def test_port_out_of_range_low(self):
        """Test parsing port number too low raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            Server.parse_server("SQL01:0")
        self.assertIn("Port must be between 1 and 65535", str(ctx.exception))

    def test_empty_user(self):
        """Test parsing empty user is treated as no impersonation."""
        result = Server.parse_server("SQL01/")
        self.assertEqual(result.hostname, "SQL01")
        self.assertEqual(result.impersonation_user, "")  # Empty string, not None

    def test_empty_database(self):
        """Test parsing empty database uses default (None)."""
        result = Server.parse_server("SQL01@")
        self.assertEqual(result.hostname, "SQL01")
        self.assertIsNone(result.database)  # Falls back to None


class TestLinkedServerChains(unittest.TestCase):
    """Test LinkedServers parsing with semicolon-separated chains."""

    def test_simple_chain(self):
        """Test parsing simple semicolon-separated chain."""
        chain = LinkedServers("SQL01;SQL02;SQL03")
        self.assertEqual(len(chain.server_chain), 3)
        self.assertEqual(chain.server_chain[0].hostname, "SQL01")
        self.assertEqual(chain.server_chain[1].hostname, "SQL02")
        self.assertEqual(chain.server_chain[2].hostname, "SQL03")

    def test_chain_with_users(self):
        """Test parsing chain with impersonation users."""
        chain = LinkedServers("SQL01/user1;SQL02/user2")
        self.assertEqual(len(chain.server_chain), 2)
        self.assertEqual(chain.server_chain[0].impersonation_user, "user1")
        self.assertEqual(chain.server_chain[1].impersonation_user, "user2")

    def test_chain_with_databases(self):
        """Test parsing chain with database contexts."""
        chain = LinkedServers("SQL01@db1;SQL02@db2")
        self.assertEqual(len(chain.server_chain), 2)
        self.assertEqual(chain.server_chain[0].database, "db1")
        self.assertEqual(chain.server_chain[1].database, "db2")

    def test_chain_mixed(self):
        """Test parsing chain with mixed components."""
        chain = LinkedServers("SQL01/admin;SQL02;SQL03@analytics")
        self.assertEqual(len(chain.server_chain), 3)
        self.assertEqual(chain.server_chain[0].impersonation_user, "admin")
        self.assertEqual(chain.server_chain[1].impersonation_user, "")
        self.assertEqual(chain.server_chain[2].database, "analytics")

    def test_chain_with_colons_in_names(self):
        """Test parsing chain with colons in bracketed server names."""
        chain = LinkedServers("[SERVER:001];[SQL:TEST]")
        self.assertEqual(len(chain.server_chain), 2)
        self.assertEqual(chain.server_chain[0].hostname, "SERVER:001")
        self.assertEqual(chain.server_chain[1].hostname, "SQL:TEST")

    def test_chain_with_semicolons_in_names(self):
        """Test parsing chain with semicolons in bracketed server names."""
        chain = LinkedServers("[SERVER;PROD];[SQL;TEST]")
        self.assertEqual(len(chain.server_chain), 2)
        self.assertEqual(chain.server_chain[0].hostname, "SERVER;PROD")
        self.assertEqual(chain.server_chain[1].hostname, "SQL;TEST")

    def test_chain_complex(self):
        """Test parsing complex chain with all features."""
        chain = LinkedServers("[SQL-01]/admin@master;[SERVER:001]/webapp@appdb;SQL03")
        self.assertEqual(len(chain.server_chain), 3)
        self.assertEqual(chain.server_chain[0].hostname, "SQL-01")
        self.assertEqual(chain.server_chain[0].impersonation_user, "admin")
        self.assertEqual(chain.server_chain[0].database, "master")
        self.assertEqual(chain.server_chain[1].hostname, "SERVER:001")
        self.assertEqual(chain.server_chain[1].impersonation_user, "webapp")
        self.assertEqual(chain.server_chain[1].database, "appdb")
        self.assertEqual(chain.server_chain[2].hostname, "SQL03")

    def test_chain_with_port_notation(self):
        """Test parsing chain where bracketed server has port notation after."""
        chain = LinkedServers("[SERVER:001]:1434;SQL02;SQL03")
        self.assertEqual(len(chain.server_chain), 3)
        self.assertEqual(chain.server_chain[0].hostname, "SERVER:001")
        self.assertEqual(chain.server_chain[0].port, 1434)

    def test_chain_string_format(self):
        """Test that get_chain_arguments() returns semicolon-separated string."""
        chain = LinkedServers("[SQL-01]/admin@master;[SERVER:001]/webapp@appdb;SQL03")
        chain_str = chain.get_chain_arguments()
        self.assertIn(";", chain_str)
        self.assertIn("/", chain_str)
        self.assertIn("@", chain_str)

    def test_empty_chain(self):
        """Test creating empty chain."""
        chain = LinkedServers()
        self.assertEqual(len(chain.server_chain), 0)
        self.assertTrue(chain.is_empty)

    def test_add_to_chain(self):
        """Test adding server to existing chain."""
        chain = LinkedServers("SQL01")
        chain.add_to_chain("SQL-02", "webapp", "testdb")
        self.assertEqual(len(chain.server_chain), 2)
        self.assertEqual(chain.server_chain[1].hostname, "SQL-02")
        self.assertEqual(chain.server_chain[1].impersonation_user, "webapp")
        self.assertEqual(chain.server_chain[1].database, "testdb")

    def test_clear_chain(self):
        """Test clearing a chain."""
        chain = LinkedServers("SQL01;SQL02;SQL03")
        self.assertEqual(len(chain.server_chain), 3)
        chain.clear()
        self.assertEqual(len(chain.server_chain), 0)
        self.assertTrue(chain.is_empty)


class TestBracketProtection(unittest.TestCase):
    """Test that brackets correctly protect special characters."""

    def test_bracket_protects_colon(self):
        """Test that colon inside brackets is part of server name."""
        server = Server.parse_server("[SERVER:001]")
        self.assertEqual(server.hostname, "SERVER:001")
        self.assertEqual(server.port, 1433)  # default, not parsed as :001

    def test_bracket_protects_semicolon(self):
        """Test that semicolon inside brackets is part of server name."""
        server = Server.parse_server("[SERVER;PROD]")
        self.assertEqual(server.hostname, "SERVER;PROD")

    def test_bracket_protects_slash(self):
        """Test that slash inside brackets is part of server name."""
        server = Server.parse_server("[SERVER/TEST]")
        self.assertEqual(server.hostname, "SERVER/TEST")

    def test_bracket_protects_at_sign(self):
        """Test that at sign inside brackets is part of server name."""
        server = Server.parse_server("[SERVER@TEST]")
        self.assertEqual(server.hostname, "SERVER@TEST")

    def test_colon_after_bracket_is_port(self):
        """Test that colon after closing bracket is port delimiter."""
        server = Server.parse_server("[SERVER:001]:1434")
        self.assertEqual(server.hostname, "SERVER:001")
        self.assertEqual(server.port, 1434)

    def test_slash_after_bracket_is_user(self):
        """Test that slash after closing bracket is user delimiter."""
        server = Server.parse_server("[SERVER/TEST]/admin")
        self.assertEqual(server.hostname, "SERVER/TEST")
        self.assertEqual(server.impersonation_user, "admin")

    def test_at_after_bracket_is_database(self):
        """Test that at sign after closing bracket is database delimiter."""
        server = Server.parse_server("[SERVER@TEST]@mydb")
        self.assertEqual(server.hostname, "SERVER@TEST")
        self.assertEqual(server.database, "mydb")


class TestRealWorldScenarios(unittest.TestCase):
    """Test real-world usage scenarios."""

    def test_standard_sql_server(self):
        """Test typical SQL Server connection."""
        server = Server.parse_server("SQLPROD01:1433")
        self.assertEqual(server.hostname, "SQLPROD01")
        self.assertEqual(server.port, 1433)

    def test_named_instance_style(self):
        """Test SQL Server named instance style (bracketed)."""
        server = Server.parse_server("[SQLSERVER\\INSTANCE01]")
        self.assertEqual(server.hostname, "SQLSERVER\\INSTANCE01")

    def test_fqdn_server(self):
        """Test fully qualified domain name."""
        server = Server.parse_server("sql.prod.company.com:1434/sa@master")
        self.assertEqual(server.hostname, "sql.prod.company.com")
        self.assertEqual(server.port, 1434)
        self.assertEqual(server.impersonation_user, "sa")
        self.assertEqual(server.database, "master")

    def test_ip_address(self):
        """Test IP address as hostname."""
        server = Server.parse_server("10.0.0.1:1434")
        self.assertEqual(server.hostname, "10.0.0.1")
        self.assertEqual(server.port, 1434)

    def test_localhost_variations(self):
        """Test localhost variations."""
        for host in ["localhost", "127.0.0.1", "(local)"]:
            server = Server.parse_server(host)
            self.assertEqual(server.hostname, host)

    def test_production_chain_scenario(self):
        """Test realistic production linked server chain."""
        chain_input = "SQLPROD01/svc_account@master;SQLAPP02/webapp@appdb;SQLRPT03@reporting"
        chain = LinkedServers(chain_input)
        self.assertEqual(len(chain.server_chain), 3)
        self.assertEqual(chain.server_chain[0].hostname, "SQLPROD01")
        self.assertEqual(chain.server_chain[0].impersonation_user, "svc_account")
        self.assertEqual(chain.server_chain[0].database, "master")

    def test_azure_sql_style(self):
        """Test Azure SQL Database style hostname."""
        server = Server.parse_server("myserver.database.windows.net:1433")
        self.assertEqual(server.hostname, "myserver.database.windows.net")
        self.assertEqual(server.port, 1433)


if __name__ == '__main__':
    unittest.main()
