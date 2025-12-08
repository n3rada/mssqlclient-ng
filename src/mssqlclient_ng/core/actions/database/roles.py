# mssqlclient_ng/core/actions/database/roles.py

# Built-in imports
from typing import Optional

# Third party imports
from loguru import logger

# Local imports
from ..base import BaseAction
from ..factory import ActionFactory
from ...services.database import DatabaseContext
from ...utils.formatters import OutputFormatter


@ActionFactory.register("roles", "Enumerate database-level roles and their members")
class Roles(BaseAction):
    """
    Enumerates database-level roles and their members in the current database.

    Displays:
    - Fixed database roles (db_owner, db_datareader, db_datawriter, etc.) and their members
    - Custom database roles and their members

    This provides a role-centric view showing which users belong to each database role.
    For server-level logins and instance-wide privileges, use the 'users' action instead.
    """

    def __init__(self):
        super().__init__()

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validates the arguments for the roles action.

        Args:
            additional_arguments: No arguments required

        Raises:
            ValueError: If arguments are provided (none expected).
        """
        # No arguments required
        pass

    def execute(self, database_context: DatabaseContext) -> Optional[dict]:
        """
        Executes the roles enumeration.

        Args:
            database_context: The DatabaseContext instance to execute the query.

        Returns:
            None
        """
        logger.info(
            "Enumerating server-level and database-level roles with their members"
        )

        # ========== SERVER-LEVEL ROLES ==========

        server_roles_query = """
            SELECT
                r.name AS RoleName,
                r.is_fixed_role AS IsFixedRole,
                r.type_desc AS RoleType,
                r.create_date AS CreateDate,
                r.modify_date AS ModifyDate
            FROM sys.server_principals r
            WHERE r.type = 'R'
            ORDER BY r.is_fixed_role DESC, r.name;
        """

        all_server_roles = database_context.query_service.execute_table(
            server_roles_query
        )

        if all_server_roles:
            # Get all server role members in a single query
            server_members_query = """
                SELECT
                    r.name AS role_name,
                    m.name AS member_name
                FROM sys.server_principals r
                INNER JOIN sys.server_role_members srm ON r.principal_id = srm.role_principal_id
                INNER JOIN sys.server_principals m ON srm.member_principal_id = m.principal_id
                WHERE r.type = 'R'
                ORDER BY r.name, m.name;
            """

            server_members = database_context.query_service.execute_table(
                server_members_query
            )

            # Build dictionary for server role members
            server_members_dict = {}
            for member_row in server_members:
                role_name = member_row["role_name"]
                member_name = member_row["member_name"]

                if role_name not in server_members_dict:
                    server_members_dict[role_name] = []
                server_members_dict[role_name].append(member_name)

            # Map members to server roles
            for role_row in all_server_roles:
                role_name = role_row["RoleName"]

                if role_name in server_members_dict:
                    role_row["Members"] = ", ".join(server_members_dict[role_name])
                else:
                    role_row["Members"] = ""

            # Separate fixed and custom server roles
            fixed_server_roles = [
                role for role in all_server_roles if role["IsFixedRole"]
            ]
            custom_server_roles = [
                role for role in all_server_roles if not role["IsFixedRole"]
            ]

            # Display Fixed Server Roles
            if fixed_server_roles:
                logger.success(f"Fixed Server Roles ({len(fixed_server_roles)} roles)")
                # Filter to only show relevant columns
                filtered_fixed = [
                    {
                        "RoleName": role["RoleName"],
                        "RoleType": role["RoleType"],
                        "CreateDate": role["CreateDate"],
                        "ModifyDate": role["ModifyDate"],
                        "Members": role.get("Members", ""),
                    }
                    for role in fixed_server_roles
                ]
                print(OutputFormatter.convert_list_of_dicts(filtered_fixed))

            # Display Custom Server Roles
            if custom_server_roles:
                logger.success(
                    f"Custom Server Roles ({len(custom_server_roles)} roles)"
                )
                # Filter to only show relevant columns
                filtered_custom = [
                    {
                        "RoleName": role["RoleName"],
                        "RoleType": role["RoleType"],
                        "CreateDate": role["CreateDate"],
                        "ModifyDate": role["ModifyDate"],
                        "Members": role.get("Members", ""),
                    }
                    for role in custom_server_roles
                ]
                print(OutputFormatter.convert_list_of_dicts(filtered_custom))

        # ========== DATABASE-LEVEL ROLES ==========

        # Query all database roles (both fixed and custom)
        query = """
            SELECT
                r.name AS RoleName,
                r.is_fixed_role AS IsFixedRole,
                r.type_desc AS RoleType,
                r.create_date AS CreateDate,
                r.modify_date AS ModifyDate
            FROM sys.database_principals r
            WHERE r.type = 'R'
            ORDER BY r.is_fixed_role DESC, r.name;
        """

        all_roles = database_context.query_service.execute_table(query)

        if not all_roles:
            logger.warning("No database roles found in current database.")
            return None

        # Get all role members in a single query for performance
        all_members_query = """
            SELECT
                r.name AS role_name,
                m.name AS member_name
            FROM sys.database_principals r
            INNER JOIN sys.database_role_members rm ON r.principal_id = rm.role_principal_id
            INNER JOIN sys.database_principals m ON rm.member_principal_id = m.principal_id
            WHERE r.type = 'R'
            ORDER BY r.name, m.name;
        """

        all_members = database_context.query_service.execute_table(all_members_query)

        # Build a dictionary for O(1) lookup: key = role_name, value = list of member names
        members_dict = {}

        for member_row in all_members:
            role_name = member_row["role_name"]
            member_name = member_row["member_name"]

            if role_name not in members_dict:
                members_dict[role_name] = []
            members_dict[role_name].append(member_name)

        # Map members to roles
        for role_row in all_roles:
            role_name = role_row["RoleName"]

            if role_name in members_dict:
                role_row["Members"] = ", ".join(members_dict[role_name])
            else:
                role_row["Members"] = ""

        # Separate fixed roles from custom roles
        fixed_roles_data = [role for role in all_roles if role["IsFixedRole"]]
        custom_roles_data = [role for role in all_roles if not role["IsFixedRole"]]

        # Display Fixed Roles
        if fixed_roles_data:
            logger.success(f"Fixed Database Roles ({len(fixed_roles_data)} roles)")
            # Filter to only show relevant columns
            filtered_fixed = [
                {
                    "RoleName": role["RoleName"],
                    "RoleType": role["RoleType"],
                    "CreateDate": role["CreateDate"],
                    "ModifyDate": role["ModifyDate"],
                    "Members": role.get("Members", ""),
                }
                for role in fixed_roles_data
            ]
            print(OutputFormatter.convert_list_of_dicts(filtered_fixed))

        # Display Custom Roles
        if custom_roles_data:
            logger.success(f"Custom Database Roles ({len(custom_roles_data)} roles)")
            # Filter to only show relevant columns
            filtered_custom = [
                {
                    "RoleName": role["RoleName"],
                    "RoleType": role["RoleType"],
                    "CreateDate": role["CreateDate"],
                    "ModifyDate": role["ModifyDate"],
                    "Members": role.get("Members", ""),
                }
                for role in custom_roles_data
            ]
            print(OutputFormatter.convert_list_of_dicts(filtered_custom))
        else:
            logger.info("No custom database roles found in current database.")

        return None

    def get_arguments(self) -> list[str]:
        """
        Returns the list of expected arguments for this action.

        Returns:
            List of argument descriptions.
        """
        return []
