from typing import List, Tuple
from loguru import logger

from mssqlclient_ng.src.actions.factory import ActionFactory
from mssqlclient_ng.src.utils.formatter import rows_to_markdown_table


def get_all_actions_info() -> List[Tuple[str, str, List[str]]]:
    """
    Get information about all available actions.

    Returns:
        List of tuples: (action_name, description, arguments)
    """
    return ActionFactory.get_available_actions()


def display_all_commands() -> None:
    """
    Display all available commands with descriptions and arguments.
    """
    logger.print("")
    logger.print("═" * 80)
    logger.print(" Available Commands".ljust(80, "═"))
    logger.print("═" * 80)

    actions = get_all_actions_info()

    if not actions:
        logger.warning("No actions registered")
        return

    # Format for display
    command_rows = []
    for name, description, arguments in actions:
        command_rows.append(
            {"Command": name, "Description": description, "Arguments": len(arguments)}
        )

    # Display as table
    if command_rows:
        result = rows_to_markdown_table(command_rows)
        logger.print(result)

    # Show detailed usage section
    logger.print("")
    logger.print("Detailed Usage:")
    logger.print("-" * 80)
    for name, description, arguments in actions:
        logger.print(f"\n  {name.upper()}")
        logger.print(f"    Description: {description}")
        if arguments:
            logger.print(f"    Arguments:")
            for arg in arguments:
                logger.print(f"      - {arg}")
        else:
            logger.print(f"    Arguments: None")

    logger.print("")
    logger.print("─" * 80)
    logger.print(f"Total: {len(actions)} commands available")
    logger.print(f"Usage: !<command> [arguments]")
    logger.print(f"Type: !help <command> for specific command help")
    logger.print("═" * 80)
    logger.print("")


def display_command_help(command_name: str) -> bool:
    """
    Display help for a specific command.

    Args:
        command_name: The command to show help for

    Returns:
        True if command found, False otherwise
    """
    if not ActionFactory.action_exists(command_name):
        logger.warning(f"Command '{command_name}' not found")
        available = ", ".join(ActionFactory.list_actions())
        logger.info(f"Available commands: {available}")
        return False

    description = ActionFactory.get_action_description(command_name)
    action = ActionFactory.get_action(command_name)
    arguments = action.get_arguments() if hasattr(action, "get_arguments") else []

    logger.print("")
    logger.print("═" * 80)
    logger.print(f" {command_name.upper()}".ljust(80, "═"))
    logger.print("═" * 80)
    logger.print(f"Description: {description}")
    logger.print("")

    if arguments:
        logger.print("Arguments:")
        for i, arg in enumerate(arguments, 1):
            logger.print(f"  {i}. {arg}")
    else:
        logger.print("Arguments: None")

    logger.print("")
    logger.print("═" * 80)
    logger.print("")

    return True


def list_all_commands() -> List[str]:
    """
    Get a simple list of all command names.

    Returns:
        List of command names
    """
    return ActionFactory.list_actions()
