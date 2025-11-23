from typing import List, Tuple

from mssqlclient_ng.src.actions.factory import ActionFactory


def get_all_actions_info() -> List[Tuple[str, str, List[str]]]:
    """
    Get information about all available actions.

    Returns:
        List of tuples: (action_name, description, arguments)
    """
    return ActionFactory.get_available_actions()
