"""
Tree action for displaying directory structure in Linux tree-style format.
"""

from typing import Optional, List, Dict, Any
from loguru import logger

from mssqlclient_ng.src.actions.base import BaseAction
from mssqlclient_ng.src.actions.factory import ActionFactory
from mssqlclient_ng.src.services.database import DatabaseContext
from mssqlclient_ng.src.utils.common import normalize_windows_path


@ActionFactory.register(
    "tree", "Display directory tree structure in Linux tree-style format"
)
class Tree(BaseAction):
    """
    Display directory tree structure using xp_dirtree.

    This action uses the undocumented but widely-used xp_dirtree extended procedure
    to enumerate directories and files on the SQL Server filesystem. The output is
    formatted to match the Linux 'tree' command style.

    The tree representation uses:
    - ├── for intermediate items
    - └── for the last item in a directory
    - │   for vertical lines continuing to subdirectories
    - Indentation to show hierarchy levels
    """

    def __init__(self):
        super().__init__()
        self._path: str = ""
        self._depth: int = 3
        self._show_files: bool = True

    def validate_arguments(self, additional_arguments: str) -> None:
        """
        Validate arguments for the tree action.

        Args:
            additional_arguments: Path and optional parameters
                Format: <path> [depth] [show_files:1|0]

        Raises:
            ValueError: If the path is empty
        """
        parts = self.split_arguments(additional_arguments)

        if not parts:
            raise ValueError("Tree action requires a directory path as an argument")

        # Normalize Windows path to handle single backslashes
        self._path = normalize_windows_path(parts[0].strip())

        # Optional depth parameter
        if len(parts) >= 2:
            try:
                self._depth = int(parts[1])
                if self._depth < 1 or self._depth > 255:
                    raise ValueError("Depth must be between 1 and 255")
            except ValueError:
                logger.warning(
                    f"Invalid depth value '{parts[1]}', using default depth of 3"
                )
                self._depth = 3

        # Optional show_files parameter
        if len(parts) >= 3:
            show_files_str = parts[2].strip().lower()
            self._show_files = show_files_str in ["1", "true", "yes"]

    def execute(self, database_context: DatabaseContext) -> Optional[str]:
        """
        Execute the tree action to display directory structure.

        Args:
            database_context: The DatabaseContext instance to execute the query

        Returns:
            The tree representation as a string
        """
        logger.info(f"Displaying tree for: {self._path}")
        logger.info(f"Depth: {self._depth}, Show files: {self._show_files}")

        # Ensure path ends with backslash for xp_dirtree
        path = self._path.rstrip("\\") + "\\"

        # Escape single quotes in path
        escaped_path = path.replace("'", "''")

        # xp_dirtree parameters:
        # @path: Directory path
        # @depth: How many levels deep to traverse (default 0 = all)
        # @file: 1 = show files, 0 = directories only (default 0)
        file_flag = 1 if self._show_files else 0

        # Create temporary table to store results
        query = f"""
            CREATE TABLE #TreeResults (
                subdirectory NVARCHAR(512),
                depth INT,
                isfile BIT
            );

            INSERT INTO #TreeResults (subdirectory, depth, isfile)
            EXEC xp_dirtree '{escaped_path}', {self._depth}, {file_flag};

            SELECT subdirectory, depth, isfile FROM #TreeResults;

            DROP TABLE #TreeResults;
        """

        try:
            results = database_context.query_service.execute_table(query)

            if not results:
                logger.warning("No files or directories found")
                print()
                print(f"{self._path}")
                print()
                print("0 directories, 0 files")
                return None

            # Debug: Log first few results to understand the structure
            logger.debug(f"Total results: {len(results)}")
            for i, r in enumerate(results[:5]):
                logger.debug(
                    f"Result {i}: subdirectory='{r.get('subdirectory')}', "
                    f"depth={r.get('depth')}, isfile={r.get('isfile')}"
                )

            # Build the tree structure
            tree_output = self._build_tree(results, path)

            # Count statistics - only count items within depth limit
            dir_count = sum(
                1
                for r in results
                if not r.get("isfile", False) and r.get("depth", 1) <= self._depth
            )
            file_count = sum(
                1
                for r in results
                if r.get("isfile", False) and r.get("depth", 1) <= self._depth
            )

            print()
            print(tree_output)
            print()
            print(
                f"{dir_count} directories"
                + (f", {file_count} files" if self._show_files else "")
            )

            return tree_output

        except Exception as e:
            logger.error(f"Failed to generate tree for '{self._path}': {e}")
            raise

    def _build_tree(self, results: List[Dict[str, Any]], root_path: str) -> str:
        """
        Build a tree representation from xp_dirtree results.

        Args:
            results: List of dictionaries with subdirectory, depth, and isfile
            root_path: The root path being displayed

        Returns:
            Tree representation as a string
        """
        lines = [root_path]

        # Group results by depth and path
        tree_structure = self._organize_tree_structure(results)

        # Build tree recursively
        if tree_structure:
            self._render_tree(tree_structure, "", lines, is_last=True)

        return "\n".join(lines)

    def _organize_tree_structure(
        self, results: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Organize flat xp_dirtree results into a hierarchical structure.

        xp_dirtree returns:
        - subdirectory: just the name (not full path)
        - depth: level from root (1, 2, 3...)
        - isfile: 1 for file, 0 for directory

        We need to track parent-child relationships using depth levels.

        Args:
            results: Flat list of results from xp_dirtree

        Returns:
            Hierarchical tree structure
        """
        tree = []

        # Stack to track the last node at each depth level
        # depth_stack[depth] = last node at that depth
        depth_stack = {}

        for result in results:
            name = result.get("subdirectory", "")
            depth = result.get("depth", 1)
            is_file = result.get("isfile", False)

            # Skip items beyond the requested depth
            if depth > self._depth:
                continue

            node = {
                "name": name,
                "depth": depth,
                "is_file": is_file,
                "children": [],
            }

            if depth == 1:
                # Top-level item - add to tree root
                tree.append(node)
                depth_stack[1] = node
            else:
                # Child item - find parent at previous depth level
                parent = depth_stack.get(depth - 1)

                if parent:
                    parent["children"].append(node)
                    depth_stack[depth] = node
                else:
                    # This shouldn't happen with proper xp_dirtree output
                    logger.warning(
                        f"Parent at depth {depth - 1} not found for: {name} at depth {depth}"
                    )

        return tree

    def _render_tree(
        self,
        nodes: List[Dict[str, Any]],
        prefix: str,
        lines: List[str],
        is_last: bool = False,
    ) -> None:
        """
        Recursively render the tree structure with proper formatting.

        Args:
            nodes: List of nodes at current level
            prefix: Current line prefix (for indentation)
            lines: List to accumulate output lines
            is_last: Whether this is the last node at current level
        """
        # Sort: directories first, then files, alphabetically
        nodes.sort(key=lambda x: (x["is_file"], x["name"].lower()))

        for i, node in enumerate(nodes):
            is_last_node = i == len(nodes) - 1

            # Determine the connector
            if is_last_node:
                connector = "└── "
                new_prefix = prefix + "    "
            else:
                connector = "├── "
                new_prefix = prefix + "│   "

            # Add file/directory indicator
            if node["is_file"]:
                display_name = node["name"]
            else:
                display_name = node["name"] + "/"

            lines.append(f"{prefix}{connector}{display_name}")

            # Recursively render children
            if node["children"]:
                self._render_tree(
                    node["children"], new_prefix, lines, is_last=is_last_node
                )

    def get_arguments(self) -> List[str]:
        """
        Get the list of arguments for this action.

        Returns:
            List of argument descriptions
        """
        return [
            "Directory path",
            "Depth (1-255, default: 3)",
            "Show files (1|0, default: 1)",
        ]
