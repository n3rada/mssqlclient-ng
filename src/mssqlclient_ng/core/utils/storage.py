# mssqlclient_ng/core/utils/storage.py

"""Persistent data storage for mssqlclient-ng.

Provides platform-appropriate directories (XDG on POSIX, APPDATA on Windows)
and a chain store for saving/loading discovered linked server chains per server.
"""

# Built-in imports
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional

# Third party imports
from loguru import logger

APP_NAME = "mssqlclient-ng"


def get_data_dir() -> Path:
    """
    Return the persistent data directory, following XDG/platform conventions.

    On POSIX: $XDG_DATA_HOME/mssqlclient-ng (defaults to ~/.local/share/mssqlclient-ng)
    On Windows: %APPDATA%/mssqlclient-ng

    Returns:
        Path to the data directory (created if needed)
    """
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA", str(Path.home())))
    else:
        base = Path(
            os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local" / "share"))
        )

    data_dir = (base / APP_NAME).resolve()
    return data_dir


def _sanitize_filename(name: str) -> str:
    """Sanitize a server name for use as a filename component."""
    # Replace characters not safe in filenames
    return "".join(c if c.isalnum() or c in ".-_" else "_" for c in name)


class ChainStore:
    """
    Persists discovered linked server chains to disk, keyed by starting server.

    Storage layout:
        <data_dir>/chains/<server_name>.json

    Each JSON file contains:
        {
            "server": "<hostname>",
            "last_updated": "<ISO timestamp>",
            "chains": [
                {
                    "endpoint": "...",
                    "login": "...",
                    "mapped_to": "...",
                    "hops": N,
                    "server_roles": "...",
                    "command": "..."
                },
                ...
            ]
        }
    """

    def __init__(self):
        self._chains_dir = get_data_dir() / "chains"

    def _get_chain_file(self, server: str) -> Path:
        """Get the JSON file path for a given starting server."""
        return self._chains_dir / f"{_sanitize_filename(server)}.json"

    def save(self, server: str, chains: List[Dict[str, Any]]) -> None:
        """
        Save discovered chains for a starting server.

        Args:
            server: The starting server hostname
            chains: List of chain row dicts (from LinkMap._display_chain_commands)
        """
        self._chains_dir.mkdir(parents=True, exist_ok=True)

        data = {
            "server": server,
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "chains": chains,
        }

        chain_file = self._get_chain_file(server)
        try:
            chain_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
            # Restrict permissions on POSIX
            if os.name != "nt":
                os.chmod(chain_file, 0o600)
            logger.info(f"Saved {len(chains)} chain(s) to {chain_file}")
        except Exception as ex:
            logger.warning(f"Failed to save chains: {ex}")

    def load(self, server: str) -> Optional[Dict[str, Any]]:
        """
        Load saved chains for a starting server.

        Args:
            server: The starting server hostname

        Returns:
            Dict with 'server', 'last_updated', 'chains' keys, or None if not found
        """
        chain_file = self._get_chain_file(server)
        if not chain_file.is_file():
            return None

        try:
            data = json.loads(chain_file.read_text(encoding="utf-8"))
            return data
        except Exception as ex:
            logger.warning(f"Failed to load chains from {chain_file}: {ex}")
            return None

    def delete(self, server: str) -> bool:
        """
        Delete saved chains for a starting server.

        Args:
            server: The starting server hostname

        Returns:
            True if deleted, False if not found
        """
        chain_file = self._get_chain_file(server)
        if chain_file.is_file():
            chain_file.unlink()
            return True
        return False

    def list_servers(self) -> List[str]:
        """
        List all servers that have saved chains.

        Returns:
            List of server names with saved chains
        """
        if not self._chains_dir.is_dir():
            return []

        servers = []
        for f in self._chains_dir.glob("*.json"):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                servers.append(data.get("server", f.stem))
            except Exception:
                servers.append(f.stem)
        return servers


class OutputCache:
    """
    Caches action output per execution context (server + user + chain + database).

    Storage layout:
        <data_dir>/cache/<context_hash>/<action_key>.txt

    The context hash is derived from execution_server, system_user,
    linked server chain, and execution database. Each action invocation
    (name + arguments) is stored as a separate file.
    """

    # Actions that manage their own caching or produce side effects
    _EXCLUDED_ACTIONS = frozenset(
        {
            "query",
            "sql",  # arbitrary queries
            "exec",
            "ole",
            "powershell",
            "pwsh",
            "run",
            "clr",  # execution
            "kill",
            "config",
            "user-add",  # mutations
            "adsi-add",
            "adsi-del",
            "adsi-delete",
            "adsi-drop",  # mutations
            "rpc",
            "data",  # toggle actions
            "upload",
            "rm",
            "del",
            "delete",  # filesystem mutations
            "unc",
            "coerce",
            "smb",
            "ntlm",  # coercion (side effect)
            "adsi-creds",
            "adsi-redirect",  # credential capture (side effect)
            "job-exec",  # agent execution
            "cm-script-add",
            "cm-script-delete",
            "cm-script-run",  # CM mutations
            "cm-admin-add",
            "cm-rbac-add",  # CM mutations
        }
    )

    def __init__(self):
        self._cache_dir = get_data_dir() / "cache"

    @staticmethod
    def _context_hash(
        execution_server: str,
        system_user: str,
        chain_spec: str,
        database: str,
    ) -> str:
        """Compute a short hash from the execution context."""
        context = f"{execution_server}|{system_user}|{chain_spec}|{database}".upper()
        return hashlib.sha256(context.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def _action_key(action_name: str, args: str) -> str:
        """Build a safe filename from action name and arguments."""
        raw = action_name if not args else f"{action_name}_{args}"
        return _sanitize_filename(raw)

    @staticmethod
    def is_cacheable(action_name: str) -> bool:
        """Check if an action is eligible for output caching."""
        return action_name.lower() not in OutputCache._EXCLUDED_ACTIONS

    def get_rows(
        self,
        execution_server: str,
        system_user: str,
        chain_spec: str,
        database: str,
        action_name: str,
        args: str,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieve cached raw row data for an action.

        Returns:
            List of row dicts (for re-rendering with any format), or None if not cached.
        """
        ctx = self._context_hash(execution_server, system_user, chain_spec, database)
        key = self._action_key(action_name, args)
        cache_file = self._cache_dir / ctx / f"{key}.json"

        if not cache_file.is_file():
            return None

        try:
            return json.loads(cache_file.read_text(encoding="utf-8"))
        except Exception as ex:
            logger.debug(f"Failed to read cache file {cache_file}: {ex}")
            return None

    def put_rows(
        self,
        execution_server: str,
        system_user: str,
        chain_spec: str,
        database: str,
        action_name: str,
        args: str,
        rows: List[Dict[str, Any]],
    ) -> None:
        """Store raw row data as JSON for format-agnostic caching."""
        ctx = self._context_hash(execution_server, system_user, chain_spec, database)
        key = self._action_key(action_name, args)
        ctx_dir = self._cache_dir / ctx

        try:
            ctx_dir.mkdir(parents=True, exist_ok=True)
            cache_file = ctx_dir / f"{key}.json"
            cache_file.write_text(json.dumps(rows, default=str), encoding="utf-8")
            if os.name != "nt":
                os.chmod(cache_file, 0o600)
            # Remove stale text cache for the same key
            (ctx_dir / f"{key}.txt").unlink(missing_ok=True)
            logger.debug(f"Cached rows for '{action_name}' in {cache_file}")
        except Exception as ex:
            logger.debug(f"Failed to cache rows: {ex}")

    def get(
        self,
        execution_server: str,
        system_user: str,
        chain_spec: str,
        database: str,
        action_name: str,
        args: str,
    ) -> Optional[str]:
        """
        Retrieve cached text output for an action in the given context.

        Returns:
            The cached output string, or None if not cached.
        """
        ctx = self._context_hash(execution_server, system_user, chain_spec, database)
        key = self._action_key(action_name, args)
        cache_file = self._cache_dir / ctx / f"{key}.txt"

        if not cache_file.is_file():
            return None

        try:
            return cache_file.read_text(encoding="utf-8")
        except Exception as ex:
            logger.debug(f"Failed to read cache file {cache_file}: {ex}")
            return None

    def put(
        self,
        execution_server: str,
        system_user: str,
        chain_spec: str,
        database: str,
        action_name: str,
        args: str,
        output: str,
    ) -> None:
        """Store rendered text output in the cache (fallback for unstructured output)."""
        ctx = self._context_hash(execution_server, system_user, chain_spec, database)
        key = self._action_key(action_name, args)
        ctx_dir = self._cache_dir / ctx

        try:
            ctx_dir.mkdir(parents=True, exist_ok=True)
            cache_file = ctx_dir / f"{key}.txt"
            cache_file.write_text(output, encoding="utf-8")
            if os.name != "nt":
                os.chmod(cache_file, 0o600)
            logger.debug(f"Cached output for '{action_name}' in {cache_file}")
        except Exception as ex:
            logger.debug(f"Failed to cache output: {ex}")

    def flush(
        self,
        execution_server: str = "",
        system_user: str = "",
        chain_spec: str = "",
        database: str = "",
    ) -> int:
        """
        Flush cached outputs. If context fields are provided, flush only
        that context directory. Otherwise flush the entire cache.

        Returns:
            Number of files deleted.
        """
        deleted = 0
        if execution_server:
            ctx = self._context_hash(
                execution_server, system_user, chain_spec, database
            )
            ctx_dir = self._cache_dir / ctx
            if ctx_dir.is_dir():
                for f in ctx_dir.iterdir():
                    f.unlink()
                    deleted += 1
                ctx_dir.rmdir()
        else:
            if self._cache_dir.is_dir():
                for ctx_dir in self._cache_dir.iterdir():
                    if ctx_dir.is_dir():
                        for f in ctx_dir.iterdir():
                            f.unlink()
                            deleted += 1
                        ctx_dir.rmdir()
        return deleted
