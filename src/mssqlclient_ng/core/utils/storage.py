# mssqlclient_ng/core/utils/storage.py

"""Persistent data storage for mssqlclient-ng.

Provides platform-appropriate directories (XDG on POSIX, APPDATA on Windows)
and a chain store for saving/loading discovered linked server chains per server.
"""

# Built-in imports
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
