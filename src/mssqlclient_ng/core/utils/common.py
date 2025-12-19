# mssqlclient_ng/core/utils/common.py

# Built-in imports
import gzip
import hashlib
import secrets
import socket
import string
from io import BytesIO
from base64 import b64decode

# Third party imports
from impacket.dcerpc.v5.dtypes import SID


def generate_random_string(length: int) -> str:
    """
    Generate a random alphanumeric string.

    Args:
        length: The length of the random string

    Returns:
        A random string of specified length
    """
    alphabet = string.ascii_lowercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def get_random_number(min_val: int, max_val: int) -> int:
    """
    Generates a random number within the specified range (inclusive of min, exclusive of max).

    Args:
        min_val: The inclusive lower bound of the random number
        max_val: The exclusive upper bound of the random number

    Returns:
        A random integer between min_val (inclusive) and max_val (exclusive)
    """
    return secrets.randbelow(max_val - min_val) + min_val


def get_hex_char(value: int, upper: bool = False) -> str:
    """
    Converts a nibble (4-bit value from 0 to 15) into its corresponding hexadecimal character.

    Example:
        get_hex_char(10, True) => 'A'
        get_hex_char(10, False) => 'a'

    Args:
        value: An integer from 0 to 15 representing the nibble
        upper: If True, returns uppercase ('A'-'F'); otherwise, lowercase ('a'-'f')

    Returns:
        A hexadecimal character corresponding to the input nibble
    """
    if value < 10:
        return chr(ord("0") + value)
    else:
        return chr((ord("A") if upper else ord("a")) + (value - 10))


def decode_and_decompress(encoded: str) -> bytes:
    """
    Decodes a base64-encoded string and decompresses it using gzip.

    Args:
        encoded: Base64-encoded gzip-compressed data

    Returns:
        Decompressed bytes
    """
    compressed_bytes = b64decode(encoded)
    with BytesIO(compressed_bytes) as input_stream:
        with gzip.GzipFile(fileobj=input_stream, mode="rb") as gzip_stream:
            return gzip_stream.read()


def hex_string_to_bytes(hex_str: str) -> bytes:
    """
    Converts a hexadecimal string to bytes.

    Args:
        hex_str: Hexadecimal string (e.g., "48656c6c6f")

    Returns:
        Bytes representation of the hex string
    """
    return bytes.fromhex(hex_str)


def bytes_to_hex_string(data: bytes) -> str:
    """
    Converts bytes to a hexadecimal string.

    Args:
        data: Bytes to convert

    Returns:
        Hexadecimal string representation (lowercase)
    """
    return data.hex()


def get_random_unused_port() -> int:
    """
    Gets a random unused TCP port by binding to port 0 and retrieving the assigned port.

    Returns:
        An available TCP port number
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def compute_sha256(input_str: str) -> str:
    """
    Computes a SHA-256 hash from an input string.

    Args:
        input_str: The string to hash

    Returns:
        Hexadecimal representation of the SHA-256 hash (lowercase)
    """
    input_bytes = input_str.encode("utf-8")
    hash_bytes = hashlib.sha256(input_bytes).digest()
    return hash_bytes.hex()


def sid_bytes_to_string(sid_bytes: bytes) -> str:
    """
    Converts binary SID to string format (S-1-5-21-...).

    Args:
        sid_bytes: Binary SID data

    Returns:
        SID in string format (e.g., S-1-5-21-...)
    """

    return SID(bytes.fromhex(sid_bytes.decode())).formatCanonical()


def normalize_windows_path(path: str) -> str:
    r"""
    Normalizes a Windows path to ensure proper backslash escaping.
    Converts single backslashes to double backslashes for SQL queries.
    If the path already has double backslashes, leaves them as-is.

    Args:
        path: Windows path that may have single or double backslashes

    Returns:
        Normalized path with proper backslash escaping

    Examples:
        normalize_windows_path("C:\\Users") -> "C:\\Users"
        normalize_windows_path(r"C:\Users") -> "C:\\Users"
    """
    # If the string representation already has double backslashes, return as-is
    # This handles cases where the user passed an already-escaped string
    if "\\\\" in path:
        return path

    # Replace single backslashes with double backslashes
    # This handles raw strings or strings with single backslashes
    return path.replace("\\", "\\\\")


def yes_no_prompt(question: str, default: bool = True) -> bool:
    """
    Prompts the user with a yes/no question in terminal style (like apt install).

    Args:
        question: The question to ask the user
        default: If True, default is 'Yes' (Y/n), if False, default is 'No' (y/N)

    Returns:
        True if user confirms, False otherwise

    Examples:
        >>> yes_no_prompt("Continue?")
        Continue? [Y/n]:

        >>> yes_no_prompt("Delete files?", default=False)
        Delete files? [y/N]:
    """
    # Format the prompt based on default value
    if default:
        prompt = f"{question} [Y/n]: "
    else:
        prompt = f"{question} [y/N]: "

    try:
        response = input(prompt).strip().lower()

        # Empty response uses the default
        if not response:
            return default

        # Check first character for y/n
        if response[0] == "y":
            return True
        elif response[0] == "n":
            return False
        else:
            # Invalid input, use default
            return default

    except (EOFError, KeyboardInterrupt):
        # Ctrl+C or Ctrl+D during prompt
        print()  # New line
        return True


def convert_table_to_dicts(headers, table_data):
    """
    Convert table format (headers, rows) to list of dicts.

    Args:
        headers: List of column header names
        table_data: List of rows (each row is a list of values)

    Returns:
        List of dictionaries where each dict represents a row

    Examples:
        >>> headers = ["Name", "Age"]
        >>> table_data = [["Alice", 30], ["Bob", 25]]
        >>> convert_table_to_dicts(headers, table_data)
        [{'Name': 'Alice', 'Age': 30}, {'Name': 'Bob', 'Age': 25}]
    """
    return [dict(zip(headers, row)) for row in table_data]
