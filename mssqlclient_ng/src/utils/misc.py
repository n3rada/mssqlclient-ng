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
