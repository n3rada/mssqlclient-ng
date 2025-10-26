import string
import secrets


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
