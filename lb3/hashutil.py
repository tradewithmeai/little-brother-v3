"""Hash and checksum utilities for Little Brother v3."""

import hashlib
from pathlib import Path
from typing import BinaryIO, Literal, Union
from urllib.parse import urlparse

from .config import get_effective_config


def hash_str(value: str, purpose: Literal["window_title", "file_path", "url", "exe_path", "free_text"]) -> str:
    """Generate purpose-scoped salted SHA-256 hash of a string.
    
    Uses the formula: H = sha256(salt_bytes || purpose || 0x00 || utf8(value))
    
    Args:
        value: The string to hash (no plaintext logging)
        purpose: The purpose domain for hash separation
    
    Returns:
        Hex SHA-256 digest string
    """
    config = get_effective_config()
    
    # Get salt as bytes from hex
    salt_hex = config.hashing.salt
    salt_bytes = bytes.fromhex(salt_hex)
    
    # Create hasher
    hasher = hashlib.sha256()
    
    # Add components: salt_bytes || purpose || 0x00 || utf8(value)
    hasher.update(salt_bytes)
    hasher.update(purpose.encode("utf-8"))
    hasher.update(b"\x00")
    hasher.update(value.encode("utf-8"))
    
    return hasher.hexdigest()


def hash_string(text: str, algorithm: str = "sha256") -> str:
    """Generate hash of a string (legacy function)."""
    hasher = hashlib.new(algorithm)
    hasher.update(text.encode("utf-8"))
    return hasher.hexdigest()


def hash_bytes(data: bytes, algorithm: str = "sha256") -> str:
    """Generate hash of bytes."""
    hasher = hashlib.new(algorithm)
    hasher.update(data)
    return hasher.hexdigest()


def hash_file(file_path: Union[str, Path], algorithm: str = "sha256") -> str:
    """Generate hash of a file."""
    hasher = hashlib.new(algorithm)
    path = Path(file_path)

    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)

    return hasher.hexdigest()


def hash_file_handle(file_handle: BinaryIO, algorithm: str = "sha256") -> str:
    """Generate hash of a file handle."""
    hasher = hashlib.new(algorithm)

    for chunk in iter(lambda: file_handle.read(8192), b""):
        hasher.update(chunk)

    return hasher.hexdigest()


def verify_hash(
    data: Union[str, bytes], expected_hash: str, algorithm: str = "sha256"
) -> bool:
    """Verify data against expected hash."""
    if isinstance(data, str):
        actual_hash = hash_string(data, algorithm)
    else:
        actual_hash = hash_bytes(data, algorithm)

    return actual_hash.lower() == expected_hash.lower()


def extract_domain(url: str) -> str:
    """Extract domain from URL for hashing.
    
    Returns the netloc (hostname:port) from the URL.
    This is a simplified approach - a full eTLD+1 implementation 
    would require a public suffix list.
    
    Args:
        url: The URL to extract domain from
        
    Returns:
        Domain string or empty string if parsing fails
    """
    try:
        parsed = urlparse(url)
        return parsed.netloc or ""
    except Exception:
        return ""


def hash_url(url: str) -> str:
    """Hash a URL using the standard url purpose."""
    return hash_str(url, "url")


def hash_domain(domain: str) -> str:
    """Hash a domain using the url purpose (domains are URL components)."""
    return hash_str(domain, "url")
