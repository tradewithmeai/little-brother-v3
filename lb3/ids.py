"""ID generation utilities for Little Brother v3."""

import threading

import ulid


class MonotonicULIDFactory:
    """Thread-safe monotonic ULID factory."""

    def __init__(self):
        self._lock = threading.Lock()

    def new_ulid(self) -> ulid.ULID:
        """Generate a new monotonic ULID."""
        # The ulid.monotonic() function is already thread-safe and monotonic
        with self._lock:
            return ulid.monotonic.new()


# Global monotonic factory
_ulid_factory = MonotonicULIDFactory()


def new_id() -> str:
    """Generate a new ULID string.

    Returns a monotonic, time-sortable ULID string that is thread-safe.
    """
    return str(_ulid_factory.new_ulid())


def generate_id() -> str:
    """Generate a new ULID string (legacy alias)."""
    return new_id()


def generate_session_id() -> str:
    """Generate a session ID."""
    return new_id()


def generate_event_id() -> str:
    """Generate an event ID."""
    return new_id()


def is_valid_id(id_str: str) -> bool:
    """Check if a string is a valid ULID."""
    try:
        if not isinstance(id_str, str):
            return False
        ulid.parse(id_str)
        return True
    except (ValueError, TypeError):
        return False
