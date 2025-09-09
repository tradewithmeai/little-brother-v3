"""Database management for Little Brother v3.

This module provides backward compatibility by re-exporting from database.py.
The new authoritative implementation is in database.py with the new schema.
"""

# Re-export all public APIs from database.py for backward compatibility
# Legacy EventRecord class for backward compatibility  
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from .database import (
    Database,
    create_test_event,
    get_database,
)


@dataclass
class EventRecord:
    """Legacy database record for events (for backward compatibility)."""

    id: str
    timestamp: float
    monitor: str
    event_type: str
    data: str  # JSON string
    session_id: Optional[str] = None

    @property
    def datetime(self) -> datetime:
        """Get datetime from timestamp."""
        return datetime.fromtimestamp(self.timestamp, timezone.utc)


# Keep legacy imports available
__all__ = [
    "Database",
    "EventRecord", 
    "get_database",
    "create_test_event",
]
