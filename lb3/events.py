"""Event model and bus for Little Brother v3."""

import contextlib
import threading
from dataclasses import dataclass
from queue import Empty, Queue
from threading import Lock
from typing import Any, Optional

from .logging_setup import get_logger

logger = get_logger("events")


@dataclass
class Event:
    """Event model that maps 1:1 to the database events table.

    All fields correspond directly to database columns.
    No plaintext titles/paths/URLs - only hashes.
    """

    # Required fields
    id: str  # ULID
    ts_utc: int  # Timestamp in milliseconds
    monitor: str  # active_window|context_snapshot|keyboard|mouse|browser|file
    action: str  # Action description
    subject_type: str  # app|window|file|url|none
    session_id: str  # ULID

    # Optional fields
    subject_id: Optional[str] = None  # ULID or None
    batch_id: Optional[str] = None  # ULID or None
    pid: Optional[int] = None
    exe_name: Optional[str] = None
    exe_path_hash: Optional[str] = None
    window_title_hash: Optional[str] = None
    url_hash: Optional[str] = None
    file_path_hash: Optional[str] = None
    attrs_json: Optional[str] = None  # Compact JSON string

    def to_dict(self) -> dict[str, Any]:
        """Convert event to dictionary for database insertion."""
        return {
            "id": self.id,
            "ts_utc": self.ts_utc,
            "monitor": self.monitor,
            "action": self.action,
            "subject_type": self.subject_type,
            "subject_id": self.subject_id,
            "session_id": self.session_id,
            "batch_id": self.batch_id,
            "pid": self.pid,
            "exe_name": self.exe_name,
            "exe_path_hash": self.exe_path_hash,
            "window_title_hash": self.window_title_hash,
            "url_hash": self.url_hash,
            "file_path_hash": self.file_path_hash,
            "attrs_json": self.attrs_json,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Event":
        """Create event from dictionary."""
        return cls(
            id=data["id"],
            ts_utc=data["ts_utc"],
            monitor=data["monitor"],
            action=data["action"],
            subject_type=data["subject_type"],
            session_id=data["session_id"],
            subject_id=data.get("subject_id"),
            batch_id=data.get("batch_id"),
            pid=data.get("pid"),
            exe_name=data.get("exe_name"),
            exe_path_hash=data.get("exe_path_hash"),
            window_title_hash=data.get("window_title_hash"),
            url_hash=data.get("url_hash"),
            file_path_hash=data.get("file_path_hash"),
            attrs_json=data.get("attrs_json"),
        )


class EventBus:
    """Lightweight in-process event bus with backpressure-safe publish/subscribe."""

    def __init__(self, max_queue_size: int = 10000):
        """Initialize event bus.

        Args:
            max_queue_size: Maximum queue size for backpressure safety
        """
        self._queue = Queue(maxsize=max_queue_size)
        self._handlers = []
        self._lock = Lock()
        self._running = False
        self._worker_thread = None

    def subscribe(self, handler) -> None:
        """Subscribe a handler to receive events.

        Args:
            handler: Callable that accepts Event objects
        """
        with self._lock:
            self._handlers.append(handler)
            logger.debug(f"Subscribed handler: {handler}")

    def unsubscribe(self, handler) -> None:
        """Unsubscribe a handler.

        Args:
            handler: Handler to remove
        """
        with self._lock:
            if handler in self._handlers:
                self._handlers.remove(handler)
                logger.debug(f"Unsubscribed handler: {handler}")

    def publish(self, event: Event, timeout: float = 1.0) -> bool:
        """Publish an event to the bus.

        Args:
            event: Event to publish
            timeout: Timeout for backpressure safety

        Returns:
            True if published successfully, False if queue full
        """
        try:
            self._queue.put(event, timeout=timeout)
            return True
        except Exception:
            logger.warning("Event queue full, dropping event")
            return False

    def start(self) -> None:
        """Start the event bus worker thread."""
        if self._running:
            return

        self._running = True
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()
        logger.info("Event bus started")

    def stop(self) -> None:
        """Stop the event bus worker thread."""
        if not self._running:
            return

        self._running = False

        # Put sentinel to wake up worker
        with contextlib.suppress(Exception):
            self._queue.put(None, timeout=1.0)

        if self._worker_thread:
            self._worker_thread.join(timeout=5.0)

        logger.info("Event bus stopped")

    def _worker_loop(self) -> None:
        """Worker thread loop that processes events."""
        while self._running:
            try:
                # Get event from queue (blocking)
                event = self._queue.get(timeout=0.5)

                # Sentinel value to stop
                if event is None:
                    break

                # Deliver to all handlers in FIFO order
                with self._lock:
                    handlers = self._handlers.copy()

                for handler in handlers:
                    try:
                        handler(event)
                    except Exception as e:
                        logger.error(f"Error in event handler {handler}: {e}")

                self._queue.task_done()

            except Empty:
                continue
            except Exception as e:
                logger.error(f"Error in event bus worker: {e}")

    def flush(self, timeout: float = 5.0) -> None:
        """Wait for all queued events to be processed.

        Args:
            timeout: Maximum time to wait
        """
        with contextlib.suppress(Exception):
            self._queue.join()


class SpoolerSink:
    """Event bus handler that writes events to the spooler."""

    def __init__(self):
        """Initialize spooler sink."""
        from .spooler import get_spooler_manager

        self.spooler_manager = get_spooler_manager()

    def __call__(self, event: Event) -> None:
        """Handle an event by writing to spooler.

        Args:
            event: Event to write
        """
        try:
            event_dict = event.to_dict()
            self.spooler_manager.write_event(event.monitor, event_dict)
        except Exception as e:
            logger.error(f"Failed to write event to spooler: {e}")


# Global event bus instance
_event_bus: Optional[EventBus] = None
_bus_lock = Lock()


def get_event_bus() -> EventBus:
    """Get the global event bus instance."""
    global _event_bus

    with _bus_lock:
        if _event_bus is None:
            _event_bus = EventBus()
        return _event_bus


def publish_event(event: Event) -> bool:
    """Publish an event to the global event bus.

    Args:
        event: Event to publish

    Returns:
        True if published successfully
    """
    bus = get_event_bus()
    return bus.publish(event)
