"""Type definitions and protocols for Little Brother v3."""

from typing import Any, Callable, Protocol, TypedDict


class Scheduler(Protocol):
    """Protocol for scheduler implementations."""

    def now(self) -> float:
        """Get current time."""
        ...

    def call_later(self, delay: float, callback: Callable[[], None]) -> Any:
        """Schedule a callback."""
        ...

    def cancel(self, handle: Any) -> None:
        """Cancel a scheduled callback."""
        ...

    def cancel_all(self) -> None:
        """Cancel all scheduled callbacks."""
        ...


class EventSource(Protocol):
    """Protocol for event sources used by monitors."""

    def start(self, *args: Any, **kwargs: Any) -> None:
        """Start the event source."""
        ...

    def stop(self) -> None:
        """Stop the event source."""
        ...


# TypedDicts for monitor attribute payloads
class KeyboardAttrs(TypedDict):
    """Keyboard monitor attributes schema."""

    keydown: int
    keyup: int
    mean_ms: float
    p95_ms: float
    stdev_ms: float
    bursts: int


class MouseAttrs(TypedDict):
    """Mouse monitor attributes schema."""

    moves: int
    distance_px: int
    click_left: int
    click_right: int
    click_middle: int
    scroll: int


class ActiveWindowAttrs(TypedDict):
    """Active window monitor attributes schema."""

    source: str
    hwnd: int
    app_id: str


class BrowserAttrs(TypedDict):
    """Browser monitor attributes schema."""

    source: str
    tab_id: int
    window_id: int


class ContextSnapshotAttrs(TypedDict):
    """Context snapshot monitor attributes schema."""

    source: str
    capture_type: str
    apps_count: int
    windows_count: int


class FileWatchAttrs(TypedDict):
    """File watch monitor attributes schema."""

    source: str
    file_size: int
    file_modified: bool
