"""Mouse dynamics monitor for Little Brother v3."""

import math
import os
import threading
from dataclasses import dataclass
from typing import Any, Callable, Optional, Protocol

from ..logging_setup import get_logger
from ..utils.scheduler import Scheduler
from .base import BatchConfig, MonitorBase

logger = get_logger("mouse")


class MouseEventSource(Protocol):
    """Protocol for mouse event sources."""

    def start(self, on_move: Callable, on_click: Callable, on_scroll: Callable) -> None:
        """Start capturing mouse events."""
        ...

    def stop(self) -> None:
        """Stop capturing mouse events."""
        ...


@dataclass
class MouseStats:
    """Mouse dynamics statistics."""

    moves: int = 0
    distance_px: int = 0
    click_left: int = 0
    click_right: int = 0
    click_middle: int = 0
    scroll: int = 0

    def reset(self) -> None:
        """Reset all stats for next batch."""
        self.moves = 0
        self.distance_px = 0
        self.click_left = 0
        self.click_right = 0
        self.click_middle = 0
        self.scroll = 0

    def to_attrs_dict(self) -> dict[str, Any]:
        """Convert to attrs_json dictionary with exact schema."""
        return {
            "moves": int(self.moves),
            "distance_px": int(self.distance_px),
            "click_left": int(self.click_left),
            "click_right": int(self.click_right),
            "click_middle": int(self.click_middle),
            "scroll": int(self.scroll),
        }


class PynputMouseSource:
    """Real pynput mouse event source."""

    def __init__(self):
        self._listener = None
        self._mouse = None
        try:
            import pynput.mouse as mouse

            self._mouse = mouse
            logger.info("pynput mouse module loaded successfully")
        except ImportError as e:
            logger.warning(f"pynput not available: {e}")

    def start(self, on_move: Callable, on_click: Callable, on_scroll: Callable) -> None:
        """Start mouse listener."""
        if not self._mouse:
            raise RuntimeError("pynput mouse not available")

        self._listener = self._mouse.Listener(
            on_move=on_move, on_click=on_click, on_scroll=on_scroll
        )
        self._listener.start()

    def stop(self) -> None:
        """Stop mouse listener."""
        if self._listener:
            self._listener.stop()
            self._listener = None


class FakeMouseSource:
    """Fake mouse source for testing with proper lifecycle management."""

    def __init__(self, mode: str = "standard"):
        """Initialize fake mouse source.

        Args:
            mode: "standard" or "inline" - inline mode prevents any threading
        """
        self._mode = mode
        self._on_move = None
        self._on_click = None
        self._on_scroll = None
        self._running = False
        self._lock = threading.Lock()

    def start(self, on_move: Callable, on_click: Callable, on_scroll: Callable) -> None:
        """Start fake mouse source (idempotent)."""
        if self._mode == "inline":
            # In inline mode, start is a no-op
            self._on_move = on_move
            self._on_click = on_click
            self._on_scroll = on_scroll
            self._running = True
            return

        with self._lock:
            if self._running:
                # Already running, just update callbacks
                self._on_move = on_move
                self._on_click = on_click
                self._on_scroll = on_scroll
                return

            self._on_move = on_move
            self._on_click = on_click
            self._on_scroll = on_scroll
            self._running = True

    def stop(self) -> None:
        """Stop fake mouse source (idempotent)."""
        if self._mode == "inline":
            # In inline mode, stop is a no-op
            self._running = False
            return

        with self._lock:
            self._running = False
            self._on_move = None
            self._on_click = None
            self._on_scroll = None

    def join(self, timeout: float = 2.0) -> bool:
        """Wait for source to finish (always returns True for fake source)."""
        return True

    def is_running(self) -> bool:
        """Check if source is running."""
        with self._lock:
            return self._running

    def simulate_move(self, x: int, y: int) -> None:
        """Simulate mouse move event."""
        if self._mode == "inline":
            if self._running and self._on_move:
                self._on_move(x, y)
            return

        with self._lock:
            if self._running and self._on_move:
                self._on_move(x, y)

    def simulate_click(self, x: int, y: int, button_name: str, pressed: bool) -> None:
        """Simulate mouse click event."""
        if self._mode == "inline":
            if self._running and self._on_click:
                # Create mock button object
                mock_button = type("Button", (), {"name": button_name})()
                self._on_click(x, y, mock_button, pressed)
            return

        with self._lock:
            if self._running and self._on_click:
                # Create mock button object
                mock_button = type("Button", (), {"name": button_name})()
                self._on_click(x, y, mock_button, pressed)

    def simulate_scroll(self, x: int, y: int, dx: int, dy: int) -> None:
        """Simulate mouse scroll event."""
        if self._mode == "inline":
            if self._running and self._on_scroll:
                self._on_scroll(x, y, dx, dy)
            return

        with self._lock:
            if self._running and self._on_scroll:
                self._on_scroll(x, y, dx, dy)

    # Inline mode methods
    def emit_move(self, x: int, y: int) -> None:
        """Emit move event synchronously (inline mode)."""
        if self._on_move:
            self._on_move(x, y)

    def emit_click(self, button_name: str, pressed: bool = True) -> None:
        """Emit click event synchronously (inline mode)."""
        if self._on_click:
            # Create mock button object
            mock_button = type("Button", (), {"name": button_name})()
            self._on_click(100, 100, mock_button, pressed)

    def emit_scroll(self, dx: int, dy: int) -> None:
        """Emit scroll event synchronously (inline mode)."""
        if self._on_scroll:
            self._on_scroll(100, 100, dx, dy)


class MouseMonitor(MonitorBase):
    """Monitor mouse dynamics without capturing raw coordinates."""

    def __init__(
        self,
        dry_run: bool = False,
        batch_config: Optional[BatchConfig] = None,
        event_source: Optional[MouseEventSource] = None,
        scheduler: Optional[Scheduler] = None,
        event_source_mode: str = "standard",
    ):
        """Initialize mouse dynamics monitor.

        Args:
            dry_run: Print events instead of emitting
            batch_config: Override batch configuration
            event_source: Mouse event source (auto-selected based on test mode)
            scheduler: Scheduler for deterministic timing
            event_source_mode: "standard" or "inline" for zero-thread testing
        """
        super().__init__(dry_run, scheduler)

        # Override batch config with mouse-specific defaults if not provided
        if batch_config:
            self.batch_config = batch_config
        else:
            # Mouse-specific defaults: 64 events or 1.5s
            self.batch_config = BatchConfig(max_size=64, max_time_s=1.5)

        # Auto-select event source based on test mode
        test_mode = os.getenv("LB3_TEST_MODE", "0") == "1"
        if event_source:
            self._event_source = event_source
        elif test_mode:
            self._event_source = FakeMouseSource(mode=event_source_mode)
        else:
            self._event_source = PynputMouseSource()

        # Stats tracking
        self._stats = MouseStats()
        self._stats_lock = threading.Lock()

        # Position tracking for distance calculation
        self._last_position: Optional[tuple[int, int]] = None

        # Track last stats flush time for deterministic testing
        self._last_stats_flush_time = self.scheduler.now()

    @property
    def name(self) -> str:
        """Monitor name."""
        return "mouse"

    def start_monitoring(self) -> None:
        """Start mouse monitoring."""
        try:
            # Start event source
            self._event_source.start(
                on_move=self._on_mouse_move,
                on_click=self._on_mouse_click,
                on_scroll=self._on_mouse_scroll,
            )
            logger.info("Mouse monitoring started")

        except Exception as e:
            logger.error(f"Failed to start mouse monitoring: {e}")
            logger.info("Continuing without mouse monitoring (degraded mode)")

    def stop_monitoring(self) -> None:
        """Stop mouse monitoring."""
        try:
            self._event_source.stop()
            logger.info("Mouse monitoring stopped")
        except Exception as e:
            logger.warning(f"Error stopping mouse monitoring: {e}")

        # Flush any remaining stats
        self._flush_stats()

    def join(self, timeout: float = 2.0) -> bool:
        """Wait for event source to finish.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            True if source finished, False if timed out
        """
        if hasattr(self._event_source, "join"):
            return self._event_source.join(timeout)
        return True

    # Inline mode convenience methods
    def emit_move_inline(self, x: int, y: int) -> None:
        """Emit a move event directly (inline mode)."""
        if hasattr(self._event_source, "emit_move"):
            self._event_source.emit_move(x, y)
        else:
            # Fallback for non-inline sources
            self._record_move_event(x, y)

    def emit_click_inline(self, button_name: str, pressed: bool = True) -> None:
        """Emit a click event directly (inline mode)."""
        if hasattr(self._event_source, "emit_click"):
            self._event_source.emit_click(button_name, pressed)
        else:
            # Fallback - create mock button
            mock_button = type("Button", (), {"name": button_name})()
            self._record_click_event(100, 100, mock_button, pressed)

    def emit_scroll_inline(self, dx: int, dy: int) -> None:
        """Emit a scroll event directly (inline mode)."""
        if hasattr(self._event_source, "emit_scroll"):
            self._event_source.emit_scroll(dx, dy)
        else:
            # Fallback for non-inline sources
            self._record_scroll_event(100, 100, dx, dy)

    def emit_events_inline(self, count: int) -> None:
        """Emit count mouse events rapidly (inline mode)."""
        for i in range(count):
            if i % 3 == 0:
                self.emit_move_inline(100 + i, 100 + i)
            elif i % 3 == 1:
                self.emit_click_inline("left", True)
            else:
                self.emit_scroll_inline(0, 1)

    def _on_mouse_move(self, x: int, y: int) -> None:
        """Handle mouse move events."""
        try:
            self._record_move_event(x, y)
        except Exception as e:
            logger.debug(f"Error recording mouse move: {e}")

    def _on_mouse_click(self, x: int, y: int, button, pressed: bool) -> None:
        """Handle mouse click events."""
        try:
            self._record_click_event(x, y, button, pressed)
        except Exception as e:
            logger.debug(f"Error recording mouse click: {e}")

    def _on_mouse_scroll(self, x: int, y: int, dx: int, dy: int) -> None:
        """Handle mouse scroll events."""
        try:
            self._record_scroll_event(x, y, dx, dy)
        except Exception as e:
            logger.debug(f"Error recording mouse scroll: {e}")

    def _record_move_event(self, x: int, y: int) -> None:
        """Record mouse move event for dynamics analysis."""
        should_flush = False

        with self._stats_lock:
            self._stats.moves += 1

            # Calculate distance if we have a previous position
            if self._last_position is not None:
                prev_x, prev_y = self._last_position
                distance = int(math.sqrt((x - prev_x) ** 2 + (y - prev_y) ** 2))
                self._stats.distance_px += distance

            self._last_position = (x, y)

            # Check if we need to flush stats (size-based)
            total_events = (
                self._stats.moves
                + self._stats.click_left
                + self._stats.click_right
                + self._stats.click_middle
                + self._stats.scroll
            )
            if total_events >= self.batch_config.max_size:
                should_flush = True

        # Flush outside the lock to avoid deadlock
        if should_flush:
            self._flush_stats(force_base_flush=True)

    def _record_click_event(self, x: int, y: int, button, pressed: bool) -> None:
        """Record mouse click event for dynamics analysis."""
        # Only count press events, not releases
        if not pressed:
            return

        should_flush = False

        with self._stats_lock:
            # Extract button name safely
            button_name = getattr(button, "name", str(button)).lower()

            if "left" in button_name:
                self._stats.click_left += 1
            elif "right" in button_name:
                self._stats.click_right += 1
            elif "middle" in button_name:
                self._stats.click_middle += 1

            # Check if we need to flush stats (size-based)
            total_events = (
                self._stats.moves
                + self._stats.click_left
                + self._stats.click_right
                + self._stats.click_middle
                + self._stats.scroll
            )
            if total_events >= self.batch_config.max_size:
                should_flush = True

        # Flush outside the lock to avoid deadlock
        if should_flush:
            self._flush_stats(force_base_flush=True)

    def _record_scroll_event(self, x: int, y: int, dx: int, dy: int) -> None:
        """Record mouse scroll event for dynamics analysis."""
        should_flush = False

        with self._stats_lock:
            # Count scroll "ticks" - treat any non-zero dx/dy as 1 tick
            if dx != 0 or dy != 0:
                self._stats.scroll += 1

            # Check if we need to flush stats (size-based)
            total_events = (
                self._stats.moves
                + self._stats.click_left
                + self._stats.click_right
                + self._stats.click_middle
                + self._stats.scroll
            )
            if total_events >= self.batch_config.max_size:
                should_flush = True

        # Flush outside the lock to avoid deadlock
        if should_flush:
            self._flush_stats(force_base_flush=True)

    def _flush_stats(self, force_base_flush: bool = False) -> None:
        """Flush current stats as an event.

        Args:
            force_base_flush: If True, also flush the base batch immediately
        """
        with self._stats_lock:
            total_events = (
                self._stats.moves
                + self._stats.click_left
                + self._stats.click_right
                + self._stats.click_middle
                + self._stats.scroll
            )
            if total_events == 0:
                return  # Nothing to flush

            # Create event with stats
            attrs = self._stats.to_attrs_dict()

            event_data = {
                "action": "stats",
                "subject_type": "none",
                "subject_id": None,
                "attrs": attrs,
            }

            # Emit the event
            self.emit(event_data)

            # Reset stats for next batch
            self._stats.reset()
            self._last_position = None  # Reset position tracking
            self._last_stats_flush_time = self.scheduler.now()

            logger.debug(f"Flushed mouse stats: {attrs}")

        # Force base flush if requested (for size-based triggers)
        if force_base_flush:
            self.flush()

    def check_time_flush_inline(self) -> bool:
        """Check and perform mouse stats flush for inline mode.

        This method first checks if mouse stats should be flushed to the base batch,
        then calls the base time flush check.

        Returns:
            True if any flush was performed, False otherwise
        """
        stats_flushed = False
        should_flush = False

        # Check if we should flush mouse stats to base batch
        with self._stats_lock:
            total_events = (
                self._stats.moves
                + self._stats.click_left
                + self._stats.click_right
                + self._stats.click_middle
                + self._stats.scroll
            )
            if total_events > 0:
                current_time = self.scheduler.now()
                if (
                    current_time - self._last_stats_flush_time
                    >= self.batch_config.max_time_s
                ):
                    should_flush = True

        # Flush outside the lock to avoid deadlock
        if should_flush:
            self._flush_stats()
            stats_flushed = True

        # Check base monitor time flush
        base_flushed = super().check_time_flush_inline()

        return stats_flushed or base_flushed
