"""Dummy heartbeat monitor for testing the event bus and monitor system."""

import time
from typing import Any

from .base import MonitorBase


class HeartbeatMonitor(MonitorBase):
    """Dummy monitor that emits heartbeat events at regular intervals."""

    def __init__(
        self, dry_run: bool = False, interval: float = 1.0, total_beats: int = 10
    ):
        """Initialize heartbeat monitor.

        Args:
            dry_run: If True, print events instead of writing
            interval: Seconds between heartbeats
            total_beats: Total number of heartbeats to emit (0 for infinite)
        """
        super().__init__(dry_run)
        self.interval = interval
        self.total_beats = total_beats
        self._beat_count = 0

    @property
    def name(self) -> str:
        """Monitor name."""
        return "heartbeat"

    @property
    def cadence_s(self) -> float:
        """Heartbeat cadence in seconds."""
        return self.interval

    def start_monitoring(self) -> None:
        """Start heartbeat monitoring."""
        self.logger.info(
            f"Starting heartbeat monitor (interval: {self.interval}s, total: {self.total_beats})"
        )
        self._beat_count = 0

        # Emit initial heartbeat
        self._emit_heartbeat()

    def stop_monitoring(self) -> None:
        """Stop heartbeat monitoring."""
        self.logger.info(f"Stopping heartbeat monitor after {self._beat_count} beats")

    def _run_loop(self) -> None:
        """Override run loop for heartbeat-specific logic."""
        try:
            self.start_monitoring()

            # Main heartbeat loop
            while not self._stop_event.is_set():
                # Check if we've reached the limit
                if self.total_beats > 0 and self._beat_count >= self.total_beats:
                    self.logger.info(f"Reached total beats limit ({self.total_beats})")
                    break

                # Wait for next heartbeat
                if self._stop_event.wait(self.interval):
                    break

                # Emit heartbeat
                self._emit_heartbeat()

        except Exception as e:
            self.logger.error(f"Error in heartbeat monitor: {e}", exc_info=True)
        finally:
            # Mark as no longer running when we finish naturally
            self._running = False
            try:
                self.stop_monitoring()
            except Exception as e:
                self.logger.error(
                    f"Error stopping heartbeat monitor: {e}", exc_info=True
                )

    def _emit_heartbeat(self) -> None:
        """Emit a heartbeat event."""
        self._beat_count += 1

        event_data = {
            "action": "heartbeat",
            "subject_type": "none",
            "attrs": {
                "beat_number": self._beat_count,
                "interval": self.interval,
                "uptime": time.time(),
                "test_data": f"heartbeat_{self._beat_count}",
            },
        }

        self.emit(event_data)
        self.logger.debug(f"Emitted heartbeat #{self._beat_count}")

    def get_stats(self) -> dict[str, Any]:
        """Get heartbeat monitor statistics."""
        return {
            "name": self.name,
            "beat_count": self._beat_count,
            "total_beats": self.total_beats,
            "interval": self.interval,
            "is_running": self._running,
        }
