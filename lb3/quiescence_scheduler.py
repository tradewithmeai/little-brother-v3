"""Quiescence scheduler for Little Brother v3."""

import threading
from typing import Optional

from .config import get_effective_config
from .logging_setup import get_logger

logger = get_logger("quiescence_scheduler")


class QuiescenceScheduler:
    """Simple timer-based scheduler for quiescence snapshots."""

    def __init__(self, context_monitor=None):
        """Initialize quiescence scheduler."""
        self._context_monitor = context_monitor
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._interval_s = 300.0  # Default 5 minutes

        # Parse interval from config
        config = get_effective_config()
        if hasattr(config, "monitors") and config.monitors.context_snapshot.quiescence:
            interval_str = config.monitors.context_snapshot.quiescence.interval
            self._interval_s = self._parse_time_string(interval_str)

        logger.info(f"Quiescence scheduler initialized (interval: {self._interval_s}s)")

    def _parse_time_string(self, time_str: str) -> float:
        """Parse time string like '300s' to seconds."""
        try:
            if time_str and time_str.endswith("s"):
                return float(time_str[:-1])
            return float(time_str)
        except (ValueError, TypeError, AttributeError):
            logger.warning(f"Failed to parse time string '{time_str}', using 300s")
            return 300.0

    def start(self):
        """Start the quiescence scheduler."""
        if self._thread is not None and self._thread.is_alive():
            logger.warning("Quiescence scheduler already running")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info("Quiescence scheduler started")

    def stop(self):
        """Stop the quiescence scheduler."""
        if self._thread is None or not self._thread.is_alive():
            return

        self._stop_event.set()
        self._thread.join(timeout=2.0)
        logger.info("Quiescence scheduler stopped")

    def _run_loop(self):
        """Main loop for quiescence scheduling."""
        try:
            while not self._stop_event.is_set():
                # Wait for interval or stop signal
                if self._stop_event.wait(timeout=self._interval_s):
                    break

                # Emit quiescence snapshot if monitor is available
                if self._context_monitor is not None:
                    try:
                        self._context_monitor.force_emit(trigger="quiescence")
                        logger.debug("Emitted quiescence snapshot")
                    except Exception as e:
                        logger.error(f"Failed to emit quiescence snapshot: {e}")
                else:
                    logger.warning(
                        "No context monitor available for quiescence snapshot"
                    )

        except Exception as e:
            logger.error(f"Error in quiescence scheduler loop: {e}", exc_info=True)

    def set_context_monitor(self, monitor):
        """Set the context monitor for quiescence snapshots."""
        self._context_monitor = monitor
        logger.debug("Context monitor set for quiescence scheduler")

    def is_running(self) -> bool:
        """Check if the scheduler is running."""
        return self._thread is not None and self._thread.is_alive()
