"""Journal spooler for Little Brother v3."""

import contextlib
import gzip
import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from .config import get_effective_config
from .logging_setup import get_logger
from .spool_quota import QuotaState, get_quota_manager

logger = get_logger("spooler")

try:
    import orjson

    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False


class JournalSpooler:
    """Atomic append-only NDJSON.gz spooler with rollover."""

    def __init__(
        self,
        monitor: str,
        spool_dir: Optional[Path] = None,
        finalize_callback: Optional[Callable[[str], None]] = None,
    ):
        """Initialize spooler for a monitor.

        Args:
            monitor: Monitor name (e.g., 'active_window', 'keyboard')
            spool_dir: Base spool directory. If None, uses config.
            finalize_callback: Optional callback to call when files are finalized
        """
        self.monitor = monitor

        if spool_dir is None:
            config = get_effective_config()
            spool_dir = Path(config.storage.spool_dir)

        self.spool_dir = spool_dir / monitor
        self.spool_dir.mkdir(parents=True, exist_ok=True)

        # Configuration
        config = get_effective_config()
        self.max_size_bytes = 8 * 1024 * 1024  # 8 MB uncompressed
        self.idle_timeout = 1.5  # seconds

        # State tracking
        self._lock = threading.Lock()
        self._current_gzip: Optional[gzip.GzipFile] = None
        self._current_file_handle: Optional[Any] = None
        self._current_path: Optional[Path] = None
        self._current_temp_path: Optional[Path] = None
        self._current_hour: Optional[str] = None
        self._file_sequence = 0  # Sequence number for files within same hour
        self._uncompressed_size = 0
        self._last_write_time = 0.0
        self._closed = False

        # Backpressure memory buffer
        self._memory_buffer: list[bytes] = []
        self._buffer_size_bytes = 0
        self._max_buffer_size = self.max_size_bytes * 2  # 2x batch size buffer
        self._quota_manager = get_quota_manager()

        # Finalization callback
        self._finalize_callback = finalize_callback

    def write_event(self, event_data: dict[str, Any]) -> None:
        """Write an event to the journal with quota backpressure.

        Args:
            event_data: Event data dict matching database schema
        """
        if self._closed:
            raise RuntimeError("Spooler has been closed")

        with self._lock:
            # Serialize event first to know its size
            if HAS_ORJSON:
                json_data = orjson.dumps(
                    event_data, option=orjson.OPT_APPEND_NEWLINE
                ).decode("utf-8")
            else:
                json_data = (
                    json.dumps(event_data, ensure_ascii=False, separators=(",", ":"))
                    + "\n"
                )

            json_bytes = json_data.encode("utf-8")
            event_size = len(json_bytes)

            # Check quota backpressure
            should_apply_pressure, delay = self._quota_manager.check_backpressure()

            if should_apply_pressure:
                # Apply soft delay if specified
                if delay is not None:
                    time.sleep(delay)

                # Check if we should buffer in memory (hard backpressure)
                usage = self._quota_manager.get_spool_usage()
                if usage.state == QuotaState.HARD:
                    self._buffer_in_memory(json_bytes, event_data)
                    return

            # Try to flush any buffered events first
            self._flush_memory_buffer()

            # Check if we need to rollover
            current_hour = self._get_current_hour()
            hour_changed = self._current_hour != current_hour
            size_exceeded = (
                self._current_gzip is not None
                and self._uncompressed_size + event_size > self.max_size_bytes
            )

            if hour_changed or size_exceeded:
                self._rollover(hour_changed)

            # Open file if needed
            if self._current_gzip is None:
                self._open_current_file()

            # Write to file (add newline for NDJSON format)
            json_line = json_bytes + b"\n"
            self._current_gzip.write(json_line)
            self._uncompressed_size += len(json_line)
            self._last_write_time = time.time()

    def flush_if_idle(self) -> None:
        """Flush and close current file if idle timeout exceeded."""
        with self._lock:
            # Try to flush memory buffer on idle
            self._flush_memory_buffer()

            if (
                self._current_gzip is not None
                and time.time() - self._last_write_time >= self.idle_timeout
            ):
                self._close_current_file()

    def close(self) -> None:
        """Close spooler and finalize any open files."""
        with self._lock:
            if self._closed:
                return

            self._close_current_file()
            self._closed = True

    def _get_current_hour(self) -> str:
        """Get current hour string for file naming."""
        now = datetime.now(timezone.utc)
        return now.strftime("%Y%m%d-%H")

    def _open_current_file(self) -> None:
        """Open current journal file for writing in proper binary mode."""
        self._current_hour = self._get_current_hour()

        # Generate filename with sequence if needed to avoid collisions
        if self._file_sequence == 0:
            filename = f"{self._current_hour}.ndjson.gz"
        else:
            filename = f"{self._current_hour}-{self._file_sequence:03d}.ndjson.gz"

        self._current_path = self.spool_dir / filename
        self._current_temp_path = self.spool_dir / f"{filename}.part"

        # Always start fresh - do not attempt to append to gzip files
        # If a .part file exists, it's likely corrupted, so we start over
        if self._current_temp_path.exists():
            try:
                self._current_temp_path.unlink()
                logger.debug(f"Removed existing .part file: {self._current_temp_path}")
            except Exception as e:
                logger.warning(f"Could not remove existing .part file: {e}")

        # Open file in binary mode with no buffering for immediate writes
        self._current_file_handle = open(self._current_temp_path, "wb", buffering=0)

        # Wrap with gzip in binary write mode
        self._current_gzip = gzip.GzipFile(
            fileobj=self._current_file_handle, mode="wb", compresslevel=6
        )

        self._uncompressed_size = 0
        logger.debug(f"Opened new journal file: {self._current_temp_path}")

    def _close_current_file(self) -> None:
        """Close and atomically rename current journal file."""
        if self._current_gzip is None and self._current_file_handle is None:
            return

        try:
            # Flush and fsync gzip data first
            if self._current_gzip:
                self._current_gzip.flush()

            # Flush and fsync underlying file handle
            if self._current_file_handle:
                self._current_file_handle.flush()
                fd = self._current_file_handle.fileno()
                os.fsync(fd)

            # Close in proper order: gzip wrapper first, then file handle
            if self._current_gzip:
                self._current_gzip.close()

            if self._current_file_handle:
                self._current_file_handle.close()

            # Atomic rename
            if self._current_temp_path and self._current_path:
                os.replace(str(self._current_temp_path), str(self._current_path))

                # Best-effort directory flush on Windows
                try:
                    dir_fd = os.open(str(self.spool_dir), os.O_RDONLY)
                    os.fsync(dir_fd)
                    os.close(dir_fd)
                except (OSError, AttributeError):
                    # Directory fsync not supported on all Windows versions
                    pass

                # Post-promotion sanity check: verify gzip file is readable
                try:
                    with gzip.open(self._current_path, "rb") as f:
                        f.read(8)  # Read first 8 bytes to verify gzip integrity
                    logger.debug(f"Finalized journal file: {self._current_path}")
                except Exception as gzip_error:
                    # Move corrupted file to .part.error and log
                    error_path = self._current_path.with_suffix(
                        self._current_path.suffix + ".part.error"
                    )
                    try:
                        os.replace(str(self._current_path), str(error_path))
                        logger.error(
                            f"Gzip sanity check failed for {self.monitor}/{self._current_path.name}: {gzip_error}"
                        )
                    except Exception as move_error:
                        logger.error(
                            f"Failed to move corrupted file {self._current_path}: {move_error}"
                        )
                    return  # Don't call finalize callback for corrupted files

                # Notify manager of finalization
                if self._finalize_callback:
                    try:
                        self._finalize_callback(self.monitor)
                    except Exception as callback_error:
                        logger.error(f"Error in finalize callback: {callback_error}")

        except Exception as e:
            logger.error(f"Error closing journal file: {e}")
            # Clean up temp file on error
            if self._current_temp_path and self._current_temp_path.exists():
                with contextlib.suppress(Exception):
                    self._current_temp_path.unlink()

        finally:
            self._current_gzip = None
            self._current_file_handle = None
            self._current_path = None
            self._current_temp_path = None
            self._current_hour = None
            self._uncompressed_size = 0

    def _buffer_in_memory(self, json_bytes: bytes, event_data: dict[str, Any]) -> None:
        """Buffer event in memory during hard backpressure."""
        # Check if buffer has space
        if self._buffer_size_bytes + len(json_bytes) > self._max_buffer_size:
            # Drop low-priority events if buffer is full
            if self._should_drop_event(event_data):
                self._quota_manager.increment_dropped_batches()
                return

            # Make room by dropping oldest buffered event
            if self._memory_buffer:
                dropped_bytes = self._memory_buffer.pop(0)
                self._buffer_size_bytes -= len(dropped_bytes)
                self._quota_manager.increment_dropped_batches()

        # Add to buffer
        self._memory_buffer.append(json_bytes)
        self._buffer_size_bytes += len(json_bytes)

    def _should_drop_event(self, event_data: dict[str, Any]) -> bool:
        """Determine if event should be dropped during hard backpressure."""
        monitor = event_data.get("monitor", "")
        # Drop heartbeat and context_snapshot events first
        return monitor in ("heartbeat", "context_snapshot")

    def _flush_memory_buffer(self) -> None:
        """Flush buffered events to disk when quota allows."""
        if not self._memory_buffer:
            return

        # Check if we can write buffered events
        usage = self._quota_manager.get_spool_usage()
        if usage.state == QuotaState.HARD:
            return  # Still under hard backpressure

        # Flush buffered events
        events_to_flush = self._memory_buffer.copy()
        self._memory_buffer.clear()
        self._buffer_size_bytes = 0

        for json_bytes in events_to_flush:
            try:
                # Check if we need to rollover
                if (
                    self._current_gzip is not None
                    and self._uncompressed_size + len(json_bytes) > self.max_size_bytes
                ):
                    self._rollover()

                # Open file if needed
                if self._current_gzip is None:
                    self._open_current_file()

                # Write buffered event (ensure it has newline for NDJSON format)
                if not json_bytes.endswith(b"\n"):
                    json_bytes += b"\n"
                self._current_gzip.write(json_bytes)
                self._uncompressed_size += len(json_bytes)

            except Exception as e:
                logger.warning(f"Error flushing buffered event: {e}")
                # Re-buffer the failed event
                self._memory_buffer.append(json_bytes)
                self._buffer_size_bytes += len(json_bytes)
                break

    def _rollover(self, hour_changed: bool = False) -> None:
        """Roll over to a new journal file."""
        self._close_current_file()

        # Reset sequence if hour changed, otherwise increment
        if hour_changed:
            self._file_sequence = 0
        else:
            self._file_sequence += 1


class SpoolerManager:
    """Manages multiple monitor spoolers."""

    def __init__(self, spool_dir: Optional[Path] = None):
        """Initialize spooler manager.

        Args:
            spool_dir: Base spool directory. If None, uses config.
        """
        if spool_dir is None:
            config = get_effective_config()
            spool_dir = Path(config.storage.spool_dir)

        self.spool_dir = spool_dir
        self._spoolers: dict[str, JournalSpooler] = {}
        self._lock = threading.Lock()

        # Event counters for observability
        self._written_by_monitor: dict[str, int] = {}
        self._finalised_files_by_monitor: dict[str, int] = {}
        self._counters_lock = threading.Lock()

    def get_spooler(self, monitor: str) -> JournalSpooler:
        """Get or create spooler for a monitor.

        Args:
            monitor: Monitor name

        Returns:
            JournalSpooler instance for the monitor
        """
        with self._lock:
            if monitor not in self._spoolers:
                self._spoolers[monitor] = JournalSpooler(
                    monitor, self.spool_dir, finalize_callback=self._on_file_finalized
                )
            return self._spoolers[monitor]

    def write_event(self, monitor: str, event_data: dict[str, Any]) -> None:
        """Write an event to the appropriate monitor spooler.

        Args:
            monitor: Monitor name
            event_data: Event data dict
        """
        spooler = self.get_spooler(monitor)
        spooler.write_event(event_data)

        # Increment write counter
        with self._counters_lock:
            self._written_by_monitor[monitor] = (
                self._written_by_monitor.get(monitor, 0) + 1
            )

    def flush_idle_spoolers(self) -> None:
        """Flush all idle spoolers."""
        with self._lock:
            for spooler in self._spoolers.values():
                try:
                    spooler.flush_if_idle()
                except Exception as e:
                    logger.error(f"Error flushing spooler: {e}")

    def close_all(self) -> None:
        """Close all spoolers."""
        with self._lock:
            for spooler in self._spoolers.values():
                try:
                    spooler.close()
                except Exception as e:
                    logger.error(f"Error closing spooler: {e}")
            self._spoolers.clear()

    def _on_file_finalized(self, monitor: str) -> None:
        """Callback when a file is finalized."""
        with self._counters_lock:
            self._finalised_files_by_monitor[monitor] = (
                self._finalised_files_by_monitor.get(monitor, 0) + 1
            )

    def get_stats(self) -> dict[str, dict[str, int]]:
        """Get spooler statistics.

        Returns:
            Dictionary with 'written_by_monitor' and 'finalised_files_by_monitor' keys
        """
        with self._counters_lock:
            return {
                "written_by_monitor": self._written_by_monitor.copy(),
                "finalised_files_by_monitor": self._finalised_files_by_monitor.copy(),
            }

    def reset_stats(self) -> dict[str, dict[str, int]]:
        """Get current stats and reset all counters.

        Returns:
            Dictionary with stats before reset
        """
        with self._counters_lock:
            current_stats = {
                "written_by_monitor": self._written_by_monitor.copy(),
                "finalised_files_by_monitor": self._finalised_files_by_monitor.copy(),
            }
            self._written_by_monitor.clear()
            self._finalised_files_by_monitor.clear()
            return current_stats


# Global spooler manager
_spooler_manager: Optional[SpoolerManager] = None
_manager_lock = threading.Lock()


def get_spooler_manager() -> SpoolerManager:
    """Get global spooler manager instance."""
    global _spooler_manager

    with _manager_lock:
        if _spooler_manager is None:
            _spooler_manager = SpoolerManager()
        return _spooler_manager


def write_event(monitor: str, event_data: dict[str, Any]) -> None:
    """Write an event to the spooler.

    Args:
        monitor: Monitor name (active_window, keyboard, etc.)
        event_data: Event data dict with required fields
    """
    manager = get_spooler_manager()
    manager.write_event(monitor, event_data)


def create_sample_event(monitor: str) -> dict[str, Any]:
    """Create a sample event for testing.

    Args:
        monitor: Monitor name

    Returns:
        Sample event dict
    """
    from .hashutil import hash_str
    from .ids import new_id

    base_event = {
        "id": new_id(),
        "ts_utc": int(time.time() * 1000),
        "monitor": monitor,
        "session_id": new_id(),
    }

    if monitor == "active_window":
        base_event.update(
            {
                "action": "window_focus",
                "subject_type": "window",
                "subject_id": new_id(),
                "pid": 1234,
                "exe_name": "notepad.exe",
                "exe_path_hash": hash_str(
                    r"C:\Windows\System32\notepad.exe", "exe_path"
                ),
                "window_title_hash": hash_str("Untitled - Notepad", "window_title"),
                "attrs_json": json.dumps(
                    {"x": 100, "y": 200, "width": 800, "height": 600}
                ),
            }
        )
    elif monitor == "keyboard":
        base_event.update(
            {
                "action": "key_press",
                "subject_type": "none",
                "attrs_json": json.dumps({"keys_per_minute": 45, "special_keys": 3}),
            }
        )
    elif monitor == "mouse":
        base_event.update(
            {
                "action": "mouse_move",
                "subject_type": "none",
                "attrs_json": json.dumps({"x": 500, "y": 300, "clicks": 0}),
            }
        )
    elif monitor == "browser":
        base_event.update(
            {
                "action": "page_visit",
                "subject_type": "url",
                "subject_id": new_id(),
                "url_hash": hash_str("https://example.com/page", "url"),
                "attrs_json": json.dumps({"title": "Example Page", "duration": 120}),
            }
        )
    elif monitor == "file":
        base_event.update(
            {
                "action": "file_access",
                "subject_type": "file",
                "subject_id": new_id(),
                "file_path_hash": hash_str(r"C:\Users\user\document.txt", "file_path"),
                "attrs_json": json.dumps({"operation": "read", "size": 1024}),
            }
        )
    else:  # context_snapshot or generic
        base_event.update(
            {
                "action": "snapshot",
                "subject_type": "none",
                "attrs_json": json.dumps({"context": "idle"}),
            }
        )

    return base_event
