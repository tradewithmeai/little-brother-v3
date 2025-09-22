"""File system monitor for Little Brother v3."""

import time
from pathlib import Path
from typing import Optional

from ..database import get_database
from ..hashutil import hash_str
from ..logging_setup import get_logger
from ..utils.scheduler import Scheduler
from .base import BatchConfig, MonitorBase

logger = get_logger("file")

# Rate limiting for error logging: tracks last log time by exception class name
_error_log_rate_limit: dict[str, float] = {}
_ERROR_LOG_INTERVAL = 30.0  # seconds


def _log_error_rate_limited(exception: Exception, message: str) -> None:
    """Log error with rate limiting to prevent spam.

    Args:
        exception: The exception that occurred
        message: Base message to log
    """
    current_time = time.time()
    exception_type = type(exception).__name__

    last_logged = _error_log_rate_limit.get(exception_type, 0)
    if current_time - last_logged >= _ERROR_LOG_INTERVAL:
        logger.warning(f"{message}: {exception}")
        _error_log_rate_limit[exception_type] = current_time


class FileWatchMonitor(MonitorBase):
    """File system monitor using watchdog."""

    def __init__(
        self,
        dry_run: bool = False,
        batch_config: Optional[BatchConfig] = None,
        scheduler: Optional[Scheduler] = None,
        watch_paths: Optional[list[str]] = None,
    ):
        """Initialize file system monitor.

        Args:
            dry_run: Print events instead of emitting
            batch_config: Override batch configuration
            scheduler: Scheduler for deterministic timing
            watch_paths: List of paths to watch (defaults to user profile areas)
        """
        super().__init__(dry_run, scheduler)

        # File monitor defaults: flush every 1.5s
        if batch_config:
            self.batch_config = batch_config
        else:
            self.batch_config = BatchConfig(max_size=100, max_time_s=1.5)

        # Default watch paths: user profile, Desktop, Documents
        if watch_paths:
            self._watch_paths = watch_paths
        else:
            self._watch_paths = self._get_default_watch_paths()

        # Watchdog components
        self._observer = None
        self._event_handler = None

        # Initialize watchdog
        try:
            from watchdog.events import FileSystemEventHandler
            from watchdog.observers import Observer

            self._Observer = Observer
            self._FileSystemEventHandler = FileSystemEventHandler
            logger.info("watchdog loaded successfully")
        except ImportError as e:
            logger.warning(f"watchdog not available: {e}")
            self._Observer = None
            self._FileSystemEventHandler = None

    @property
    def name(self) -> str:
        """Monitor name."""
        return "file"

    def _get_default_watch_paths(self) -> list[str]:
        """Get default Windows user profile watch paths."""
        paths = []

        # User profile root
        user_profile = Path.home()
        paths.append(str(user_profile))

        # Desktop
        desktop = user_profile / "Desktop"
        if desktop.exists():
            paths.append(str(desktop))

        # Documents
        documents = user_profile / "Documents"
        if documents.exists():
            paths.append(str(documents))

        logger.info(f"Default watch paths: {paths}")
        return paths

    def start_monitoring(self) -> None:
        """Start file system monitoring."""
        if not self._Observer:
            logger.warning("Watchdog not available, file monitoring disabled")
            return

        if not self._watch_paths:
            logger.warning("No watch paths configured")
            return

        try:
            # Create observer and event handler
            self._observer = self._Observer()
            self._event_handler = self._create_event_handler()

            # Add watchers for each path
            for path_str in self._watch_paths:
                try:
                    path = Path(path_str).expanduser().resolve()
                    if not path.exists():
                        logger.warning(f"Watch path does not exist: {path}")
                        continue

                    self._observer.schedule(
                        self._event_handler, str(path), recursive=True
                    )
                    logger.info(f"Watching path: {path}")

                except Exception as e:
                    logger.warning(f"Failed to watch path {path_str}: {e}")

            # Start observer
            self._observer.start()
            logger.info("File system monitoring started")

        except Exception as e:
            logger.error(f"Failed to start file monitoring: {e}")
            logger.info("Continuing without file monitoring (degraded mode)")

    def stop_monitoring(self) -> None:
        """Stop file system monitoring."""
        if self._observer:
            try:
                self._observer.stop()
                self._observer.join(timeout=2.0)
                logger.info("File system monitoring stopped")
            except Exception as e:
                logger.warning(f"Error stopping file monitoring: {e}")
            finally:
                self._observer = None
                self._event_handler = None

    def _create_event_handler(self):
        """Create watchdog file system event handler."""

        class FileWatchEventHandler(self._FileSystemEventHandler):
            def __init__(self, monitor_instance):
                super().__init__()
                self.monitor = monitor_instance

            def on_created(self, event):
                if not event.is_directory:
                    self.monitor._on_file_event("created", event.src_path)

            def on_deleted(self, event):
                if not event.is_directory:
                    self.monitor._on_file_event("deleted", event.src_path)

            def on_moved(self, event):
                if not event.is_directory:
                    # Handle as renamed
                    self.monitor._on_file_event(
                        "renamed", event.dest_path, event.src_path
                    )

            def on_modified(self, event):
                # We'll treat modify as "opened/saved/closed" - detect via patterns
                if not event.is_directory:
                    self.monitor._on_file_event("saved", event.src_path)

        return FileWatchEventHandler(self)

    def _on_file_event(
        self, action: str, file_path: str, moved_from_path: str = None
    ) -> None:
        """Process file system event."""
        try:
            # Extract extension (lowercased, no dot)
            ext = self._extract_extension(file_path)

            # Hash the file path
            file_path_hash = hash_str(file_path, "file_path")

            # Get or create file record
            subject_id = self._get_or_create_file_record(file_path_hash, ext)

            # Create attrs for event
            attrs = {
                "op": "watchdog",
                "src": "FileWatchEventHandler",
                "moved_from": moved_from_path is not None,
            }

            if moved_from_path:
                # For renames, also track the old path hash
                attrs["old_path_hash"] = hash_str(moved_from_path, "file_path")

            # Create event data
            event_data = {
                "action": action,
                "subject_type": "file",
                "subject_id": subject_id,
                "attrs": attrs,
            }

            # Emit the event
            self.emit(event_data)

            logger.debug(f"File event: {action} for file_id={subject_id}, ext={ext}")

        except Exception as e:
            logger.debug(f"Error processing file event: {e}")

    def _extract_extension(self, file_path: str) -> str:
        """Extract file extension (lowercased, no dot)."""
        try:
            path = Path(file_path)
            ext = path.suffix.lower()
            if ext.startswith("."):
                ext = ext[1:]
            return ext or ""  # Return empty string for no extension
        except Exception:
            return ""

    def _get_or_create_file_record(self, path_hash: str, ext: str) -> str:
        """Get existing file record or create new one.

        Args:
            path_hash: Hash of the file path
            ext: File extension (no dot, lowercased)

        Returns:
            File ID (ULID)
        """
        try:
            db = get_database()
            ts_ms = int(self.scheduler.now() * 1000)  # Convert to milliseconds
            return db.upsert_file_record(path_hash, ext, ts_ms)

        except Exception as e:
            _log_error_rate_limited(e, "Database error managing file record")
            # Skip event emission for this occurrence, monitoring continues
            from ..ids import new_id

            return new_id()
