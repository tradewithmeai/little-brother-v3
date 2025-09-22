"""Logging configuration for Little Brother v3."""

import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Global logging state
_logging_initialized = False
_logging_lock = threading.Lock()
_session_id = None
_logged_messages = set()  # Track messages logged once per process


class ContextFormatter(logging.Formatter):
    """Custom formatter that includes session_id, monitor, and thread context."""

    def __init__(self):
        # UTC timestamp format with session_id, monitor, and thread
        super().__init__(
            fmt=(
                "%(asctime)s.%(msecs)03d [%(session_id)s] [%(monitor)s] "
                "[%(threadName)s] %(levelname)s - %(message)s"
            ),
            datefmt="%Y-%m-%dT%H:%M:%S",
        )

    def formatTime(self, record, datefmt=None):
        """Format time in UTC."""
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()

    def format(self, record):
        """Add context fields to log record."""
        # Add session_id if not present
        if not hasattr(record, "session_id"):
            record.session_id = _session_id or "unknown"

        # Add monitor if not present
        if not hasattr(record, "monitor"):
            # Try to extract monitor from logger name
            if hasattr(record, "name") and "." in record.name:
                parts = record.name.split(".")
                if len(parts) >= 2 and parts[0] == "lb3":
                    record.monitor = parts[1]
                else:
                    record.monitor = "system"
            else:
                record.monitor = "system"

        return super().format(record)


def setup_logging(
    console_level: str = "INFO",
    file_level: str = "DEBUG",
    session_id: Optional[str] = None,
    log_dir: Optional[Path] = None,
    console: bool = True,
) -> logging.Logger:
    """Set up centralized logging configuration.

    Args:
        console_level: Console log level (INFO by default)
        file_level: File log level (DEBUG by default)
        session_id: Session identifier for context
        log_dir: Log directory (./lb_data/logs/ by default)
        console: Whether to enable console logging

    Returns:
        Configured logger instance
    """
    global _logging_initialized, _session_id

    with _logging_lock:
        if _logging_initialized:
            return logging.getLogger("lb3")

        _session_id = session_id or "unknown"

        # Get root logger for lb3
        logger = logging.getLogger("lb3")
        logger.setLevel(logging.DEBUG)  # Allow all levels through

        # Clear any existing handlers
        logger.handlers.clear()
        logger.propagate = False  # Don't propagate to root logger

        # Create custom formatter
        formatter = ContextFormatter()

        # Console handler
        if console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(getattr(logging, console_level.upper()))
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        # File handler - per-run log file
        if log_dir is None:
            log_dir = Path("lb_data/logs")

        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)

        # Generate per-run log filename: YYYYMMDD_HHMMSS-PID.log
        now = datetime.now(timezone.utc)
        pid = os.getpid()
        log_filename = f"{now.strftime('%Y%m%d_%H%M%S')}-{pid}.log"
        log_file = log_dir / log_filename

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(getattr(logging, file_level.upper()))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        _logging_initialized = True

        logger.info(f"Logging initialized - session: {_session_id}, file: {log_file}")

        return logger


def get_logger(name: str, monitor: Optional[str] = None) -> logging.Logger:
    """Get a logger instance with optional monitor context.

    Args:
        name: Logger name (e.g., 'spooler', 'active_window')
        monitor: Optional monitor name for context

    Returns:
        Logger instance
    """
    if not _logging_initialized:
        # Initialize with defaults if not already done
        setup_logging()

    logger_name = f"lb3.{name}"
    logger = logging.getLogger(logger_name)

    # Store monitor context for this logger
    if monitor:
        # Create a LoggerAdapter to inject monitor context
        return MonitorLoggerAdapter(logger, {"monitor": monitor})

    return logger


class MonitorLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that injects monitor context."""

    def process(self, msg, kwargs):
        """Add monitor context to log records."""
        return msg, kwargs

    def makeRecord(
        self,
        name,
        level,
        fn,
        lno,
        msg,
        args,
        exc_info,
        func=None,
        extra=None,
        sinfo=None,
    ):
        """Create log record with monitor context."""
        if extra is None:
            extra = {}
        extra.update(self.extra)

        # Get the underlying logger to create the record
        return self.logger.makeRecord(
            name, level, fn, lno, msg, args, exc_info, func, extra, sinfo
        )


def set_session_id(session_id: str) -> None:
    """Set the global session ID for logging context."""
    global _session_id
    _session_id = session_id


def get_session_id() -> Optional[str]:
    """Get the current session ID."""
    return _session_id


def log_once(
    logger: logging.Logger,
    level: int,
    message: str,
    *args,
    key: Optional[str] = None,
    **kwargs,
) -> None:
    """Log a message only once per process run.

    Args:
        logger: Logger instance to use
        level: Logging level (e.g., logging.INFO)
        message: Message to log (with format placeholders)
        *args: Message format arguments
        key: Optional custom key for deduplication (allows time-based keys)
        **kwargs: Additional logging kwargs
    """
    global _logged_messages

    # Create a unique key for this message
    if key:
        message_key = f"{logger.name}:{level}:{key}"
    else:
        formatted_msg = message % args if args else message
        message_key = f"{logger.name}:{level}:{formatted_msg}"

    with _logging_lock:
        if message_key not in _logged_messages:
            _logged_messages.add(message_key)
            logger.log(level, message, *args, **kwargs)
