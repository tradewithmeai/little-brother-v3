"""Journal spooler for Little Brother v3."""

import gzip
import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from .config import get_effective_config
from .logging_setup import get_logger

logger = get_logger("spooler")

try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False


class JournalSpooler:
    """Atomic append-only NDJSON.gz spooler with rollover."""

    def __init__(self, monitor: str, spool_dir: Optional[Path] = None):
        """Initialize spooler for a monitor.
        
        Args:
            monitor: Monitor name (e.g., 'active_window', 'keyboard')
            spool_dir: Base spool directory. If None, uses config.
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
        self._current_file: Optional[gzip.GzipFile] = None
        self._current_path: Optional[Path] = None
        self._current_temp_path: Optional[Path] = None
        self._current_hour: Optional[str] = None
        self._file_sequence = 0  # Sequence number for files within same hour
        self._uncompressed_size = 0
        self._last_write_time = 0.0
        self._closed = False

    def write_event(self, event_data: Dict[str, Any]) -> None:
        """Write an event to the journal.
        
        Args:
            event_data: Event data dict matching database schema
        """
        if self._closed:
            raise RuntimeError("Spooler has been closed")

        with self._lock:
            # Serialize event first to know its size
            if HAS_ORJSON:
                json_data = orjson.dumps(event_data, option=orjson.OPT_APPEND_NEWLINE).decode('utf-8')
            else:
                json_data = json.dumps(event_data, ensure_ascii=False, separators=(',', ':')) + '\n'
            
            json_bytes = json_data.encode('utf-8')
            event_size = len(json_bytes)
            
            # Check if we need to rollover
            current_hour = self._get_current_hour()
            hour_changed = self._current_hour != current_hour
            size_exceeded = (self._current_file is not None and 
                           self._uncompressed_size + event_size > self.max_size_bytes)
            
            if hour_changed or size_exceeded:
                self._rollover(hour_changed)

            # Open file if needed
            if self._current_file is None:
                self._open_current_file()

            # Write to file
            self._current_file.write(json_bytes)
            self._uncompressed_size += event_size
            self._last_write_time = time.time()

    def flush_if_idle(self) -> None:
        """Flush and close current file if idle timeout exceeded."""
        with self._lock:
            if (self._current_file is not None and 
                time.time() - self._last_write_time >= self.idle_timeout):
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
        return now.strftime('%Y%m%d-%H')

    def _open_current_file(self) -> None:
        """Open current journal file for writing."""
        self._current_hour = self._get_current_hour()
        
        # Generate filename with sequence if needed to avoid collisions
        if self._file_sequence == 0:
            filename = f"{self._current_hour}.ndjson.gz"
        else:
            filename = f"{self._current_hour}-{self._file_sequence:03d}.ndjson.gz"
        
        self._current_path = self.spool_dir / filename
        self._current_temp_path = self.spool_dir / f"{filename}.part"
        
        # Check if we're resuming an existing .part file
        existing_size = 0
        if self._current_temp_path.exists():
            # If resuming, read existing content to track size
            try:
                with gzip.open(str(self._current_temp_path), 'rt', encoding='utf-8') as f:
                    content = f.read()
                    existing_size = len(content.encode('utf-8'))
            except Exception:
                existing_size = 0
        
        # Open gzip file in append mode with temp name
        self._current_file = gzip.open(
            str(self._current_temp_path), 
            'ab',  # append binary mode
            compresslevel=6
        )
        
        self._uncompressed_size = existing_size
        logger.debug(f"Opened journal file: {self._current_temp_path} (existing size: {existing_size})")

    def _close_current_file(self) -> None:
        """Close and atomically rename current journal file."""
        if self._current_file is None:
            return

        try:
            # Flush and fsync
            self._current_file.flush()
            
            # Get file descriptor for fsync
            fd = self._current_file.fileobj.fileno()
            os.fsync(fd)
            
            # Close file
            self._current_file.close()
            
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
                
                logger.debug(f"Finalized journal file: {self._current_path}")
            
        except Exception as e:
            logger.error(f"Error closing journal file: {e}")
            # Clean up temp file on error
            if self._current_temp_path and self._current_temp_path.exists():
                try:
                    self._current_temp_path.unlink()
                except Exception:
                    pass
        
        finally:
            self._current_file = None
            self._current_path = None
            self._current_temp_path = None
            self._current_hour = None
            self._uncompressed_size = 0

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
        self._spoolers: Dict[str, JournalSpooler] = {}
        self._lock = threading.Lock()

    def get_spooler(self, monitor: str) -> JournalSpooler:
        """Get or create spooler for a monitor.
        
        Args:
            monitor: Monitor name
            
        Returns:
            JournalSpooler instance for the monitor
        """
        with self._lock:
            if monitor not in self._spoolers:
                self._spoolers[monitor] = JournalSpooler(monitor, self.spool_dir)
            return self._spoolers[monitor]

    def write_event(self, monitor: str, event_data: Dict[str, Any]) -> None:
        """Write an event to the appropriate monitor spooler.
        
        Args:
            monitor: Monitor name
            event_data: Event data dict
        """
        spooler = self.get_spooler(monitor)
        spooler.write_event(event_data)

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


def write_event(monitor: str, event_data: Dict[str, Any]) -> None:
    """Write an event to the spooler.
    
    Args:
        monitor: Monitor name (active_window, keyboard, etc.)
        event_data: Event data dict with required fields
    """
    manager = get_spooler_manager()
    manager.write_event(monitor, event_data)


def create_sample_event(monitor: str) -> Dict[str, Any]:
    """Create a sample event for testing.
    
    Args:
        monitor: Monitor name
        
    Returns:
        Sample event dict
    """
    from .hashutil import hash_str
    from .ids import new_id
    
    base_event = {
        'id': new_id(),
        'ts_utc': int(time.time() * 1000),
        'monitor': monitor,
        'session_id': new_id(),
    }
    
    if monitor == 'active_window':
        base_event.update({
            'action': 'window_focus',
            'subject_type': 'window',
            'subject_id': new_id(),
            'pid': 1234,
            'exe_name': 'notepad.exe',
            'exe_path_hash': hash_str(r'C:\Windows\System32\notepad.exe', 'exe_path'),
            'window_title_hash': hash_str('Untitled - Notepad', 'window_title'),
            'attrs_json': json.dumps({'x': 100, 'y': 200, 'width': 800, 'height': 600})
        })
    elif monitor == 'keyboard':
        base_event.update({
            'action': 'key_press',
            'subject_type': 'none',
            'attrs_json': json.dumps({'keys_per_minute': 45, 'special_keys': 3})
        })
    elif monitor == 'mouse':
        base_event.update({
            'action': 'mouse_move',
            'subject_type': 'none',
            'attrs_json': json.dumps({'x': 500, 'y': 300, 'clicks': 0})
        })
    elif monitor == 'browser':
        base_event.update({
            'action': 'page_visit',
            'subject_type': 'url',
            'subject_id': new_id(),
            'url_hash': hash_str('https://example.com/page', 'url'),
            'attrs_json': json.dumps({'title': 'Example Page', 'duration': 120})
        })
    elif monitor == 'file':
        base_event.update({
            'action': 'file_access',
            'subject_type': 'file',
            'subject_id': new_id(),
            'file_path_hash': hash_str(r'C:\Users\user\document.txt', 'file_path'),
            'attrs_json': json.dumps({'operation': 'read', 'size': 1024})
        })
    else:  # context_snapshot or generic
        base_event.update({
            'action': 'snapshot',
            'subject_type': 'none',
            'attrs_json': json.dumps({'context': 'idle'})
        })
    
    return base_event