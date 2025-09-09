"""Journal importer for Little Brother v3."""

import gzip
import json
import sqlite3
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import get_effective_config
from .database import get_database
from .logging_setup import get_logger

logger = get_logger("importer")

try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False


class ImportError(Exception):
    """Import-specific error."""
    pass


class JournalImporter:
    """Imports NDJSON.gz journal files into SQLite."""

    def __init__(self, spool_dir: Optional[Path] = None):
        """Initialize importer.
        
        Args:
            spool_dir: Base spool directory. If None, uses config.
        """
        if spool_dir is None:
            config = get_effective_config()
            spool_dir = Path(config.storage.spool_dir)
        
        self.spool_dir = spool_dir
        self.done_dir = spool_dir / "_done"
        self.done_dir.mkdir(parents=True, exist_ok=True)

    def flush_monitor(self, monitor: str, batch_size: int = 1000, db=None) -> Dict[str, Any]:
        """Flush journal files for a specific monitor.
        
        Args:
            monitor: Monitor name to flush
            batch_size: Number of events to insert per batch
            db: Optional database connection to reuse
            
        Returns:
            Dict with import statistics
        """
        start_time = time.time()
        monitor_dir = self.spool_dir / monitor
        if not monitor_dir.exists():
            logger.warning(f"Monitor directory does not exist: {monitor_dir}")
            return {
                "monitor": monitor,
                "files_processed": 0,
                "events_imported": 0,
                "duplicates_skipped": 0,
                "invalid_events": 0,
                "duration_seconds": 0.0,
                "events_per_minute": 0.0,
                "files_with_errors": 0,
                "errors": []
            }

        # Find all complete journal files (exclude .part files)
        journal_files = []
        for file_path in monitor_dir.glob("*.ndjson.gz"):
            if not file_path.name.endswith(".part"):
                journal_files.append(file_path)

        journal_files.sort()  # Process in chronological order
        
        stats = {
            "monitor": monitor,
            "files_processed": 0,
            "events_imported": 0,
            "duplicates_skipped": 0,
            "invalid_events": 0,
            "duration_seconds": 0.0,
            "events_per_minute": 0.0,
            "files_with_errors": 0,
            "errors": []
        }

        for file_path in journal_files:
            try:
                file_stats = self._import_journal_file(file_path, batch_size, db)
                stats["events_imported"] += file_stats["events_imported"]
                stats["duplicates_skipped"] += file_stats["duplicates_skipped"]
                stats["invalid_events"] += file_stats["invalid_events"]
                stats["files_processed"] += 1
                
                # Move to done directory on success
                done_monitor_dir = self.done_dir / monitor
                done_monitor_dir.mkdir(parents=True, exist_ok=True)
                
                done_path = done_monitor_dir / file_path.name
                
                # Handle duplicate filenames by adding timestamp suffix
                counter = 1
                while done_path.exists():
                    name_parts = file_path.name.split('.')
                    name_parts[0] += f"-{counter}"
                    done_path = done_monitor_dir / ".".join(name_parts)
                    counter += 1
                
                file_path.rename(done_path)
                logger.info(f"Imported and moved: {file_path} -> {done_path}")
                
            except Exception as e:
                stats["files_with_errors"] += 1
                error_msg = f"Failed to import {file_path}: {e}"
                stats["errors"].append(error_msg)
                logger.error(error_msg)
                
                # Write error sidecar file only if it doesn't exist
                error_file_path = file_path.with_suffix(file_path.suffix + '.error')
                if not error_file_path.exists():
                    self._write_error_sidecar(file_path, str(e))

        # Calculate performance metrics
        duration = time.time() - start_time
        stats["duration_seconds"] = duration
        
        if duration > 0 and stats["events_imported"] > 0:
            stats["events_per_minute"] = (stats["events_imported"] / duration) * 60.0
        
        return stats

    def flush_all_monitors(self, batch_size: int = 1000, db=None) -> Dict[str, Any]:
        """Flush journal files for all monitors.
        
        Args:
            batch_size: Number of events to insert per batch
            db: Optional database connection to reuse
            
        Returns:
            Dict with overall import statistics
        """
        start_time = time.time()
        overall_stats = {
            "total_files_processed": 0,
            "total_events_imported": 0,
            "total_duplicates_skipped": 0,
            "total_invalid_events": 0,
            "total_duration_seconds": 0.0,
            "overall_events_per_minute": 0.0,
            "total_files_with_errors": 0,
            "monitor_stats": {}
        }

        # Process each monitor directory
        for monitor_dir in self.spool_dir.iterdir():
            if monitor_dir.is_dir() and not monitor_dir.name.startswith("_"):
                monitor = monitor_dir.name
                monitor_stats = self.flush_monitor(monitor, batch_size, db)
                
                overall_stats["monitor_stats"][monitor] = monitor_stats
                overall_stats["total_files_processed"] += monitor_stats["files_processed"]
                overall_stats["total_events_imported"] += monitor_stats["events_imported"]
                overall_stats["total_duplicates_skipped"] += monitor_stats["duplicates_skipped"]
                overall_stats["total_invalid_events"] += monitor_stats["invalid_events"]
                overall_stats["total_files_with_errors"] += monitor_stats["files_with_errors"]

        # Calculate overall performance metrics
        duration = time.time() - start_time
        overall_stats["total_duration_seconds"] = duration
        
        if duration > 0 and overall_stats["total_events_imported"] > 0:
            overall_stats["overall_events_per_minute"] = (overall_stats["total_events_imported"] / duration) * 60.0

        return overall_stats

    def _import_journal_file(self, file_path: Path, batch_size: int, db=None) -> Dict[str, int]:
        """Import a single journal file.
        
        Args:
            file_path: Path to journal file
            batch_size: Number of events to insert per batch
            db: Optional database connection to reuse
            
        Returns:
            Dict with file import statistics
            
        Raises:
            ImportError: If import fails
        """
        stats = {
            "events_imported": 0,
            "duplicates_skipped": 0,
            "invalid_events": 0
        }
        
        db_passed_in = db is not None
        if db is None:
            db = get_database()
        
        try:
            # Read and validate events in batches
            event_batch = []
            line_number = 0
            
            for line_number, event_data in enumerate(self._read_journal_lines(file_path), 1):
                try:
                    # Validate event structure
                    self._validate_event(event_data, line_number)
                    event_batch.append(event_data)
                    
                    # Insert batch when full
                    if len(event_batch) >= batch_size:
                        batch_stats = self._insert_event_batch(db, event_batch)
                        stats["events_imported"] += batch_stats["events_inserted"]
                        stats["duplicates_skipped"] += batch_stats["duplicates_skipped"]
                        event_batch = []
                        
                except Exception as e:
                    stats["invalid_events"] += 1
                    error_msg = f"Invalid event at line {line_number}: {e}"
                    self._write_error_sidecar(file_path, error_msg, line_number)
                    raise ImportError(error_msg)
            
            # Insert remaining events
            if event_batch:
                batch_stats = self._insert_event_batch(db, event_batch)
                stats["events_imported"] += batch_stats["events_inserted"]
                stats["duplicates_skipped"] += batch_stats["duplicates_skipped"]
                
        except Exception as e:
            if not isinstance(e, ImportError):
                raise ImportError(f"Failed to read journal file: {e}")
            raise
            
        finally:
            # Only close the database if we created it ourselves
            if not db_passed_in:
                db.close()

        return stats

    def _read_journal_lines(self, file_path: Path) -> Generator[Dict[str, Any], None, None]:
        """Read and parse lines from a journal file.
        
        Args:
            file_path: Path to journal file
            
        Yields:
            Parsed event data dicts
        """
        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        if HAS_ORJSON:
                            event_data = orjson.loads(line)
                        else:
                            event_data = json.loads(line)
                        yield event_data
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.warning(f"Failed to parse line in {file_path}: {e}")
                        continue
                        
        except Exception as e:
            raise ImportError(f"Failed to read journal file {file_path}: {e}")

    def _validate_event(self, event_data: Dict[str, Any], line_number: int) -> None:
        """Validate event data structure.
        
        Args:
            event_data: Event data dict
            line_number: Line number for error reporting
            
        Raises:
            ImportError: If validation fails
        """
        required_fields = ['id', 'ts_utc', 'monitor', 'action', 'subject_type', 'session_id']
        
        for field in required_fields:
            if field not in event_data:
                raise ImportError(f"Missing required field '{field}' at line {line_number}")
        
        # Validate field types
        if not isinstance(event_data['id'], str):
            raise ImportError(f"Field 'id' must be string at line {line_number}")
        
        if not isinstance(event_data['ts_utc'], int):
            raise ImportError(f"Field 'ts_utc' must be integer at line {line_number}")
        
        # Validate monitor value
        valid_monitors = ['active_window', 'context_snapshot', 'keyboard', 'mouse', 'browser', 'file', 'heartbeat']
        if event_data['monitor'] not in valid_monitors:
            raise ImportError(f"Invalid monitor '{event_data['monitor']}' at line {line_number}")
        
        # Validate subject_type value
        valid_subject_types = ['app', 'window', 'file', 'url', 'none']
        if event_data['subject_type'] not in valid_subject_types:
            raise ImportError(f"Invalid subject_type '{event_data['subject_type']}' at line {line_number}")

    def _insert_event_batch(self, db, event_batch: List[Dict[str, Any]]) -> Dict[str, int]:
        """Insert a batch of events with idempotency and duplicate tracking.
        
        Args:
            db: Database instance
            event_batch: List of event data dicts
            
        Returns:
            Dict with insertion statistics
        """
        if not event_batch:
            return {"events_inserted": 0, "duplicates_skipped": 0}
        
        conn = db._get_connection()
        
        try:
            # Use executemany with INSERT OR IGNORE for better performance
            prev_total_changes = conn.total_changes
            
            # Prepare batch data
            batch_data = []
            for event_data in event_batch:
                batch_data.append((
                    event_data.get('id'),
                    event_data.get('ts_utc'),
                    event_data.get('monitor'),
                    event_data.get('action'),
                    event_data.get('subject_type'),
                    event_data.get('subject_id'),
                    event_data.get('session_id'),
                    event_data.get('batch_id'),
                    event_data.get('pid'),
                    event_data.get('exe_name'),
                    event_data.get('exe_path_hash'),
                    event_data.get('window_title_hash'),
                    event_data.get('url_hash'),
                    event_data.get('file_path_hash'),
                    event_data.get('attrs_json')
                ))
            
            # Execute batch insert
            conn.executemany("""
                INSERT OR IGNORE INTO events (
                    id, ts_utc, monitor, action, subject_type, subject_id,
                    session_id, batch_id, pid, exe_name, exe_path_hash,
                    window_title_hash, url_hash, file_path_hash, attrs_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch_data)
            
            conn.commit()
            
            # Calculate actual insertions vs duplicates
            events_inserted = conn.total_changes - prev_total_changes
            duplicates_skipped = len(event_batch) - events_inserted
            
            logger.debug(f"Batch processed: {events_inserted} inserted, {duplicates_skipped} duplicates skipped")
            
            return {
                "events_inserted": events_inserted,
                "duplicates_skipped": duplicates_skipped
            }
            
        except sqlite3.OperationalError as e:
            # Handle database lock/unavailability
            conn.rollback()
            if "database is locked" in str(e).lower():
                logger.warning(f"Database locked during batch insert: {e}")
                # Return zero counts to allow processing to continue with other files
                return {"events_inserted": 0, "duplicates_skipped": 0}
            else:
                raise ImportError(f"Database error during batch insert: {e}")
        except Exception as e:
            conn.rollback()
            raise ImportError(f"Batch insert failed: {e}")

    def _write_error_sidecar(self, file_path: Path, error_msg: str, line_number: Optional[int] = None) -> None:
        """Write error sidecar file.
        
        Args:
            file_path: Path to journal file that failed
            error_msg: Error message
            line_number: Optional line number where error occurred
        """
        error_file_path = file_path.with_suffix(file_path.suffix + '.error')
        
        error_info = {
            "error_message": error_msg,
            "timestamp": int(time.time() * 1000),
            "file_path": str(file_path)
        }
        
        if line_number is not None:
            error_info["first_failing_line"] = line_number
        
        try:
            with open(error_file_path, 'w', encoding='utf-8') as f:
                json.dump(error_info, f, indent=2)
            logger.info(f"Wrote error sidecar: {error_file_path}")
        except Exception as e:
            logger.error(f"Failed to write error sidecar {error_file_path}: {e}")


def get_importer() -> JournalImporter:
    """Get journal importer instance."""
    return JournalImporter()