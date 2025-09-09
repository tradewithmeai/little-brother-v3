"""Spool directory management for Little Brother v3."""

import json
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import orjson

from .config import SpoolConfig
from .ids import generate_id


@dataclass
class SpoolEntry:
    """Spool file entry."""

    id: str
    timestamp: float
    monitor: str
    data: Dict[str, Any]

    @classmethod
    def create(cls, monitor: str, data: Dict[str, Any]) -> "SpoolEntry":
        """Create a new spool entry."""
        return cls(
            id=generate_id(),
            timestamp=datetime.now(timezone.utc).timestamp(),
            monitor=monitor,
            data=data,
        )


class SpoolManager:
    """Manages spool directories for monitors."""

    def __init__(self, config: SpoolConfig) -> None:
        self.config = config
        self.base_path = Path(config.base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def get_monitor_spool_dir(self, monitor_name: str) -> Path:
        """Get spool directory for a monitor."""
        spool_dir = self.base_path / monitor_name
        spool_dir.mkdir(exist_ok=True)
        return spool_dir

    def write_entry(self, entry: SpoolEntry) -> Path:
        """Write an entry to the spool."""
        spool_dir = self.get_monitor_spool_dir(entry.monitor)

        # Generate filename with timestamp and ID
        timestamp_str = datetime.fromtimestamp(entry.timestamp, timezone.utc).strftime(
            "%Y%m%d_%H%M%S"
        )
        filename = f"{timestamp_str}_{entry.id}.json"
        file_path = spool_dir / filename

        # Write entry as JSON
        try:
            # Use orjson for better performance
            data = orjson.dumps(asdict(entry), option=orjson.OPT_UTC_Z)
            file_path.write_bytes(data)
        except (ImportError, AttributeError):
            # Fallback to standard json
            with file_path.open("w", encoding="utf-8") as f:
                json.dump(
                    asdict(entry),
                    f,
                    ensure_ascii=False,
                    indent=None,
                    separators=(",", ":"),
                )

        return file_path

    def read_entry(self, file_path: Path) -> Optional[SpoolEntry]:
        """Read an entry from a spool file."""
        if not file_path.exists():
            return None

        try:
            # Try orjson first
            try:
                data = orjson.loads(file_path.read_bytes())
            except (ImportError, AttributeError):
                # Fallback to standard json
                with file_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)

            return SpoolEntry(**data)
        except Exception:
            return None

    def list_entries(self, monitor_name: str) -> Iterator[Path]:
        """List all spool entries for a monitor."""
        spool_dir = self.get_monitor_spool_dir(monitor_name)

        # Return files sorted by name (which includes timestamp)
        json_files = sorted(spool_dir.glob("*.json"))
        return iter(json_files)

    def cleanup_old_files(
        self, monitor_name: str, max_files: Optional[int] = None
    ) -> int:
        """Clean up old spool files for a monitor."""
        if max_files is None:
            max_files = self.config.max_files_per_monitor

        files = list(self.list_entries(monitor_name))

        if len(files) <= max_files:
            return 0

        # Remove oldest files
        files_to_remove = files[:-max_files]
        removed_count = 0

        for file_path in files_to_remove:
            try:
                file_path.unlink()
                removed_count += 1
            except OSError:
                pass

        return removed_count

    def get_file_count(self, monitor_name: str) -> int:
        """Get count of spool files for a monitor."""
        spool_dir = self.get_monitor_spool_dir(monitor_name)
        return len(list(spool_dir.glob("*.json")))
