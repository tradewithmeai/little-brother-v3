"""Spool quota management with backpressure and auto-recovery."""

import logging
import threading
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional

from .config import get_effective_config
from .logging_setup import get_logger, log_once

logger = get_logger("spool_quota")


class QuotaState(Enum):
    """Spool quota states."""

    NORMAL = "normal"
    SOFT = "soft"
    HARD = "hard"


@dataclass
class QuotaUsage:
    """Spool quota usage information."""

    used_bytes: int
    quota_bytes: int
    soft_bytes: int
    hard_bytes: int
    state: QuotaState
    dropped_batches: int = 0


class SpoolQuotaManager:
    """Manages spool directory quota with backpressure and auto-recovery."""

    def __init__(self, spool_dir: Optional[Path] = None):
        """Initialize quota manager."""
        self.config = get_effective_config()
        self.spool_dir = Path(spool_dir or self.config.storage.spool_dir)

        # Quota settings
        self.quota_mb = self.config.storage.spool_quota_mb
        self.soft_pct = self.config.storage.spool_soft_pct
        self.hard_pct = self.config.storage.spool_hard_pct
        self.log_interval_s = self.config.logging.quota_log_interval_s

        # Computed thresholds
        self.quota_bytes = self.quota_mb * 1024 * 1024
        self.soft_bytes = int(self.quota_bytes * self.soft_pct / 100)
        self.hard_bytes = int(self.quota_bytes * self.hard_pct / 100)

        # State tracking
        self._cached_usage: Optional[QuotaUsage] = None
        self._last_scan_time = 0
        self._scan_interval = 30  # 30 second cache
        self._dropped_batches = 0
        self._lock = threading.RLock()

        logger.debug(
            f"Spool quota initialized: {self.quota_mb}MB, soft={self.soft_pct}%, hard={self.hard_pct}%"
        )

    def get_spool_usage(self) -> QuotaUsage:
        """Get current spool usage with caching."""
        with self._lock:
            current_time = time.time()

            # Use cached result if recent
            if (
                self._cached_usage is not None
                and current_time - self._last_scan_time < self._scan_interval
            ):
                return self._cached_usage

            # Scan directory for actual usage
            used_bytes = self._scan_spool_usage()
            state = self._compute_state(used_bytes)

            self._cached_usage = QuotaUsage(
                used_bytes=used_bytes,
                quota_bytes=self.quota_bytes,
                soft_bytes=self.soft_bytes,
                hard_bytes=self.hard_bytes,
                state=state,
                dropped_batches=self._dropped_batches,
            )
            self._last_scan_time = current_time

            return self._cached_usage

    def _scan_spool_usage(self) -> int:
        """Scan spool directory and return total bytes used."""
        total_bytes = 0

        if not self.spool_dir.exists():
            return 0

        try:
            # Count only *.ndjson.gz files (including in _done)
            # Exclude *.part and *.error from quota accounting
            for file_path in self.spool_dir.rglob("*.ndjson.gz"):
                try:
                    # Skip .part and .error files
                    if file_path.name.endswith((".part", ".error")):
                        continue
                    total_bytes += file_path.stat().st_size
                except OSError:
                    # Skip files that can't be stat'd (deleted, permissions, etc)
                    continue

        except Exception as e:
            logger.warning(f"Error scanning spool directory: {e}")

        return total_bytes

    def _compute_state(self, used_bytes: int) -> QuotaState:
        """Compute quota state based on usage."""
        if used_bytes >= self.hard_bytes:
            return QuotaState.HARD
        elif used_bytes >= self.soft_bytes:
            return QuotaState.SOFT
        else:
            return QuotaState.NORMAL

    def update_usage_on_file_op(self, file_size_delta: int):
        """Update cached usage when files are added/removed."""
        with self._lock:
            if self._cached_usage is not None:
                new_used = max(0, self._cached_usage.used_bytes + file_size_delta)
                new_state = self._compute_state(new_used)

                # Update cached usage
                self._cached_usage = QuotaUsage(
                    used_bytes=new_used,
                    quota_bytes=self.quota_bytes,
                    soft_bytes=self.soft_bytes,
                    hard_bytes=self.hard_bytes,
                    state=new_state,
                    dropped_batches=self._dropped_batches,
                )

    def check_backpressure(self) -> tuple[bool, Optional[float]]:
        """Check if backpressure should be applied.

        Returns:
            (should_apply_pressure, delay_seconds)
        """
        usage = self.get_spool_usage()

        if usage.state == QuotaState.HARD:
            # Hard backpressure - pause writes
            log_key = f"hard_backpressure_{int(time.time() // self.log_interval_s)}"
            log_once(
                logger,
                logging.WARNING,
                f"Hard spool quota exceeded ({usage.used_bytes // (1024*1024)}MB/{self.quota_mb}MB). "
                "Pausing writes, buffering in memory.",
                key=log_key,
            )
            self._was_in_backpressure = True
            return True, None

        elif usage.state == QuotaState.SOFT:
            # Soft backpressure - delay flushes
            log_key = f"soft_backpressure_{int(time.time() // self.log_interval_s)}"
            log_once(
                logger,
                logging.INFO,
                f"Soft spool quota reached ({usage.used_bytes // (1024*1024)}MB/{self.quota_mb}MB). "
                "Applying flush delays.",
                key=log_key,
            )
            self._was_in_backpressure = True
            return True, 0.3  # 300ms delay for soft backpressure

        return False, None

    def can_write_batch(self, estimated_size: int = 0) -> bool:
        """Check if a batch can be written to disk."""
        usage = self.get_spool_usage()

        # Always allow writes in normal and soft states
        if usage.state != QuotaState.HARD:
            return True

        # In hard state, check if this write would push us significantly over
        return not (
            estimated_size > 0
            and (usage.used_bytes + estimated_size) > (self.hard_bytes * 1.1)
        )

    def increment_dropped_batches(self, count: int = 1):
        """Increment dropped batch counter."""
        with self._lock:
            self._dropped_batches += count
            if self._cached_usage is not None:
                self._cached_usage.dropped_batches = self._dropped_batches

    def check_recovery(self) -> bool:
        """Check if quota state has recovered and log if so."""
        usage = self.get_spool_usage()

        # Check for recovery from backpressure
        if usage.state == QuotaState.NORMAL:
            # Check if we were previously in backpressure
            if hasattr(self, "_was_in_backpressure") and self._was_in_backpressure:
                logger.info("Spool backpressure cleared - resuming normal operation")
                self._was_in_backpressure = False
                return True
        else:
            # Mark that we're in backpressure
            self._was_in_backpressure = True

        return False

    def get_largest_done_files(self, limit: int = 5) -> list[tuple[str, str, int]]:
        """Get largest files in _done directories for diagnostics.

        Returns list of (monitor_name, filename, size_bytes) tuples.
        No plaintext paths returned beyond monitor dir + filename.
        """
        files = []
        done_dir = self.spool_dir / "_done"

        if not done_dir.exists():
            return files

        try:
            for monitor_dir in done_dir.iterdir():
                if not monitor_dir.is_dir():
                    continue

                monitor_name = monitor_dir.name

                for file_path in monitor_dir.glob("*.ndjson.gz"):
                    try:
                        size = file_path.stat().st_size
                        filename = file_path.name
                        files.append((monitor_name, filename, size))
                    except OSError:
                        continue

        except Exception as e:
            logger.debug(f"Error scanning done files: {e}")

        # Sort by size descending and return top N
        files.sort(key=lambda x: x[2], reverse=True)
        return files[:limit]


# Global quota manager instance
_quota_manager: Optional[SpoolQuotaManager] = None
_quota_lock = threading.Lock()


def get_quota_manager() -> SpoolQuotaManager:
    """Get global quota manager instance."""
    global _quota_manager

    if _quota_manager is None:
        with _quota_lock:
            if _quota_manager is None:
                _quota_manager = SpoolQuotaManager()

    return _quota_manager


def reset_quota_manager():
    """Reset global quota manager (for testing)."""
    global _quota_manager
    with _quota_lock:
        _quota_manager = None
