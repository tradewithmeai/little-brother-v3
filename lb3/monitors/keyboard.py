"""Keyboard dynamics monitor for Little Brother v3."""

import json
import math
import os
import statistics
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Protocol

from ..config import get_effective_config
from ..logging_setup import get_logger
from ..utils.scheduler import Scheduler
from .base import BatchConfig, MonitorBase

logger = get_logger("keyboard")


class GuardrailViolationError(Exception):
    """Raised when code attempts to access plaintext key data."""
    pass


class KeyboardEventSource(Protocol):
    """Protocol for keyboard event sources."""
    
    def start(self, on_press: Callable, on_release: Callable) -> None:
        """Start capturing keyboard events."""
        ...
    
    def stop(self) -> None:
        """Stop capturing keyboard events."""
        ...


@dataclass 
class KeyEvent:
    """Minimal key event data for timing analysis only."""
    timestamp: float
    event_type: str  # "keydown" or "keyup"
    
    # GUARDRAIL: Explicitly no key data fields
    # Any attempt to add key_char, key_code, or similar fields
    # should be blocked at code review level


@dataclass
class KeyboardStats:
    """Keyboard dynamics statistics."""
    keydown_count: int = 0
    keyup_count: int = 0
    intervals: List[float] = field(default_factory=list)
    burst_count: int = 0
    
    def reset(self) -> None:
        """Reset all stats for next batch."""
        self.keydown_count = 0
        self.keyup_count = 0
        self.intervals.clear()
        self.burst_count = 0
    
    def to_attrs_dict(self) -> Dict[str, Any]:
        """Convert to attrs_json dictionary with exact schema."""
        if len(self.intervals) == 0:
            # Handle empty intervals case with safe defaults
            return {
                "keydown": int(self.keydown_count),
                "keyup": int(self.keyup_count),
                "mean_ms": 0.0,
                "p95_ms": 0.0,
                "stdev_ms": 0.0,
                "bursts": int(self.burst_count)
            }
        
        # Calculate statistics with NaN/inf protection
        clean_intervals = [x for x in self.intervals if not (math.isnan(x) or math.isinf(x))]
        
        try:
            if len(clean_intervals) > 0:
                mean_ms = statistics.mean(clean_intervals)
                if math.isnan(mean_ms) or math.isinf(mean_ms):
                    mean_ms = 0.0
            else:
                mean_ms = 0.0
        except statistics.StatisticsError:
            mean_ms = 0.0
        
        try:
            if len(clean_intervals) > 0:
                p95_ms = self._percentile(clean_intervals, 95)
                if math.isnan(p95_ms) or math.isinf(p95_ms):
                    p95_ms = 0.0
            else:
                p95_ms = 0.0
        except:
            p95_ms = 0.0
        
        try:
            if len(clean_intervals) > 1:
                stdev_ms = statistics.stdev(clean_intervals)
                if math.isnan(stdev_ms) or math.isinf(stdev_ms):
                    stdev_ms = 0.0
            else:
                stdev_ms = 0.0
        except (statistics.StatisticsError, AttributeError):
            stdev_ms = 0.0
        
        # Ensure exact schema with proper types
        return {
            "keydown": int(self.keydown_count),
            "keyup": int(self.keyup_count),
            "mean_ms": float(mean_ms),
            "p95_ms": float(p95_ms),
            "stdev_ms": float(stdev_ms),
            "bursts": int(self.burst_count)
        }
    
    def _percentile(self, data: List[float], percentile: float) -> float:
        """Calculate percentile of data."""
        if not data:
            return 0.0
        
        sorted_data = sorted(data)
        k = (len(sorted_data) - 1) * percentile / 100.0
        f = int(k)
        c = k - f
        
        if f + 1 < len(sorted_data):
            return sorted_data[f] * (1 - c) + sorted_data[f + 1] * c
        else:
            return sorted_data[f]


class PynputKeyboardSource:
    """Real pynput keyboard event source."""
    
    def __init__(self):
        self._listener = None
        self._keyboard = None
        try:
            import pynput.keyboard as keyboard
            self._keyboard = keyboard
            logger.info("pynput keyboard module loaded successfully")
        except ImportError as e:
            logger.warning(f"pynput not available: {e}")
    
    def start(self, on_press: Callable, on_release: Callable) -> None:
        """Start keyboard listener."""
        if not self._keyboard:
            raise RuntimeError("pynput keyboard not available")
        
        self._listener = self._keyboard.Listener(
            on_press=on_press,
            on_release=on_release
        )
        self._listener.start()
    
    def stop(self) -> None:
        """Stop keyboard listener."""
        if self._listener:
            self._listener.stop()
            self._listener = None


class FakeKeyboardSource:
    """Fake keyboard source for testing with proper lifecycle management."""
    
    def __init__(self, mode: str = "standard"):
        """Initialize fake keyboard source.
        
        Args:
            mode: "standard" or "inline" - inline mode prevents any threading
        """
        self._mode = mode
        self._on_press = None
        self._on_release = None
        self._running = False
        self._lock = threading.Lock()
    
    def start(self, on_press: Callable, on_release: Callable) -> None:
        """Start fake keyboard source (idempotent)."""
        if self._mode == "inline":
            # In inline mode, start is a no-op
            self._on_press = on_press
            self._on_release = on_release
            self._running = True
            return
        
        with self._lock:
            if self._running:
                # Already running, just update callbacks
                self._on_press = on_press
                self._on_release = on_release
                return
            
            self._on_press = on_press
            self._on_release = on_release
            self._running = True
    
    def stop(self) -> None:
        """Stop fake keyboard source (idempotent)."""
        if self._mode == "inline":
            # In inline mode, stop is a no-op
            self._running = False
            return
        
        with self._lock:
            self._running = False
            self._on_press = None
            self._on_release = None
    
    def join(self, timeout: float = 2.0) -> bool:
        """Wait for source to finish (always returns True for fake source)."""
        return True
    
    def is_running(self) -> bool:
        """Check if source is running."""
        with self._lock:
            return self._running
    
    def simulate_keydown(self, mock_key=None) -> None:
        """Simulate keydown event."""
        if self._mode == "inline":
            if self._running and self._on_press:
                self._on_press(mock_key or object())
            return
        
        with self._lock:
            if self._running and self._on_press:
                # Use mock key (guardrailed - never accessed)
                self._on_press(mock_key or object())
    
    def simulate_keyup(self, mock_key=None) -> None:
        """Simulate keyup event.""" 
        if self._mode == "inline":
            if self._running and self._on_release:
                self._on_release(mock_key or object())
            return
        
        with self._lock:
            if self._running and self._on_release:
                # Use mock key (guardrailed - never accessed)
                self._on_release(mock_key or object())
    
    # Inline mode methods
    def emit_down(self, mock_key=None) -> None:
        """Emit keydown event synchronously (inline mode)."""
        if self._on_press:
            self._on_press(mock_key or object())
    
    def emit_up(self, mock_key=None) -> None:
        """Emit keyup event synchronously (inline mode)."""
        if self._on_release:
            self._on_release(mock_key or object())
    
    def emit_sequence(self, deltas_ms: List[float], scheduler=None) -> None:
        """Emit a sequence of keydown events with timing deltas.
        
        Args:
            deltas_ms: List of delays in milliseconds between keydowns
            scheduler: ManualScheduler to advance time with
        """
        for i, delta_ms in enumerate(deltas_ms):
            if scheduler:
                scheduler.advance(delta_ms / 1000.0)
            self.emit_down()
            if i % 2 == 0:  # Alternate with keyups for realistic pattern
                self.emit_up()


class KeyboardMonitor(MonitorBase):
    """Monitor keyboard dynamics without capturing plaintext keys."""
    
    def __init__(self, 
                 dry_run: bool = False,
                 clock: Optional[Callable[[], float]] = None,
                 batch_config: Optional[BatchConfig] = None,
                 event_source: Optional[KeyboardEventSource] = None,
                 scheduler: Optional[Scheduler] = None,
                 event_source_mode: str = "standard"):
        """Initialize keyboard dynamics monitor.
        
        Args:
            dry_run: Print events instead of emitting
            clock: Time source (defaults to scheduler.now())
            batch_config: Override batch configuration
            event_source: Keyboard event source (auto-selected based on test mode)
            scheduler: Scheduler for deterministic timing
            event_source_mode: "standard" or "inline" for zero-thread testing
        """
        super().__init__(dry_run, scheduler)
        
        # Enforce guardrails first
        config = get_effective_config()
        if not config.guardrails.no_global_text_keylogging:
            raise GuardrailViolationError(
                "Keyboard monitor requires guardrails.no_global_text_keylogging=true"
            )
        
        # Dependency injection
        self._clock = clock or self.scheduler.now
        if batch_config:
            self.batch_config = batch_config
        
        # Auto-select event source based on test mode
        test_mode = os.getenv('LB3_TEST_MODE', '0') == '1'
        if event_source:
            self._event_source = event_source
        elif test_mode:
            self._event_source = FakeKeyboardSource(mode=event_source_mode)
        else:
            self._event_source = PynputKeyboardSource()
        
        # Stats tracking
        self._stats = KeyboardStats()
        self._stats_lock = threading.Lock()
        
        # Event timing tracking
        self._last_key_time: Optional[float] = None
        self._recent_events: List[float] = []  # For burst detection
        
        # Burst detection settings (heuristic: ≥5 keys within 500ms)
        self._burst_threshold_keys = 5
        self._burst_threshold_ms = 500.0
        
        # Track last stats flush time for deterministic testing
        self._last_stats_flush_time = self._clock()
    
    @property
    def name(self) -> str:
        """Monitor name."""
        return "keyboard"
    
    def start_monitoring(self) -> None:
        """Start keyboard monitoring."""
        try:
            # Start event source with guardrailed callbacks
            self._event_source.start(
                on_press=self._on_key_press,
                on_release=self._on_key_release
            )
            logger.info("Keyboard monitoring started")
            
        except Exception as e:
            logger.error(f"Failed to start keyboard monitoring: {e}")
            logger.info("Continuing without keyboard monitoring (degraded mode)")
    
    def stop_monitoring(self) -> None:
        """Stop keyboard monitoring."""
        try:
            self._event_source.stop()
            logger.info("Keyboard monitoring stopped")
        except Exception as e:
            logger.warning(f"Error stopping keyboard monitoring: {e}")
        
        # Flush any remaining stats
        self._flush_stats()
    
    def join(self, timeout: float = 2.0) -> bool:
        """Wait for event source to finish.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if source finished, False if timed out
        """
        if hasattr(self._event_source, 'join'):
            return self._event_source.join(timeout)
        return True
    
    # Inline mode convenience methods
    def emit_keydown_inline(self) -> None:
        """Emit a keydown event directly (inline mode)."""
        if hasattr(self._event_source, 'emit_down'):
            self._event_source.emit_down()
        else:
            # Fallback for non-inline sources
            self._record_key_event("keydown")
    
    def emit_keyup_inline(self) -> None:
        """Emit a keyup event directly (inline mode)."""
        if hasattr(self._event_source, 'emit_up'):
            self._event_source.emit_up()
        else:
            # Fallback for non-inline sources
            self._record_key_event("keyup")
    
    def emit_keys_inline(self, count: int) -> None:
        """Emit count keydown/keyup pairs rapidly (inline mode)."""
        for i in range(count):
            self.emit_keydown_inline()
            self.emit_keyup_inline()
    
    def _on_key_press(self, key) -> None:
        """Handle key press events - GUARDRAILED."""
        # GUARDRAIL ENFORCEMENT: Never access key.char or key content
        # Only capture timing information
        try:
            self._record_key_event("keydown")
        except Exception as e:
            logger.debug(f"Error recording key press: {e}")
    
    def _on_key_release(self, key) -> None:
        """Handle key release events - GUARDRAILED."""
        # GUARDRAIL ENFORCEMENT: Never access key.char or key content
        # Only capture timing information
        try:
            self._record_key_event("keyup")
        except Exception as e:
            logger.debug(f"Error recording key release: {e}")
    
    def _record_key_event(self, event_type: str) -> None:
        """Record key event for dynamics analysis."""
        current_time = self._clock()
        current_time_ms = current_time * 1000
        should_flush = False
        
        with self._stats_lock:
            # Update counts
            if event_type == "keydown":
                self._stats.keydown_count += 1
            elif event_type == "keyup":
                self._stats.keyup_count += 1
            
            # Calculate inter-key interval for keydown events
            if event_type == "keydown":
                if self._last_key_time is not None:
                    interval_ms = (current_time - self._last_key_time) * 1000
                    self._stats.intervals.append(interval_ms)
                
                self._last_key_time = current_time
                
                # Burst detection
                self._recent_events.append(current_time_ms)
                self._detect_bursts(current_time_ms)
            
            # Check if we need to flush stats (size-based)
            total_events = self._stats.keydown_count + self._stats.keyup_count
            if total_events >= self.batch_config.max_size:
                should_flush = True
        
        # Flush outside the lock to avoid deadlock
        if should_flush:
            self._flush_stats(force_base_flush=True)
    
    def _detect_bursts(self, current_time_ms: float) -> None:
        """Detect typing bursts based on recent key events."""
        # Remove old events outside the burst window
        cutoff_time = current_time_ms - self._burst_threshold_ms
        self._recent_events = [t for t in self._recent_events if t >= cutoff_time]
        
        # Check for burst
        if len(self._recent_events) >= self._burst_threshold_keys:
            self._stats.burst_count += 1
            # Clear recent events to avoid double-counting
            self._recent_events.clear()
    
    
    def _flush_stats(self, force_base_flush: bool = False) -> None:
        """Flush current stats as an event.
        
        Args:
            force_base_flush: If True, also flush the base batch immediately
        """
        with self._stats_lock:
            if self._stats.keydown_count == 0 and self._stats.keyup_count == 0:
                return  # Nothing to flush
            
            # Create event with stats
            attrs = self._stats.to_attrs_dict()
            
            # GUARDRAIL: Assert no plaintext data before emission
            self._assert_no_plaintext(attrs)
            
            event_data = {
                'action': 'stats',
                'subject_type': 'none',
                'subject_id': None,
                'attrs': attrs
            }
            
            # Emit the event
            self.emit(event_data)
            
            # Reset stats for next batch
            self._stats.reset()
            self._last_stats_flush_time = self._clock()
            
            logger.debug(f"Flushed keyboard stats: {attrs}")
        
        # Force base flush if requested (for size-based triggers)
        if force_base_flush:
            self.flush()
    
    def check_time_flush_inline(self) -> bool:
        """Check and perform keyboard stats flush for inline mode.
        
        This method first checks if keyboard stats should be flushed to the base batch,
        then calls the base time flush check.
        
        Returns:
            True if any flush was performed, False otherwise
        """
        stats_flushed = False
        should_flush = False
        
        # Check if we should flush keyboard stats to base batch
        with self._stats_lock:
            if self._stats.keydown_count > 0 or self._stats.keyup_count > 0:
                current_time = self._clock()
                if current_time - self._last_stats_flush_time >= self.batch_config.max_time_s:
                    should_flush = True
        
        # Flush outside the lock to avoid deadlock
        if should_flush:
            self._flush_stats()
            stats_flushed = True
        
        # Check base monitor time flush
        base_flushed = super().check_time_flush_inline()
        
        return stats_flushed or base_flushed
    
    def _assert_no_plaintext(self, payload: Dict[str, Any]) -> None:
        """Assert that payload contains no plaintext key data."""
        payload_str = json.dumps(payload).lower()
        
        # Check for suspicious patterns that might indicate plaintext leakage
        forbidden_patterns = [
            'key_char', 'char', 'vk_', 'scan_code', 'text', 'letter', 'digit',
            'password', 'username', 'secret', 'private'
        ]
        
        for pattern in forbidden_patterns:
            if pattern in payload_str:
                raise GuardrailViolationError(
                    f"Potential plaintext data detected in payload: {pattern}"
                )


# GUARDRAIL: Block any attempt to create functions that access key text
def _blocked_key_text_access(*args, **kwargs):
    """Dummy function that blocks any attempt to access key text."""
    raise GuardrailViolationError(
        "Access to key text is blocked by guardrails.no_global_text_keylogging"
    )

# Block common key text access patterns
get_key_char = _blocked_key_text_access
get_key_text = _blocked_key_text_access  
extract_key_content = _blocked_key_text_access
access_key_vk = _blocked_key_text_access
read_key_scan_code = _blocked_key_text_access