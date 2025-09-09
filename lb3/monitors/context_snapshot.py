"""Context snapshot monitor for Little Brother v3."""

import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional

from ..config import get_effective_config
from ..events import Event, get_event_bus
from ..logging_setup import get_logger
from .base import MonitorBase

logger = get_logger("context_snapshot")


@dataclass
class ActivityCounters:
    """Rolling activity counters since last snapshot."""
    kb_down: int = 0
    kb_up: int = 0
    mouse_moves: int = 0
    mouse_clicks: int = 0
    mouse_scroll: int = 0
    
    def reset(self) -> None:
        """Reset all counters to zero."""
        self.kb_down = 0
        self.kb_up = 0
        self.mouse_moves = 0
        self.mouse_clicks = 0
        self.mouse_scroll = 0
    
    def to_dict(self) -> Dict[str, int]:
        """Convert to dictionary for attrs_json."""
        return {
            "kb_down": self.kb_down,
            "kb_up": self.kb_up,
            "mouse_moves": self.mouse_moves,
            "mouse_clicks": self.mouse_clicks,
            "mouse_scroll": self.mouse_scroll
        }


class ContextSnapshotMonitor(MonitorBase):
    """Monitor that emits context snapshots on foreground changes and idle gaps."""
    
    def __init__(self, dry_run: bool = False):
        """Initialize context snapshot monitor."""
        super().__init__(dry_run)
        
        # Parse context_idle_gap from config
        config = get_effective_config()
        idle_gap_str = config.heartbeat.poll_intervals.context_idle_gap
        self._idle_gap_s = self._parse_time_string(idle_gap_str)
        
        # Activity tracking
        self._counters = ActivityCounters()
        self._counters_lock = threading.Lock()
        
        # Event tracking for gap detection
        self._last_event_time = time.time()
        self._last_event_monitor: Optional[str] = None
        self._last_snapshot_time = 0.0
        self._gap_window_start = 0.0  # For deduplication
        
        # Event bus subscription
        self._event_bus = get_event_bus()
        self._subscribed = False
    
    @property
    def name(self) -> str:
        """Monitor name."""
        return "context_snapshot"
    
    @property
    def poll_interval_s(self) -> float:
        """Poll interval for checking idle gaps."""
        # Poll more frequently than the idle gap to ensure timely detection
        return min(1.0, self._idle_gap_s / 7.0)
    
    def _parse_time_string(self, time_str: str) -> float:
        """Parse time string like '7.0s' to seconds."""
        try:
            if time_str and time_str.endswith('s'):
                return float(time_str[:-1])
            return float(time_str)
        except (ValueError, TypeError, AttributeError):
            logger.warning(f"Failed to parse time string '{time_str}', using 7.0s")
            return 7.0
    
    def start_monitoring(self) -> None:
        """Start context snapshot monitoring."""
        try:
            # Subscribe to event bus to observe other monitors' events
            self._event_bus.subscribe(self._handle_event)
            self._subscribed = True
            
            # Initialize timestamps
            current_time = time.time()
            self._last_event_time = current_time
            self._last_snapshot_time = current_time
            self._gap_window_start = current_time
            
            # Start event bus if not already running
            self._event_bus.start()
            
            logger.info(f"Context snapshot monitoring started (idle gap: {self._idle_gap_s}s)")
            
        except Exception as e:
            logger.error(f"Failed to start context snapshot monitoring: {e}")
            raise
    
    def stop_monitoring(self) -> None:
        """Stop context snapshot monitoring."""
        try:
            if self._subscribed:
                self._event_bus.unsubscribe(self._handle_event)
                self._subscribed = False
            
            logger.info("Context snapshot monitoring stopped")
            
        except Exception as e:
            logger.warning(f"Error stopping context snapshot monitoring: {e}")
    
    def _run_loop(self) -> None:
        """Override run loop to handle gap detection."""
        try:
            self.start_monitoring()
            
            while not self.should_stop():
                # Check for idle gap
                self._check_idle_gap()
                
                # Wait for next poll or stop signal
                if self.wait_or_stop(self.poll_interval_s):
                    break
                    
        except Exception as e:
            logger.error(f"Error in context snapshot monitor: {e}", exc_info=True)
        finally:
            try:
                self.stop_monitoring()
            except Exception as e:
                logger.error(f"Error stopping context snapshot monitor: {e}", exc_info=True)
    
    def _handle_event(self, event: Event) -> None:
        """Handle events from other monitors."""
        try:
            current_time = time.time()
            
            # Ignore heartbeat events for idle gap detection (they are dummy events)
            if event.monitor == "heartbeat":
                return
            
            # Update last event tracking for non-heartbeat events
            self._last_event_time = current_time
            self._last_event_monitor = event.monitor
            
            # Count activity events for rolling counters
            self._update_activity_counters(event)
            
            # Check for foreground change trigger
            if event.monitor == "active_window" and event.action == "window_change":
                # Set last_event_monitor to active_window when triggered by foreground change
                self._last_event_monitor = "active_window"
                self._emit_snapshot(trigger="foreground_change")
            
        except Exception as e:
            logger.error(f"Error handling event: {e}")
    
    def _update_activity_counters(self, event: Event) -> None:
        """Update activity counters based on event type."""
        with self._counters_lock:
            if event.monitor == "keyboard":
                if event.action in ["keydown", "key_press"]:
                    self._counters.kb_down += 1
                elif event.action in ["keyup", "key_release"]:
                    self._counters.kb_up += 1
            
            elif event.monitor == "mouse":
                if event.action == "move":
                    self._counters.mouse_moves += 1
                elif event.action in ["click", "left_click", "right_click", "middle_click"]:
                    self._counters.mouse_clicks += 1
                elif event.action in ["scroll", "wheel"]:
                    self._counters.mouse_scroll += 1
    
    def _check_idle_gap(self) -> None:
        """Check if we should emit a snapshot due to idle gap."""
        try:
            current_time = time.time()
            time_since_last_event = current_time - self._last_event_time
            
            # Only emit if we've exceeded the idle gap and haven't emitted for this gap window
            if (time_since_last_event >= self._idle_gap_s and 
                current_time > self._gap_window_start + self._idle_gap_s):
                
                self._emit_snapshot(trigger="idle_gap")
                # Set new gap window to prevent spam
                self._gap_window_start = current_time
                
        except Exception as e:
            logger.error(f"Error checking idle gap: {e}")
    
    def _emit_snapshot(self, trigger: str) -> None:
        """Emit a context snapshot event."""
        try:
            current_time = time.time()
            current_time_ms = int(current_time * 1000)
            
            # Get current counters and calculate time since last snapshot
            with self._counters_lock:
                counter_data = self._counters.to_dict()
                # Reset counters after capturing
                self._counters.reset()
            
            # Calculate time since last snapshot
            since_ms = int((current_time - self._last_snapshot_time) * 1000)
            
            # Build attrs_json
            attrs = {
                **counter_data,
                "since_ms": since_ms,
                "last_event_monitor": self._last_event_monitor
            }
            
            # Create snapshot event
            event_data = {
                'action': 'snapshot',
                'subject_type': 'none',
                'subject_id': None,
                'attrs': attrs
            }
            
            # Emit the event
            self.emit(event_data)
            
            # Update last snapshot time
            self._last_snapshot_time = current_time
            
            logger.debug(f"Emitted context snapshot (trigger: {trigger}, counters: {counter_data})")
            
        except Exception as e:
            logger.error(f"Error emitting snapshot: {e}")