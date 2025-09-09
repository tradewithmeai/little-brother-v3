"""Base monitor classes and interfaces."""

import json
import os
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from ..config import MonitorConfig, get_effective_config
from ..event_bus import Event, EventType, event_bus
from ..events import Event as DBEvent
from ..events import publish_event
from ..ids import generate_id, generate_session_id, new_id
from ..logging_setup import get_logger
from ..utils.scheduler import Scheduler, get_scheduler


class MonitorState(Enum):
    """Monitor state enumeration."""
    
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    ERROR = "error"


@dataclass
class MonitorEvent:
    """Standard monitor event data."""
    
    monitor_name: str
    event_type: str
    timestamp: float
    data: Dict[str, Any]
    session_id: Optional[str] = None
    
    @classmethod
    def create(cls, monitor_name: str, event_type: str, data: Dict[str, Any]) -> "MonitorEvent":
        """Create a new monitor event."""
        return cls(
            monitor_name=monitor_name,
            event_type=event_type,
            timestamp=datetime.now(timezone.utc).timestamp(),
            data=data
        )


@dataclass 
class BatchConfig:
    """Configuration for monitor batching."""
    max_size: int = 100  # Max events per batch
    max_time_s: float = 5.0  # Max time to hold events before flush
    
    @classmethod
    def from_config_string(cls, config_str: str) -> 'BatchConfig':
        """Parse config from string like '128 or 1.5s'."""
        try:
            parts = config_str.lower().split(' or ')
            max_size = 100
            max_time_s = 5.0
            
            for part in parts:
                part = part.strip()
                if part.endswith('s'):
                    # Time value
                    max_time_s = float(part[:-1])
                elif part.isdigit():
                    # Count value
                    max_size = int(part)
            
            return cls(max_size=max_size, max_time_s=max_time_s)
        except Exception:
            # Return defaults on parse error
            return cls()


class BaseMonitor(ABC):
    """Original base class for all monitors."""
    
    def __init__(self, name: str, config: MonitorConfig) -> None:
        self.name = name
        self.config = config
        self.logger = get_logger(f"monitor.{name}")
        
        self._state = MonitorState.STOPPED
        self._session_id: Optional[str] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._error: Optional[Exception] = None
    
    @property
    def state(self) -> MonitorState:
        """Get current monitor state."""
        return self._state
    
    @property
    def session_id(self) -> Optional[str]:
        """Get current session ID."""
        return self._session_id
    
    @property
    def is_running(self) -> bool:
        """Check if monitor is running."""
        return self._state == MonitorState.RUNNING
    
    @property
    def last_error(self) -> Optional[Exception]:
        """Get last error if any."""
        return self._error
    
    def start(self) -> None:
        """Start the monitor."""
        if self._state != MonitorState.STOPPED:
            raise RuntimeError(f"Monitor {self.name} is not in stopped state")
        
        self._state = MonitorState.STARTING
        self._session_id = generate_session_id()
        self._stop_event.clear()
        self._error = None
        
        # Emit start event
        self._emit_event("start", {"session_id": self._session_id})
        
        # Start monitor thread
        self._thread = threading.Thread(
            target=self._run_wrapper,
            name=f"Monitor-{self.name}",
            daemon=True
        )
        self._thread.start()
        
        self.logger.info(f"Monitor {self.name} started with session {self._session_id}")
    
    def stop(self) -> None:
        """Stop the monitor."""
        if self._state == MonitorState.STOPPED:
            return
        
        self._state = MonitorState.STOPPING
        self._stop_event.set()
        
        # Wait for thread to finish
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
            
            if self._thread.is_alive():
                self.logger.warning(f"Monitor {self.name} thread did not stop gracefully")
        
        self._state = MonitorState.STOPPED
        self._emit_event("stop", {"session_id": self._session_id})
        
        self.logger.info(f"Monitor {self.name} stopped")
    
    def _run_wrapper(self) -> None:
        """Wrapper for monitor run method with error handling."""
        try:
            self._state = MonitorState.RUNNING
            self.run()
        except Exception as e:
            self._error = e
            self._state = MonitorState.ERROR
            self._emit_event("error", {
                "session_id": self._session_id,
                "error": str(e),
                "error_type": type(e).__name__
            })
            self.logger.error(f"Monitor {self.name} error: {e}", exc_info=True)
        finally:
            if self._state != MonitorState.STOPPING:
                self._state = MonitorState.STOPPED
    
    @abstractmethod
    def run(self) -> None:
        """Main monitor loop - must be implemented by subclasses."""
        pass
    
    def _emit_event(self, event_type: str, data: Dict[str, Any]) -> None:
        """Emit a monitor event."""
        event = Event(
            event_type=EventType.MONITOR_DATA,
            source=f"monitor.{self.name}",
            data={
                "monitor": self.name,
                "event_type": event_type,
                "session_id": self._session_id,
                **data
            },
            timestamp=datetime.now(timezone.utc).timestamp(),
            event_id=generate_id()
        )
        
        event_bus.emit(event)
    
    def _should_stop(self) -> bool:
        """Check if monitor should stop."""
        return self._stop_event.is_set()
    
    def _wait_or_stop(self, timeout: float) -> bool:
        """Wait for timeout or until stop signal. Returns True if should stop."""
        return self._stop_event.wait(timeout)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get monitor statistics."""
        return {
            "name": self.name,
            "state": self._state.value,
            "session_id": self._session_id,
            "config": {
                "enabled": self.config.enabled,
                "interval": self.config.interval
            },
            "last_error": str(self._error) if self._error else None
        }


class MonitorBase(ABC):
    """Abstract base class for monitors with batching and lifecycle management."""
    
    def __init__(self, dry_run: bool = False, scheduler: Optional[Scheduler] = None):
        """Initialize monitor base.
        
        Args:
            dry_run: If True, print events to console instead of writing
            scheduler: Optional scheduler for deterministic timing (defaults to production scheduler)
        """
        self.dry_run = dry_run
        self.logger = get_logger(f"monitor.{self.name}")
        
        # Scheduler for deterministic timing
        test_mode = os.getenv('LB3_TEST_MODE', '0') == '1'
        self.scheduler = scheduler or get_scheduler(test_mode=test_mode)
        
        # Get configuration
        config = get_effective_config()
        if hasattr(config, 'batch') and hasattr(config.batch, 'flush_thresholds'):
            # Try to get monitor-specific config, fallback to defaults
            config_str = getattr(config.batch.flush_thresholds, f'{self.name}_events', '100 or 5.0s')
            self.batch_config = BatchConfig.from_config_string(config_str)
        else:
            self.batch_config = BatchConfig()
        
        # Session and batching state
        self._session_id = new_id()
        self._batch: List[DBEvent] = []
        self._batch_lock = threading.Lock()
        self._last_flush_time = self.scheduler.now()
        self._flush_handle = None
        
        # Lifecycle state
        self._running = False
        self._stop_event = threading.Event()
        self._thread = None
        self._inline_mode = False
        
    @property
    @abstractmethod
    def name(self) -> str:
        """Monitor name (e.g. 'active_window', 'keyboard')."""
        pass
    
    @abstractmethod
    def start_monitoring(self) -> None:
        """Start the actual monitoring process.
        
        This is called in the monitor thread after initialization.
        Implement your monitoring logic here.
        """
        pass
    
    @abstractmethod
    def stop_monitoring(self) -> None:
        """Stop the actual monitoring process.
        
        Clean up any resources here.
        """
        pass
    
    @property
    def cadence_s(self) -> Optional[float]:
        """Optional cadence in seconds for periodic monitoring."""
        return None
    
    @property
    def poll_interval_s(self) -> Optional[float]:
        """Optional poll interval in seconds."""
        return None
    
    def start(self) -> None:
        """Start the monitor."""
        if self._running:
            self.logger.warning(f"Monitor {self.name} is already running")
            return
        
        self._running = True
        self._stop_event.clear()
        
        # Start monitor thread
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"Monitor-{self.name}",
            daemon=True
        )
        self._thread.start()
        
        self.logger.info(f"Started monitor {self.name} (session: {self._session_id})")
    
    def start_inline_for_tests(self) -> None:
        """Start monitor in inline mode (no background threads) for tests."""
        if self._running:
            self.logger.warning(f"Monitor {self.name} is already running")
            return
        
        self._running = True
        self._inline_mode = True
        self._stop_event.clear()
        
        # Initialize monitoring without starting threads
        self.start_monitoring()
        
        self.logger.info(f"Started monitor {self.name} in inline mode (session: {self._session_id})")
    
    def stop(self) -> None:
        """Stop the monitor."""
        if not self._running:
            return
        
        self._running = False
        self._stop_event.set()
        
        # Cancel scheduled tasks
        self.scheduler.cancel_all()
        
        # Wait for thread to finish (only if not in inline mode)
        if not self._inline_mode and self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
            if self._thread.is_alive():
                self.logger.warning(f"Monitor {self.name} thread did not stop gracefully")
        
        # Stop monitoring (only in inline mode, otherwise _run_loop handles it)
        if self._inline_mode:
            try:
                self.stop_monitoring()
            except Exception as e:
                self.logger.error(f"Error stopping monitor {self.name}: {e}", exc_info=True)
        
        # Final flush
        self.flush()
        
        self.logger.info(f"Stopped monitor {self.name}")
    
    def join(self, timeout: float = 2.0) -> bool:
        """Wait for monitor thread to finish.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if thread finished, False if timed out
        """
        # In inline mode, return immediately
        if self._inline_mode:
            return True
        
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
            return not self._thread.is_alive()
        return True
    
    def _run_loop(self) -> None:
        """Main monitor loop with exception handling."""
        try:
            self.start_monitoring()
            
            # Schedule periodic flush if configured
            self._schedule_flush()
            
            # Main loop
            while not self._stop_event.is_set():
                # Wait for stop signal or poll interval
                poll_interval = self.poll_interval_s or self.cadence_s or 1.0
                if self._stop_event.wait(poll_interval):
                    break
                    
        except Exception as e:
            self.logger.error(f"Error in monitor {self.name}: {e}", exc_info=True)
        finally:
            try:
                self.stop_monitoring()
            except Exception as e:
                self.logger.error(f"Error stopping monitor {self.name}: {e}", exc_info=True)
    
    def emit(self, event_dict: Dict[str, Any]) -> None:
        """Emit an event with validation and enrichment.
        
        Args:
            event_dict: Event data dictionary
        """
        try:
            # Validate required fields
            self._validate_event(event_dict)
            
            # Enrich with IDs and timestamps
            enriched = self._enrich_event(event_dict)
            
            # Convert to Event object
            event = DBEvent.from_dict(enriched)
            
            # Add to batch or emit directly
            self._add_to_batch(event)
            
        except Exception as e:
            self.logger.error(f"Failed to emit event: {e}", exc_info=True)
    
    def _validate_event(self, event_dict: Dict[str, Any]) -> None:
        """Validate event has required fields."""
        required_fields = ['action', 'subject_type']
        for field in required_fields:
            if field not in event_dict:
                raise ValueError(f"Missing required field '{field}'")
        
        # Validate monitor field matches
        if 'monitor' in event_dict and event_dict['monitor'] != self.name:
            raise ValueError(f"Event monitor '{event_dict['monitor']}' doesn't match monitor name '{self.name}'")
    
    def _enrich_event(self, event_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich event with IDs, timestamps, and session info."""
        enriched = event_dict.copy()
        
        # Add required fields if missing
        if 'id' not in enriched:
            enriched['id'] = new_id()
        if 'ts_utc' not in enriched:
            enriched['ts_utc'] = int(time.time() * 1000)
        if 'monitor' not in enriched:
            enriched['monitor'] = self.name
        if 'session_id' not in enriched:
            enriched['session_id'] = self._session_id
        
        # Ensure attrs_json is a string
        if 'attrs' in enriched:
            attrs = enriched.pop('attrs')
            enriched['attrs_json'] = json.dumps(attrs, separators=(',', ':'))
        elif 'attrs_json' not in enriched:
            enriched['attrs_json'] = None
        
        return enriched
    
    def _add_to_batch(self, event: DBEvent) -> None:
        """Add event to batch and check flush conditions."""
        with self._batch_lock:
            self._batch.append(event)
            
            # Check size-based flush
            if len(self._batch) >= self.batch_config.max_size:
                self._flush_batch()
                # Reschedule next time-based flush after size flush
                self._schedule_flush()
    
    def _schedule_flush(self) -> None:
        """Schedule the next time-based flush."""
        # Don't schedule in inline mode - tests will manually advance scheduler
        if self._inline_mode:
            return
        
        if self._flush_handle:
            self.scheduler.cancel(self._flush_handle)
        
        def flush_callback():
            self._time_based_flush()
        
        self._flush_handle = self.scheduler.call_later(
            self.batch_config.max_time_s,
            flush_callback
        )
    
    def _time_based_flush(self) -> None:
        """Handle time-based flush and reschedule."""
        with self._batch_lock:
            if self._batch:  # Only flush if we have events
                self._flush_batch()
        
        # Schedule next flush if still running
        if self._running and not self._stop_event.is_set():
            self._schedule_flush()
    
    def flush(self) -> None:
        """Force flush of all batched events."""
        with self._batch_lock:
            if self._batch:
                self._flush_batch()
    
    def check_time_flush_inline(self) -> bool:
        """Check and perform time-based flush for inline mode.
        
        Returns:
            True if flush was performed, False otherwise
        """
        if not self._inline_mode:
            return False
        
        with self._batch_lock:
            current_time = self.scheduler.now()
            if (self._batch and 
                current_time - self._last_flush_time >= self.batch_config.max_time_s):
                self._flush_batch()
                return True
        return False
    
    def _flush_batch(self) -> None:
        """Flush the current batch (must be called with lock held)."""
        if not self._batch:
            return
        
        batch = self._batch.copy()
        self._batch.clear()
        self._last_flush_time = self.scheduler.now()
        
        # Process batch
        if self.dry_run:
            self._print_events(batch)
        else:
            self._emit_events(batch)
    
    def _print_events(self, events: List[DBEvent]) -> None:
        """Print events to console for dry-run mode."""
        for event in events:
            event_dict = event.to_dict()
            # Pretty print with basic formatting
            print(f"[{self.name}] {event.action} at {event.ts_utc}: {json.dumps(event_dict, indent=2)}")
    
    def _emit_events(self, events: List[DBEvent]) -> None:
        """Emit events to the event bus."""
        for event in events:
            success = publish_event(event)
            if not success:
                self.logger.warning("Failed to publish event to bus")
    
    def should_stop(self) -> bool:
        """Check if monitor should stop."""
        return self._stop_event.is_set()
    
    def wait_or_stop(self, timeout: float) -> bool:
        """Wait for timeout or stop signal. Returns True if should stop."""
        return self._stop_event.wait(timeout)