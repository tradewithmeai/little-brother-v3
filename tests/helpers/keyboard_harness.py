"""Test harness helper for keyboard monitor testing."""

from typing import List, Optional
from unittest.mock import Mock, patch

from lb3.monitors.keyboard import BatchConfig, FakeKeyboardSource, KeyboardMonitor
from lb3.utils.scheduler import ManualScheduler


def build_inline_keyboard_monitor(
    batch_time_s: float = 0.3,
    batch_size: int = 999,
    scheduler: Optional[ManualScheduler] = None,
    fake_source: Optional[FakeKeyboardSource] = None,
    dry_run: bool = True
) -> KeyboardMonitor:
    """Build a keyboard monitor in inline mode for testing.
    
    Args:
        batch_time_s: Time-based flush interval in seconds
        batch_size: Size-based flush threshold
        scheduler: ManualScheduler instance (created if None)
        fake_source: FakeKeyboardSource instance (created if None)
        dry_run: Whether to run in dry-run mode
        
    Returns:
        Configured KeyboardMonitor in inline mode
    """
    if scheduler is None:
        scheduler = ManualScheduler()
    
    if fake_source is None:
        fake_source = FakeKeyboardSource(mode="inline")
    
    batch_config = BatchConfig(max_size=batch_size, max_time_s=batch_time_s)
    
    # Mock config with guardrails enabled
    mock_config = Mock()
    mock_config.guardrails.no_global_text_keylogging = True
    
    with patch('lb3.monitors.keyboard.get_effective_config', return_value=mock_config):
        monitor = KeyboardMonitor(
            dry_run=dry_run,
            batch_config=batch_config,
            event_source=fake_source,
            scheduler=scheduler,
            event_source_mode="inline"
        )
    
    return monitor


def feed_keys(
    monitor: KeyboardMonitor,
    n: int,
    spacing_ms: float = 50.0,
    scheduler: Optional[ManualScheduler] = None
) -> None:
    """Feed n key events to the monitor with specified spacing.
    
    Args:
        monitor: KeyboardMonitor instance
        n: Number of key pairs (down/up) to feed
        spacing_ms: Milliseconds between each key event
        scheduler: ManualScheduler to advance time with
    """
    spacing_s = spacing_ms / 1000.0
    
    for i in range(n):
        if scheduler:
            scheduler.advance(spacing_s)
        
        monitor.emit_keydown_inline()
        
        if scheduler:
            scheduler.advance(spacing_s / 2)  # Small gap between down/up
        
        monitor.emit_keyup_inline()


def feed_burst_keys(
    monitor: KeyboardMonitor,
    n: int,
    total_time_ms: float = 300.0,
    scheduler: Optional[ManualScheduler] = None
) -> None:
    """Feed n key events in a burst pattern.
    
    Args:
        monitor: KeyboardMonitor instance
        n: Number of keydown events to feed rapidly
        total_time_ms: Total time for the burst in milliseconds
        scheduler: ManualScheduler to advance time with
    """
    if n <= 1:
        return
    
    spacing_s = (total_time_ms / 1000.0) / (n - 1)
    
    for i in range(n):
        if i > 0 and scheduler:
            scheduler.advance(spacing_s)
        
        monitor.emit_keydown_inline()


def advance_time_and_check_flush(
    monitor: KeyboardMonitor,
    scheduler: ManualScheduler,
    advance_s: float
) -> bool:
    """Advance time and check if a time-based flush occurs.
    
    Args:
        monitor: KeyboardMonitor instance
        scheduler: ManualScheduler to advance
        advance_s: Time to advance in seconds
        
    Returns:
        True if flush occurred, False otherwise
    """
    scheduler.advance(advance_s)
    return monitor.check_time_flush_inline()


class KeyboardTestHarness:
    """Complete test harness for keyboard monitor testing."""
    
    def __init__(self, batch_time_s: float = 0.3, batch_size: int = 999):
        """Initialize test harness.
        
        Args:
            batch_time_s: Time-based flush interval
            batch_size: Size-based flush threshold
        """
        self.scheduler = ManualScheduler()
        self.fake_source = FakeKeyboardSource(mode="inline")
        self.monitor = build_inline_keyboard_monitor(
            batch_time_s=batch_time_s,
            batch_size=batch_size,
            scheduler=self.scheduler,
            fake_source=self.fake_source
        )
        self.emitted_events = []
        
        # Capture emissions
        self.monitor.emit = self._capture_emit
    
    def _capture_emit(self, event_data):
        """Capture emitted events."""
        self.emitted_events.append(event_data)
    
    def start_inline(self):
        """Start monitor in inline mode."""
        self.monitor.start_inline_for_tests()
    
    def stop(self):
        """Stop monitor."""
        self.monitor.stop()
    
    def feed_keys(self, n: int, spacing_ms: float = 50.0):
        """Feed n key pairs with spacing."""
        feed_keys(self.monitor, n, spacing_ms, self.scheduler)
    
    def feed_burst(self, n: int, total_time_ms: float = 300.0):
        """Feed n keys in burst pattern."""
        feed_burst_keys(self.monitor, n, total_time_ms, self.scheduler)
    
    def advance_time(self, advance_s: float) -> bool:
        """Advance time and check for flush."""
        return advance_time_and_check_flush(self.monitor, self.scheduler, advance_s)
    
    def get_keyboard_events(self) -> List[dict]:
        """Get emitted keyboard events."""
        return [e for e in self.emitted_events if e.get('action') == 'stats']
    
    def clear_events(self):
        """Clear captured events."""
        self.emitted_events.clear()
    
    def get_stats(self) -> dict:
        """Get current monitor stats."""
        return self.monitor._stats.to_attrs_dict()