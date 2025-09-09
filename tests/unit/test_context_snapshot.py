"""Unit tests for context snapshot monitor."""

import threading
import time
from unittest.mock import Mock, patch

import pytest

from lb3.events import Event
from lb3.monitors.context_snapshot import ActivityCounters, ContextSnapshotMonitor


class TestActivityCounters:
    """Test activity counter functionality."""
    
    def test_initialization(self):
        """Test counter initialization."""
        counters = ActivityCounters()
        
        assert counters.kb_down == 0
        assert counters.kb_up == 0
        assert counters.mouse_moves == 0
        assert counters.mouse_clicks == 0
        assert counters.mouse_scroll == 0
    
    def test_reset(self):
        """Test counter reset functionality."""
        counters = ActivityCounters()
        counters.kb_down = 5
        counters.mouse_moves = 10
        counters.mouse_clicks = 3
        
        counters.reset()
        
        assert counters.kb_down == 0
        assert counters.kb_up == 0
        assert counters.mouse_moves == 0
        assert counters.mouse_clicks == 0
        assert counters.mouse_scroll == 0
    
    def test_to_dict(self):
        """Test conversion to dictionary."""
        counters = ActivityCounters()
        counters.kb_down = 2
        counters.kb_up = 3
        counters.mouse_moves = 15
        counters.mouse_clicks = 5
        counters.mouse_scroll = 1
        
        result = counters.to_dict()
        
        expected = {
            "kb_down": 2,
            "kb_up": 3,
            "mouse_moves": 15,
            "mouse_clicks": 5,
            "mouse_scroll": 1
        }
        assert result == expected


class TestContextSnapshotMonitor:
    """Test ContextSnapshotMonitor functionality."""
    
    @pytest.fixture
    def mock_config(self):
        """Mock configuration."""
        config = Mock()
        config.heartbeat.poll_intervals.context_idle_gap = "7.0s"
        return config
    
    @pytest.fixture
    def mock_event_bus(self):
        """Mock event bus."""
        bus = Mock()
        bus.subscribe = Mock()
        bus.unsubscribe = Mock()
        bus.start = Mock()
        return bus
    
    def test_initialization(self, mock_config):
        """Test monitor initialization."""
        with patch('lb3.monitors.context_snapshot.get_effective_config', return_value=mock_config):
            monitor = ContextSnapshotMonitor(dry_run=True)
        
        assert monitor.name == "context_snapshot"
        assert monitor._idle_gap_s == 7.0
        assert isinstance(monitor._counters, ActivityCounters)
        assert not monitor._subscribed
    
    def test_parse_time_string(self, mock_config):
        """Test time string parsing."""
        with patch('lb3.monitors.context_snapshot.get_effective_config', return_value=mock_config):
            monitor = ContextSnapshotMonitor(dry_run=True)
        
        # Test valid time strings
        assert monitor._parse_time_string("7.0s") == 7.0
        assert monitor._parse_time_string("3.5s") == 3.5
        assert monitor._parse_time_string("10.0") == 10.0
        
        # Test invalid strings (should return default)
        assert monitor._parse_time_string("invalid") == 7.0
        assert monitor._parse_time_string(None) == 7.0
    
    def test_poll_interval_calculation(self, mock_config):
        """Test poll interval calculation."""
        with patch('lb3.monitors.context_snapshot.get_effective_config', return_value=mock_config):
            monitor = ContextSnapshotMonitor(dry_run=True)
        
        # Should be min(1.0, idle_gap_s / 7.0)
        expected = min(1.0, 7.0 / 7.0)  # 1.0
        assert monitor.poll_interval_s == expected
        
        # Test with smaller gap
        monitor._idle_gap_s = 3.5
        expected = min(1.0, 3.5 / 7.0)  # 0.5
        assert monitor.poll_interval_s == expected
    
    @patch('lb3.monitors.context_snapshot.get_event_bus')
    def test_start_monitoring(self, mock_get_bus, mock_config, mock_event_bus):
        """Test start monitoring."""
        mock_get_bus.return_value = mock_event_bus
        
        with patch('lb3.monitors.context_snapshot.get_effective_config', return_value=mock_config):
            monitor = ContextSnapshotMonitor(dry_run=True)
        
        monitor.start_monitoring()
        
        assert monitor._subscribed
        mock_event_bus.subscribe.assert_called_once_with(monitor._handle_event)
        mock_event_bus.start.assert_called_once()
    
    @patch('lb3.monitors.context_snapshot.get_event_bus')
    def test_stop_monitoring(self, mock_get_bus, mock_config, mock_event_bus):
        """Test stop monitoring."""
        mock_get_bus.return_value = mock_event_bus
        
        with patch('lb3.monitors.context_snapshot.get_effective_config', return_value=mock_config):
            monitor = ContextSnapshotMonitor(dry_run=True)
        
        monitor.start_monitoring()
        monitor.stop_monitoring()
        
        assert not monitor._subscribed
        mock_event_bus.unsubscribe.assert_called_once_with(monitor._handle_event)
    
    def test_update_activity_counters_keyboard(self, mock_config):
        """Test keyboard activity counter updates."""
        with patch('lb3.monitors.context_snapshot.get_effective_config', return_value=mock_config):
            monitor = ContextSnapshotMonitor(dry_run=True)
        
        # Test keydown event
        keydown_event = Event(
            id="test1", ts_utc=int(time.time() * 1000), monitor="keyboard", 
            action="keydown", subject_type="none", session_id="sess1"
        )
        monitor._update_activity_counters(keydown_event)
        assert monitor._counters.kb_down == 1
        
        # Test keyup event
        keyup_event = Event(
            id="test2", ts_utc=int(time.time() * 1000), monitor="keyboard",
            action="keyup", subject_type="none", session_id="sess1"
        )
        monitor._update_activity_counters(keyup_event)
        assert monitor._counters.kb_up == 1
        
        # Test key_press (alternative action name)
        keypress_event = Event(
            id="test3", ts_utc=int(time.time() * 1000), monitor="keyboard",
            action="key_press", subject_type="none", session_id="sess1"
        )
        monitor._update_activity_counters(keypress_event)
        assert monitor._counters.kb_down == 2
    
    def test_update_activity_counters_mouse(self, mock_config):
        """Test mouse activity counter updates."""
        with patch('lb3.monitors.context_snapshot.get_effective_config', return_value=mock_config):
            monitor = ContextSnapshotMonitor(dry_run=True)
        
        # Test mouse move event
        move_event = Event(
            id="test1", ts_utc=int(time.time() * 1000), monitor="mouse",
            action="move", subject_type="none", session_id="sess1"
        )
        monitor._update_activity_counters(move_event)
        assert monitor._counters.mouse_moves == 1
        
        # Test click events
        click_events = ["click", "left_click", "right_click", "middle_click"]
        for i, action in enumerate(click_events):
            click_event = Event(
                id=f"test{i+2}", ts_utc=int(time.time() * 1000), monitor="mouse",
                action=action, subject_type="none", session_id="sess1"
            )
            monitor._update_activity_counters(click_event)
        
        assert monitor._counters.mouse_clicks == 4
        
        # Test scroll event
        scroll_event = Event(
            id="test6", ts_utc=int(time.time() * 1000), monitor="mouse",
            action="scroll", subject_type="none", session_id="sess1"
        )
        monitor._update_activity_counters(scroll_event)
        assert monitor._counters.mouse_scroll == 1
    
    def test_handle_event_updates_timestamps(self, mock_config):
        """Test that handling events updates timestamps."""
        with patch('lb3.monitors.context_snapshot.get_effective_config', return_value=mock_config):
            monitor = ContextSnapshotMonitor(dry_run=True)
        
        initial_time = monitor._last_event_time
        initial_monitor = monitor._last_event_monitor
        
        # Create test event
        test_event = Event(
            id="test1", ts_utc=int(time.time() * 1000), monitor="keyboard",
            action="keydown", subject_type="none", session_id="sess1"
        )
        
        time.sleep(0.1)  # Small delay to ensure time difference
        monitor._handle_event(test_event)
        
        assert monitor._last_event_time > initial_time
        assert monitor._last_event_monitor == "keyboard"
    
    def test_handle_event_foreground_change_triggers_snapshot(self, mock_config):
        """Test that foreground change events trigger snapshots."""
        with patch('lb3.monitors.context_snapshot.get_effective_config', return_value=mock_config):
            monitor = ContextSnapshotMonitor(dry_run=True)
        
        # Mock the emit snapshot method
        monitor._emit_snapshot = Mock()
        
        # Create active window event
        window_event = Event(
            id="test1", ts_utc=int(time.time() * 1000), monitor="active_window",
            action="window_change", subject_type="window", session_id="sess1"
        )
        
        monitor._handle_event(window_event)
        
        monitor._emit_snapshot.assert_called_once_with(trigger="foreground_change")
    
    def test_check_idle_gap_no_emit_before_gap(self, mock_config):
        """Test that idle gap check doesn't emit before gap time."""
        with patch('lb3.monitors.context_snapshot.get_effective_config', return_value=mock_config):
            monitor = ContextSnapshotMonitor(dry_run=True)
        
        monitor._emit_snapshot = Mock()
        
        # Set recent event time
        monitor._last_event_time = time.time() - 3.0  # 3 seconds ago
        monitor._gap_window_start = time.time() - 3.0
        
        monitor._check_idle_gap()
        
        # Should not emit because gap time (7.0s) not exceeded
        monitor._emit_snapshot.assert_not_called()
    
    def test_check_idle_gap_emits_after_gap(self, mock_config):
        """Test that idle gap check emits after gap time."""
        with patch('lb3.monitors.context_snapshot.get_effective_config', return_value=mock_config):
            monitor = ContextSnapshotMonitor(dry_run=True)
        
        monitor._emit_snapshot = Mock()
        
        # Set old event time and gap window
        current_time = time.time()
        monitor._last_event_time = current_time - 8.0  # 8 seconds ago
        monitor._gap_window_start = current_time - 8.0  # 8 seconds ago
        
        monitor._check_idle_gap()
        
        # Should emit because gap time exceeded
        monitor._emit_snapshot.assert_called_once_with(trigger="idle_gap")
        
        # Gap window should be updated
        assert monitor._gap_window_start > current_time - 1.0
    
    def test_check_idle_gap_deduplication(self, mock_config):
        """Test that idle gap deduplication works."""
        with patch('lb3.monitors.context_snapshot.get_effective_config', return_value=mock_config):
            monitor = ContextSnapshotMonitor(dry_run=True)
        
        monitor._emit_snapshot = Mock()
        
        # Set conditions for gap emission
        current_time = time.time()
        monitor._last_event_time = current_time - 8.0
        monitor._gap_window_start = current_time - 1.0  # Recent gap window
        
        monitor._check_idle_gap()
        
        # Should not emit because we're still in the current gap window
        monitor._emit_snapshot.assert_not_called()
    
    def test_emit_snapshot_structure(self, mock_config):
        """Test snapshot emission structure and content."""
        with patch('lb3.monitors.context_snapshot.get_effective_config', return_value=mock_config):
            monitor = ContextSnapshotMonitor(dry_run=True)
        
        # Set up some activity counters
        monitor._counters.kb_down = 5
        monitor._counters.mouse_moves = 10
        monitor._last_event_monitor = "keyboard"
        
        # Set up last snapshot time for since_ms calculation
        monitor._last_snapshot_time = time.time() - 2.0
        
        # Mock emit method to capture event
        emitted_events = []
        def capture_emit(event_data):
            emitted_events.append(event_data)
        
        monitor.emit = capture_emit
        
        monitor._emit_snapshot(trigger="test")
        
        assert len(emitted_events) == 1
        event = emitted_events[0]
        
        # Check event structure
        assert event['action'] == 'snapshot'
        assert event['subject_type'] == 'none'
        assert event['subject_id'] is None
        assert 'attrs' in event
        
        # Check attrs content
        attrs = event['attrs']
        assert attrs['kb_down'] == 5
        assert attrs['kb_up'] == 0
        assert attrs['mouse_moves'] == 10
        assert attrs['mouse_clicks'] == 0
        assert attrs['mouse_scroll'] == 0
        assert attrs['last_event_monitor'] == "keyboard"
        assert isinstance(attrs['since_ms'], int)
        assert attrs['since_ms'] >= 1900  # Should be around 2000ms
    
    def test_emit_snapshot_resets_counters(self, mock_config):
        """Test that emitting snapshot resets counters."""
        with patch('lb3.monitors.context_snapshot.get_effective_config', return_value=mock_config):
            monitor = ContextSnapshotMonitor(dry_run=True)
        
        # Set up activity counters
        monitor._counters.kb_down = 5
        monitor._counters.mouse_moves = 10
        
        monitor.emit = Mock()  # Mock emit to avoid actual emission
        
        monitor._emit_snapshot(trigger="test")
        
        # Counters should be reset
        assert monitor._counters.kb_down == 0
        assert monitor._counters.mouse_moves == 0
    
    def test_emit_snapshot_updates_last_snapshot_time(self, mock_config):
        """Test that emitting snapshot updates last snapshot time."""
        with patch('lb3.monitors.context_snapshot.get_effective_config', return_value=mock_config):
            monitor = ContextSnapshotMonitor(dry_run=True)
        
        monitor.emit = Mock()  # Mock emit to avoid actual emission
        
        old_time = monitor._last_snapshot_time
        time.sleep(0.1)
        
        monitor._emit_snapshot(trigger="test")
        
        assert monitor._last_snapshot_time > old_time
    
    def test_ignores_non_activity_events(self, mock_config):
        """Test that non-activity events don't affect counters."""
        with patch('lb3.monitors.context_snapshot.get_effective_config', return_value=mock_config):
            monitor = ContextSnapshotMonitor(dry_run=True)
        
        # Create non-activity events
        events = [
            Event(id="1", ts_utc=int(time.time() * 1000), monitor="active_window", 
                  action="window_change", subject_type="window", session_id="s1"),
            Event(id="2", ts_utc=int(time.time() * 1000), monitor="context_snapshot", 
                  action="snapshot", subject_type="none", session_id="s1"),
            Event(id="3", ts_utc=int(time.time() * 1000), monitor="browser", 
                  action="navigate", subject_type="url", session_id="s1"),
        ]
        
        for event in events:
            monitor._update_activity_counters(event)
        
        # Counters should remain zero
        counters = monitor._counters.to_dict()
        assert all(count == 0 for count in counters.values())
    
    def test_thread_safety_counters(self, mock_config):
        """Test thread safety of counter operations."""
        with patch('lb3.monitors.context_snapshot.get_effective_config', return_value=mock_config):
            monitor = ContextSnapshotMonitor(dry_run=True)
        
        # Create multiple threads that update counters
        def update_counters():
            for i in range(100):
                event = Event(
                    id=f"test{i}", ts_utc=int(time.time() * 1000), monitor="keyboard",
                    action="keydown", subject_type="none", session_id="sess1"
                )
                monitor._update_activity_counters(event)
        
        threads = [threading.Thread(target=update_counters) for _ in range(5)]
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Should have exactly 500 keydown events
        assert monitor._counters.kb_down == 500


if __name__ == "__main__":
    pytest.main([__file__, "-v"])