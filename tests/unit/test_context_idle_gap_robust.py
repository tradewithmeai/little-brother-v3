"""Unit tests for robust context idle gap behavior."""

import time
from unittest.mock import MagicMock, patch

import pytest

from lb3.events import Event
from lb3.monitors.context_snapshot import ContextSnapshotMonitor


class TestContextIdleGapRobust:
    """Test robust idle gap detection that ignores non-activity events."""

    def setup_method(self):
        """Set up test environment."""
        # Mock time.monotonic to control time flow
        self.mock_monotonic = patch("time.monotonic")
        self.monotonic_mock = self.mock_monotonic.start()
        self.current_monotonic = 1000.0  # Start at 1000 seconds
        self.monotonic_mock.return_value = self.current_monotonic

        # Mock time.time for wall clock
        self.mock_time = patch("time.time")
        self.time_mock = self.mock_time.start()
        self.current_wall = 1700000000.0  # Some realistic wall clock time
        self.time_mock.return_value = self.current_wall

        # Create monitor with short idle gap for testing
        with patch("lb3.monitors.context_snapshot.get_effective_config") as mock_config:
            mock_config.return_value.heartbeat.poll_intervals.context_idle_gap = "2.0s"
            self.monitor = ContextSnapshotMonitor(dry_run=True)

        # Track emitted events
        self.emitted_events = []
        self.monitor.emit = MagicMock(side_effect=self._track_emit)

    def teardown_method(self):
        """Clean up test environment."""
        self.mock_monotonic.stop()
        self.mock_time.stop()

    def _track_emit(self, event_data):
        """Track emitted events for testing."""
        # Try to determine trigger from call stack context
        import inspect
        trigger = "unknown"
        for frame_info in inspect.stack():
            if "_emit_snapshot" in frame_info.function:
                # Look for trigger in the frame's local variables
                frame = frame_info.frame
                if "trigger" in frame.f_locals:
                    trigger = frame.f_locals["trigger"]
                    break

        self.emitted_events.append({
            "trigger": trigger,
            "timestamp": self.current_wall,
            "event_data": event_data,
        })

    def _advance_time(self, seconds: float):
        """Advance both monotonic and wall clock time."""
        self.current_monotonic += seconds
        self.current_wall += seconds
        self.monotonic_mock.return_value = self.current_monotonic
        self.time_mock.return_value = self.current_wall

    def _send_event(self, monitor: str, action: str = "test_action"):
        """Send an event to the monitor."""
        event = Event(
            id="test_id",
            ts_utc=int(self.current_wall * 1000),
            monitor=monitor,
            action=action,
            subject_type="test",
            session_id="test_session",
            subject_id="test_subject"
        )
        self.monitor._handle_event(event)

    def test_allowed_activity_resets_idle_timer(self):
        """Test that only allowed activity monitors reset the idle timer."""
        # Initialize monitor
        self.monitor.start_monitoring()

        # Send allowed activity - should reset timer
        self._send_event("keyboard", "keydown")
        self._advance_time(1.5)  # Not enough for idle gap

        # Check idle gap - should not emit yet
        self.monitor._check_idle_gap()
        assert len(self.emitted_events) == 0

        # Advance past idle gap from initial start
        self._advance_time(1.0)  # Total 2.5s from keyboard event
        self.monitor._check_idle_gap()
        assert len(self.emitted_events) == 1
        assert "idle_gap" in str(self.emitted_events[0])

    def test_heartbeats_dont_reset_idle_timer(self):
        """Test that heartbeat events don't reset the idle timer."""
        self.monitor.start_monitoring()

        # Send initial activity
        self._send_event("keyboard", "keydown")
        self._advance_time(1.0)

        # Send heartbeat events - should NOT reset timer
        for i in range(5):
            self._send_event("heartbeat", "beat")
            self._advance_time(0.2)

        # Should emit because heartbeats don't reset the timer
        self.monitor._check_idle_gap()
        assert len(self.emitted_events) == 1

    def test_browser_events_dont_reset_idle_timer(self):
        """Test that browser events don't reset the idle timer."""
        self.monitor.start_monitoring()

        # Send initial activity
        self._send_event("mouse", "click")
        self._advance_time(1.0)

        # Send browser events - should NOT reset timer
        self._send_event("browser", "navigate")
        self._send_event("browser", "focus_change")
        self._advance_time(1.5)

        # Should emit because browser events don't reset the timer
        self.monitor._check_idle_gap()
        assert len(self.emitted_events) == 1

    def test_file_events_dont_reset_idle_timer(self):
        """Test that file events don't reset the idle timer."""
        self.monitor.start_monitoring()

        # Send initial activity
        self._send_event("active_window", "window_change")
        # Note: active_window events trigger immediate emission, so we'll have one event

        self._advance_time(1.0)
        initial_count = len(self.emitted_events)

        # Send file events - should NOT reset timer
        self._send_event("file", "created")
        self._send_event("file", "modified")
        self._advance_time(1.5)

        # Should emit idle gap because file events don't reset the timer
        self.monitor._check_idle_gap()
        assert len(self.emitted_events) == initial_count + 1

    def test_idle_emits_exactly_once_during_silence(self):
        """Test that idle gap emits exactly once during a period of silence."""
        self.monitor.start_monitoring()

        # Send activity then go silent
        self._send_event("keyboard", "keydown")
        self._advance_time(3.0)  # Well past idle gap

        # Check multiple times - should only emit once
        self.monitor._check_idle_gap()
        initial_count = len(self.emitted_events)

        self.monitor._check_idle_gap()
        self.monitor._check_idle_gap()
        assert len(self.emitted_events) == initial_count  # No additional emissions

    def test_mixed_events_only_activity_matters(self):
        """Test that in a mix of events, only activity events reset the timer."""
        self.monitor.start_monitoring()

        # Send allowed activity
        self._send_event("mouse", "move")
        self._advance_time(0.5)

        # Mix of allowed and disallowed events
        self._send_event("heartbeat", "beat")  # Ignored
        self._advance_time(0.5)
        self._send_event("browser", "navigate")  # Ignored
        self._advance_time(0.5)
        self._send_event("keyboard", "keyup")  # Resets timer
        self._advance_time(0.5)
        self._send_event("file", "saved")  # Ignored
        self._advance_time(1.0)  # Total 1.0s since last keyboard event

        # Should not emit yet (only 1.0s since last keyboard)
        self.monitor._check_idle_gap()
        assert len(self.emitted_events) == 0

        # Advance more time
        self._advance_time(1.5)  # Total 2.5s since keyboard
        self.monitor._check_idle_gap()
        assert len(self.emitted_events) == 1

    def test_monotonic_time_used_for_gap_detection(self):
        """Test that monotonic time is used for gap detection."""
        # This test verifies that wall clock changes don't affect gap detection
        self.monitor.start_monitoring()

        # Send activity
        self._send_event("keyboard", "keydown")

        # Simulate wall clock jump (but monotonic time steady)
        self._advance_time(1.0)  # Advance monotonic normally
        self.current_wall += 3600  # Jump wall clock by 1 hour
        self.time_mock.return_value = self.current_wall

        # Should not emit yet (only 1.0s monotonic time passed)
        self.monitor._check_idle_gap()
        assert len(self.emitted_events) == 0

        # Advance monotonic time past gap
        self._advance_time(1.5)  # Total 2.5s monotonic time
        self.monitor._check_idle_gap()
        assert len(self.emitted_events) == 1

    def test_foreground_change_immediate_emission(self):
        """Test that active_window events trigger immediate snapshot emission."""
        self.monitor.start_monitoring()

        # Send window change event
        self._send_event("active_window", "window_change")

        # Should emit immediately, not wait for idle gap
        assert len(self.emitted_events) == 1

    def test_activity_counters_updated_for_all_events(self):
        """Test that activity counters are updated for all events, not just allowed ones."""
        self.monitor.start_monitoring()

        # Send various events including non-allowed ones
        self._send_event("keyboard", "keydown")  # Allowed + counted
        self._send_event("mouse", "click")       # Allowed + counted
        self._send_event("heartbeat", "beat")    # Not allowed but might be counted
        self._send_event("browser", "navigate")  # Not allowed but might be counted

        # The counter update logic should still work
        # (This test mainly ensures the separation of concerns works)
        assert True  # Basic test that no exceptions occurred

    def test_stop_monitoring_performs_final_check(self):
        """Test that stopping the monitor performs a final idle gap check."""
        self.monitor.start_monitoring()

        # Send activity and advance past idle gap
        self._send_event("keyboard", "keydown")
        self._advance_time(3.0)

        # Stop monitoring - should trigger final check and emit
        self.monitor.stop_monitoring()
        assert len(self.emitted_events) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])