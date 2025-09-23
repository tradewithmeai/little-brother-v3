"""Integration tests for context idle gap with real event bus."""

import tempfile
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from lb3.config import get_effective_config
from lb3.events import Event, get_event_bus
from lb3.monitors.context_snapshot import ContextSnapshotMonitor


class TestContextIdleGapIntegration:
    """Integration tests for robust idle gap with event bus."""

    def setup_method(self):
        """Set up test environment with temporary configuration."""
        self.temp_dir = None
        self.monitor = None
        self.event_bus = None
        self.setup_temp_config()

        # Track emitted events
        self.emitted_events = []

    def teardown_method(self):
        """Clean up test environment."""
        try:
            if self.monitor:
                self.monitor.stop()
            if self.event_bus:
                self.event_bus.stop()
        except Exception:
            pass

        if self.temp_dir:
            try:
                self.temp_dir.cleanup()
            except Exception:
                pass

    def setup_temp_config(self):
        """Set up temporary configuration."""
        self.temp_dir = tempfile.TemporaryDirectory()
        temp_path = Path(self.temp_dir.name)

        # Create minimal config with short idle gap for testing
        db_path = str(temp_path / "local.db").replace("\\", "/")
        spool_path = str(temp_path / "spool").replace("\\", "/")
        config_content = f"""time_zone_handling: UTC_store_only
storage:
  sqlite_path: {db_path}
  spool_dir: {spool_path}
heartbeat:
  poll_intervals:
    context_idle_gap: 1.5s  # Short gap for fast testing
"""

        config_path = temp_path / "config.yaml"
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(config_content)

        # Mock config to use temp directory
        with patch("lb3.config.Config.get_config_path") as mock_path:
            mock_path.return_value = config_path
            # Force config reload
            if hasattr(get_effective_config, "_config"):
                get_effective_config._config = None
            from lb3.config import Config
            Config._instance = None

    def test_heartbeats_dont_prevent_idle_emission(self):
        """Test that heartbeat events don't prevent idle gap emission."""
        # Create monitor with event tracking
        monitor = ContextSnapshotMonitor(dry_run=True)

        # Override emit to track calls
        original_emit = monitor.emit
        def track_emit(event_data):
            self.emitted_events.append({
                "action": event_data.get("action"),
                "attrs": event_data.get("attrs", {}),
                "time": time.time()
            })
            return original_emit(event_data)
        monitor.emit = track_emit

        # Start monitoring
        event_bus = get_event_bus()
        event_bus.start()
        monitor.start()

        try:
            # Wait for initialization
            time.sleep(0.1)

            # Send initial keyboard activity to reset timer
            keyboard_event = Event(
                monitor="keyboard",
                action="keydown",
                subject_type="key",
                subject_id="a",
                attrs={"vk_code": 65}
            )
            event_bus.publish(keyboard_event)

            # Wait a bit
            time.sleep(0.3)
            initial_count = len(self.emitted_events)

            # Send heartbeat events during the idle period
            for i in range(5):
                heartbeat_event = Event(
                    monitor="heartbeat",
                    action="beat",
                    subject_type="none",
                    subject_id=None,
                    attrs={"beat": i}
                )
                event_bus.publish(heartbeat_event)
                time.sleep(0.2)

            # Wait for idle gap to trigger (1.5s + buffer)
            time.sleep(1.8)

            # Should have emitted idle gap despite heartbeats
            idle_emissions = [e for e in self.emitted_events[initial_count:]
                            if "since_ms" in e.get("attrs", {})]
            assert len(idle_emissions) >= 1, f"Expected idle emission, got events: {self.emitted_events[initial_count:]}"

        finally:
            monitor.stop()
            event_bus.stop()

    def test_browser_and_file_events_ignored(self):
        """Test that browser and file events don't reset idle timer."""
        monitor = ContextSnapshotMonitor(dry_run=True)

        # Track emissions
        original_emit = monitor.emit
        def track_emit(event_data):
            self.emitted_events.append({
                "action": event_data.get("action"),
                "attrs": event_data.get("attrs", {}),
                "time": time.time()
            })
            return original_emit(event_data)
        monitor.emit = track_emit

        event_bus = get_event_bus()
        event_bus.start()
        monitor.start()

        try:
            time.sleep(0.1)

            # Send mouse activity to start timer
            mouse_event = Event(
                monitor="mouse",
                action="click",
                subject_type="button",
                subject_id="left",
                attrs={"x": 100, "y": 200}
            )
            event_bus.publish(mouse_event)
            time.sleep(0.3)
            initial_count = len(self.emitted_events)

            # Send browser events (should be ignored)
            browser_event = Event(
                monitor="browser",
                action="navigate",
                subject_type="tab",
                subject_id="tab_1",
                attrs={"url_hash": "abc123"}
            )
            event_bus.publish(browser_event)

            # Send file events (should be ignored)
            file_event = Event(
                monitor="file",
                action="created",
                subject_type="file",
                subject_id="file_1",
                attrs={"file_path_hash": "def456"}
            )
            event_bus.publish(file_event)

            # Wait for idle gap
            time.sleep(1.8)

            # Should emit because browser/file events don't reset timer
            idle_emissions = [e for e in self.emitted_events[initial_count:]
                            if "since_ms" in e.get("attrs", {})]
            assert len(idle_emissions) >= 1, "Browser/file events incorrectly reset idle timer"

        finally:
            monitor.stop()
            event_bus.stop()

    def test_only_activity_resets_timer(self):
        """Test that only keyboard/mouse/active_window events reset the timer."""
        monitor = ContextSnapshotMonitor(dry_run=True)

        # Track emissions
        original_emit = monitor.emit
        def track_emit(event_data):
            self.emitted_events.append({
                "action": event_data.get("action"),
                "attrs": event_data.get("attrs", {}),
                "time": time.time()
            })
            return original_emit(event_data)
        monitor.emit = track_emit

        event_bus = get_event_bus()
        event_bus.start()
        monitor.start()

        try:
            time.sleep(0.1)

            # Send initial activity
            keyboard_event = Event(
                monitor="keyboard",
                action="keydown",
                subject_type="key",
                subject_id="space",
                attrs={"vk_code": 32}
            )
            event_bus.publish(keyboard_event)
            time.sleep(0.5)

            # Send non-activity events
            for monitor_name in ["heartbeat", "browser", "file"]:
                non_activity_event = Event(
                    monitor=monitor_name,
                    action="test_action",
                    subject_type="test",
                    subject_id="test_id",
                    attrs={}
                )
                event_bus.publish(non_activity_event)
                time.sleep(0.1)

            # Send another activity event to reset timer
            mouse_event = Event(
                monitor="mouse",
                action="move",
                subject_type="cursor",
                subject_id=None,
                attrs={"x": 150, "y": 250}
            )
            event_bus.publish(mouse_event)

            # Wait less than idle gap from mouse event
            time.sleep(1.0)

            # Should not emit yet (mouse event reset the timer)
            recent_idle = [e for e in self.emitted_events
                          if "since_ms" in e.get("attrs", {}) and
                          time.time() - e["time"] < 0.5]
            assert len(recent_idle) == 0, "Timer was not properly reset by mouse event"

            # Wait for idle gap from mouse event
            time.sleep(1.0)

            # Now should emit
            final_idle = [e for e in self.emitted_events
                         if "since_ms" in e.get("attrs", {}) and
                         time.time() - e["time"] < 0.5]
            assert len(final_idle) >= 1, "Expected idle emission after mouse activity timeout"

        finally:
            monitor.stop()
            event_bus.stop()

    def test_window_change_immediate_emission(self):
        """Test that window change events trigger immediate emission."""
        monitor = ContextSnapshotMonitor(dry_run=True)

        # Track emissions
        original_emit = monitor.emit
        def track_emit(event_data):
            self.emitted_events.append({
                "action": event_data.get("action"),
                "attrs": event_data.get("attrs", {}),
                "time": time.time()
            })
            return original_emit(event_data)
        monitor.emit = track_emit

        event_bus = get_event_bus()
        event_bus.start()
        monitor.start()

        try:
            time.sleep(0.1)
            initial_count = len(self.emitted_events)

            # Send window change event
            window_event = Event(
                monitor="active_window",
                action="window_change",
                subject_type="window",
                subject_id="window_1",
                attrs={"window_title_hash": "abc123"}
            )
            event_bus.publish(window_event)

            # Should emit immediately
            time.sleep(0.2)

            immediate_emissions = len(self.emitted_events) - initial_count
            assert immediate_emissions >= 1, "Window change should trigger immediate emission"

        finally:
            monitor.stop()
            event_bus.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])