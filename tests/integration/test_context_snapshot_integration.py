"""Integration tests for context snapshot monitor."""

import json
import sys
import time
from unittest import skipUnless

import pytest

from lb3.events import Event, get_event_bus
from lb3.ids import new_id
from lb3.monitors.context_snapshot import ContextSnapshotMonitor


@skipUnless(sys.platform == "win32", "Context snapshot integration tests only run on Windows")
class TestContextSnapshotIntegration:
    """Integration tests for ContextSnapshotMonitor with real event flow."""
    
    def setup_method(self):
        """Set up test environment."""
        self.context_monitor = None
        self.window_monitor = None
        self.event_bus = None
        self.received_events = []
        
    def teardown_method(self):
        """Clean up test environment."""
        # Stop monitors
        if self.context_monitor:
            try:
                self.context_monitor.stop()
            except:
                pass
        
        if self.window_monitor:
            try:
                self.window_monitor.stop()
            except:
                pass
        
        # Stop event bus
        if self.event_bus:
            try:
                self.event_bus.stop()
            except:
                pass
    
    def _setup_event_collection(self):
        """Set up event bus and collection."""
        self.event_bus = get_event_bus()
        
        def event_collector(event):
            self.received_events.append(event)
        
        self.event_bus.subscribe(event_collector)
        self.event_bus.start()
    
    def _get_snapshot_events(self):
        """Get snapshot events from received events."""
        return [e for e in self.received_events 
                if e.monitor == "context_snapshot" and e.action == "snapshot"]
    
    def _get_window_events(self):
        """Get active window events from received events."""
        return [e for e in self.received_events 
                if e.monitor == "active_window" and e.action == "window_change"]
    
    def test_snapshot_on_foreground_change(self):
        """Test that snapshots are emitted on foreground changes."""
        # Setup event collection
        self._setup_event_collection()
        
        # Create and start context monitor
        self.context_monitor = ContextSnapshotMonitor(dry_run=False)
        self.context_monitor.start()
        
        # Wait for initialization
        time.sleep(0.5)
        self.received_events.clear()
        
        # Simulate window change event by publishing directly
        window_event = Event(
            id=new_id(),
            ts_utc=int(time.time() * 1000),
            monitor="active_window",
            action="window_change",
            subject_type="window",
            subject_id=new_id(),
            session_id=new_id(),
            exe_name="notepad.exe",
            attrs_json='{"source": "win32+poll", "hwnd": 12345}'
        )
        
        # Publish the event
        self.event_bus.publish(window_event)
        
        # Wait for snapshot to be emitted
        time.sleep(1.0)
        
        # Check that we got a snapshot event
        snapshots = self._get_snapshot_events()
        assert len(snapshots) >= 1, f"Expected at least 1 snapshot, got {len(snapshots)}"
        
        snapshot = snapshots[0]
        assert snapshot.monitor == "context_snapshot"
        assert snapshot.action == "snapshot"
        assert snapshot.subject_type == "none"
        assert snapshot.subject_id is None
        
        # Check attrs_json structure
        attrs = json.loads(snapshot.attrs_json)
        expected_keys = ["kb_down", "kb_up", "mouse_moves", "mouse_clicks", 
                        "mouse_scroll", "since_ms", "last_event_monitor"]
        for key in expected_keys:
            assert key in attrs, f"Missing key {key} in attrs"
        
        assert attrs["last_event_monitor"] == "active_window"
    
    def test_snapshot_after_idle_gap(self):
        """Test that snapshots are emitted after idle gaps."""
        # Setup event collection
        self._setup_event_collection()
        
        # Create context monitor with shorter idle gap for testing
        self.context_monitor = ContextSnapshotMonitor(dry_run=False)
        # Hack: Override idle gap for faster testing
        self.context_monitor._idle_gap_s = 2.0  # 2 seconds for testing
        
        self.context_monitor.start()
        
        # Wait for initialization
        time.sleep(0.5)
        
        # Simulate some activity then go idle
        activity_event = Event(
            id=new_id(),
            ts_utc=int(time.time() * 1000),
            monitor="keyboard",
            action="keydown",
            subject_type="none",
            session_id=new_id()
        )
        self.event_bus.publish(activity_event)
        
        # Clear events and wait for idle gap
        time.sleep(1.0)
        self.received_events.clear()
        
        # Wait for idle gap (2s + some buffer)
        time.sleep(3.5)
        
        # Check that we got an idle gap snapshot
        snapshots = self._get_snapshot_events()
        assert len(snapshots) >= 1, f"Expected idle gap snapshot, got {len(snapshots)}"
        
        snapshot = snapshots[0]
        attrs = json.loads(snapshot.attrs_json)
        assert attrs["last_event_monitor"] == "keyboard"
        assert attrs["kb_down"] == 1  # Should have captured the keyboard event
    
    def test_no_spam_during_activity(self):
        """Test that no extra snapshots are generated during sustained activity."""
        # Setup event collection
        self._setup_event_collection()
        
        # Create context monitor with normal idle gap
        self.context_monitor = ContextSnapshotMonitor(dry_run=False)
        self.context_monitor.start()
        
        # Wait for initialization
        time.sleep(0.5)
        initial_count = len(self._get_snapshot_events())
        
        # Simulate sustained activity (events every 0.5s for 4 seconds)
        # This should prevent idle gap snapshots
        for i in range(8):
            activity_event = Event(
                id=new_id(),
                ts_utc=int(time.time() * 1000),
                monitor="keyboard" if i % 2 == 0 else "mouse",
                action="keydown" if i % 2 == 0 else "move",
                subject_type="none",
                session_id=new_id()
            )
            self.event_bus.publish(activity_event)
            time.sleep(0.5)
        
        # Check that no additional snapshots were generated during activity
        final_count = len(self._get_snapshot_events())
        spam_snapshots = final_count - initial_count
        
        # Should be 0 or at most 1 (if there was an initial foreground change)
        assert spam_snapshots <= 1, f"Expected no spam snapshots, got {spam_snapshots}"
    
    def test_activity_counter_accuracy(self):
        """Test that activity counters accurately track events."""
        # Setup event collection
        self._setup_event_collection()
        
        # Create context monitor
        self.context_monitor = ContextSnapshotMonitor(dry_run=False)
        self.context_monitor.start()
        
        # Wait for initialization and clear events
        time.sleep(0.5)
        self.received_events.clear()
        
        # Generate specific activity patterns
        events_to_send = [
            ("keyboard", "keydown", 3),  # 3 keydown events
            ("keyboard", "keyup", 2),    # 2 keyup events
            ("mouse", "move", 5),        # 5 mouse moves
            ("mouse", "click", 4),       # 4 mouse clicks
            ("mouse", "scroll", 1),      # 1 mouse scroll
        ]
        
        for monitor, action, count in events_to_send:
            for i in range(count):
                event = Event(
                    id=new_id(),
                    ts_utc=int(time.time() * 1000),
                    monitor=monitor,
                    action=action,
                    subject_type="none",
                    session_id=new_id()
                )
                self.event_bus.publish(event)
                time.sleep(0.1)  # Small delay between events
        
        # Trigger a snapshot with a foreground change
        window_event = Event(
            id=new_id(),
            ts_utc=int(time.time() * 1000),
            monitor="active_window",
            action="window_change",
            subject_type="window",
            subject_id=new_id(),
            session_id=new_id()
        )
        self.event_bus.publish(window_event)
        
        # Wait for snapshot
        time.sleep(1.0)
        
        # Find the snapshot event
        snapshots = self._get_snapshot_events()
        assert len(snapshots) >= 1, "Expected at least one snapshot"
        
        snapshot = snapshots[-1]  # Get the latest snapshot
        attrs = json.loads(snapshot.attrs_json)
        
        # Verify counter accuracy
        assert attrs["kb_down"] == 3, f"Expected 3 kb_down, got {attrs['kb_down']}"
        assert attrs["kb_up"] == 2, f"Expected 2 kb_up, got {attrs['kb_up']}"
        assert attrs["mouse_moves"] == 5, f"Expected 5 mouse_moves, got {attrs['mouse_moves']}"
        assert attrs["mouse_clicks"] == 4, f"Expected 4 mouse_clicks, got {attrs['mouse_clicks']}"
        assert attrs["mouse_scroll"] == 1, f"Expected 1 mouse_scroll, got {attrs['mouse_scroll']}"
    
    def test_counter_reset_after_snapshot(self):
        """Test that counters are reset after emitting a snapshot."""
        # Setup event collection
        self._setup_event_collection()
        
        # Create context monitor
        self.context_monitor = ContextSnapshotMonitor(dry_run=False)
        self.context_monitor.start()
        
        # Wait for initialization
        time.sleep(0.5)
        
        # Generate some activity
        for i in range(3):
            event = Event(
                id=new_id(),
                ts_utc=int(time.time() * 1000),
                monitor="keyboard",
                action="keydown",
                subject_type="none",
                session_id=new_id()
            )
            self.event_bus.publish(event)
        
        # Trigger first snapshot
        self.received_events.clear()
        window_event1 = Event(
            id=new_id(),
            ts_utc=int(time.time() * 1000),
            monitor="active_window",
            action="window_change",
            subject_type="window",
            subject_id=new_id(),
            session_id=new_id()
        )
        self.event_bus.publish(window_event1)
        time.sleep(1.0)
        
        # Should have first snapshot with kb_down = 3
        snapshots1 = self._get_snapshot_events()
        assert len(snapshots1) >= 1
        attrs1 = json.loads(snapshots1[0].attrs_json)
        assert attrs1["kb_down"] == 3
        
        # Generate more activity
        for i in range(2):
            event = Event(
                id=new_id(),
                ts_utc=int(time.time() * 1000),
                monitor="mouse",
                action="click",
                subject_type="none",
                session_id=new_id()
            )
            self.event_bus.publish(event)
        
        # Trigger second snapshot
        self.received_events.clear()
        window_event2 = Event(
            id=new_id(),
            ts_utc=int(time.time() * 1000),
            monitor="active_window",
            action="window_change",
            subject_type="window",
            subject_id=new_id(),
            session_id=new_id()
        )
        self.event_bus.publish(window_event2)
        time.sleep(1.0)
        
        # Second snapshot should only have new activity (counters were reset)
        snapshots2 = self._get_snapshot_events()
        assert len(snapshots2) >= 1
        attrs2 = json.loads(snapshots2[0].attrs_json)
        assert attrs2["kb_down"] == 0  # Should be reset
        assert attrs2["mouse_clicks"] == 2  # Should have new activity
    
    def test_deduplication_in_gap_window(self):
        """Test that snapshots are not duplicated within the same gap window."""
        # Setup event collection
        self._setup_event_collection()
        
        # Create context monitor with short gap for testing
        self.context_monitor = ContextSnapshotMonitor(dry_run=False)
        self.context_monitor._idle_gap_s = 1.5  # 1.5 seconds
        
        self.context_monitor.start()
        
        # Wait for initialization
        time.sleep(0.5)
        
        # Generate activity then let it go idle
        event = Event(
            id=new_id(),
            ts_utc=int(time.time() * 1000),
            monitor="keyboard",
            action="keydown",
            subject_type="none",
            session_id=new_id()
        )
        self.event_bus.publish(event)
        
        # Clear events and wait for first idle gap
        time.sleep(0.5)
        self.received_events.clear()
        
        # Wait for idle gap to trigger
        time.sleep(2.5)  # Should trigger one idle gap snapshot
        
        # Count idle gap snapshots (should be exactly 1)
        snapshots = self._get_snapshot_events()
        initial_snapshot_count = len(snapshots)
        
        # Wait a bit more (should not generate additional snapshots)
        time.sleep(2.0)
        
        final_snapshots = self._get_snapshot_events()
        final_snapshot_count = len(final_snapshots)
        
        # Should not have additional snapshots due to deduplication
        additional_snapshots = final_snapshot_count - initial_snapshot_count
        assert additional_snapshots == 0, f"Expected no additional snapshots, got {additional_snapshots}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])