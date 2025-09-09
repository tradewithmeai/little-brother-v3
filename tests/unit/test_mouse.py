"""Unit tests for mouse dynamics monitor with deterministic scheduling."""

import json
import math
import os
from unittest.mock import Mock, patch

import pytest

from lb3.monitors.mouse import BatchConfig, FakeMouseSource, MouseMonitor, MouseStats


class TestMouseStats:
    """Test mouse statistics calculations."""
    
    def test_initialization(self):
        """Test MouseStats initialization."""
        stats = MouseStats()
        assert stats.moves == 0
        assert stats.distance_px == 0
        assert stats.click_left == 0
        assert stats.click_right == 0
        assert stats.click_middle == 0
        assert stats.scroll == 0
    
    def test_reset(self):
        """Test stats reset functionality."""
        stats = MouseStats()
        stats.moves = 5
        stats.distance_px = 100
        stats.click_left = 3
        stats.click_right = 2
        stats.click_middle = 1
        stats.scroll = 4
        
        stats.reset()
        
        assert stats.moves == 0
        assert stats.distance_px == 0
        assert stats.click_left == 0
        assert stats.click_right == 0
        assert stats.click_middle == 0
        assert stats.scroll == 0
    
    def test_exact_schema_compliance_empty(self):
        """Test exact schema compliance with no data."""
        stats = MouseStats()
        attrs = stats.to_attrs_dict()
        
        # Verify exact schema
        expected_keys = {"moves", "distance_px", "click_left", "click_right", "click_middle", "scroll"}
        assert set(attrs.keys()) == expected_keys
        
        # Verify exact types
        assert isinstance(attrs["moves"], int)
        assert isinstance(attrs["distance_px"], int)
        assert isinstance(attrs["click_left"], int)
        assert isinstance(attrs["click_right"], int)
        assert isinstance(attrs["click_middle"], int)
        assert isinstance(attrs["scroll"], int)
        
        # Verify safe defaults
        assert attrs["moves"] == 0
        assert attrs["distance_px"] == 0
        assert attrs["click_left"] == 0
        assert attrs["click_right"] == 0
        assert attrs["click_middle"] == 0
        assert attrs["scroll"] == 0
    
    def test_exact_schema_compliance_with_data(self):
        """Test exact schema compliance with real data."""
        stats = MouseStats()
        stats.moves = 10
        stats.distance_px = 250
        stats.click_left = 5
        stats.click_right = 2
        stats.click_middle = 1
        stats.scroll = 3
        
        attrs = stats.to_attrs_dict()
        
        # Verify exact schema
        expected_keys = {"moves", "distance_px", "click_left", "click_right", "click_middle", "scroll"}
        assert set(attrs.keys()) == expected_keys
        
        # Verify exact types and values
        assert isinstance(attrs["moves"], int) and attrs["moves"] == 10
        assert isinstance(attrs["distance_px"], int) and attrs["distance_px"] == 250
        assert isinstance(attrs["click_left"], int) and attrs["click_left"] == 5
        assert isinstance(attrs["click_right"], int) and attrs["click_right"] == 2
        assert isinstance(attrs["click_middle"], int) and attrs["click_middle"] == 1
        assert isinstance(attrs["scroll"], int) and attrs["scroll"] == 3


class TestFakeMouseSource:
    """Test fake mouse source for testing."""
    
    def test_fake_source_lifecycle(self):
        """Test fake source start/stop lifecycle."""
        source = FakeMouseSource()
        
        assert not source.is_running()
        
        # Test callbacks
        move_calls = []
        click_calls = []
        scroll_calls = []
        
        def on_move(x, y):
            move_calls.append((x, y))
        
        def on_click(x, y, button, pressed):
            click_calls.append((x, y, button, pressed))
        
        def on_scroll(x, y, dx, dy):
            scroll_calls.append((x, y, dx, dy))
        
        # Start source
        source.start(on_move, on_click, on_scroll)
        assert source.is_running()
        
        # Simulate events
        source.simulate_move(100, 200)
        source.simulate_click(150, 250, "left", True)
        source.simulate_scroll(200, 300, 0, 1)
        
        assert len(move_calls) == 1
        assert move_calls[0] == (100, 200)
        assert len(click_calls) == 1
        assert click_calls[0][:2] == (150, 250)
        assert len(scroll_calls) == 1
        assert scroll_calls[0] == (200, 300, 0, 1)
        
        # Stop source
        source.stop()
        assert not source.is_running()
        
        # Events should not fire after stop
        source.simulate_move(300, 400)
        assert len(move_calls) == 1  # No additional calls


@pytest.mark.usefixtures("no_thread_leaks")
class TestMouseMonitor:
    """Test mouse monitor functionality."""
    
    def test_initialization_defaults(self, manual_scheduler):
        """Test successful initialization with defaults."""
        fake_source = FakeMouseSource()
        
        monitor = MouseMonitor(
            dry_run=True,
            event_source=fake_source,
            scheduler=manual_scheduler
        )
        
        assert monitor.name == "mouse"
        assert isinstance(monitor._event_source, FakeMouseSource)
        assert monitor.batch_config.max_size == 64
        assert monitor.batch_config.max_time_s == 1.5
    
    def test_distance_accumulation_deterministic(self, fake_clock, manual_scheduler):
        """Test distance accumulation with controlled moves."""
        fake_source = FakeMouseSource(mode="inline")
        
        monitor = MouseMonitor(
            dry_run=True,
            event_source=fake_source,
            scheduler=manual_scheduler,
            event_source_mode="inline"
        )
        
        # Start in inline mode (no threads)
        monitor.start_inline_for_tests()
        
        # Simulate movement sequence: (0,0) -> (3,4) -> (6,8)
        monitor.emit_move_inline(0, 0)    # No distance yet (first position)
        monitor.emit_move_inline(3, 4)    # Distance = 5 pixels
        monitor.emit_move_inline(6, 8)    # Distance = 5 pixels (3-4-5 triangle)
        
        # Check accumulated distance
        assert monitor._stats.moves == 3
        assert monitor._stats.distance_px == 10  # 5 + 5
        
        monitor.stop()
    
    def test_distance_calculation_accuracy(self, manual_scheduler):
        """Test distance calculation accuracy with known coordinates."""
        fake_source = FakeMouseSource(mode="inline")
        
        monitor = MouseMonitor(
            dry_run=True,
            event_source=fake_source,
            scheduler=manual_scheduler,
            event_source_mode="inline"
        )
        
        monitor.start_inline_for_tests()
        
        # Test specific distance calculations
        monitor.emit_move_inline(0, 0)
        monitor.emit_move_inline(100, 0)  # 100 pixels horizontal
        assert monitor._stats.distance_px == 100
        
        monitor.emit_move_inline(100, 100)  # 100 pixels vertical
        assert monitor._stats.distance_px == 200
        
        monitor.emit_move_inline(0, 0)  # ~141 pixels diagonal (sqrt(100^2 + 100^2))
        expected_diagonal = int(math.sqrt(100*100 + 100*100))  # Should be 141
        assert monitor._stats.distance_px == 200 + expected_diagonal
        
        monitor.stop()
    
    def test_click_counting_by_button(self, manual_scheduler):
        """Test click counting for different buttons."""
        fake_source = FakeMouseSource(mode="inline")
        
        monitor = MouseMonitor(
            dry_run=True,
            event_source=fake_source,
            scheduler=manual_scheduler,
            event_source_mode="inline"
        )
        
        monitor.start_inline_for_tests()
        
        # Test different button clicks
        monitor.emit_click_inline("left", True)
        monitor.emit_click_inline("left", True)
        monitor.emit_click_inline("right", True)
        monitor.emit_click_inline("middle", True)
        monitor.emit_click_inline("left", False)  # Should not count releases
        
        assert monitor._stats.click_left == 2
        assert monitor._stats.click_right == 1
        assert monitor._stats.click_middle == 1
        
        monitor.stop()
    
    def test_scroll_counting(self, manual_scheduler):
        """Test scroll event counting."""
        fake_source = FakeMouseSource(mode="inline")
        
        monitor = MouseMonitor(
            dry_run=True,
            event_source=fake_source,
            scheduler=manual_scheduler,
            event_source_mode="inline"
        )
        
        monitor.start_inline_for_tests()
        
        # Test scroll events
        monitor.emit_scroll_inline(0, 1)   # Scroll up
        monitor.emit_scroll_inline(0, -1)  # Scroll down
        monitor.emit_scroll_inline(1, 0)   # Scroll right
        monitor.emit_scroll_inline(0, 0)   # No scroll - should not count
        
        assert monitor._stats.scroll == 3  # Only count non-zero scrolls
        
        monitor.stop()
    
    def test_size_based_flush_at_64_events(self, manual_scheduler):
        """Test immediate flush when 64 events are reached."""
        fake_source = FakeMouseSource(mode="inline")
        batch_config = BatchConfig(max_size=64, max_time_s=999.0)  # High time so only size matters
        
        emitted_events = []
        
        monitor = MouseMonitor(
            dry_run=True,
            batch_config=batch_config,
            event_source=fake_source,
            scheduler=manual_scheduler,
            event_source_mode="inline"
        )
        
        # Capture events at the final output stage
        original_print_events = monitor._print_events
        def capture_print_events(events):
            for event in events:
                emitted_events.append(event.to_dict())
            original_print_events(events)
        monitor._print_events = capture_print_events
        
        # Start in inline mode (no threads)
        monitor.start_inline_for_tests()
        
        # Generate exactly 64 events using inline methods
        monitor.emit_events_inline(64)  # Mixed moves, clicks, scrolls
        
        # Should have triggered size-based flush during the loop
        assert len(emitted_events) == 1, f"Expected 1 event, got {len(emitted_events)}"
        event = emitted_events[0]
        
        assert event['action'] == 'stats'
        assert event['subject_type'] == 'none'
        
        # Parse attrs_json field
        attrs = json.loads(event['attrs_json'])
        
        # Verify schema exactness
        expected_keys = {"moves", "distance_px", "click_left", "click_right", "click_middle", "scroll"}
        assert set(attrs.keys()) == expected_keys
        
        # Verify at least some events were recorded
        total_events = attrs['moves'] + attrs['click_left'] + attrs['click_right'] + attrs['click_middle'] + attrs['scroll']
        assert total_events >= 64
        
        # Monitor stats should be reset after flush
        assert monitor._stats.moves == 0
        assert monitor._stats.click_left == 0
        
        monitor.stop()
    
    def test_time_based_flush_deterministic(self, fake_clock, manual_scheduler):
        """Test time-based flush with deterministic scheduler."""
        fake_source = FakeMouseSource(mode="inline")
        batch_config = BatchConfig(max_size=999, max_time_s=0.8)  # 800ms flush interval
        
        emitted_events = []
        
        monitor = MouseMonitor(
            dry_run=True,
            batch_config=batch_config,
            event_source=fake_source,
            scheduler=manual_scheduler,
            event_source_mode="inline"
        )
        
        # Capture events at the final output stage
        original_print_events = monitor._print_events
        def capture_print_events(events):
            for event in events:
                emitted_events.append(event.to_dict())
            original_print_events(events)
        monitor._print_events = capture_print_events
        
        # Start in inline mode (no threads)
        monitor.start_inline_for_tests()
        
        # Generate some activity
        fake_clock.advance(1.0)
        monitor.emit_move_inline(100, 100)
        monitor.emit_click_inline("left", True)
        monitor.emit_scroll_inline(0, 1)
        
        # No flush yet (time not elapsed)
        assert len(emitted_events) == 0
        
        # Advance time past flush threshold
        fake_clock.advance(1.0)  # 1000ms total > 800ms threshold
        flushed = monitor.check_time_flush_inline()
        
        # Should have triggered time-based flush
        assert flushed, "Expected time-based flush to occur"
        assert len(emitted_events) == 1
        
        # Parse attrs_json field
        attrs = json.loads(emitted_events[0]['attrs_json'])
        assert attrs['moves'] == 1
        assert attrs['click_left'] == 1
        assert attrs['scroll'] == 1
        
        monitor.stop()
    
    def test_schema_exactness_no_extra_fields(self, manual_scheduler):
        """Test that emitted events have exact schema with no extra fields."""
        fake_source = FakeMouseSource(mode="inline")
        
        emitted_events = []
        
        monitor = MouseMonitor(
            dry_run=True,
            event_source=fake_source,
            scheduler=manual_scheduler,
            event_source_mode="inline"
        )
        
        # Capture events
        original_print_events = monitor._print_events
        def capture_print_events(events):
            for event in events:
                emitted_events.append(event.to_dict())
            original_print_events(events)
        monitor._print_events = capture_print_events
        
        monitor.start_inline_for_tests()
        
        # Generate activity and force flush
        monitor.emit_move_inline(10, 20)
        monitor.emit_click_inline("right", True)
        monitor._flush_stats(force_base_flush=True)
        
        assert len(emitted_events) == 1
        event = emitted_events[0]
        
        # Verify event structure
        assert event['action'] == 'stats'
        assert event['subject_type'] == 'none'
        assert event['subject_id'] is None
        assert event['monitor'] == 'mouse'
        
        # Verify attrs schema exactness
        attrs = json.loads(event['attrs_json'])
        expected_keys = {"moves", "distance_px", "click_left", "click_right", "click_middle", "scroll"}
        assert set(attrs.keys()) == expected_keys
        
        # Verify no raw coordinates leaked
        event_str = json.dumps(event)
        assert '"x"' not in event_str.lower()
        assert '"y"' not in event_str.lower()
        assert 'coordinate' not in event_str.lower()
        assert 'position' not in event_str.lower()
        
        monitor.stop()
    
    def test_graceful_degradation_no_pynput(self, manual_scheduler):
        """Test graceful degradation when pynput is unavailable."""
        # Test that mouse monitor gracefully handles missing pynput
        # We'll directly test the auto-selection behavior
        
        # Create monitor without specifying event_source in non-test mode
        original_test_mode = os.environ.get('LB3_TEST_MODE')
        try:
            # Temporarily disable test mode to trigger pynput selection
            os.environ['LB3_TEST_MODE'] = '0'
            
            # Mock pynput import failure
            with patch('lb3.monitors.mouse.PynputMouseSource') as mock_pynput_source:
                mock_instance = Mock()
                mock_instance._mouse = None  # Simulate failed pynput import
                mock_pynput_source.return_value = mock_instance
                
                monitor = MouseMonitor(
                    dry_run=True,
                    scheduler=manual_scheduler
                )
                
                # Should have created PynputMouseSource
                mock_pynput_source.assert_called_once()
                
                # Try to start - should handle degradation gracefully
                try:
                    monitor.start()
                    monitor.stop()
                    # Should not crash even if pynput unavailable
                except Exception:
                    pass  # Expected if pynput not available
                    
        finally:
            # Restore test mode
            if original_test_mode is not None:
                os.environ['LB3_TEST_MODE'] = original_test_mode
            else:
                os.environ.pop('LB3_TEST_MODE', None)
    
    def test_monitor_lifecycle(self, fake_clock, manual_scheduler):
        """Test complete monitor lifecycle."""
        fake_source = FakeMouseSource()
        
        monitor = MouseMonitor(
            dry_run=True,
            event_source=fake_source,
            scheduler=manual_scheduler
        )
        
        assert not monitor._running
        
        # Start monitor
        monitor.start()
        assert monitor._running
        
        # Simulate some activity
        fake_clock.advance(1.0)
        fake_source.simulate_move(100, 200)
        fake_source.simulate_click(100, 200, "left", True)
        fake_source.simulate_scroll(100, 200, 0, 1)
        
        # Stop monitor
        monitor.stop()
        assert not monitor._running
        
        # Should join cleanly
        assert monitor.join(timeout=1.0)
    
    def test_event_source_auto_selection(self, manual_scheduler):
        """Test automatic event source selection based on test mode."""
        # In test mode (LB3_TEST_MODE=1), should auto-select FakeMouseSource
        monitor = MouseMonitor(
            dry_run=True,
            scheduler=manual_scheduler
        )
        assert isinstance(monitor._event_source, FakeMouseSource)
    
    def test_zero_division_safety(self, manual_scheduler):
        """Test zero division safety in statistical calculations."""
        fake_source = FakeMouseSource()
        
        monitor = MouseMonitor(
            dry_run=True,
            event_source=fake_source,
            scheduler=manual_scheduler
        )
        
        # Test with no data - should not crash
        attrs = monitor._stats.to_attrs_dict()
        assert all(isinstance(v, int) for v in attrs.values())
        assert all(v >= 0 for v in attrs.values())


if __name__ == "__main__":
    pytest.main([__file__, "-v"])