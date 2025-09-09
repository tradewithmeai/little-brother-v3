"""Integration tests for mouse monitor with realistic scenarios."""

import json
from unittest.mock import patch

import pytest

from lb3.monitors.mouse import BatchConfig, FakeMouseSource, MouseMonitor


@pytest.mark.usefixtures("no_thread_leaks")
class TestMouseIntegration:
    """Integration tests for mouse monitor."""
    
    def test_simulated_mouse_activity(self, fake_clock, manual_scheduler):
        """Test mouse monitor with simulated mouse activity."""
        # Setup event collection
        collected_events = []
        
        def collect_event(event):
            collected_events.append(event)
        
        # Create monitor with production-like settings
        fake_source = FakeMouseSource()
        batch_config = BatchConfig(max_size=64, max_time_s=1.5)
        
        monitor = MouseMonitor(
            dry_run=False,
            batch_config=batch_config,
            event_source=fake_source,
            scheduler=manual_scheduler
        )
        
        # Mock the event publishing
        with patch('lb3.monitors.base.publish_event', side_effect=collect_event):
            monitor.start()
            
            # Simulate mouse activity pattern
            movements = [
                (100, 100), (150, 120), (200, 140), (250, 160), (300, 180),
                (320, 200), (340, 220), (360, 240), (380, 260), (400, 280)
            ]
            
            for i, (x, y) in enumerate(movements):
                fake_clock.advance(0.1)  # 100ms between movements
                fake_source.simulate_move(x, y)
                
                # Add some clicks
                if i % 3 == 0:
                    fake_source.simulate_click(x, y, "left", True)
                elif i % 5 == 0:
                    fake_source.simulate_click(x, y, "right", True)
                
                # Add some scrolls
                if i % 4 == 0:
                    fake_source.simulate_scroll(x, y, 0, 1)
            
            # Wait for time-based flush
            fake_clock.advance(2.0)
            manual_scheduler.advance(2.0)
            
            monitor.stop()
        
        # Verify events were generated
        assert len(collected_events) > 0
        
        # Check event structure
        event = collected_events[0]
        assert event.monitor == "mouse"
        assert event.action == "stats"
        assert event.subject_type == "none"
        assert event.subject_id is None
        
        # Parse and verify stats
        attrs = json.loads(event.attrs_json)
        
        # Should have recorded movements
        assert attrs['moves'] > 0
        assert attrs['distance_px'] > 0  # Should have accumulated distance
        
        # Should have recorded some clicks and scrolls
        total_activity = (attrs['moves'] + attrs['click_left'] + 
                         attrs['click_right'] + attrs['scroll'])
        assert total_activity > 0
        
        # Verify no coordinate leakage
        event_str = str(event.to_dict())
        assert 'coordinate' not in event_str.lower()
        assert '"x"' not in event_str.lower()
        assert '"y"' not in event_str.lower()
    
    def test_burst_mouse_activity_size_flush(self, manual_scheduler):
        """Test size-based flush with burst mouse activity."""
        # Setup event collection
        collected_events = []
        
        def collect_event(event):
            collected_events.append(event)
        
        fake_source = FakeMouseSource()
        batch_config = BatchConfig(max_size=32, max_time_s=10.0)  # Size-based flush at 32
        
        monitor = MouseMonitor(
            dry_run=False,
            batch_config=batch_config,
            event_source=fake_source,
            scheduler=manual_scheduler
        )
        
        # Mock the event publishing
        with patch('lb3.monitors.base.publish_event', side_effect=collect_event):
            monitor.start()
            
            # Generate burst activity to trigger size flush
            for i in range(40):  # More than 32 to trigger flush
                fake_source.simulate_move(i * 10, i * 5)
                if i % 2 == 0:
                    fake_source.simulate_click(i * 10, i * 5, "left", True)
                if i % 3 == 0:
                    fake_source.simulate_scroll(i * 10, i * 5, 0, 1)
            
            monitor.stop()
        
        # Should have triggered size-based flush
        assert len(collected_events) >= 1
        
        event = collected_events[0]
        attrs = json.loads(event.attrs_json)
        
        # Verify significant activity was recorded
        total_events = (attrs['moves'] + attrs['click_left'] + 
                       attrs['click_right'] + attrs['click_middle'] + 
                       attrs['scroll'])
        assert total_events >= 32  # Should have flushed at size limit
        
        # Verify distance accumulation
        assert attrs['distance_px'] > 0
    
    def test_comprehensive_stats_accuracy(self, manual_scheduler):
        """Test comprehensive statistics accuracy with known inputs."""
        collected_events = []
        
        def collect_event(event):
            collected_events.append(event)
        
        fake_source = FakeMouseSource()
        
        monitor = MouseMonitor(
            dry_run=False,
            event_source=fake_source,
            scheduler=manual_scheduler
        )
        
        with patch('lb3.monitors.base.publish_event', side_effect=collect_event):
            monitor.start()
            
            # Generate known activity pattern
            # 5 moves: (0,0) -> (3,4) -> (6,8) -> (9,12) -> (12,16)
            moves = [(0, 0), (3, 4), (6, 8), (9, 12), (12, 16)]
            expected_distance = 0
            
            for i, (x, y) in enumerate(moves):
                fake_source.simulate_move(x, y)
                if i > 0:
                    prev_x, prev_y = moves[i-1]
                    dist = int(((x - prev_x) ** 2 + (y - prev_y) ** 2) ** 0.5)
                    expected_distance += dist
            
            # Known clicks
            fake_source.simulate_click(0, 0, "left", True)
            fake_source.simulate_click(0, 0, "left", True)
            fake_source.simulate_click(0, 0, "right", True)
            fake_source.simulate_click(0, 0, "middle", True)
            
            # Known scrolls
            fake_source.simulate_scroll(0, 0, 0, 1)
            fake_source.simulate_scroll(0, 0, 0, -1)
            fake_source.simulate_scroll(0, 0, 1, 0)
            
            # Force flush
            monitor._flush_stats(force_base_flush=True)
            monitor.stop()
        
        assert len(collected_events) >= 1
        
        event = collected_events[0]
        attrs = json.loads(event.attrs_json)
        
        # Verify exact counts
        assert attrs['moves'] == 5
        assert attrs['distance_px'] == expected_distance
        assert attrs['click_left'] == 2
        assert attrs['click_right'] == 1
        assert attrs['click_middle'] == 1
        assert attrs['scroll'] == 3
    
    def test_time_based_flush_realistic_timing(self, fake_clock, manual_scheduler):
        """Test time-based flush with realistic timing scenarios."""
        collected_events = []
        
        def collect_event(event):
            collected_events.append(event)
        
        fake_source = FakeMouseSource()
        batch_config = BatchConfig(max_size=1000, max_time_s=1.2)  # 1.2 second flush
        
        monitor = MouseMonitor(
            dry_run=False,
            batch_config=batch_config,
            event_source=fake_source,
            scheduler=manual_scheduler
        )
        
        with patch('lb3.monitors.base.publish_event', side_effect=collect_event):
            monitor.start()
            
            # Period 1: Generate some activity
            fake_source.simulate_move(100, 100)
            fake_source.simulate_click(100, 100, "left", True)
            fake_clock.advance(0.5)  # Not enough time to flush
            
            fake_source.simulate_move(150, 150)
            fake_source.simulate_scroll(150, 150, 0, 1)
            fake_clock.advance(0.8)  # Total 1.3s > 1.2s threshold
            
            # Trigger time-based flush
            executed_tasks = manual_scheduler.advance(0.8)
            
            monitor.stop()
        
        # Should have generated time-based flush
        assert len(collected_events) >= 1
        
        event = collected_events[0]
        attrs = json.loads(event.attrs_json)
        
        # Should have activity from the period
        assert attrs['moves'] == 2
        assert attrs['click_left'] == 1
        assert attrs['scroll'] == 1
        assert attrs['distance_px'] > 0  # Distance from (100,100) to (150,150)
    
    def test_end_to_end_mouse_simulation(self, fake_clock, manual_scheduler):
        """Test end-to-end mouse simulation with mixed activity."""
        collected_events = []
        
        def collect_event(event):
            collected_events.append(event)
        
        fake_source = FakeMouseSource()
        batch_config = BatchConfig(max_size=50, max_time_s=1.0)
        
        monitor = MouseMonitor(
            dry_run=False,
            batch_config=batch_config,
            event_source=fake_source,
            scheduler=manual_scheduler
        )
        
        with patch('lb3.monitors.base.publish_event', side_effect=collect_event):
            monitor.start()
            
            total_moves = 0
            total_clicks = 0
            total_scrolls = 0
            
            # Simulate ~3 seconds of mouse activity
            for cycle in range(3):
                # Light mouse movement: 15 moves per cycle
                for i in range(15):
                    x, y = 100 + i * 10, 100 + i * 5
                    fake_source.simulate_move(x, y)
                    total_moves += 1
                    fake_clock.advance(0.05)  # 50ms between moves
                
                # Some clicking activity
                for i in range(8):
                    button = ["left", "right", "middle"][i % 3]
                    fake_source.simulate_click(200, 200, button, True)
                    total_clicks += 1
                    fake_clock.advance(0.1)  # 100ms between clicks
                
                # Some scrolling
                for i in range(5):
                    fake_source.simulate_scroll(300, 300, 0, 1 if i % 2 else -1)
                    total_scrolls += 1
                    fake_clock.advance(0.08)  # 80ms between scrolls
                
                # Force periodic flush by advancing scheduler
                fake_clock.advance(0.2)
                manual_scheduler.advance(1.1)  # Advance past flush interval
        
        monitor.stop()
        
        # Should have generated multiple events due to size and time flushes
        assert len(collected_events) > 0
        
        # Aggregate stats from all events
        total_recorded_moves = sum(json.loads(e.attrs_json)['moves'] for e in collected_events)
        total_recorded_distance = sum(json.loads(e.attrs_json)['distance_px'] for e in collected_events)
        total_recorded_clicks = sum(
            json.loads(e.attrs_json)['click_left'] + 
            json.loads(e.attrs_json)['click_right'] + 
            json.loads(e.attrs_json)['click_middle'] 
            for e in collected_events
        )
        total_recorded_scrolls = sum(json.loads(e.attrs_json)['scroll'] for e in collected_events)
        
        # Verify activity was captured (allowing for some events to be in progress)
        assert total_recorded_moves > 0
        assert total_recorded_distance > 0
        assert total_recorded_clicks > 0
        assert total_recorded_scrolls > 0
        
        # Verify all events have proper structure
        for event in collected_events:
            assert event.monitor == "mouse"
            assert event.action == "stats"
            assert event.subject_type == "none"
            
            # Verify schema compliance
            attrs = json.loads(event.attrs_json)
            expected_keys = {"moves", "distance_px", "click_left", "click_right", "click_middle", "scroll"}
            assert set(attrs.keys()) == expected_keys


if __name__ == "__main__":
    pytest.main([__file__, "-v"])