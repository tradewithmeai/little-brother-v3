"""Unit tests for MonitorBase and related classes."""

import json
import threading
import time
from unittest.mock import MagicMock, patch

from lb3.events import Event
from lb3.monitors.base import BatchConfig, MonitorBase


class TestBatchConfig:
    """Test BatchConfig functionality."""
    
    def test_default_config(self):
        """Test default BatchConfig values."""
        config = BatchConfig()
        assert config.max_size == 100
        assert config.max_time_s == 5.0
    
    def test_custom_config(self):
        """Test custom BatchConfig values."""
        config = BatchConfig(max_size=50, max_time_s=2.5)
        assert config.max_size == 50
        assert config.max_time_s == 2.5
    
    def test_from_config_string_count_only(self):
        """Test parsing config string with count only."""
        config = BatchConfig.from_config_string("128")
        assert config.max_size == 128
        assert config.max_time_s == 5.0  # Default
    
    def test_from_config_string_time_only(self):
        """Test parsing config string with time only."""
        config = BatchConfig.from_config_string("2.5s")
        assert config.max_size == 100  # Default
        assert config.max_time_s == 2.5
    
    def test_from_config_string_both(self):
        """Test parsing config string with both values."""
        config = BatchConfig.from_config_string("128 or 1.5s")
        assert config.max_size == 128
        assert config.max_time_s == 1.5
    
    def test_from_config_string_reversed(self):
        """Test parsing config string with reversed order."""
        config = BatchConfig.from_config_string("2.0s or 64")
        assert config.max_size == 64
        assert config.max_time_s == 2.0
    
    def test_from_config_string_invalid(self):
        """Test parsing invalid config string returns defaults."""
        config = BatchConfig.from_config_string("invalid")
        assert config.max_size == 100
        assert config.max_time_s == 5.0
    
    def test_from_config_string_empty(self):
        """Test parsing empty config string returns defaults."""
        config = BatchConfig.from_config_string("")
        assert config.max_size == 100
        assert config.max_time_s == 5.0


class MockMonitor(MonitorBase):
    """Mock monitor for testing MonitorBase."""
    
    def __init__(self, dry_run=False, name="test_monitor"):
        self._monitor_name = name
        super().__init__(dry_run)
        self.start_called = False
        self.stop_called = False
        self.monitoring_exception = None
    
    @property
    def name(self):
        return self._monitor_name
    
    def start_monitoring(self):
        self.start_called = True
        if self.monitoring_exception:
            raise self.monitoring_exception
    
    def stop_monitoring(self):
        self.stop_called = True


class TestMonitorBase:
    """Test MonitorBase functionality."""
    
    @patch('lb3.monitors.base.get_effective_config')
    def test_monitor_initialization(self, mock_config):
        """Test MonitorBase initialization."""
        # Mock config
        mock_config_obj = MagicMock()
        mock_config_obj.batch.flush_thresholds.test_monitor_events = "64 or 2.0s"
        mock_config.return_value = mock_config_obj
        
        monitor = MockMonitor(dry_run=True)
        
        assert monitor.dry_run is True
        assert monitor.name == "test_monitor"
        assert monitor.batch_config.max_size == 64
        assert monitor.batch_config.max_time_s == 2.0
        assert monitor._session_id is not None
        assert len(monitor._batch) == 0
        assert monitor._running is False
    
    @patch('lb3.monitors.base.get_effective_config')
    def test_monitor_config_fallback(self, mock_config):
        """Test monitor config fallback to defaults."""
        # Mock config without batch section
        mock_config_obj = MagicMock()
        del mock_config_obj.batch  # Remove batch attribute
        mock_config.return_value = mock_config_obj
        
        monitor = MockMonitor()
        
        # Should use defaults
        assert monitor.batch_config.max_size == 100
        assert monitor.batch_config.max_time_s == 5.0
    
    @patch('lb3.monitors.base.get_effective_config')
    def test_start_stop_lifecycle(self, mock_config):
        """Test monitor start/stop lifecycle."""
        mock_config.return_value = MagicMock()
        
        monitor = MockMonitor()
        
        # Initially not running
        assert not monitor._running
        
        # Start monitor
        monitor.start()
        assert monitor._running
        assert monitor.start_called
        
        # Wait briefly for thread to start
        time.sleep(0.1)
        
        # Stop monitor
        monitor.stop()
        assert not monitor._running
        assert monitor.stop_called
    
    @patch('lb3.monitors.base.get_effective_config')
    def test_double_start(self, mock_config):
        """Test that double start is handled gracefully."""
        mock_config.return_value = MagicMock()
        
        monitor = MockMonitor()
        
        monitor.start()
        assert monitor._running
        
        # Second start should be ignored
        monitor.start()
        assert monitor._running
        
        monitor.stop()
    
    @patch('lb3.monitors.base.get_effective_config')
    def test_emit_event_validation(self, mock_config):
        """Test event validation in emit method."""
        mock_config.return_value = MagicMock()
        
        monitor = MockMonitor(dry_run=True)
        
        # Valid event
        valid_event = {
            'action': 'test_action',
            'subject_type': 'test_subject'
        }
        
        # Should not raise exception
        monitor.emit(valid_event)
        
        # Invalid event - missing action
        invalid_event = {
            'subject_type': 'test_subject'
        }
        
        # Should handle exception gracefully (logged but not raised)
        monitor.emit(invalid_event)
        
        # Invalid event - monitor mismatch
        mismatch_event = {
            'action': 'test_action',
            'subject_type': 'test_subject',
            'monitor': 'wrong_monitor'
        }
        
        # Should handle exception gracefully
        monitor.emit(mismatch_event)
    
    @patch('lb3.monitors.base.get_effective_config')
    def test_event_enrichment(self, mock_config):
        """Test event enrichment during emit."""
        mock_config.return_value = MagicMock()
        
        monitor = MockMonitor(dry_run=True)
        
        # Capture printed events
        printed_events = []
        original_print = monitor._print_events
        
        def capture_print(events):
            printed_events.extend(events)
        
        monitor._print_events = capture_print
        
        # Emit basic event
        basic_event = {
            'action': 'test_action',
            'subject_type': 'test_subject',
            'attrs': {'key': 'value'}
        }
        
        monitor.emit(basic_event)
        monitor.flush()  # Force flush
        
        assert len(printed_events) == 1
        event = printed_events[0]
        
        # Check enrichment
        assert event.id is not None
        assert event.ts_utc is not None
        assert event.monitor == "test_monitor"
        assert event.session_id == monitor._session_id
        assert event.action == "test_action"
        assert event.subject_type == "test_subject"
        
        # Check attrs conversion to JSON
        attrs_dict = json.loads(event.attrs_json)
        assert attrs_dict['key'] == 'value'
    
    @patch('lb3.monitors.base.get_effective_config')
    def test_batching_size_threshold(self, mock_config):
        """Test batching based on size threshold."""
        # Mock config with small batch size
        mock_config_obj = MagicMock()
        mock_config_obj.batch.flush_thresholds.test_monitor_events = "2 or 10.0s"
        mock_config.return_value = mock_config_obj
        
        monitor = MockMonitor(dry_run=True)
        
        # Capture printed events
        printed_events = []
        
        def capture_print(events):
            printed_events.extend(events)
        
        monitor._print_events = capture_print
        
        # Emit first event - should not flush yet
        monitor.emit({'action': 'test1', 'subject_type': 'test'})
        assert len(printed_events) == 0
        
        # Emit second event - should trigger flush
        monitor.emit({'action': 'test2', 'subject_type': 'test'})
        assert len(printed_events) == 2
        
        # Check events were processed in order
        assert printed_events[0].action == 'test1'
        assert printed_events[1].action == 'test2'
    
    @patch('lb3.monitors.base.get_effective_config')
    def test_batching_time_threshold(self, mock_config):
        """Test batching based on time threshold."""
        # Mock config with short time threshold
        mock_config_obj = MagicMock()
        mock_config_obj.batch.flush_thresholds.test_monitor_events = "100 or 0.1s"
        mock_config.return_value = mock_config_obj
        
        monitor = MockMonitor(dry_run=True)
        
        # Start monitor to enable time-based flushing
        monitor.start()
        
        # Capture printed events
        printed_events = []
        
        def capture_print(events):
            printed_events.extend(events)
        
        monitor._print_events = capture_print
        
        # Emit event
        monitor.emit({'action': 'test', 'subject_type': 'test'})
        
        # Should not flush immediately
        assert len(printed_events) == 0
        
        # Wait for time threshold and manually trigger check
        time.sleep(0.2)
        monitor._check_time_flush()
        
        # Should have flushed by now
        assert len(printed_events) == 1
        assert printed_events[0].action == 'test'
        
        monitor.stop()
    
    @patch('lb3.monitors.base.get_effective_config')
    def test_manual_flush(self, mock_config):
        """Test manual flush functionality."""
        mock_config.return_value = MagicMock()
        
        monitor = MockMonitor(dry_run=True)
        
        # Capture printed events
        printed_events = []
        
        def capture_print(events):
            printed_events.extend(events)
        
        monitor._print_events = capture_print
        
        # Emit events
        monitor.emit({'action': 'test1', 'subject_type': 'test'})
        monitor.emit({'action': 'test2', 'subject_type': 'test'})
        
        # Should not have flushed yet
        assert len(printed_events) == 0
        
        # Manual flush
        monitor.flush()
        
        # Should have flushed both events
        assert len(printed_events) == 2
        assert printed_events[0].action == 'test1'
        assert printed_events[1].action == 'test2'
    
    @patch('lb3.monitors.base.get_effective_config')
    @patch('lb3.monitors.base.publish_event')
    def test_non_dry_run_mode(self, mock_publish, mock_config):
        """Test non-dry-run mode uses event bus."""
        mock_config.return_value = MagicMock()
        mock_publish.return_value = True
        
        monitor = MockMonitor(dry_run=False)
        
        # Emit event
        monitor.emit({'action': 'test', 'subject_type': 'test'})
        monitor.flush()
        
        # Should have published event
        mock_publish.assert_called_once()
        
        # Get the published event
        published_event = mock_publish.call_args[0][0]
        assert isinstance(published_event, Event)
        assert published_event.action == 'test'
        assert published_event.subject_type == 'test'
    
    @patch('lb3.monitors.base.get_effective_config')
    def test_exception_handling_in_monitoring(self, mock_config):
        """Test exception handling in monitoring loop."""
        mock_config.return_value = MagicMock()
        
        monitor = MockMonitor()
        monitor.monitoring_exception = RuntimeError("Test error")
        
        # Start monitor - should handle exception
        monitor.start()
        
        # Wait briefly for thread to process
        time.sleep(0.1)
        
        # Monitor should still be in started state initially
        # but the thread should have handled the exception
        
        monitor.stop()
        
        # Both start and stop should have been called despite exception
        assert monitor.start_called
        assert monitor.stop_called
    
    @patch('lb3.monitors.base.get_effective_config')
    def test_thread_safety(self, mock_config):
        """Test thread safety of batch operations."""
        mock_config.return_value = MagicMock()
        
        monitor = MockMonitor(dry_run=True)
        
        # Capture printed events
        printed_events = []
        
        def capture_print(events):
            printed_events.extend(events)
        
        monitor._print_events = capture_print
        
        # Function to emit events from multiple threads
        def emit_events(start_idx, count):
            for i in range(count):
                monitor.emit({
                    'action': f'test_{start_idx + i}',
                    'subject_type': 'test'
                })
        
        # Create multiple threads
        threads = []
        for i in range(3):
            thread = threading.Thread(
                target=emit_events,
                args=(i * 10, 5)
            )
            threads.append(thread)
        
        # Start all threads
        for thread in threads:
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Flush to get all events
        monitor.flush()
        
        # Should have received all 15 events
        assert len(printed_events) == 15
        
        # All events should be valid
        for event in printed_events:
            assert event.action.startswith('test_')
            assert event.subject_type == 'test'
    
    @patch('lb3.monitors.base.get_effective_config')
    def test_should_stop_wait_or_stop(self, mock_config):
        """Test should_stop and wait_or_stop methods."""
        mock_config.return_value = MagicMock()
        
        monitor = MockMonitor()
        
        # Initially should not stop
        assert not monitor.should_stop()
        
        # wait_or_stop with short timeout should return False (no stop signal)
        start_time = time.time()
        result = monitor.wait_or_stop(0.1)
        elapsed = time.time() - start_time
        
        assert result is False
        assert elapsed >= 0.1
        
        # Set stop signal
        monitor._stop_event.set()
        
        # Now should stop
        assert monitor.should_stop()
        
        # wait_or_stop should return immediately with True
        start_time = time.time()
        result = monitor.wait_or_stop(1.0)  # Long timeout
        elapsed = time.time() - start_time
        
        assert result is True
        assert elapsed < 0.1  # Should return quickly