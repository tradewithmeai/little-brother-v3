"""Unit tests for keyboard dynamics monitor with deterministic scheduling."""

import json
from unittest.mock import Mock, patch

import pytest

from lb3.monitors.keyboard import (
    BatchConfig,
    FakeKeyboardSource,
    GuardrailViolationError,
    KeyboardMonitor,
    KeyboardStats,
)


class TestKeyboardStats:
    """Test keyboard statistics calculations."""
    
    def test_initialization(self):
        """Test KeyboardStats initialization."""
        stats = KeyboardStats()
        assert stats.keydown_count == 0
        assert stats.keyup_count == 0
        assert stats.intervals == []
        assert stats.burst_count == 0
    
    def test_reset(self):
        """Test stats reset functionality."""
        stats = KeyboardStats()
        stats.keydown_count = 5
        stats.keyup_count = 3
        stats.intervals = [100.0, 200.0]
        stats.burst_count = 1
        
        stats.reset()
        
        assert stats.keydown_count == 0
        assert stats.keyup_count == 0
        assert stats.intervals == []
        assert stats.burst_count == 0
    
    def test_exact_schema_compliance_empty(self):
        """Test exact schema compliance with no data."""
        stats = KeyboardStats()
        attrs = stats.to_attrs_dict()
        
        # Verify exact schema
        expected_keys = {"keydown", "keyup", "mean_ms", "p95_ms", "stdev_ms", "bursts"}
        assert set(attrs.keys()) == expected_keys
        
        # Verify exact types
        assert isinstance(attrs["keydown"], int)
        assert isinstance(attrs["keyup"], int)
        assert isinstance(attrs["mean_ms"], (int, float))
        assert isinstance(attrs["p95_ms"], (int, float))
        assert isinstance(attrs["stdev_ms"], (int, float))
        assert isinstance(attrs["bursts"], int)
        
        # Verify safe defaults
        assert attrs["keydown"] == 0
        assert attrs["keyup"] == 0
        assert attrs["mean_ms"] == 0.0
        assert attrs["p95_ms"] == 0.0
        assert attrs["stdev_ms"] == 0.0
        assert attrs["bursts"] == 0
    
    def test_exact_schema_compliance_with_data(self):
        """Test exact schema compliance with real data."""
        stats = KeyboardStats()
        stats.keydown_count = 10
        stats.keyup_count = 10
        stats.intervals = [50.0, 100.0, 150.0, 200.0, 250.0]
        stats.burst_count = 2
        
        attrs = stats.to_attrs_dict()
        
        # Verify exact schema
        expected_keys = {"keydown", "keyup", "mean_ms", "p95_ms", "stdev_ms", "bursts"}
        assert set(attrs.keys()) == expected_keys
        
        # Verify exact types
        assert isinstance(attrs["keydown"], int)
        assert isinstance(attrs["keyup"], int) 
        assert isinstance(attrs["mean_ms"], (int, float))
        assert isinstance(attrs["p95_ms"], (int, float))
        assert isinstance(attrs["stdev_ms"], (int, float))
        assert isinstance(attrs["bursts"], int)
        
        # Verify calculated values
        assert attrs["keydown"] == 10
        assert attrs["keyup"] == 10
        assert attrs["mean_ms"] == 150.0  # Average
        assert attrs["bursts"] == 2
    
    def test_nan_inf_protection(self):
        """Test protection against NaN/inf values."""
        stats = KeyboardStats()
        stats.keydown_count = 1
        stats.intervals = [float('inf'), float('nan'), 100.0]
        
        attrs = stats.to_attrs_dict()
        
        # Should filter out NaN/inf and use only valid values
        assert attrs["mean_ms"] == 100.0
        assert attrs["p95_ms"] == 100.0
        assert attrs["stdev_ms"] == 0.0  # Only one valid value
    
    def test_single_interval_stdev_zero(self):
        """Test standard deviation with single interval."""
        stats = KeyboardStats()
        stats.intervals = [100.0]
        stats.keydown_count = 2
        
        attrs = stats.to_attrs_dict()
        
        assert attrs["mean_ms"] == 100.0
        assert attrs["p95_ms"] == 100.0
        assert attrs["stdev_ms"] == 0.0  # Single value has no deviation


class TestFakeKeyboardSource:
    """Test fake keyboard source for testing."""
    
    def test_fake_source_lifecycle(self):
        """Test fake source start/stop lifecycle."""
        source = FakeKeyboardSource()
        
        assert not source.is_running()
        
        # Test callbacks
        press_calls = []
        release_calls = []
        
        def on_press(key):
            press_calls.append(key)
        
        def on_release(key):
            release_calls.append(key)
        
        # Start source
        source.start(on_press, on_release)
        assert source.is_running()
        
        # Simulate events
        source.simulate_keydown("mock_key")
        source.simulate_keyup("mock_key")
        
        assert len(press_calls) == 1
        assert len(release_calls) == 1
        
        # Stop source
        source.stop()
        assert not source.is_running()
        
        # Events should not fire after stop
        source.simulate_keydown("mock_key")
        assert len(press_calls) == 1  # No additional calls


@pytest.mark.usefixtures("no_thread_leaks")
class TestKeyboardMonitor:
    """Test keyboard monitor functionality."""
    
    @pytest.fixture
    def mock_config(self):
        """Mock configuration with guardrails enabled."""
        config = Mock()
        config.guardrails.no_global_text_keylogging = True
        return config
    
    @pytest.fixture 
    def mock_config_disabled_guardrails(self):
        """Mock configuration with guardrails disabled."""
        config = Mock()
        config.guardrails.no_global_text_keylogging = False
        return config
    
    def test_guardrail_enforcement_at_init(self, mock_config_disabled_guardrails):
        """Test that monitor rejects disabled guardrails at initialization."""
        with patch('lb3.monitors.keyboard.get_effective_config', 
                  return_value=mock_config_disabled_guardrails):
            with pytest.raises(GuardrailViolationError):
                KeyboardMonitor(dry_run=True)
    
    def test_initialization_with_guardrails(self, mock_config, fake_clock, manual_scheduler):
        """Test successful initialization with guardrails enabled."""
        fake_source = FakeKeyboardSource()
        
        with patch('lb3.monitors.keyboard.get_effective_config', return_value=mock_config):
            monitor = KeyboardMonitor(
                dry_run=True,
                clock=fake_clock,
                event_source=fake_source,
                scheduler=manual_scheduler
            )
        
        assert monitor.name == "keyboard"
        assert isinstance(monitor._event_source, FakeKeyboardSource)
        assert monitor._clock == fake_clock
    
    def test_deterministic_timing_with_fake_clock(self, mock_config, fake_clock, manual_scheduler):
        """Test deterministic timing using injected clock."""
        fake_source = FakeKeyboardSource()
        
        with patch('lb3.monitors.keyboard.get_effective_config', return_value=mock_config):
            monitor = KeyboardMonitor(
                dry_run=True,
                clock=fake_clock,
                event_source=fake_source,
                scheduler=manual_scheduler
            )
        
        # Record events with controlled timing
        fake_clock.advance(1.0)
        monitor._record_key_event("keydown")
        
        fake_clock.advance(0.1)  # 100ms later
        monitor._record_key_event("keydown")
        
        fake_clock.advance(0.15)  # 150ms later
        monitor._record_key_event("keydown")
        
        # Verify timing calculation
        assert len(monitor._stats.intervals) == 2
        assert abs(monitor._stats.intervals[0] - 100.0) < 0.01  # 0.1s * 1000
        assert abs(monitor._stats.intervals[1] - 150.0) < 0.01  # 0.15s * 1000
    
    def test_size_based_flush_at_128_events(self, mock_config, manual_scheduler):
        """Test immediate flush when 128 events are reached."""
        fake_source = FakeKeyboardSource(mode="inline")
        batch_config = BatchConfig(max_size=128, max_time_s=999.0)  # High time so only size matters
        
        emitted_events = []
        
        with patch('lb3.monitors.keyboard.get_effective_config', return_value=mock_config):
            monitor = KeyboardMonitor(
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
        
        # Generate exactly 128 events using inline methods
        monitor.emit_keys_inline(64)  # 64 pairs = 128 events
        
        # Should have triggered size-based flush during the loop
        assert len(emitted_events) == 1, f"Expected 1 event, got {len(emitted_events)}"
        event = emitted_events[0]
        
        assert event['action'] == 'stats'
        assert event['subject_type'] == 'none'
        
        # Parse attrs_json field
        attrs = json.loads(event['attrs_json'])
        assert attrs['keydown'] == 64
        assert attrs['keyup'] == 64
        
        # Monitor stats should be reset after flush
        assert monitor._stats.keydown_count == 0
        assert monitor._stats.keyup_count == 0
        
        monitor.stop()
    
    def test_time_based_flush_deterministic(self, mock_config, fake_clock, manual_scheduler):
        """Test time-based flush with deterministic scheduler."""
        fake_source = FakeKeyboardSource(mode="inline")
        batch_config = BatchConfig(max_size=999, max_time_s=0.3)  # 300ms flush interval
        
        emitted_events = []
        
        with patch('lb3.monitors.keyboard.get_effective_config', return_value=mock_config):
            monitor = KeyboardMonitor(
                dry_run=True,
                clock=fake_clock,
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
        monitor.emit_keys_inline(2)  # 2 key pairs
        
        # No flush yet (time not elapsed)
        assert len(emitted_events) == 0
        
        # Advance time past flush threshold
        fake_clock.advance(0.4)  # 400ms total > 300ms threshold
        flushed = monitor.check_time_flush_inline()
        
        # Should have triggered time-based flush
        assert flushed, "Expected time-based flush to occur"
        assert len(emitted_events) == 1
        
        # Parse attrs_json field
        attrs = json.loads(emitted_events[0]['attrs_json'])
        assert attrs['keydown'] == 2
        assert attrs['keyup'] == 2
        
        monitor.stop()
    
    def test_burst_detection_logic(self, mock_config, fake_clock, manual_scheduler):
        """Test burst detection with controlled timing."""
        fake_source = FakeKeyboardSource()
        
        with patch('lb3.monitors.keyboard.get_effective_config', return_value=mock_config):
            monitor = KeyboardMonitor(
                dry_run=True,
                clock=fake_clock,
                event_source=fake_source,
                scheduler=manual_scheduler
            )
        
        # Simulate rapid typing (burst): 6 keys in 300ms
        base_time = 1.0
        for i in range(6):
            fake_clock.advance(0.05)  # 50ms between keys
            monitor._record_key_event("keydown")
        
        # Should detect at least one burst (≥5 keys within 500ms)
        assert monitor._stats.burst_count >= 1
    
    def test_statistical_accuracy(self, mock_config, manual_scheduler):
        """Test statistical calculations accuracy.""" 
        fake_source = FakeKeyboardSource()
        
        with patch('lb3.monitors.keyboard.get_effective_config', return_value=mock_config):
            monitor = KeyboardMonitor(
                dry_run=True,
                event_source=fake_source,
                scheduler=manual_scheduler
            )
        
        # Set known intervals for verification
        known_intervals = [50.0, 100.0, 150.0, 200.0, 250.0]  # ms
        monitor._stats.intervals = known_intervals.copy()
        monitor._stats.keydown_count = 5
        monitor._stats.keyup_count = 5
        monitor._stats.burst_count = 0
        
        attrs = monitor._stats.to_attrs_dict()
        
        # Verify calculations
        assert attrs["mean_ms"] == 150.0  # Average
        assert attrs["keydown"] == 5
        assert attrs["keyup"] == 5
        assert attrs["bursts"] == 0
        
        # P95 should be high (95% of [50,100,150,200,250] ≈ 240)
        assert 230 <= attrs["p95_ms"] <= 250
        
        # Standard deviation should be reasonable
        assert 70 <= attrs["stdev_ms"] <= 90
    
    def test_zero_division_safety(self, mock_config, manual_scheduler):
        """Test zero division safety in statistical calculations."""
        fake_source = FakeKeyboardSource()
        
        with patch('lb3.monitors.keyboard.get_effective_config', return_value=mock_config):
            monitor = KeyboardMonitor(
                dry_run=True,
                event_source=fake_source,
                scheduler=manual_scheduler
            )
        
        # Test with no data
        attrs = monitor._stats.to_attrs_dict()
        assert attrs["mean_ms"] == 0.0
        assert attrs["p95_ms"] == 0.0
        assert attrs["stdev_ms"] == 0.0
        
        # Test with single interval
        monitor._stats.intervals = [100.0]
        monitor._stats.keydown_count = 2
        attrs = monitor._stats.to_attrs_dict()
        
        assert attrs["mean_ms"] == 100.0
        assert attrs["p95_ms"] == 100.0
        assert attrs["stdev_ms"] == 0.0  # Single value has no deviation
    
    def test_guardrail_plaintext_attempt(self, mock_config, manual_scheduler):
        """Test that plaintext access attempts raise GuardrailViolationError."""
        from lb3.monitors.keyboard import get_key_char, get_key_text
        
        # Test blocked functions
        with pytest.raises(GuardrailViolationError):
            get_key_char("test")
        
        with pytest.raises(GuardrailViolationError):
            get_key_text("test")
        
        # Test payload assertion
        fake_source = FakeKeyboardSource()
        
        with patch('lb3.monitors.keyboard.get_effective_config', return_value=mock_config):
            monitor = KeyboardMonitor(
                dry_run=True,
                event_source=fake_source,
                scheduler=manual_scheduler
            )
        
        # Should raise error if suspicious patterns detected
        with pytest.raises(GuardrailViolationError):
            monitor._assert_no_plaintext({"key_char": "a", "text": "hello"})
    
    def test_monitor_lifecycle(self, mock_config, fake_clock, manual_scheduler):
        """Test complete monitor lifecycle."""
        fake_source = FakeKeyboardSource()
        
        with patch('lb3.monitors.keyboard.get_effective_config', return_value=mock_config):
            monitor = KeyboardMonitor(
                dry_run=True,
                clock=fake_clock,
                event_source=fake_source,
                scheduler=manual_scheduler
            )
        
        assert not monitor._running
        
        # Start monitor
        monitor.start()
        assert monitor._running
        
        # Simulate some activity
        fake_clock.advance(1.0)
        fake_source.simulate_keydown()
        fake_source.simulate_keyup()
        
        # Stop monitor
        monitor.stop()
        assert not monitor._running
        
        # Should join cleanly
        assert monitor.join(timeout=1.0)
    
    def test_event_source_auto_selection(self, mock_config, manual_scheduler):
        """Test automatic event source selection based on test mode."""
        with patch('lb3.monitors.keyboard.get_effective_config', return_value=mock_config):
            # In test mode (LB3_TEST_MODE=1), should auto-select FakeKeyboardSource
            monitor = KeyboardMonitor(
                dry_run=True,
                scheduler=manual_scheduler
            )
            assert isinstance(monitor._event_source, FakeKeyboardSource)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])