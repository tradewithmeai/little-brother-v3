"""Integration tests for keyboard dynamics monitor with deterministic scheduling."""

import builtins
import contextlib
import json
import threading
from unittest.mock import Mock, patch

import pytest

from lb3.events import get_event_bus
from lb3.monitors.keyboard import BatchConfig, FakeKeyboardSource, KeyboardMonitor


@pytest.mark.usefixtures("no_thread_leaks")
class TestKeyboardIntegration:
    """Integration tests for KeyboardMonitor with event bus."""

    def setup_method(self):
        """Set up test environment."""
        self.monitor = None
        self.event_bus = None
        self.received_events = []

    def teardown_method(self):
        """Clean up test environment."""
        if self.monitor:
            try:
                self.monitor.stop()
                self.monitor.join()
            except Exception:
                pass

        if self.event_bus:
            with contextlib.suppress(builtins.BaseException):
                self.event_bus.stop()

    def _setup_event_collection(self):
        """Set up event bus for collecting emitted events."""
        self.event_bus = get_event_bus()

        def event_collector(event):
            self.received_events.append(event)

        self.event_bus.subscribe(event_collector)
        self.event_bus.start()

    def _get_keyboard_events(self):
        """Get keyboard events from received events."""
        return [
            e
            for e in self.received_events
            if e.monitor == "keyboard" and e.action == "stats"
        ]

    @pytest.fixture
    def mock_config(self):
        """Mock configuration with guardrails enabled."""
        config = Mock()
        config.guardrails.no_global_text_keylogging = True
        return config

    def test_keyboard_monitor_dry_run_lifecycle(self, mock_config, manual_scheduler):
        """Test keyboard monitor lifecycle in dry-run mode."""
        fake_source = FakeKeyboardSource()

        with patch(
            "lb3.monitors.keyboard.get_effective_config", return_value=mock_config
        ):
            self.monitor = KeyboardMonitor(
                dry_run=True, event_source=fake_source, scheduler=manual_scheduler
            )

        assert self.monitor.name == "keyboard"
        assert not self.monitor._running

        # Start monitor
        self.monitor.start()
        assert self.monitor._running

        # Stop monitor
        self.monitor.stop()
        assert not self.monitor._running

        # Should join cleanly
        assert self.monitor.join()

    def test_simulated_typing_events(self, mock_config, fake_clock, manual_scheduler):
        """Test keyboard monitor with simulated typing events."""
        # Setup event collection
        self._setup_event_collection()

        fake_source = FakeKeyboardSource()

        with patch(
            "lb3.monitors.keyboard.get_effective_config", return_value=mock_config
        ):
            self.monitor = KeyboardMonitor(
                dry_run=False,
                clock=fake_clock,
                event_source=fake_source,
                scheduler=manual_scheduler,
            )

        self.monitor.start()

        # Clear any initial events
        self.received_events.clear()

        # Simulate typing pattern: "hello world" with realistic timing
        typing_pattern = [
            ("press", 0.0),  # h
            ("release", 0.05),
            ("press", 0.15),  # e
            ("release", 0.20),
            ("press", 0.35),  # l
            ("release", 0.40),
            ("press", 0.50),  # l
            ("release", 0.55),
            ("press", 0.70),  # o
            ("release", 0.75),
            ("press", 1.20),  # space (pause)
            ("release", 1.25),
            ("press", 1.40),  # w
            ("release", 1.45),
            ("press", 1.55),  # o
            ("release", 1.60),
            ("press", 1.70),  # r
            ("release", 1.75),
            ("press", 1.85),  # l
            ("release", 1.90),
            ("press", 1.95),  # d
            ("release", 2.00),
        ]

        for event_type, delay in typing_pattern:
            fake_clock.advance(delay - fake_clock())

            if event_type == "press":
                fake_source.simulate_keydown()
            else:
                fake_source.simulate_keyup()

        # Force flush stats
        self.monitor._flush_stats()

        # Check that we got keyboard stats events
        keyboard_events = self._get_keyboard_events()
        assert (
            len(keyboard_events) >= 1
        ), f"Expected at least 1 keyboard event, got {len(keyboard_events)}"

        event = keyboard_events[0]
        assert event.monitor == "keyboard"
        assert event.action == "stats"
        assert event.subject_type == "none"
        assert event.subject_id is None

        # Parse attrs_json and verify exact schema
        attrs = json.loads(event.attrs_json)
        required_keys = {"keydown", "keyup", "mean_ms", "p95_ms", "stdev_ms", "bursts"}
        assert set(attrs.keys()) == required_keys

        # Verify exact field types
        assert isinstance(attrs["keydown"], int)
        assert isinstance(attrs["keyup"], int)
        assert isinstance(attrs["mean_ms"], (int, float))
        assert isinstance(attrs["p95_ms"], (int, float))
        assert isinstance(attrs["stdev_ms"], (int, float))
        assert isinstance(attrs["bursts"], int)

        # Verify realistic values
        assert attrs["keydown"] == 11  # 11 key presses
        assert attrs["keyup"] == 11  # 11 key releases
        assert attrs["mean_ms"] > 0  # Should have calculated intervals
        assert attrs["p95_ms"] >= attrs["mean_ms"]  # P95 >= mean
        assert attrs["stdev_ms"] >= 0  # Standard deviation non-negative
        assert attrs["bursts"] >= 0  # Burst count non-negative

        # Verify no plaintext data anywhere
        event_dict = event.to_dict()
        event_str = str(event_dict)
        assert "hello" not in event_str.lower()
        assert "world" not in event_str.lower()

    def test_burst_detection_integration(
        self, mock_config, fake_clock, manual_scheduler
    ):
        """Test burst detection with realistic rapid typing."""
        # Setup event collection
        self._setup_event_collection()

        fake_source = FakeKeyboardSource()

        with patch(
            "lb3.monitors.keyboard.get_effective_config", return_value=mock_config
        ):
            self.monitor = KeyboardMonitor(
                dry_run=False,
                clock=fake_clock,
                event_source=fake_source,
                scheduler=manual_scheduler,
            )

        self.monitor.start()
        self.received_events.clear()

        # Simulate rapid typing burst (10 keys in 300ms)
        for _i in range(10):
            fake_clock.advance(0.03)  # 30ms between keys
            fake_source.simulate_keydown()

        # Force flush to get the event
        self.monitor._flush_stats()

        keyboard_events = self._get_keyboard_events()
        assert len(keyboard_events) >= 1

        attrs = json.loads(keyboard_events[0].attrs_json)
        assert attrs["keydown"] == 10
        assert attrs["bursts"] >= 1  # Should detect at least one burst

    def test_statistical_accuracy_integration(self, mock_config, manual_scheduler):
        """Test statistical calculations with controlled intervals."""
        fake_source = FakeKeyboardSource()

        with patch(
            "lb3.monitors.keyboard.get_effective_config", return_value=mock_config
        ):
            self.monitor = KeyboardMonitor(
                dry_run=True, event_source=fake_source, scheduler=manual_scheduler
            )

        # Manually set up known intervals for verification
        known_intervals = [50.0, 100.0, 150.0, 200.0, 250.0]  # ms
        self.monitor._stats.intervals = known_intervals.copy()
        self.monitor._stats.keydown_count = 5
        self.monitor._stats.keyup_count = 5
        self.monitor._stats.burst_count = 0

        attrs = self.monitor._stats.to_attrs_dict()

        # Verify calculations
        assert attrs["mean_ms"] == 150.0  # Average
        assert attrs["keydown"] == 5
        assert attrs["keyup"] == 5
        assert attrs["bursts"] == 0

        # P95 should be high (95% of [50,100,150,200,250] ≈ 240)
        assert 230 <= attrs["p95_ms"] <= 250

        # Standard deviation should be reasonable
        assert 70 <= attrs["stdev_ms"] <= 90

    def test_batch_flushing_time_based(self, mock_config, fake_clock, manual_scheduler):
        """Test time-based batch flushing."""
        # Setup event collection
        self._setup_event_collection()

        fake_source = FakeKeyboardSource()
        batch_config = BatchConfig(max_size=999, max_time_s=0.5)  # 500ms for testing

        with patch(
            "lb3.monitors.keyboard.get_effective_config", return_value=mock_config
        ):
            self.monitor = KeyboardMonitor(
                dry_run=False,
                clock=fake_clock,
                batch_config=batch_config,
                event_source=fake_source,
                scheduler=manual_scheduler,
            )

        self.monitor.start()
        self.received_events.clear()

        # Generate some activity
        fake_source.simulate_keydown()
        fake_source.simulate_keydown()

        # Advance time to trigger time-based flush
        fake_clock.advance(0.6)  # Past the 500ms threshold
        manual_scheduler.advance(0.6)

        keyboard_events = self._get_keyboard_events()
        assert len(keyboard_events) >= 1, "Expected time-based flush"

        attrs = json.loads(keyboard_events[0].attrs_json)
        assert attrs["keydown"] == 2

    def test_batch_flushing_size_based(self, mock_config, manual_scheduler):
        """Test size-based batch flushing."""
        # Setup event collection
        self._setup_event_collection()

        fake_source = FakeKeyboardSource()
        batch_config = BatchConfig(max_size=5, max_time_s=999.0)  # Size-based only

        with patch(
            "lb3.monitors.keyboard.get_effective_config", return_value=mock_config
        ):
            self.monitor = KeyboardMonitor(
                dry_run=False,
                batch_config=batch_config,
                event_source=fake_source,
                scheduler=manual_scheduler,
            )

        self.monitor.start()
        self.received_events.clear()

        # Generate events up to threshold
        for _i in range(5):
            fake_source.simulate_keydown()

        keyboard_events = self._get_keyboard_events()
        assert len(keyboard_events) >= 1, "Expected size-based flush"

        attrs = json.loads(keyboard_events[0].attrs_json)
        assert attrs["keydown"] == 5

    def test_performance_under_load(self, mock_config, manual_scheduler):
        """Test keyboard monitor performance under sustained typing."""
        fake_source = FakeKeyboardSource()

        with patch(
            "lb3.monitors.keyboard.get_effective_config", return_value=mock_config
        ):
            self.monitor = KeyboardMonitor(
                dry_run=True, event_source=fake_source, scheduler=manual_scheduler
            )

        # Simulate sustained typing
        import time

        start_time = time.time()

        for _i in range(1000):  # 1000 key events
            fake_source.simulate_keydown()
            fake_source.simulate_keyup()

        end_time = time.time()

        # Should complete quickly (< 1 second for 1000 events)
        duration = end_time - start_time
        assert (
            duration < 1.0
        ), f"Keyboard monitor too slow: {duration:.2f}s for 1000 events"

        # Verify final stats
        assert self.monitor._stats.keydown_count == 1000
        assert self.monitor._stats.keyup_count == 1000
        assert len(self.monitor._stats.intervals) == 999  # 1000 keys = 999 intervals

    def test_concurrent_access_safety(self, mock_config, manual_scheduler):
        """Test thread safety under concurrent access."""
        fake_source = FakeKeyboardSource()

        with patch(
            "lb3.monitors.keyboard.get_effective_config", return_value=mock_config
        ):
            self.monitor = KeyboardMonitor(
                dry_run=True, event_source=fake_source, scheduler=manual_scheduler
            )

        errors = []

        def typing_worker():
            try:
                for _i in range(100):
                    fake_source.simulate_keydown()
            except Exception as e:
                errors.append(e)

        # Start multiple typing threads
        threads = [threading.Thread(target=typing_worker) for _ in range(5)]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # Should have no errors
        assert len(errors) == 0, f"Thread safety errors: {errors}"

        # Should have correct total count
        assert self.monitor._stats.keydown_count == 500  # 5 threads * 100 keys each

    def test_zero_division_safety(self, mock_config, manual_scheduler):
        """Test zero division safety in statistical calculations."""
        fake_source = FakeKeyboardSource()

        with patch(
            "lb3.monitors.keyboard.get_effective_config", return_value=mock_config
        ):
            self.monitor = KeyboardMonitor(
                dry_run=True, event_source=fake_source, scheduler=manual_scheduler
            )

        # Test with no data
        attrs = self.monitor._stats.to_attrs_dict()
        assert attrs["mean_ms"] == 0.0
        assert attrs["p95_ms"] == 0.0
        assert attrs["stdev_ms"] == 0.0

        # Test with single interval
        self.monitor._stats.intervals = [100.0]
        self.monitor._stats.keydown_count = 2
        attrs = self.monitor._stats.to_attrs_dict()

        assert attrs["mean_ms"] == 100.0
        assert attrs["p95_ms"] == 100.0
        assert attrs["stdev_ms"] == 0.0  # Single value has no deviation

    def test_end_to_end_typing_simulation(
        self, mock_config, fake_clock, manual_scheduler
    ):
        """Test end-to-end typing simulation with ~5s of activity via advance()."""
        # Setup event collection
        self._setup_event_collection()

        fake_source = FakeKeyboardSource()
        batch_config = BatchConfig(max_size=999, max_time_s=1.0)  # 1 second intervals

        with patch(
            "lb3.monitors.keyboard.get_effective_config", return_value=mock_config
        ):
            self.monitor = KeyboardMonitor(
                dry_run=False,
                clock=fake_clock,
                batch_config=batch_config,
                event_source=fake_source,
                scheduler=manual_scheduler,
            )

        self.monitor.start()
        self.received_events.clear()

        # Simulate 5 seconds of varied typing activity
        total_keydowns = 0
        total_keyups = 0

        for second in range(5):
            # Different typing patterns per second
            if second % 2 == 0:
                # Moderate typing: 10 keys per second
                for _i in range(10):
                    fake_clock.advance(0.1)
                    fake_source.simulate_keydown()
                    total_keydowns += 1
                    fake_source.simulate_keyup()
                    total_keyups += 1
            else:
                # Burst typing: 20 keys in 0.5s, then pause
                for _i in range(20):
                    fake_clock.advance(0.025)
                    fake_source.simulate_keydown()
                    total_keydowns += 1

                # Pause for 0.5s
                fake_clock.advance(0.5)

        # Force periodic flushes by advancing scheduler time
        for _flush_cycle in range(5):
            fake_clock.advance(1.1)  # Advance past flush interval
            manual_scheduler.advance(1.1)

        # Should have multiple flush events
        keyboard_events = self._get_keyboard_events()
        assert len(keyboard_events) >= 1, "Expected periodic time-based flushes"

        # Verify final event has proper schema
        final_event = keyboard_events[-1]
        attrs = json.loads(final_event.attrs_json)

        # Exact schema compliance
        expected_keys = {"keydown", "keyup", "mean_ms", "p95_ms", "stdev_ms", "bursts"}
        assert set(attrs.keys()) == expected_keys

        # Type verification
        assert isinstance(attrs["keydown"], int)
        assert isinstance(attrs["keyup"], int)
        assert isinstance(attrs["mean_ms"], (int, float))
        assert isinstance(attrs["p95_ms"], (int, float))
        assert isinstance(attrs["stdev_ms"], (int, float))
        assert isinstance(attrs["bursts"], int)

        # Should have detected bursts due to rapid typing patterns
        total_events_processed = sum(
            json.loads(e.attrs_json)["keydown"] for e in keyboard_events
        )
        assert total_events_processed > 0

        print(f"Sample keyboard event with exact attrs schema: {attrs}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
