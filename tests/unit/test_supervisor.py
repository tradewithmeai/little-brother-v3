"""Unit tests for monitor supervisor."""

import signal
import threading
import time
from unittest.mock import Mock, patch

import pytest

from lb3.supervisor import MonitorSupervisor, create_standard_supervisor


@pytest.mark.usefixtures("no_thread_leaks")
class TestMonitorSupervisor:
    """Unit tests for MonitorSupervisor."""

    def test_supervisor_initialization(self):
        """Test supervisor initializes correctly."""
        supervisor = MonitorSupervisor(dry_run=True, verbose=True)

        assert supervisor.dry_run is True
        assert supervisor.verbose is True
        assert not supervisor.is_running()
        assert not supervisor.is_shutdown_requested()
        assert len(supervisor._monitors) == 0

    def test_add_monitor_success(self):
        """Test adding monitor successfully."""
        supervisor = MonitorSupervisor(dry_run=True)

        # Mock monitor class
        mock_monitor_class = Mock()
        mock_monitor = Mock()
        mock_monitor_class.return_value = mock_monitor

        supervisor.add_monitor(mock_monitor_class, "test_monitor", param1="value1")

        # Verify monitor was created with correct parameters
        mock_monitor_class.assert_called_once_with(dry_run=True, param1="value1")

        # Verify monitor was added to lists
        assert len(supervisor._monitors) == 1
        assert supervisor._monitors[0] == mock_monitor

        status = supervisor.get_monitor_status()
        assert "test_monitor" in status
        assert status["test_monitor"]["started"] is False
        assert status["test_monitor"]["error"] is None

    def test_add_monitor_failure(self):
        """Test handling monitor creation failure."""
        supervisor = MonitorSupervisor(dry_run=True)

        # Mock monitor class that raises exception
        mock_monitor_class = Mock()
        mock_monitor_class.side_effect = Exception("Monitor creation failed")

        supervisor.add_monitor(mock_monitor_class, "failing_monitor")

        # Verify error was captured
        status = supervisor.get_monitor_status()
        assert "failing_monitor" in status
        assert status["failing_monitor"]["monitor"] is None
        assert status["failing_monitor"]["error"] == "Monitor creation failed"

    @patch("lb3.supervisor.get_event_bus")
    @patch("lb3.supervisor.SpoolerSink")
    def test_start_all_success(self, mock_spooler_sink_class, mock_get_event_bus):
        """Test starting all monitors successfully."""
        # Mock event bus and spooler
        mock_event_bus = Mock()
        mock_spooler_sink = Mock()
        mock_get_event_bus.return_value = mock_event_bus
        mock_spooler_sink_class.return_value = mock_spooler_sink

        supervisor = MonitorSupervisor(dry_run=False)  # Not dry-run to test event bus

        # Add mock monitors
        mock_monitor1 = Mock()
        mock_monitor2 = Mock()
        supervisor._monitors = [mock_monitor1, mock_monitor2]
        supervisor._monitor_status = {
            "monitor1": {
                "name": "monitor1",
                "monitor": mock_monitor1,
                "started": False,
                "error": None,
            },
            "monitor2": {
                "name": "monitor2",
                "monitor": mock_monitor2,
                "started": False,
                "error": None,
            },
        }

        results = supervisor.start_all()

        # Verify event bus was started
        mock_event_bus.start.assert_called_once()
        mock_event_bus.subscribe.assert_called_once_with(mock_spooler_sink)

        # Verify monitors were started
        mock_monitor1.start.assert_called_once()
        mock_monitor2.start.assert_called_once()

        # Verify results
        assert results["monitor1"] is True
        assert results["monitor2"] is True
        assert supervisor.is_running()

    def test_start_all_dry_run_no_event_bus(self):
        """Test starting monitors in dry-run mode doesn't start event bus."""
        supervisor = MonitorSupervisor(dry_run=True)

        # Add mock monitor
        mock_monitor = Mock()
        supervisor._monitors = [mock_monitor]
        supervisor._monitor_status = {
            "monitor1": {
                "name": "monitor1",
                "monitor": mock_monitor,
                "started": False,
                "error": None,
            }
        }

        results = supervisor.start_all()

        # Verify event bus was not started
        assert supervisor._event_bus is None
        assert supervisor._spooler_sink is None

        # Verify monitor was started
        mock_monitor.start.assert_called_once()
        assert results["monitor1"] is True

    def test_start_all_monitor_failure(self):
        """Test handling monitor start failure."""
        supervisor = MonitorSupervisor(dry_run=True)

        # Add mock monitors - one fails, one succeeds
        mock_monitor1 = Mock()
        mock_monitor1.start.side_effect = Exception("Start failed")
        mock_monitor2 = Mock()

        supervisor._monitors = [mock_monitor1, mock_monitor2]
        supervisor._monitor_status = {
            "failing_monitor": {
                "name": "failing_monitor",
                "monitor": mock_monitor1,
                "started": False,
                "error": None,
            },
            "working_monitor": {
                "name": "working_monitor",
                "monitor": mock_monitor2,
                "started": False,
                "error": None,
            },
        }

        results = supervisor.start_all()

        # Verify results
        assert results["failing_monitor"] is False
        assert results["working_monitor"] is True

        # Verify status updated
        status = supervisor.get_monitor_status()
        assert status["failing_monitor"]["error"] == "Start failed"
        assert status["working_monitor"]["error"] is None

    def test_stop_all(self):
        """Test stopping all monitors."""
        supervisor = MonitorSupervisor(dry_run=True)
        supervisor._running = True

        # Add mock monitors
        mock_monitor1 = Mock()
        mock_monitor2 = Mock()

        supervisor._monitor_status = {
            "monitor1": {
                "name": "monitor1",
                "monitor": mock_monitor1,
                "started": True,
                "error": None,
            },
            "monitor2": {
                "name": "monitor2",
                "monitor": mock_monitor2,
                "started": True,
                "error": None,
            },
        }

        supervisor.stop_all(timeout_seconds=1.0)

        # Verify monitors were stopped
        mock_monitor1.stop.assert_called_once()
        mock_monitor2.stop.assert_called_once()

        # Verify status updated
        status = supervisor.get_monitor_status()
        assert status["monitor1"]["started"] is False
        assert status["monitor2"]["started"] is False
        assert not supervisor.is_running()

    def test_stop_all_with_event_bus(self):
        """Test stopping with event bus cleanup."""
        supervisor = MonitorSupervisor(dry_run=False)
        supervisor._running = True

        # Mock event bus and spooler
        supervisor._event_bus = Mock()
        supervisor._spooler_sink = Mock()

        supervisor.stop_all(timeout_seconds=1.0)

        # Verify cleanup
        supervisor._spooler_sink.close.assert_called_once()
        supervisor._event_bus.stop.assert_called_once()
        assert not supervisor.is_running()

    def test_wait_for_threads_timeout(self):
        """Test bounded timeout for thread joins."""
        supervisor = MonitorSupervisor(dry_run=True)

        # Mock monitor with thread that doesn't stop
        mock_monitor = Mock()
        mock_thread = Mock()
        mock_thread.is_alive.return_value = True
        mock_monitor._thread = mock_thread

        supervisor._monitor_status = {
            "hanging_monitor": {
                "name": "hanging_monitor",
                "monitor": mock_monitor,
                "started": True,
                "error": None,
            }
        }

        # Should return quickly even with hanging thread
        start_time = time.time()
        supervisor._wait_for_threads(timeout_seconds=0.1)
        elapsed = time.time() - start_time

        # Should not hang - should respect timeout
        assert elapsed < 1.0  # Should finish much faster than 1 second
        mock_thread.join.assert_called()

    def test_request_shutdown(self):
        """Test shutdown request mechanism."""
        supervisor = MonitorSupervisor(dry_run=True)

        assert not supervisor.is_shutdown_requested()

        supervisor.request_shutdown()

        assert supervisor.is_shutdown_requested()

    def test_wait_until_shutdown_immediate(self):
        """Test wait_until_shutdown returns immediately when shutdown requested."""
        supervisor = MonitorSupervisor(dry_run=True)
        supervisor.request_shutdown()

        # Should return immediately
        start_time = time.time()
        supervisor.wait_until_shutdown(check_interval=0.1)
        elapsed = time.time() - start_time

        assert elapsed < 0.05  # Should return almost immediately

    def test_wait_until_shutdown_monitors_finished(self):
        """Test wait_until_shutdown returns when monitors finish naturally."""
        supervisor = MonitorSupervisor(dry_run=True)
        supervisor._running = True

        # Mock monitor that stops running
        mock_monitor = Mock()
        mock_monitor._running = True
        supervisor._monitor_status = {
            "monitor1": {
                "name": "monitor1",
                "monitor": mock_monitor,
                "started": True,
                "error": None,
            }
        }

        # Simulate monitor stopping after short delay
        def stop_after_delay():
            time.sleep(0.1)
            mock_monitor._running = False

        stop_thread = threading.Thread(target=stop_after_delay)
        stop_thread.start()

        # Should return when monitor stops
        start_time = time.time()
        supervisor.wait_until_shutdown(check_interval=0.05)
        elapsed = time.time() - start_time

        # Should return after monitor stops (around 0.1s)
        assert 0.05 <= elapsed <= 0.5

        stop_thread.join()

    def test_signal_handler_setup(self):
        """Test signal handlers are set up correctly."""
        with patch("signal.signal") as mock_signal:
            MonitorSupervisor(dry_run=True)

            # Verify signal handlers were registered
            assert mock_signal.call_count >= 2  # At least SIGINT and SIGTERM

            # Check SIGINT handler
            sigint_calls = [
                call
                for call in mock_signal.call_args_list
                if call[0][0] == signal.SIGINT
            ]
            assert len(sigint_calls) == 1

            # Check SIGTERM handler
            sigterm_calls = [
                call
                for call in mock_signal.call_args_list
                if call[0][0] == signal.SIGTERM
            ]
            assert len(sigterm_calls) == 1


class TestCreateStandardSupervisor:
    """Test standard supervisor factory function."""

    @patch("lb3.supervisor.HeartbeatMonitor")
    @patch("lb3.supervisor.ContextSnapshotMonitor")
    @patch("lb3.supervisor.KeyboardMonitor")
    @patch("lb3.supervisor.MouseMonitor")
    @patch("lb3.supervisor.FileWatchMonitor")
    @patch("lb3.supervisor.BrowserMonitor")
    def test_create_standard_supervisor_dry_run(self, *mock_monitors):
        """Test creating standard supervisor in dry-run mode."""
        supervisor = create_standard_supervisor(dry_run=True, duration=30)

        # Should have all standard monitors
        status = supervisor.get_monitor_status()
        expected_monitors = {
            "heartbeat",
            "context",
            "keyboard",
            "mouse",
            "file",
            "browser",
        }

        # On Windows, should also have window monitor
        import sys

        if sys.platform == "win32":
            expected_monitors.add("window")

        assert set(status.keys()) >= expected_monitors  # Allow for additional monitors

    @patch("lb3.supervisor.HeartbeatMonitor")
    @patch("lb3.supervisor.ContextSnapshotMonitor")
    @patch("lb3.supervisor.KeyboardMonitor")
    @patch("lb3.supervisor.MouseMonitor")
    @patch("lb3.supervisor.FileWatchMonitor")
    @patch("lb3.supervisor.BrowserMonitor")
    def test_create_standard_supervisor_production(self, *mock_monitors):
        """Test creating standard supervisor for production."""
        supervisor = create_standard_supervisor(dry_run=False, verbose=True)

        assert supervisor.dry_run is False
        assert supervisor.verbose is True

        # Should have monitors configured
        status = supervisor.get_monitor_status()
        assert len(status) >= 6  # At least the core monitors


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
