"""Integration tests for browser monitor with CDP and fallback modes."""

import json
from unittest.mock import Mock, patch

import pytest

from lb3.monitors.browser import BatchConfig, BrowserMonitor


@pytest.mark.usefixtures("no_thread_leaks")
class TestBrowserIntegration:
    """Integration tests for BrowserMonitor."""

    def test_fallback_mode_browser_switching(self, fake_clock, manual_scheduler):
        """Test fallback mode with multiple browser window switches."""
        collected_events = []

        def collect_event(event):
            collected_events.append(event)

        monitor = BrowserMonitor(
            dry_run=False,
            batch_config=BatchConfig(max_size=10, max_time_s=2.0),
            scheduler=manual_scheduler,
        )

        # Simulate browser window switching sequence
        window_sequence = [
            {
                "exe_name": "chrome.exe",
                "window_title": "Google Search - Google Chrome",
                "pid": 1001,
            },
            {
                "exe_name": "firefox.exe",
                "window_title": "Mozilla Firefox Start Page",
                "pid": 1002,
            },
            {
                "exe_name": "chrome.exe",
                "window_title": "GitHub - Google Chrome",
                "pid": 1001,
            },
            {"exe_name": "msedge.exe", "window_title": "Microsoft Edge", "pid": 1003},
        ]

        with patch("lb3.monitors.base.publish_event", side_effect=collect_event):
            monitor.start()

            for window_info in window_sequence:
                with patch.object(
                    monitor, "_get_active_window_info", return_value=window_info
                ):
                    monitor.run_monitor_cycle()
                    fake_clock.advance(0.5)

            # Trigger time flush
            fake_clock.advance(2.5)
            manual_scheduler.advance(2.5)

            monitor.stop()

        # Should have event for each browser window
        assert len(collected_events) == 4

        # Verify event sequence
        exe_names = [json.loads(e.attrs_json)["exe_name"] for e in collected_events]
        assert exe_names == ["chrome.exe", "firefox.exe", "chrome.exe", "msedge.exe"]

        # All should be fallback events
        for event in collected_events:
            assert event.monitor == "browser"
            assert event.action == "tab_switch"
            assert event.subject_type == "url"
            assert event.subject_id is None  # No URL in fallback

            attrs = json.loads(event.attrs_json)
            assert attrs["source"] == "fallback"
            assert "window_title_hash" in attrs
            assert attrs["window_title_present"] is True

    def test_fallback_mode_non_browser_filtering(self, manual_scheduler):
        """Test fallback mode filters out non-browser windows."""
        collected_events = []

        def collect_event(event):
            collected_events.append(event)

        monitor = BrowserMonitor(dry_run=False, scheduler=manual_scheduler)

        # Mix of browser and non-browser windows
        window_sequence = [
            {
                "exe_name": "notepad.exe",
                "window_title": "Untitled - Notepad",
                "pid": 2001,
            },
            {"exe_name": "chrome.exe", "window_title": "Google Chrome", "pid": 1001},
            {"exe_name": "calc.exe", "window_title": "Calculator", "pid": 2002},
            {"exe_name": "explorer.exe", "window_title": "File Explorer", "pid": 2003},
            {"exe_name": "firefox.exe", "window_title": "Firefox Browser", "pid": 1002},
        ]

        with patch("lb3.monitors.base.publish_event", side_effect=collect_event):
            monitor.start()

            for window_info in window_sequence:
                with patch.object(
                    monitor, "_get_active_window_info", return_value=window_info
                ):
                    monitor.run_monitor_cycle()

            monitor.stop()

        # Should only have events for browser windows (Chrome and Firefox)
        assert len(collected_events) == 2

        exe_names = [json.loads(e.attrs_json)["exe_name"] for e in collected_events]
        assert "chrome.exe" in exe_names
        assert "firefox.exe" in exe_names
        assert "notepad.exe" not in exe_names
        assert "calc.exe" not in exe_names
        assert "explorer.exe" not in exe_names

    def test_fallback_mode_different_window_states(self, fake_clock, manual_scheduler):
        """Test fallback mode handles different window state changes."""
        collected_events = []

        def collect_event(event):
            collected_events.append(event)

        monitor = BrowserMonitor(dry_run=False, scheduler=manual_scheduler)

        # Different window states to test proper change detection
        window_states = [
            {"exe_name": "chrome.exe", "window_title": "Page 1 - Chrome", "pid": 1001},
            {"exe_name": "chrome.exe", "window_title": "Page 2 - Chrome", "pid": 1001},
            {
                "exe_name": "firefox.exe",
                "window_title": "Page 1 - Firefox",
                "pid": 1002,
            },
        ]

        with patch("lb3.monitors.base.publish_event", side_effect=collect_event):
            monitor.start()

            for _i, window_state in enumerate(window_states):
                with patch.object(
                    monitor, "_get_active_window_info", return_value=window_state
                ):
                    monitor.run_monitor_cycle()
                    fake_clock.advance(1.0)

            monitor.stop()

        # Should have events for each different window state
        assert len(collected_events) == len(window_states)

        # Verify progression
        attrs1 = json.loads(collected_events[0].attrs_json)
        assert attrs1["exe_name"] == "chrome.exe"

        attrs2 = json.loads(collected_events[1].attrs_json)
        assert attrs2["exe_name"] == "chrome.exe"

        attrs3 = json.loads(collected_events[2].attrs_json)
        assert attrs3["exe_name"] == "firefox.exe"

        # All should be fallback events
        for event in collected_events:
            attrs = json.loads(event.attrs_json)
            assert attrs["source"] == "fallback"

    def test_cdp_plugin_integration_mock(self, manual_scheduler):
        """Test CDP plugin integration with mocked plugin."""
        collected_events = []

        def collect_event(event):
            collected_events.append(event)

        # Mock config to enable CDP
        with patch("lb3.monitors.browser.get_effective_config") as mock_config:
            config = Mock()
            config.browser.integration.chrome_remote_debug_port = 9222
            config.plugins.enabled = ["browser_cdp"]
            config.heartbeat.poll_intervals.browser = "2.0s"
            mock_config.return_value = config

            # Mock CDP plugin
            with patch("lb3.plugins.browser_cdp.BrowserCDPPlugin") as mock_cdp_class:
                mock_cdp_instance = Mock()
                mock_cdp_instance.is_available.return_value = True
                mock_cdp_instance._running = True

                # Mock CDP plugin emits events
                def mock_run_cycle():
                    if collected_events:  # Only emit once to avoid infinite loop
                        return
                    # Simulate CDP event
                    event_data = {
                        "action": "tab_open",
                        "subject_type": "url",
                        "subject_id": "test_url_id",
                        "attrs": {
                            "source": "cdp",
                            "targetId": "test_target_123",
                            "tab_title_present": True,
                        },
                    }
                    collected_events.append(
                        Mock(
                            monitor="browser",
                            action="tab_open",
                            subject_type="url",
                            subject_id="test_url_id",
                            attrs_json=json.dumps(event_data["attrs"]),
                        )
                    )

                mock_cdp_instance.run_monitor_cycle = mock_run_cycle
                mock_cdp_class.return_value = mock_cdp_instance

                monitor = BrowserMonitor(dry_run=False, scheduler=manual_scheduler)

                # Should have CDP plugin loaded
                assert monitor._cdp_plugin is not None

                monitor.start()

                # Run monitoring cycle (should use CDP mode)
                monitor.run_monitor_cycle()

                monitor.stop()

        # Should have CDP event
        assert len(collected_events) == 1

        event = collected_events[0]
        assert event.monitor == "browser"
        assert event.action == "tab_open"
        assert event.subject_type == "url"
        assert event.subject_id == "test_url_id"

        attrs = json.loads(event.attrs_json)
        assert attrs["source"] == "cdp"
        assert attrs["targetId"] == "test_target_123"
        assert attrs["tab_title_present"] is True

    def test_mixed_browser_types_comprehensive(self, fake_clock, manual_scheduler):
        """Test comprehensive browser type detection and event generation."""
        collected_events = []

        def collect_event(event):
            collected_events.append(event)

        monitor = BrowserMonitor(
            dry_run=False,
            batch_config=BatchConfig(max_size=20, max_time_s=3.0),
            scheduler=manual_scheduler,
        )

        # Comprehensive browser test scenarios
        browser_scenarios = [
            # Chrome variants
            {
                "exe_name": "chrome.exe",
                "window_title": "Google Search - Google Chrome",
                "pid": 1001,
            },
            {
                "exe_name": "chrome.exe",
                "window_title": "YouTube - Google Chrome",
                "pid": 1001,
            },
            # Firefox
            {"exe_name": "firefox.exe", "window_title": "Mozilla Firefox", "pid": 1002},
            {
                "exe_name": "firefox.exe",
                "window_title": "Reddit - Mozilla Firefox",
                "pid": 1002,
            },
            # Edge
            {"exe_name": "msedge.exe", "window_title": "Microsoft Edge", "pid": 1003},
            {
                "exe_name": "msedge.exe",
                "window_title": "LinkedIn - Microsoft Edge",
                "pid": 1003,
            },
            # Other browsers
            {"exe_name": "brave.exe", "window_title": "Brave Browser", "pid": 1004},
            {"exe_name": "opera.exe", "window_title": "Opera Browser", "pid": 1005},
        ]

        with patch("lb3.monitors.base.publish_event", side_effect=collect_event):
            monitor.start()

            for _i, scenario in enumerate(browser_scenarios):
                with patch.object(
                    monitor, "_get_active_window_info", return_value=scenario
                ):
                    monitor.run_monitor_cycle()
                    fake_clock.advance(0.2)  # Small delay between switches

            monitor.stop()

        # Should have events for all scenarios
        assert len(collected_events) == len(browser_scenarios)

        # Verify each browser type was detected
        exe_names_detected = set(
            json.loads(e.attrs_json)["exe_name"] for e in collected_events
        )
        expected_browsers = {
            "chrome.exe",
            "firefox.exe",
            "msedge.exe",
            "brave.exe",
            "opera.exe",
        }
        assert exe_names_detected == expected_browsers

        # All should be fallback events with proper structure
        for event in collected_events:
            assert event.monitor == "browser"
            assert event.action == "tab_switch"
            assert event.subject_type == "url"
            assert event.subject_id is None

            attrs = json.loads(event.attrs_json)
            assert attrs["source"] == "fallback"
            assert "exe_name" in attrs
            assert attrs["exe_name"] in expected_browsers
            assert "window_title_present" in attrs
            assert "window_title_hash" in attrs

    def test_batch_flushing_behavior(self, fake_clock, manual_scheduler):
        """Test browser monitor batching behavior."""
        collected_events = []

        def collect_event(event):
            collected_events.append(event)

        # Configure small batch size for testing
        batch_config = BatchConfig(max_size=3, max_time_s=5.0)
        monitor = BrowserMonitor(
            dry_run=False, batch_config=batch_config, scheduler=manual_scheduler
        )

        # Generate sequence of browser events
        browser_windows = [
            {"exe_name": "chrome.exe", "window_title": f"Tab {i} - Chrome", "pid": 1001}
            for i in range(5)  # More than batch size
        ]

        with patch("lb3.monitors.base.publish_event", side_effect=collect_event):
            monitor.start()

            for window_info in browser_windows:
                with patch.object(
                    monitor, "_get_active_window_info", return_value=window_info
                ):
                    monitor.run_monitor_cycle()
                    fake_clock.advance(0.1)

            # Trigger time-based flush
            fake_clock.advance(6.0)
            manual_scheduler.advance(6.0)

            monitor.stop()

        # Should have all events (may be in batches)
        assert len(collected_events) == 5

        # All should be properly structured browser events
        for event in collected_events:
            assert event.monitor == "browser"
            assert event.action == "tab_switch"
            attrs = json.loads(event.attrs_json)
            assert attrs["source"] == "fallback"
            assert attrs["exe_name"] == "chrome.exe"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
