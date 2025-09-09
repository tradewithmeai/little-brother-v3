"""Unit tests for browser monitor."""

import json
from unittest.mock import Mock, patch

import pytest

from lb3.monitors.browser import BrowserMonitor


@pytest.mark.usefixtures("no_thread_leaks")
class TestBrowserMonitor:
    """Unit tests for BrowserMonitor."""
    
    def test_monitor_name(self):
        """Test monitor name property."""
        monitor = BrowserMonitor(dry_run=True)
        assert monitor.name == "browser"
    
    def test_parse_interval(self):
        """Test interval parsing."""
        monitor = BrowserMonitor(dry_run=True)
        
        assert monitor._parse_interval("2.0s") == 2.0
        assert monitor._parse_interval("1.5s") == 1.5
        assert monitor._parse_interval("3.0") == 3.0
        assert monitor._parse_interval("invalid") == 2.0  # Fallback
    
    def test_browser_exe_classification(self):
        """Test browser executable classification."""
        monitor = BrowserMonitor(dry_run=True)
        
        known_browsers = monitor._known_browser_exes
        
        # Should recognize common browsers
        assert "chrome.exe" in known_browsers
        assert "firefox.exe" in known_browsers
        assert "msedge.exe" in known_browsers
        assert "brave.exe" in known_browsers
        
        # Should not recognize non-browsers
        assert "notepad.exe" not in known_browsers
        assert "calc.exe" not in known_browsers
    
    def test_cdp_plugin_loading_disabled(self):
        """Test CDP plugin loading when disabled."""
        with patch('lb3.monitors.browser.get_effective_config') as mock_config:
            # Mock config with CDP disabled
            config = Mock()
            config.browser.integration.chrome_remote_debug_port = 0
            config.plugins.enabled = []
            config.heartbeat.poll_intervals.browser = "2.0s"
            mock_config.return_value = config
            
            monitor = BrowserMonitor(dry_run=True)
            
            # CDP plugin should not be loaded
            assert monitor._cdp_plugin is None
    
    def test_cdp_plugin_loading_enabled_but_unavailable(self):
        """Test CDP plugin loading when enabled but Chrome not available."""
        with patch('lb3.monitors.browser.get_effective_config') as mock_config:
            # Mock config with CDP enabled
            config = Mock()
            config.browser.integration.chrome_remote_debug_port = 9222
            config.plugins.enabled = ["browser_cdp"]
            config.heartbeat.poll_intervals.browser = "2.0s"
            mock_config.return_value = config
            
            # Mock CDP plugin that's unavailable
            with patch('lb3.plugins.browser_cdp.BrowserCDPPlugin') as mock_cdp_class:
                mock_cdp_instance = Mock()
                mock_cdp_instance.is_available.return_value = False
                mock_cdp_class.return_value = mock_cdp_instance
                
                monitor = BrowserMonitor(dry_run=True)
                
                # CDP plugin should be None since it's unavailable
                assert monitor._cdp_plugin is None
    
    def test_fallback_window_detection_non_browser(self, manual_scheduler):
        """Test fallback mode ignores non-browser windows.""" 
        collected_events = []
        
        def collect_event(event):
            collected_events.append(event)
        
        monitor = BrowserMonitor(
            dry_run=False,
            scheduler=manual_scheduler
        )
        
        # Mock active window to return non-browser
        mock_window_info = {
            'exe_name': 'notepad.exe',
            'window_title': 'Untitled - Notepad',
            'pid': 12345
        }
        
        with patch('lb3.monitors.base.publish_event', side_effect=collect_event):
            with patch.object(monitor, '_get_active_window_info', return_value=mock_window_info):
                monitor.start()
                
                # Run monitoring cycle  
                monitor.run_monitor_cycle()
                
                monitor.stop()
        
        # Should not emit events for non-browser windows
        assert len(collected_events) == 0
    
    def test_fallback_window_detection_browser(self, manual_scheduler):
        """Test fallback mode detects browser windows."""
        collected_events = []
        
        def collect_event(event):
            collected_events.append(event)
        
        monitor = BrowserMonitor(
            dry_run=False,
            scheduler=manual_scheduler
        )
        
        # Mock active window to return browser
        mock_window_info = {
            'exe_name': 'chrome.exe',
            'window_title': 'Google - Google Chrome',
            'pid': 12345
        }
        
        with patch('lb3.monitors.base.publish_event', side_effect=collect_event):
            with patch.object(monitor, '_get_active_window_info', return_value=mock_window_info):
                monitor.start()
                
                # Run monitoring cycle
                monitor.run_monitor_cycle()
                
                monitor.stop()
        
        # Should emit browser event
        assert len(collected_events) == 1
        
        event = collected_events[0]
        assert event.monitor == "browser"
        assert event.action == "tab_switch"
        assert event.subject_type == "url"
        assert event.subject_id is None  # No URL in fallback mode
        
        # Check attrs
        attrs = json.loads(event.attrs_json)
        assert attrs["source"] == "fallback"
        assert attrs["exe_name"] == "chrome.exe"
        assert attrs["window_title_present"] is True
        assert "window_title_hash" in attrs
    
    def test_fallback_window_change_detection(self, fake_clock, manual_scheduler):
        """Test fallback mode detects window changes correctly."""
        collected_events = []
        
        def collect_event(event):
            collected_events.append(event)
        
        monitor = BrowserMonitor(
            dry_run=False,
            scheduler=manual_scheduler
        )
        
        # Different windows to test change detection
        window_sequence = [
            {'exe_name': 'firefox.exe', 'window_title': 'Mozilla Firefox', 'pid': 12345},
            {'exe_name': 'firefox.exe', 'window_title': 'Mozilla Firefox', 'pid': 12345},  # Same - should be skipped
            {'exe_name': 'chrome.exe', 'window_title': 'Google Chrome', 'pid': 12346},  # Different - should emit
        ]
        
        with patch('lb3.monitors.base.publish_event', side_effect=collect_event):
            monitor.start()
            
            # First detection - should create event
            with patch.object(monitor, '_get_active_window_info', return_value=window_sequence[0]):
                monitor.run_monitor_cycle()
            fake_clock.advance(1.0)
            
            # Second detection - same window, should be skipped
            with patch.object(monitor, '_get_active_window_info', return_value=window_sequence[1]):
                monitor.run_monitor_cycle()
            fake_clock.advance(1.0)
            
            # Third detection - different window, should create event
            with patch.object(monitor, '_get_active_window_info', return_value=window_sequence[2]):
                monitor.run_monitor_cycle()
            
            monitor.stop()
        
        # Should have 2 events: first Firefox, then Chrome
        assert len(collected_events) == 2
        
        # First event should be Firefox
        attrs1 = json.loads(collected_events[0].attrs_json)
        assert attrs1["exe_name"] == "firefox.exe"
        
        # Second event should be Chrome
        attrs2 = json.loads(collected_events[1].attrs_json)
        assert attrs2["exe_name"] == "chrome.exe"
        
        for event in collected_events:
            assert event.action == "tab_switch"
            attrs = json.loads(event.attrs_json)
            assert attrs["source"] == "fallback"
    
    def test_fallback_different_browsers(self, manual_scheduler):
        """Test fallback mode handles different browser types."""
        collected_events = []
        
        def collect_event(event):
            collected_events.append(event)
        
        monitor = BrowserMonitor(
            dry_run=False,
            scheduler=manual_scheduler
        )
        
        # Test different browsers
        browser_windows = [
            {'exe_name': 'chrome.exe', 'window_title': 'Google Chrome', 'pid': 1},
            {'exe_name': 'firefox.exe', 'window_title': 'Mozilla Firefox', 'pid': 2},  
            {'exe_name': 'msedge.exe', 'window_title': 'Microsoft Edge', 'pid': 3},
            {'exe_name': 'brave.exe', 'window_title': 'Brave Browser', 'pid': 4}
        ]
        
        with patch('lb3.monitors.base.publish_event', side_effect=collect_event):
            monitor.start()
            
            for window_info in browser_windows:
                with patch.object(monitor, '_get_active_window_info', return_value=window_info):
                    monitor.run_monitor_cycle()
            
            monitor.stop()
        
        # Should have events for each browser
        assert len(collected_events) == 4
        
        exe_names = [json.loads(e.attrs_json)["exe_name"] for e in collected_events]
        assert "chrome.exe" in exe_names
        assert "firefox.exe" in exe_names
        assert "msedge.exe" in exe_names
        assert "brave.exe" in exe_names
    
    @patch('lb3.monitors.browser.hash_str')
    def test_window_title_hashing_invoked(self, mock_hash_str, manual_scheduler):
        """Test window title hashing is invoked."""
        mock_hash_str.return_value = "mocked_hash"
        
        collected_events = []
        
        def collect_event(event):
            collected_events.append(event)
        
        monitor = BrowserMonitor(
            dry_run=False,
            scheduler=manual_scheduler
        )
        
        mock_window_info = {
            'exe_name': 'chrome.exe',
            'window_title': 'Test Page - Google Chrome',
            'pid': 12345
        }
        
        with patch('lb3.monitors.base.publish_event', side_effect=collect_event):
            with patch.object(monitor, '_get_active_window_info', return_value=mock_window_info):
                monitor.start()
                monitor.run_monitor_cycle()
                monitor.stop()
        
        # Verify hash_str was called with window title
        hash_calls = [call for call in mock_hash_str.call_args_list 
                      if len(call[0]) >= 2 and call[0][1] == "window_title"]
        assert len(hash_calls) >= 1
        
        # Verify first call was with our window title
        assert hash_calls[0][0][0] == "Test Page - Google Chrome"
    
    def test_no_plaintext_title_leakage(self, manual_scheduler):
        """Test no plaintext window titles appear in events."""
        collected_events = []
        
        def collect_event(event):
            collected_events.append(event)
        
        monitor = BrowserMonitor(
            dry_run=False,
            scheduler=manual_scheduler
        )
        
        secret_title = "Confidential Bank Statement - Chrome"
        mock_window_info = {
            'exe_name': 'chrome.exe',
            'window_title': secret_title,
            'pid': 12345
        }
        
        with patch('lb3.monitors.base.publish_event', side_effect=collect_event):
            with patch.object(monitor, '_get_active_window_info', return_value=mock_window_info):
                monitor.start()
                monitor.run_monitor_cycle()
                monitor.stop()
        
        assert len(collected_events) == 1
        
        # Check event for plaintext leakage
        event = collected_events[0]
        event_dict = event.to_dict()
        event_str = json.dumps(event_dict)
        
        # Should not contain plaintext title components
        assert "Confidential" not in event_str
        assert "Bank Statement" not in event_str
        assert secret_title not in event_str
    
    def test_empty_window_title_handling(self, manual_scheduler):
        """Test handling of empty window titles."""
        collected_events = []
        
        def collect_event(event):
            collected_events.append(event)
        
        monitor = BrowserMonitor(
            dry_run=False,
            scheduler=manual_scheduler
        )
        
        mock_window_info = {
            'exe_name': 'firefox.exe',
            'window_title': '',  # Empty title
            'pid': 12345
        }
        
        with patch('lb3.monitors.base.publish_event', side_effect=collect_event):
            with patch.object(monitor, '_get_active_window_info', return_value=mock_window_info):
                monitor.start()
                monitor.run_monitor_cycle()
                monitor.stop()
        
        assert len(collected_events) == 1
        
        event = collected_events[0]
        attrs = json.loads(event.attrs_json)
        assert attrs["window_title_present"] is False
        assert "window_title_hash" not in attrs  # No hash for empty title
    
    def test_batch_configuration_from_config(self):
        """Test batch configuration uses browser poll interval from config."""
        with patch('lb3.monitors.browser.get_effective_config') as mock_config:
            config = Mock()
            config.browser.integration.chrome_remote_debug_port = 0
            config.plugins.enabled = []
            config.heartbeat.poll_intervals.browser = "3.5s"
            mock_config.return_value = config
            
            monitor = BrowserMonitor(dry_run=True)
            
            # Should use configured interval
            assert monitor.batch_config.max_time_s == 3.5
    
    def test_cdp_mode_availability_check(self):
        """Test CDP mode availability checking."""
        with patch('lb3.monitors.browser.get_effective_config') as mock_config:
            config = Mock()
            config.browser.integration.chrome_remote_debug_port = 9222
            config.plugins.enabled = ["browser_cdp"]
            config.heartbeat.poll_intervals.browser = "2.0s"
            mock_config.return_value = config
            
            # Mock CDP plugin as available
            with patch('lb3.plugins.browser_cdp.BrowserCDPPlugin') as mock_cdp_class:
                mock_cdp_instance = Mock()
                mock_cdp_instance.is_available.return_value = True
                mock_cdp_class.return_value = mock_cdp_instance
                
                monitor = BrowserMonitor(dry_run=True)
                
                # CDP plugin should be loaded
                assert monitor._cdp_plugin is not None
                assert monitor._cdp_plugin.is_available.return_value is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])