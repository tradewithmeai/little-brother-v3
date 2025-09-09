"""Integration tests for CLI run command."""

import json
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from lb3.cli import app


class TestCliRun:
    """Test CLI run command integration."""
    
    def test_run_dry_run_help(self):
        """Test run command help."""
        runner = CliRunner()
        result = runner.invoke(app, ['run', '--help'])
        
        assert result.exit_code == 0
        assert '--dry-run' in result.output
        assert '--duration' in result.output
    
    def test_run_dry_run_basic(self):
        """Test basic dry-run functionality."""
        runner = CliRunner()
        result = runner.invoke(app, ['run', '--dry-run', '--duration', '1'])
        
        assert result.exit_code == 0
        assert '[DRY-RUN]' in result.output
        assert 'Starting heartbeat monitor' in result.output
        assert 'Events will be printed to console' in result.output
        assert 'Monitoring stopped' in result.output
    
    def test_run_dry_run_events_printed(self):
        """Test that events are actually printed in dry-run mode."""
        runner = CliRunner()
        result = runner.invoke(app, ['run', '--dry-run', '--duration', '2'])
        
        assert result.exit_code == 0
        
        # Should contain heartbeat events
        assert '[heartbeat] heartbeat at' in result.output
        
        # Parse the JSON events from output
        lines = result.output.split('\\n')
        event_lines = [line for line in lines if line.startswith('[heartbeat]')]
        
        # Should have at least one event
        assert len(event_lines) >= 1
        
        # Check event structure
        for event_line in event_lines:
            # Extract JSON part after the timestamp
            json_start = event_line.find('{')
            if json_start != -1:
                json_str = event_line[json_start:]
                event_data = json.loads(json_str)
                
                # Verify event structure
                assert 'id' in event_data
                assert 'ts_utc' in event_data
                assert event_data['monitor'] == 'heartbeat'
                assert event_data['action'] == 'heartbeat'
                assert event_data['subject_type'] == 'none'
                assert 'session_id' in event_data
                assert 'attrs_json' in event_data
                
                # Check attrs_json content
                attrs = json.loads(event_data['attrs_json'])
                assert 'beat_number' in attrs
                assert 'interval' in attrs
                assert attrs['interval'] == 1.0
    
    def test_run_dry_run_custom_duration(self):
        """Test dry-run with custom duration."""
        runner = CliRunner()
        
        # Test with very short duration
        start_time = time.time()
        result = runner.invoke(app, ['run', '--dry-run', '--duration', '1'])
        end_time = time.time()
        
        assert result.exit_code == 0
        
        # Should complete in roughly the specified duration + some buffer
        elapsed = end_time - start_time
        assert 1.0 <= elapsed <= 3.0  # Allow some buffer for startup/cleanup
    
    def test_run_dry_run_event_ordering(self):
        """Test that events are printed in correct order."""
        runner = CliRunner()
        result = runner.invoke(app, ['run', '--dry-run', '--duration', '3'])
        
        assert result.exit_code == 0
        
        # Extract beat numbers from events
        lines = result.output.split('\\n')
        beat_numbers = []
        
        for line in lines:
            if '[heartbeat] heartbeat at' in line:
                json_start = line.find('{')
                if json_start != -1:
                    json_str = line[json_start:]
                    try:
                        event_data = json.loads(json_str)
                        attrs = json.loads(event_data['attrs_json'])
                        beat_numbers.append(attrs['beat_number'])
                    except (json.JSONDecodeError, KeyError):
                        pass
        
        # Should have at least 2 beats in 3 seconds
        assert len(beat_numbers) >= 2
        
        # Beat numbers should be sequential starting from 1
        for i, beat_num in enumerate(beat_numbers):
            assert beat_num == i + 1
    
    @patch('lb3.cli.get_event_bus')
    @patch('lb3.cli.SpoolerSink')
    def test_run_normal_mode_setup(self, mock_spooler_sink, mock_get_bus):
        """Test that normal mode sets up event bus and spooler correctly."""
        mock_bus = mock_get_bus.return_value
        mock_sink = mock_spooler_sink.return_value
        
        runner = CliRunner()
        
        # Use timeout to avoid infinite run
        with patch('signal.signal'):  # Mock signal handling
            with patch('time.sleep', side_effect=KeyboardInterrupt):  # Interrupt quickly
                result = runner.invoke(app, ['run'])
        
        # Should have started bus and added sink
        mock_bus.start.assert_called_once()
        mock_bus.subscribe.assert_called_once_with(mock_sink)
        mock_bus.stop.assert_called_once()
    
    def test_run_error_handling(self):
        """Test error handling in run command."""
        runner = CliRunner()
        
        # Test with invalid duration (should still work as it defaults)
        result = runner.invoke(app, ['run', '--dry-run', '--duration', '0'])
        
        # Even with 0 duration, should start but finish quickly
        assert result.exit_code == 0
        assert '[DRY-RUN]' in result.output
    
    def test_run_creates_spool_files(self):
        """Test that normal run creates spool files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock config to use temp directory
            with patch('lb3.config.Config.get_config_path') as mock_path:
                config_path = Path(temp_dir) / 'config.yaml'
                mock_path.return_value = config_path
                
                # Create minimal config
                config_content = f'''
time_zone_handling: "UTC_store_only"
storage:
  sqlite_path: "{temp_dir}/local.db"
  spool_dir: "{temp_dir}/spool"
'''
                
                config_path.parent.mkdir(parents=True, exist_ok=True)
                config_path.write_text(config_content)
                
                runner = CliRunner()
                
                # Run for short time with timeout
                with patch('time.sleep') as mock_sleep:
                    # Make sleep raise KeyboardInterrupt after first call
                    mock_sleep.side_effect = [None, KeyboardInterrupt()]
                    
                    result = runner.invoke(app, ['run'])
                
                # Should have started successfully
                assert 'Starting monitoring system' in result.output
                
                # Check if spool directory structure was created
                spool_dir = Path(temp_dir) / 'spool' / 'heartbeat'
                assert spool_dir.exists(), f"Spool directory not created: {spool_dir}"
    
    def test_run_version_accessible(self):
        """Test that version command works alongside run."""
        runner = CliRunner()
        result = runner.invoke(app, ['version'])
        
        assert result.exit_code == 0
        assert 'Little Brother v3' in result.output
    
    def test_run_with_config_show(self):
        """Test that config commands work alongside run."""
        runner = CliRunner()
        result = runner.invoke(app, ['config', 'show'])
        
        assert result.exit_code == 0
        # Should show YAML config
        assert 'time_zone_handling:' in result.output or 'storage:' in result.output


class TestHeartbeatMonitorIntegration:
    """Test HeartbeatMonitor integration through CLI."""
    
    def test_heartbeat_monitor_lifecycle(self):
        """Test heartbeat monitor complete lifecycle through CLI."""
        from lb3.monitors.heartbeat import HeartbeatMonitor
        
        # Test creating monitor directly
        monitor = HeartbeatMonitor(dry_run=True, interval=0.5, total_beats=3)
        
        captured_events = []
        
        # Override print method to capture events
        original_print = monitor._print_events
        
        def capture_events(events):
            captured_events.extend(events)
        
        monitor._print_events = capture_events
        
        # Test lifecycle
        assert not monitor._running
        
        monitor.start()
        assert monitor._running
        
        # Wait for monitor to complete
        start_time = time.time()
        while monitor._running and (time.time() - start_time) < 5:
            time.sleep(0.1)
        
        monitor.stop()
        assert not monitor._running
        
        # Should have captured 3 events
        assert len(captured_events) == 3
        
        # Events should be in order
        for i, event in enumerate(captured_events):
            assert event.action == 'heartbeat'
            assert event.monitor == 'heartbeat'
            attrs = json.loads(event.attrs_json)
            assert attrs['beat_number'] == i + 1
    
    def test_heartbeat_monitor_stats(self):
        """Test heartbeat monitor statistics."""
        from lb3.monitors.heartbeat import HeartbeatMonitor
        
        monitor = HeartbeatMonitor(dry_run=True, interval=1.0, total_beats=5)
        
        # Initial stats
        stats = monitor.get_stats()
        assert stats['name'] == 'heartbeat'
        assert stats['beat_count'] == 0
        assert stats['total_beats'] == 5
        assert stats['interval'] == 1.0
        assert stats['is_running'] is False
        
        monitor.start()
        
        # Running stats
        stats = monitor.get_stats()
        assert stats['is_running'] is True
        
        # Let it run briefly
        time.sleep(1.5)
        
        stats = monitor.get_stats()
        assert stats['beat_count'] >= 1
        
        monitor.stop()
        
        # Final stats
        stats = monitor.get_stats()
        assert stats['is_running'] is False
    
    def test_event_bus_integration_with_heartbeat(self):
        """Test event bus integration with heartbeat monitor."""
        from lb3.events import get_event_bus
        from lb3.monitors.heartbeat import HeartbeatMonitor
        
        bus = get_event_bus()
        received_events = []
        
        def event_handler(event):
            received_events.append(event)
        
        bus.subscribe(event_handler)
        bus.start()
        
        # Create monitor that publishes to event bus
        monitor = HeartbeatMonitor(dry_run=False, interval=0.5, total_beats=2)
        
        monitor.start()
        
        # Wait for completion
        start_time = time.time()
        while monitor._running and (time.time() - start_time) < 5:
            time.sleep(0.1)
        
        monitor.stop()
        
        # Give event bus time to process
        time.sleep(0.2)
        
        bus.stop()
        
        # Should have received events through bus
        assert len(received_events) >= 2
        
        # Events should be properly formatted
        for i, event in enumerate(received_events[:2]):  # Check first 2
            assert event.action == 'heartbeat'
            assert event.monitor == 'heartbeat'
            assert isinstance(event.attrs_json, str)
            
            attrs = json.loads(event.attrs_json)
            assert attrs['beat_number'] == i + 1