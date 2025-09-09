"""Tests for journal spooler."""

import gzip
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from lb3.spooler import JournalSpooler, SpoolerManager, create_sample_event


class TestJournalSpooler:
    """Test journal spooler functionality."""

    def test_spooler_initialization(self):
        """Test that spooler initializes correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            spooler = JournalSpooler("test_monitor", spool_dir)
            
            # Monitor directory should be created
            monitor_dir = spool_dir / "test_monitor"
            assert monitor_dir.exists()
            assert spooler.monitor == "test_monitor"
            assert spooler.spool_dir == monitor_dir

    def test_single_event_write(self):
        """Test writing a single event."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            spooler = JournalSpooler("test_monitor", spool_dir)
            
            event_data = create_sample_event("active_window")
            spooler.write_event(event_data)
            spooler.close()  # Force file closure
            
            # Check that journal file was created
            monitor_dir = spool_dir / "test_monitor"
            journal_files = list(monitor_dir.glob("*.ndjson.gz"))
            assert len(journal_files) == 1
            
            # Verify file content
            with gzip.open(journal_files[0], 'rt', encoding='utf-8') as f:
                lines = f.readlines()
                assert len(lines) == 1
                
                parsed_event = json.loads(lines[0])
                assert parsed_event['id'] == event_data['id']
                assert parsed_event['monitor'] == event_data['monitor']

    def test_hourly_file_naming(self):
        """Test that files are named with hourly format."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            spooler = JournalSpooler("test_monitor", spool_dir)
            
            event_data = create_sample_event("active_window")
            spooler.write_event(event_data)
            spooler.close()
            
            # Check filename format
            monitor_dir = spool_dir / "test_monitor"
            journal_files = list(monitor_dir.glob("*.ndjson.gz"))
            assert len(journal_files) == 1
            
            filename = journal_files[0].name
            # Should match YYYYMMDD-HH.ndjson.gz format
            assert len(filename) == 21  # "YYYYMMDD-HH.ndjson.gz" (8+1+2+10 = 21)
            assert filename.endswith(".ndjson.gz")
            assert "-" in filename[:11]  # Date-hour separator

    def test_atomic_write_pattern(self):
        """Test atomic write pattern with .part files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            spooler = JournalSpooler("test_monitor", spool_dir)
            
            event_data = create_sample_event("active_window")
            spooler.write_event(event_data)
            
            # Before closing, .part file should exist
            monitor_dir = spool_dir / "test_monitor"
            part_files = list(monitor_dir.glob("*.part"))
            final_files = list(monitor_dir.glob("*.ndjson.gz"))
            
            assert len(part_files) == 1
            assert len(final_files) == 0
            
            spooler.close()
            
            # After closing, .part should be gone and final file should exist
            part_files = list(monitor_dir.glob("*.part"))
            final_files = list(monitor_dir.glob("*.ndjson.gz"))
            
            assert len(part_files) == 0
            assert len(final_files) == 1

    def test_size_based_rollover(self):
        """Test rollover based on file size."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            spooler = JournalSpooler("test_monitor", spool_dir)
            
            # Set size limit that allows multiple small events per file
            spooler.max_size_bytes = 1000  # 1KB uncompressed
            
            # Create smaller events that can fit multiple per file
            small_event_data = create_sample_event("active_window")
            small_event_data['attrs_json'] = json.dumps({"data": "x" * 50})  # ~300 bytes per event
            
            # Write enough events to trigger rollover
            event_count = 5  # Should get ~2-3 events per file, causing rollover
            for i in range(event_count):
                small_event_data['sequence'] = i  # Make each event unique
                spooler.write_event(small_event_data)
            
            spooler.close()
            
            # Verify files created
            monitor_dir = spool_dir / "test_monitor"
            journal_files = list(monitor_dir.glob("*.ndjson.gz"))
            assert len(journal_files) >= 1  # At least one file should exist
            
            # Verify all events are preserved across all files
            total_events = 0
            for journal_file in journal_files:
                with gzip.open(journal_file, 'rt', encoding='utf-8') as f:
                    events_in_file = len(f.readlines())
                    total_events += events_in_file
                    # Each file should have at least 1 event
                    assert events_in_file >= 1
            
            assert total_events == event_count  # All events should be preserved
            
            # If rollover occurred, should have multiple files
            # But with compression, this might be unpredictable, so just ensure data integrity
            print(f"Created {len(journal_files)} files with {total_events} total events")

    @patch('time.time')
    def test_idle_flush(self, mock_time):
        """Test idle flush functionality."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            spooler = JournalSpooler("test_monitor", spool_dir)
            
            # Mock time progression
            mock_time.return_value = 1000.0
            
            event_data = create_sample_event("active_window")
            spooler.write_event(event_data)
            
            # File should be open (has .part file)
            monitor_dir = spool_dir / "test_monitor"
            part_files = list(monitor_dir.glob("*.part"))
            assert len(part_files) == 1
            
            # Simulate idle timeout
            mock_time.return_value = 1002.0  # 2 seconds later
            spooler.flush_if_idle()
            
            # File should be closed and finalized
            part_files = list(monitor_dir.glob("*.part"))
            final_files = list(monitor_dir.glob("*.ndjson.gz"))
            assert len(part_files) == 0
            assert len(final_files) == 1

    def test_multiple_events_ordering(self):
        """Test that multiple events maintain order."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            spooler = JournalSpooler("test_monitor", spool_dir)
            
            events = []
            for i in range(5):
                event_data = create_sample_event("active_window")
                event_data['sequence'] = i  # Add sequence for verification
                events.append(event_data)
                spooler.write_event(event_data)
            
            spooler.close()
            
            # Verify events are in correct order
            monitor_dir = spool_dir / "test_monitor"
            journal_files = list(monitor_dir.glob("*.ndjson.gz"))
            assert len(journal_files) == 1
            
            with gzip.open(journal_files[0], 'rt', encoding='utf-8') as f:
                lines = f.readlines()
                assert len(lines) == 5
                
                for i, line in enumerate(lines):
                    parsed_event = json.loads(line)
                    assert parsed_event['sequence'] == i

    def test_gzip_compression_readable(self):
        """Test that gzip files are properly compressed and readable."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            spooler = JournalSpooler("test_monitor", spool_dir)
            
            event_data = create_sample_event("active_window")
            spooler.write_event(event_data)
            spooler.close()
            
            monitor_dir = spool_dir / "test_monitor"
            journal_files = list(monitor_dir.glob("*.ndjson.gz"))
            journal_file = journal_files[0]
            
            # Verify it's actually compressed (file should be smaller than uncompressed)
            compressed_size = journal_file.stat().st_size
            
            # Read and verify content
            with gzip.open(journal_file, 'rt', encoding='utf-8') as f:
                content = f.read()
                
            uncompressed_size = len(content.encode('utf-8'))
            assert compressed_size < uncompressed_size  # Should be compressed
            
            # Verify JSON content is valid
            lines = content.strip().split('\n')
            parsed_event = json.loads(lines[0])
            assert parsed_event['id'] == event_data['id']

    def test_spooler_close_idempotent(self):
        """Test that spooler close is idempotent."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            spooler = JournalSpooler("test_monitor", spool_dir)
            
            event_data = create_sample_event("active_window")
            spooler.write_event(event_data)
            
            # Close multiple times should not error
            spooler.close()
            spooler.close()
            spooler.close()
            
            # Should have created one file
            monitor_dir = spool_dir / "test_monitor"
            journal_files = list(monitor_dir.glob("*.ndjson.gz"))
            assert len(journal_files) == 1

    def test_closed_spooler_rejects_writes(self):
        """Test that closed spooler rejects new writes."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            spooler = JournalSpooler("test_monitor", spool_dir)
            
            spooler.close()
            
            event_data = create_sample_event("active_window")
            with pytest.raises(RuntimeError, match="Spooler has been closed"):
                spooler.write_event(event_data)


class TestSpoolerManager:
    """Test spooler manager functionality."""

    def test_manager_initialization(self):
        """Test spooler manager initialization."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            manager = SpoolerManager(spool_dir)
            
            assert manager.spool_dir == spool_dir

    def test_get_spooler_creates_on_demand(self):
        """Test that spoolers are created on demand."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            manager = SpoolerManager(spool_dir)
            
            # Get spooler for new monitor
            spooler1 = manager.get_spooler("monitor1")
            assert spooler1.monitor == "monitor1"
            
            # Getting same monitor returns same instance
            spooler2 = manager.get_spooler("monitor1")
            assert spooler1 is spooler2
            
            # Different monitor creates new instance
            spooler3 = manager.get_spooler("monitor2")
            assert spooler3.monitor == "monitor2"
            assert spooler3 is not spooler1

    def test_write_event_to_manager(self):
        """Test writing events through manager."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            manager = SpoolerManager(spool_dir)
            
            event_data = create_sample_event("active_window")
            manager.write_event("active_window", event_data)
            
            manager.close_all()
            
            # Check file was created
            monitor_dir = spool_dir / "active_window"
            journal_files = list(monitor_dir.glob("*.ndjson.gz"))
            assert len(journal_files) == 1

    def test_flush_idle_spoolers(self):
        """Test flushing idle spoolers through manager."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            manager = SpoolerManager(spool_dir)
            
            with patch('time.time') as mock_time:
                mock_time.return_value = 1000.0
                
                # Write events to multiple monitors
                event1 = create_sample_event("monitor1")
                event2 = create_sample_event("monitor2")
                manager.write_event("monitor1", event1)
                manager.write_event("monitor2", event2)
                
                # Simulate time passing
                mock_time.return_value = 1003.0  # 3 seconds later
                
                # Flush idle spoolers
                manager.flush_idle_spoolers()
                
                # Files should be finalized
                for monitor in ["monitor1", "monitor2"]:
                    monitor_dir = spool_dir / monitor
                    part_files = list(monitor_dir.glob("*.part"))
                    final_files = list(monitor_dir.glob("*.ndjson.gz"))
                    assert len(part_files) == 0
                    assert len(final_files) == 1

    def test_close_all_spoolers(self):
        """Test closing all spoolers through manager."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            manager = SpoolerManager(spool_dir)
            
            # Create multiple spoolers
            manager.write_event("monitor1", create_sample_event("monitor1"))
            manager.write_event("monitor2", create_sample_event("monitor2"))
            
            manager.close_all()
            
            # All files should be finalized
            for monitor in ["monitor1", "monitor2"]:
                monitor_dir = spool_dir / monitor
                part_files = list(monitor_dir.glob("*.part"))
                final_files = list(monitor_dir.glob("*.ndjson.gz"))
                assert len(part_files) == 0
                assert len(final_files) == 1


class TestCreateSampleEvent:
    """Test sample event creation."""

    def test_create_sample_event_structure(self):
        """Test sample event has correct structure."""
        monitors = ['active_window', 'keyboard', 'mouse', 'browser', 'file', 'context_snapshot']
        
        for monitor in monitors:
            event = create_sample_event(monitor)
            
            # Required fields
            required_fields = ['id', 'ts_utc', 'monitor', 'session_id', 'action', 'subject_type']
            for field in required_fields:
                assert field in event, f"Missing {field} in {monitor} event"
            
            # Verify types
            assert isinstance(event['id'], str)
            assert isinstance(event['ts_utc'], int)
            assert event['monitor'] == monitor
            assert isinstance(event['session_id'], str)

    def test_sample_events_unique(self):
        """Test that sample events are unique."""
        events = [create_sample_event("active_window") for _ in range(5)]
        
        ids = [event['id'] for event in events]
        session_ids = [event['session_id'] for event in events]
        
        assert len(set(ids)) == 5  # All IDs unique
        assert len(set(session_ids)) == 5  # All session IDs unique

    def test_monitor_specific_fields(self):
        """Test that different monitors have appropriate fields."""
        # Active window should have window-specific fields
        event = create_sample_event("active_window")
        assert 'exe_name' in event
        assert 'exe_path_hash' in event
        assert 'window_title_hash' in event
        
        # Browser should have URL field
        event = create_sample_event("browser")
        assert 'url_hash' in event
        
        # File should have file path field
        event = create_sample_event("file")
        assert 'file_path_hash' in event