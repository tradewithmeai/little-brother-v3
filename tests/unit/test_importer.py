"""Tests for journal importer."""

import gzip
import json
import tempfile
from pathlib import Path

import pytest

from lb3.database import Database
from lb3.importer import ImportError, JournalImporter
from lb3.spooler import JournalSpooler, create_sample_event


class TestJournalImporter:
    """Test journal importer functionality."""

    def test_importer_initialization(self):
        """Test that importer initializes correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            importer = JournalImporter(spool_dir)
            
            assert importer.spool_dir == spool_dir
            assert importer.done_dir == spool_dir / "_done"
            assert importer.done_dir.exists()

    def test_flush_nonexistent_monitor(self):
        """Test flushing nonexistent monitor."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            importer = JournalImporter(spool_dir)
            
            stats = importer.flush_monitor("nonexistent_monitor")
            
            assert stats["monitor"] == "nonexistent_monitor"
            assert stats["files_processed"] == 0
            assert stats["events_imported"] == 0

    def test_flush_empty_monitor_directory(self):
        """Test flushing monitor with no journal files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            
            # Create empty monitor directory
            monitor_dir = spool_dir / "test_monitor"
            monitor_dir.mkdir()
            
            importer = JournalImporter(spool_dir)
            stats = importer.flush_monitor("test_monitor")
            
            assert stats["monitor"] == "test_monitor"
            assert stats["files_processed"] == 0
            assert stats["events_imported"] == 0

    def test_simple_import_roundtrip(self):
        """Test basic import roundtrip."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            db_path = Path(temp_dir) / "test.db"
            
            # Create test events using spooler
            spooler = JournalSpooler("test_monitor", spool_dir)
            for _ in range(3):
                event = create_sample_event("active_window")
                spooler.write_event(event)
            spooler.close()
            
            # Import events
            importer = JournalImporter(spool_dir)
            db = Database(db_path)
            
            try:
                stats = importer.flush_monitor("test_monitor", db=db)
                
                assert stats["files_processed"] == 1
                assert stats["events_imported"] == 3
                assert stats["files_with_errors"] == 0
                
                # Verify events in database
                counts = db.get_table_counts()
                assert counts["events"] == 3
            finally:
                db.close()

    def test_idempotent_import(self):
        """Test that import is idempotent (no duplicates on repeat)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            db_path = Path(temp_dir) / "test.db"
            
            # Create test events
            spooler = JournalSpooler("test_monitor", spool_dir)
            for _ in range(3):
                event = create_sample_event("active_window")
                spooler.write_event(event)
            spooler.close()
            
            # Import once
            importer = JournalImporter(spool_dir)
            db = Database(db_path)
            
            try:
                stats1 = importer.flush_monitor("test_monitor", db=db)
                assert stats1["events_imported"] == 3
                
                # Verify initial count
                counts1 = db.get_table_counts()
                assert counts1["events"] == 3
                
                # Create the same events again (to test idempotency)
                # We need to create a new journal file with same event IDs
                monitor_dir = spool_dir / "test_monitor" 
                done_dir = spool_dir / "_done" / "test_monitor"
                
                # Copy the processed file back for re-import
                done_files = list(done_dir.glob("*.ndjson.gz"))
                if done_files:
                    import shutil
                    shutil.copy2(done_files[0], monitor_dir / "duplicate.ndjson.gz")
                
                # Import again - should not create duplicates
                stats2 = importer.flush_monitor("test_monitor", db=db)
                assert stats2["events_imported"] == 0  # No new events (duplicates ignored)
                
                # Verify count unchanged
                counts2 = db.get_table_counts()
                assert counts2["events"] == 3  # Same count
            finally:
                db.close()

    def test_file_moved_to_done_on_success(self):
        """Test that files are moved to _done directory on successful import."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            db_path = Path(temp_dir) / "test.db"
            
            # Create test events
            spooler = JournalSpooler("test_monitor", spool_dir)
            event = create_sample_event("active_window")
            spooler.write_event(event)
            spooler.close()
            
            # Get original file name
            monitor_dir = spool_dir / "test_monitor"
            original_files = list(monitor_dir.glob("*.ndjson.gz"))
            assert len(original_files) == 1
            original_filename = original_files[0].name
            
            # Import
            importer = JournalImporter(spool_dir)
            db = Database(db_path)
            
            try:
                stats = importer.flush_monitor("test_monitor", db=db)
                assert stats["files_processed"] == 1
                
                # Original file should be gone
                remaining_files = list(monitor_dir.glob("*.ndjson.gz"))
                assert len(remaining_files) == 0
                
                # File should be in _done directory
                done_dir = spool_dir / "_done" / "test_monitor"
                done_files = list(done_dir.glob("*.ndjson.gz"))
                assert len(done_files) == 1
                assert done_files[0].name == original_filename
            finally:
                db.close()

    def test_part_files_ignored(self):
        """Test that .part files are ignored during import."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            monitor_dir = spool_dir / "test_monitor"
            monitor_dir.mkdir()
            
            # Create a .part file manually
            part_file = monitor_dir / "20231201-12.ndjson.gz.part"
            with gzip.open(part_file, 'wt', encoding='utf-8') as f:
                event = create_sample_event("active_window")
                f.write(json.dumps(event) + '\n')
            
            # Import should ignore .part file
            importer = JournalImporter(spool_dir)
            stats = importer.flush_monitor("test_monitor")
            
            assert stats["files_processed"] == 0
            assert stats["events_imported"] == 0
            
            # .part file should still exist
            assert part_file.exists()

    def test_invalid_json_creates_error_sidecar(self):
        """Test that invalid JSON creates error sidecar file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            monitor_dir = spool_dir / "test_monitor"
            monitor_dir.mkdir()
            
            # Create file with invalid content that triggers validation error
            journal_file = monitor_dir / "20231201-12.ndjson.gz"
            with gzip.open(journal_file, 'wt', encoding='utf-8') as f:
                # Write event missing required field to trigger validation error
                f.write('{"valid": "json"}\n')  # Missing all required fields
            
            # Import should fail
            importer = JournalImporter(spool_dir)
            stats = importer.flush_monitor("test_monitor")
            
            assert stats["files_processed"] == 0  # Failed to process
            assert stats["files_with_errors"] == 1
            
            # Error sidecar should be created
            error_file = journal_file.with_suffix(journal_file.suffix + '.error')
            assert error_file.exists()
            
            with open(error_file, encoding='utf-8') as f:
                error_info = json.load(f)
                assert "error_message" in error_info
                assert "first_failing_line" in error_info
                assert error_info["first_failing_line"] == 1

    def test_missing_required_field_creates_error(self):
        """Test that missing required fields create validation errors."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            monitor_dir = spool_dir / "test_monitor"
            monitor_dir.mkdir()
            
            # Create event with missing required field
            invalid_event = create_sample_event("active_window")
            del invalid_event['monitor']  # Remove required field
            
            journal_file = monitor_dir / "20231201-12.ndjson.gz"
            with gzip.open(journal_file, 'wt', encoding='utf-8') as f:
                f.write(json.dumps(invalid_event) + '\n')
            
            # Import should fail
            importer = JournalImporter(spool_dir)
            stats = importer.flush_monitor("test_monitor")
            
            assert stats["files_with_errors"] == 1
            
            # Error sidecar should mention missing field
            error_file = journal_file.with_suffix(journal_file.suffix + '.error')
            assert error_file.exists()
            
            with open(error_file, encoding='utf-8') as f:
                error_info = json.load(f)
                assert "monitor" in error_info["error_message"]

    def test_invalid_monitor_value_creates_error(self):
        """Test that invalid monitor values create validation errors."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            monitor_dir = spool_dir / "test_monitor"
            monitor_dir.mkdir()
            
            # Create event with invalid monitor value
            invalid_event = create_sample_event("active_window")
            invalid_event['monitor'] = 'invalid_monitor'
            
            journal_file = monitor_dir / "20231201-12.ndjson.gz"
            with gzip.open(journal_file, 'wt', encoding='utf-8') as f:
                f.write(json.dumps(invalid_event) + '\n')
            
            # Import should fail
            importer = JournalImporter(spool_dir)
            stats = importer.flush_monitor("test_monitor")
            
            assert stats["files_with_errors"] == 1

    def test_flush_all_monitors(self):
        """Test flushing all monitors at once."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            db_path = Path(temp_dir) / "test.db"
            
            # Create events for multiple monitors
            monitors = ["active_window", "keyboard"]
            for monitor in monitors:
                spooler = JournalSpooler(monitor, spool_dir)
                for _ in range(2):
                    if monitor == "active_window":
                        event = create_sample_event("active_window")
                    else:
                        event = create_sample_event("keyboard")
                    spooler.write_event(event)
                spooler.close()
            
            # Import all monitors
            importer = JournalImporter(spool_dir)
            db = Database(db_path)
            
            try:
                stats = importer.flush_all_monitors(db=db)
                
                assert stats["total_files_processed"] == 2
                assert stats["total_events_imported"] == 4
                assert stats["total_files_with_errors"] == 0
                
                # Check per-monitor stats
                assert "active_window" in stats["monitor_stats"]
                assert "keyboard" in stats["monitor_stats"]
                
                for monitor in monitors:
                    monitor_stats = stats["monitor_stats"][monitor]
                    assert monitor_stats["files_processed"] == 1
                    assert monitor_stats["events_imported"] == 2
                
                # Verify database
                counts = db.get_table_counts()
                assert counts["events"] == 4
            finally:
                db.close()

    def test_batch_insertion_performance(self):
        """Test batch insertion with larger dataset."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            db_path = Path(temp_dir) / "test.db"
            
            # Create larger dataset
            spooler = JournalSpooler("test_monitor", spool_dir)
            event_count = 50
            
            for _ in range(event_count):
                event = create_sample_event("active_window")
                spooler.write_event(event)
            spooler.close()
            
            # Import with small batch size
            importer = JournalImporter(spool_dir)
            db = Database(db_path)
            
            try:
                stats = importer.flush_monitor("test_monitor", batch_size=10, db=db)
                
                assert stats["events_imported"] == event_count
                
                # Verify all events in database
                counts = db.get_table_counts()
                assert counts["events"] == event_count
            finally:
                db.close()

    def test_concurrent_import_safety(self):
        """Test that import handles concurrent database access safely."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            db_path = Path(temp_dir) / "test.db"
            
            # Create test events
            spooler = JournalSpooler("test_monitor", spool_dir)
            for _ in range(5):
                event = create_sample_event("active_window")
                spooler.write_event(event)
            spooler.close()
            
            # Import while database is in use
            db = Database(db_path)
            
            try:
                # Insert a direct event to simulate concurrent use
                from lb3.database import create_test_event
                direct_event = create_test_event()
                db.insert_event(direct_event)
                
                # Now import should work alongside existing data
                importer = JournalImporter(spool_dir)
                stats = importer.flush_monitor("test_monitor", db=db)
                
                assert stats["events_imported"] == 5
                
                # Should have both direct event and imported events
                counts = db.get_table_counts()
                assert counts["events"] == 6  # 1 direct + 5 imported
            finally:
                db.close()


class TestImportValidation:
    """Test import validation logic."""

    def test_validate_event_success(self):
        """Test successful event validation."""
        importer = JournalImporter()
        
        valid_event = create_sample_event("active_window")
        
        # Should not raise exception
        importer._validate_event(valid_event, 1)

    def test_validate_event_missing_fields(self):
        """Test validation failure for missing fields."""
        importer = JournalImporter()
        
        # Test each required field
        required_fields = ['id', 'ts_utc', 'monitor', 'action', 'subject_type', 'session_id']
        
        for missing_field in required_fields:
            event = create_sample_event("active_window")
            del event[missing_field]
            
            with pytest.raises(ImportError, match=f"Missing required field '{missing_field}'"):
                importer._validate_event(event, 1)

    def test_validate_event_wrong_types(self):
        """Test validation failure for wrong field types."""
        importer = JournalImporter()
        
        # Test wrong ID type
        event = create_sample_event("active_window")
        event['id'] = 123  # Should be string
        
        with pytest.raises(ImportError, match="Field 'id' must be string"):
            importer._validate_event(event, 1)
        
        # Test wrong timestamp type
        event = create_sample_event("active_window")
        event['ts_utc'] = "not_an_int"  # Should be int
        
        with pytest.raises(ImportError, match="Field 'ts_utc' must be integer"):
            importer._validate_event(event, 1)

    def test_validate_event_invalid_enum_values(self):
        """Test validation failure for invalid enum values."""
        importer = JournalImporter()
        
        # Test invalid monitor
        event = create_sample_event("active_window")
        event['monitor'] = 'invalid_monitor'
        
        with pytest.raises(ImportError, match="Invalid monitor"):
            importer._validate_event(event, 1)
        
        # Test invalid subject_type
        event = create_sample_event("active_window")
        event['subject_type'] = 'invalid_subject'
        
        with pytest.raises(ImportError, match="Invalid subject_type"):
            importer._validate_event(event, 1)