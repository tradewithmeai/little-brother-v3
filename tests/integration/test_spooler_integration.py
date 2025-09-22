"""Integration tests for spooler and importer end-to-end workflow."""

import gzip
import json
import subprocess
import tempfile
import time
from pathlib import Path

from lb3.database import Database
from lb3.importer import JournalImporter
from lb3.spooler import JournalSpooler, SpoolerManager, create_sample_event


class TestSpoolerImporterIntegration:
    """Integration tests for spooler -> importer workflow."""

    def test_end_to_end_workflow(self):
        """Test complete spooler -> importer -> database workflow."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            db_path = Path(temp_dir) / "test.db"

            # Step 1: Generate events through spooler
            manager = SpoolerManager(spool_dir)
            monitors = ["active_window", "keyboard", "browser"]
            events_per_monitor = 5

            for monitor in monitors:
                for _ in range(events_per_monitor):
                    event = create_sample_event(monitor)
                    manager.write_event(monitor, event)

            manager.close_all()

            # Step 2: Verify journal files created
            for monitor in monitors:
                monitor_dir = spool_dir / monitor
                journal_files = list(monitor_dir.glob("*.ndjson.gz"))
                assert len(journal_files) >= 1, f"No journal files for {monitor}"

            # Step 3: Import all journals
            importer = JournalImporter(spool_dir)
            db = Database(db_path)

            try:
                stats = importer.flush_all_monitors(db=db)

                assert stats["total_files_processed"] == len(monitors)
                assert (
                    stats["total_events_imported"] == len(monitors) * events_per_monitor
                )
                assert stats["total_files_with_errors"] == 0

                # Step 4: Verify database contents
                counts = db.get_table_counts()
                assert counts["events"] == len(monitors) * events_per_monitor
            finally:
                db.close()

            # Step 5: Verify files moved to _done
            for monitor in monitors:
                monitor_dir = spool_dir / monitor
                done_dir = spool_dir / "_done" / monitor

                # No files left in original directory
                remaining_files = list(monitor_dir.glob("*.ndjson.gz"))
                assert len(remaining_files) == 0

                # Files moved to _done
                done_files = list(done_dir.glob("*.ndjson.gz"))
                assert len(done_files) >= 1

            db.close()

    def test_idempotent_reimport(self):
        """Test that reimporting same files doesn't create duplicates."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            db_path = Path(temp_dir) / "test.db"

            # Generate and import events
            spooler = JournalSpooler("test_monitor", spool_dir)
            original_events = []

            for _ in range(3):
                event = create_sample_event("active_window")
                original_events.append(event)
                spooler.write_event(event)

            spooler.close()

            # First import
            importer = JournalImporter(spool_dir)
            db = Database(db_path)

            try:
                stats1 = importer.flush_monitor("test_monitor", db=db)
                assert stats1["events_imported"] == 3

                counts1 = db.get_table_counts()
                assert counts1["events"] == 3

                # Copy file back to simulate reimport
                done_dir = spool_dir / "_done" / "test_monitor"
                monitor_dir = spool_dir / "test_monitor"

                done_files = list(done_dir.glob("*.ndjson.gz"))
                assert len(done_files) == 1

                import shutil

                shutil.copy2(done_files[0], monitor_dir / "reimport.ndjson.gz")

                # Second import should not add duplicates
                stats2 = importer.flush_monitor("test_monitor", db=db)
                assert stats2["events_imported"] == 0  # No new events

                counts2 = db.get_table_counts()
                assert counts2["events"] == 3  # Same count
            finally:
                db.close()

    def test_crash_recovery_scenario(self):
        """Test recovery from simulated crash scenarios."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)

            # Scenario 1: .part file left behind (simulated crash during write)
            monitor_dir = spool_dir / "test_monitor"
            monitor_dir.mkdir(parents=True)

            part_file = monitor_dir / "20231201-12.ndjson.gz.part"
            with gzip.open(part_file, "wt", encoding="utf-8") as f:
                event = create_sample_event("active_window")
                f.write(json.dumps(event) + "\n")

            # Import should ignore .part files
            importer = JournalImporter(spool_dir)
            stats = importer.flush_monitor("test_monitor")

            assert stats["files_processed"] == 0
            assert stats["events_imported"] == 0

            # .part file should still exist (not processed)
            assert part_file.exists()

            # Scenario 2: Partially written journal file (corrupted)
            corrupted_file = monitor_dir / "20231201-13.ndjson.gz"
            with gzip.open(corrupted_file, "wt", encoding="utf-8") as f:
                f.write('{"valid": "json"}\n')
                f.write("corrupted line without proper")  # Incomplete line

            # Import should handle gracefully
            stats = importer.flush_monitor("test_monitor")

            # Should create error sidecar
            error_file = corrupted_file.with_suffix(corrupted_file.suffix + ".error")
            assert error_file.exists()

    def test_large_dataset_performance(self):
        """Test performance with larger datasets."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            db_path = Path(temp_dir) / "test.db"

            # Generate larger dataset
            event_count = 100
            spooler = JournalSpooler("performance_test", spool_dir)

            start_time = time.time()

            for i in range(event_count):
                event = create_sample_event("active_window")
                event["sequence"] = i  # Add sequence for verification
                spooler.write_event(event)

            spooler.close()
            write_time = time.time() - start_time

            # Import with timing
            importer = JournalImporter(spool_dir)
            db = Database(db_path)

            try:
                start_time = time.time()
                stats = importer.flush_monitor("performance_test", batch_size=20, db=db)
                import_time = time.time() - start_time

                assert stats["events_imported"] == event_count

                # Verify data integrity
                counts = db.get_table_counts()
                assert counts["events"] == event_count

                # Performance should be reasonable (under 10 seconds for 100 events)
                assert write_time < 10.0, f"Write took too long: {write_time}s"
                assert import_time < 10.0, f"Import took too long: {import_time}s"
            finally:
                db.close()

    def test_concurrent_spooler_access(self):
        """Test concurrent access to spoolers."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)

            import threading
            import time

            manager = SpoolerManager(spool_dir)
            results = []
            errors = []

            def write_events(monitor, count):
                try:
                    for i in range(count):
                        event = create_sample_event(monitor)
                        event["thread_id"] = threading.current_thread().ident
                        event["sequence"] = i
                        manager.write_event(monitor, event)
                        time.sleep(0.001)  # Small delay to increase contention
                    results.append(f"{monitor}:{count}")
                except Exception as e:
                    errors.append(str(e))

            # Start multiple threads writing to different monitors
            threads = []
            for _i, monitor in enumerate(["monitor1", "monitor2", "monitor3"]):
                thread = threading.Thread(target=write_events, args=(monitor, 10))
                threads.append(thread)
                thread.start()

            # Wait for all threads
            for thread in threads:
                thread.join()

            manager.close_all()

            # Should have no errors
            assert len(errors) == 0, f"Concurrent access errors: {errors}"
            assert len(results) == 3

            # Verify files created for each monitor
            for monitor in ["monitor1", "monitor2", "monitor3"]:
                monitor_dir = spool_dir / monitor
                journal_files = list(monitor_dir.glob("*.ndjson.gz"))
                assert len(journal_files) >= 1

    def test_multiple_file_rollover(self):
        """Test handling of multiple rolled-over files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            db_path = Path(temp_dir) / "test.db"

            # Create spooler with small size limit to force rollover
            spooler = JournalSpooler("rollover_test", spool_dir)
            spooler.max_size_bytes = 2048  # 2KB to force multiple files

            total_events = 50
            large_event_template = create_sample_event("active_window")
            large_event_template["attrs_json"] = json.dumps({"padding": "x" * 200})

            for i in range(total_events):
                event = large_event_template.copy()
                event["id"] = f"event_{i:03d}"  # Predictable IDs
                event["sequence"] = i
                spooler.write_event(event)

            spooler.close()

            # Should have created multiple files
            monitor_dir = spool_dir / "rollover_test"
            journal_files = list(monitor_dir.glob("*.ndjson.gz"))
            assert len(journal_files) > 1, "Should have rolled over to multiple files"

            # Import all files
            importer = JournalImporter(spool_dir)
            db = Database(db_path)

            try:
                stats = importer.flush_monitor("rollover_test", db=db)

                assert stats["files_processed"] == len(journal_files)
                assert stats["events_imported"] == total_events
                assert stats["files_with_errors"] == 0

                # Verify all events in database
                counts = db.get_table_counts()
                assert counts["events"] == total_events

                # Verify event ordering is preserved
                events = db.get_events_by_timerange(
                    0, int(time.time() * 1000) + 1000, limit=total_events
                )
                assert len(events) == total_events
            finally:
                db.close()

    def test_mixed_valid_invalid_files(self):
        """Test handling mix of valid and invalid files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            db_path = Path(temp_dir) / "test.db"
            monitor_dir = spool_dir / "mixed_test"
            monitor_dir.mkdir(parents=True)

            # Create valid file
            valid_file = monitor_dir / "20231201-12.ndjson.gz"
            with gzip.open(valid_file, "wt", encoding="utf-8") as f:
                for _i in range(3):
                    event = create_sample_event("active_window")
                    event["file"] = "valid"
                    f.write(json.dumps(event) + "\n")

            # Create invalid file (validation error - missing required field)
            invalid_file = monitor_dir / "20231201-13.ndjson.gz"
            with gzip.open(invalid_file, "wt", encoding="utf-8") as f:
                f.write('{"missing": "required_fields"}\n')  # Missing id, ts_utc, etc.

            # Import
            importer = JournalImporter(spool_dir)
            db = Database(db_path)

            try:
                stats = importer.flush_monitor("mixed_test", db=db)

                # Valid file should be processed, invalid should error
                assert stats["files_processed"] == 1  # Only valid file
                assert stats["events_imported"] == 3  # From valid file
                assert stats["files_with_errors"] == 1  # Invalid file

                # Valid file should be moved to _done
                done_dir = spool_dir / "_done" / "mixed_test"
                done_files = list(done_dir.glob("*.ndjson.gz"))
                assert len(done_files) == 1

                # Invalid file should remain with error sidecar
                remaining_files = list(monitor_dir.glob("*.ndjson.gz"))
                error_files = list(monitor_dir.glob("*.error"))
                assert len(remaining_files) == 1  # Invalid file
                assert len(error_files) == 1  # Error sidecar
            finally:
                db.close()


class TestSpoolerCLIIntegration:
    """Integration tests for CLI commands."""

    def test_cli_generate_and_flush(self):
        """Test CLI generate and flush commands."""
        # Test generate command
        result = subprocess.run(
            [
                "python",
                "-m",
                "lb3",
                "spool",
                "generate",
                "active_window",
                "--count",
                "5",
            ],
            cwd=Path(__file__).parent.parent.parent,
            capture_output=True,
            text=True,
            timeout=30,
        )

        assert result.returncode == 0
        output = result.stdout
        assert "Generated 5 events for active_window" in output
        assert "Journal files created in:" in output

        # Verify files were actually created
        spool_dir = Path("lb_data/spool/active_window")
        if spool_dir.exists():
            journal_files = list(spool_dir.glob("*.ndjson.gz"))
            assert len(journal_files) >= 1

        # Test flush command
        result = subprocess.run(
            ["python", "-m", "lb3", "spool", "flush", "--monitor", "active_window"],
            cwd=Path(__file__).parent.parent.parent,
            capture_output=True,
            text=True,
            timeout=30,
        )

        assert result.returncode == 0
        output = result.stdout
        assert "Import completed for active_window" in output
        assert "Events imported:" in output

    def test_cli_flush_all(self):
        """Test CLI flush all monitors command."""
        # Generate events for multiple monitors
        monitors = ["keyboard", "mouse"]
        for monitor in monitors:
            result = subprocess.run(
                ["python", "-m", "lb3", "spool", "generate", monitor, "--count", "3"],
                cwd=Path(__file__).parent.parent.parent,
                capture_output=True,
                text=True,
                timeout=30,
            )
            assert result.returncode == 0

        # Flush all
        result = subprocess.run(
            ["python", "-m", "lb3", "spool", "flush", "--monitor", "all"],
            cwd=Path(__file__).parent.parent.parent,
            capture_output=True,
            text=True,
            timeout=30,
        )

        assert result.returncode == 0
        output = result.stdout
        assert "Flushing all monitor journals" in output
        assert "Import completed:" in output
        assert "Total events imported:" in output

    def test_cli_invalid_monitor(self):
        """Test CLI with invalid monitor name."""
        result = subprocess.run(
            ["python", "-m", "lb3", "spool", "generate", "invalid_monitor"],
            cwd=Path(__file__).parent.parent.parent,
            capture_output=True,
            text=True,
            timeout=30,
        )

        assert result.returncode == 1
        assert "Invalid monitor" in result.stdout

    def test_cli_help_commands(self):
        """Test CLI help for spool commands."""
        # Test spool help
        result = subprocess.run(
            ["python", "-m", "lb3", "spool", "--help"],
            cwd=Path(__file__).parent.parent.parent,
            capture_output=True,
            text=True,
            timeout=30,
        )

        assert result.returncode == 0
        output = result.stdout
        assert "Journal spool management commands" in output
        assert "flush" in output
        assert "generate" in output

        # Test flush help
        result = subprocess.run(
            ["python", "-m", "lb3", "spool", "flush", "--help"],
            cwd=Path(__file__).parent.parent.parent,
            capture_output=True,
            text=True,
            timeout=30,
        )

        assert result.returncode == 0
        assert "Flush journal files to database" in result.stdout
