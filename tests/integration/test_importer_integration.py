"""Integration tests for journal importer with performance benchmarking."""

import gzip
import json
import tempfile
import time
from pathlib import Path

import pytest

from lb3.database import Database
from lb3.importer import JournalImporter
from lb3.spooler import SpoolerManager, create_sample_event


@pytest.mark.usefixtures("no_thread_leaks")
class TestImporterIntegration:
    """Integration tests for importer with performance benchmarking."""

    def test_generate_and_import_10k_events_benchmark(self):
        """Test generating 10k synthetic events and importing with performance measurement."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            db_path = Path(temp_dir) / "benchmark.db"

            # Generate 10k events across 3 monitors
            event_counts = {"browser": 4000, "active_window": 3000, "keyboard": 3000}
            total_events = sum(event_counts.values())

            print(f"\nGenerating {total_events} synthetic events...")
            generation_start = time.time()

            # Use SpoolerManager to create events
            manager = SpoolerManager(spool_dir=spool_dir)

            for monitor, count in event_counts.items():
                for _ in range(count):
                    event = create_sample_event(monitor)
                    manager.write_event(monitor, event)

            # Close all spoolers to finalize files
            manager.close_all()
            generation_time = time.time() - generation_start
            print(f"Generation completed in {generation_time:.2f}s")

            # Verify files were created
            for monitor in event_counts:
                monitor_dir = spool_dir / monitor
                journal_files = list(monitor_dir.glob("*.ndjson.gz"))
                assert len(journal_files) > 0, f"No journal files created for {monitor}"

            # Import all events and measure performance
            print("Starting import benchmark...")
            import_start = time.time()

            importer = JournalImporter(spool_dir=spool_dir)
            db = Database(db_path)

            try:
                stats = importer.flush_all_monitors(batch_size=1000, db=db)

                import_time = time.time() - import_start
                events_per_minute = (
                    stats["total_events_imported"] / import_time
                ) * 60.0

                print("\nBenchmark Results:")
                print(f"Total events imported: {stats['total_events_imported']}")
                print(f"Import time: {import_time:.2f}s")
                print(f"Throughput: {events_per_minute:.0f} events/min")
                print(f"Duplicates skipped: {stats['total_duplicates_skipped']}")
                print(f"Invalid events: {stats['total_invalid_events']}")

                # Verify import was successful
                assert stats["total_events_imported"] == total_events
                assert stats["total_files_with_errors"] == 0
                assert stats["total_invalid_events"] == 0

                # Performance requirement: ≥ 5,000 events/min
                assert (
                    events_per_minute >= 5000
                ), f"Performance below target: {events_per_minute:.0f} events/min (target: ≥5000)"

                # Verify database contents
                counts = db.get_table_counts()
                assert counts["events"] == total_events

                # Verify _done directory populated
                done_dir = spool_dir / "_done"
                assert done_dir.exists()

                for monitor in event_counts:
                    monitor_done_dir = done_dir / monitor
                    assert monitor_done_dir.exists()
                    done_files = list(monitor_done_dir.glob("*.ndjson.gz"))
                    assert len(done_files) > 0, f"No files moved to _done for {monitor}"

            finally:
                db.close()

    def test_idempotent_import_repeat_benchmark(self):
        """Test repeating import on same files yields zero additional rows."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            db_path = Path(temp_dir) / "idempotent.db"

            # Generate 2k events for faster test
            event_count = 2000
            manager = SpoolerManager(spool_dir=spool_dir)

            for _ in range(event_count):
                event = create_sample_event("browser")
                manager.write_event("browser", event)

            manager.close_all()

            # First import
            importer = JournalImporter(spool_dir=spool_dir)
            db = Database(db_path)

            try:
                # Initial import
                stats1 = importer.flush_all_monitors(db=db)

                assert stats1["total_events_imported"] == event_count
                assert stats1["total_duplicates_skipped"] == 0

                initial_count = db.get_table_counts()["events"]
                assert initial_count == event_count

                # Simulate running import again by moving files back
                done_dir = spool_dir / "_done" / "browser"
                browser_dir = spool_dir / "browser"
                browser_dir.mkdir(exist_ok=True)

                # Copy files back from _done to simulate re-running
                import shutil

                done_files = list(done_dir.glob("*.ndjson.gz"))
                for done_file in done_files:
                    shutil.copy2(done_file, browser_dir / f"rerun_{done_file.name}")

                # Second import - should detect all as duplicates
                stats2 = importer.flush_all_monitors(db=db)

                assert (
                    stats2["total_events_imported"] == 0
                ), "Should not import any new events on second run"
                assert (
                    stats2["total_duplicates_skipped"] == event_count
                ), "Should skip all duplicates"

                # Verify database count unchanged
                final_count = db.get_table_counts()["events"]
                assert (
                    final_count == initial_count
                ), "Database count should remain stable"

                print("\nIdempotency test results:")
                print(
                    f"First run: {stats1['total_events_imported']} imported, {stats1['total_duplicates_skipped']} skipped"
                )
                print(
                    f"Second run: {stats2['total_events_imported']} imported, {stats2['total_duplicates_skipped']} skipped"
                )
                print(f"Database rows: {initial_count} -> {final_count} (stable)")

            finally:
                db.close()

    def test_error_handling_with_valid_files(self):
        """Test error handling creates .error sidecars for corrupt inputs while processing valid files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            db_path = Path(temp_dir) / "error_handling.db"

            # Create valid events using spooler
            manager = SpoolerManager(spool_dir=spool_dir)

            valid_count = 100
            for _ in range(valid_count):
                event = create_sample_event("browser")
                manager.write_event("browser", event)

            manager.close_all()

            # Create a corrupt file manually
            browser_dir = spool_dir / "browser"
            corrupt_file = browser_dir / "corrupt.ndjson.gz"

            with gzip.open(corrupt_file, "wt", encoding="utf-8") as f:
                # Write one valid event
                valid_event = create_sample_event("browser")
                f.write(json.dumps(valid_event) + "\n")

                # Write corrupt event (missing required field)
                corrupt_event = {
                    "id": "corrupt-id",
                    "ts_utc": 1234567890,
                }  # Missing required fields
                f.write(json.dumps(corrupt_event) + "\n")

            # Import should handle errors gracefully
            importer = JournalImporter(spool_dir=spool_dir)
            db = Database(db_path)

            try:
                stats = importer.flush_all_monitors(db=db)

                # Should have processed valid files but had error with corrupt file
                assert (
                    stats["total_events_imported"] == valid_count
                )  # Only valid events imported
                assert stats["total_files_with_errors"] == 1  # One corrupt file
                assert (
                    stats["total_files_processed"] >= 1
                )  # At least the valid files processed

                # Check error sidecar was created
                error_file = corrupt_file.with_suffix(corrupt_file.suffix + ".error")
                assert (
                    error_file.exists()
                ), "Error sidecar should be created for corrupt file"

                with open(error_file) as f:
                    error_data = json.load(f)
                    assert "error_message" in error_data
                    assert "first_failing_line" in error_data
                    assert (
                        error_data["first_failing_line"] == 2
                    )  # Second line was corrupt

                # Corrupt file should still be in place (not moved to _done)
                assert corrupt_file.exists(), "Corrupt file should remain in place"

                # Valid files should be in _done directory
                done_dir = spool_dir / "_done" / "browser"
                done_files = list(done_dir.glob("*.ndjson.gz"))
                assert len(done_files) >= 1, "Valid files should be moved to _done"

                # Verify only valid events are in database
                counts = db.get_table_counts()
                assert counts["events"] == valid_count

                print("\nError handling test results:")
                print(f"Valid events imported: {stats['total_events_imported']}")
                print(f"Files with errors: {stats['total_files_with_errors']}")
                print(f"Error sidecar created: {error_file.exists()}")

            finally:
                db.close()

    def test_mixed_monitors_comprehensive_import(self):
        """Test comprehensive import across multiple monitor types."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)
            db_path = Path(temp_dir) / "mixed.db"

            # Create events for supported monitor types with sample generators
            monitor_configs = {
                "browser": 200,
                "active_window": 150,
                "keyboard": 100,
                "mouse": 100,
                "file": 100,
            }

            total_expected = sum(monitor_configs.values())

            print(f"\nCreating events for {len(monitor_configs)} monitor types...")
            manager = SpoolerManager(spool_dir=spool_dir)

            for monitor, count in monitor_configs.items():
                for _ in range(count):
                    event = create_sample_event(monitor)
                    manager.write_event(monitor, event)

            manager.close_all()

            # Import all monitors
            start_time = time.time()
            importer = JournalImporter(spool_dir=spool_dir)
            db = Database(db_path)

            try:
                stats = importer.flush_all_monitors(batch_size=500, db=db)

                duration = time.time() - start_time
                events_per_minute = (stats["total_events_imported"] / duration) * 60.0

                # Verify comprehensive import
                assert stats["total_events_imported"] == total_expected
                assert stats["total_files_with_errors"] == 0
                assert len(stats["monitor_stats"]) == len(monitor_configs)

                # Verify per-monitor statistics
                for monitor, expected_count in monitor_configs.items():
                    monitor_stats = stats["monitor_stats"][monitor]
                    assert (
                        monitor_stats["events_imported"] == expected_count
                    ), f"Wrong count for {monitor}"

                # Verify _done directories created for all monitors
                done_dir = spool_dir / "_done"
                for monitor in monitor_configs:
                    monitor_done_dir = done_dir / monitor
                    assert (
                        monitor_done_dir.exists()
                    ), f"_done directory not created for {monitor}"
                    done_files = list(monitor_done_dir.glob("*.ndjson.gz"))
                    assert len(done_files) > 0, f"No files moved to _done for {monitor}"

                # Verify database contents
                counts = db.get_table_counts()
                assert counts["events"] == total_expected

                print("\nMixed monitors import results:")
                print(f"Total events imported: {stats['total_events_imported']}")
                print(f"Monitors processed: {len(stats['monitor_stats'])}")
                print(f"Duration: {duration:.2f}s")
                print(f"Throughput: {events_per_minute:.0f} events/min")

                # Performance should still meet target even with mixed types
                assert (
                    events_per_minute >= 3000
                ), f"Mixed import performance below minimum: {events_per_minute:.0f} events/min"

            finally:
                db.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
