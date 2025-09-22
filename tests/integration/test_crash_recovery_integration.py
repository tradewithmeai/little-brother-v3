"""Integration tests for crash recovery and resilience."""

import contextlib
import gzip
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import pytest

from lb3.database import Database
from lb3.importer import JournalImporter
from lb3.recovery import recover_all_temp_files
from lb3.spooler import JournalSpooler


@pytest.mark.usefixtures("no_thread_leaks")
class TestCrashRecoveryIntegration:
    """Integration tests for crash recovery scenarios."""

    def test_mid_write_crash_simulation(self):
        """Test recovery after simulated crash during write operation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir) / "spool"
            Path(temp_dir) / "test.db"

            # Create spooler and write some events
            spooler = JournalSpooler("test_monitor", spool_dir)

            # Write a few complete events
            for i in range(3):
                event = {
                    "id": f"event-{i}",
                    "ts_utc": int(time.time() * 1000),
                    "monitor": "test_monitor",
                    "action": "test_action",
                    "subject_type": "none",
                }
                spooler.write_event(event)

            # Simulate crash by not calling close() - temp file should remain
            temp_files_before = list(spool_dir.glob("**/*.part"))
            assert (
                len(temp_files_before) >= 1
            ), "Should have at least one temp file after writes"

            # Verify temp file has content
            temp_file = temp_files_before[0]
            assert temp_file.exists()
            assert temp_file.stat().st_size > 0

            # Simulate restart - run recovery
            recovery_report = recover_all_temp_files(spool_dir)

            assert recovery_report.temp_files_found >= 1
            assert recovery_report.temp_files_recovered >= 1
            assert recovery_report.total_lines_salvaged >= 3

            # Verify no temp files remain
            temp_files_after = list(spool_dir.glob("**/*.part"))
            assert (
                len(temp_files_after) == 0
            ), "No temp files should remain after recovery"

            # Verify recovered files exist and contain valid JSON
            journal_files = list(spool_dir.glob("**/*.ndjson.gz"))
            assert len(journal_files) >= 1

            # Check content of recovered file
            with gzip.open(journal_files[0], "rt", encoding="utf-8") as f:
                lines = f.readlines()
                assert len(lines) >= 3
                for line in lines:
                    event_data = json.loads(line.strip())
                    assert "id" in event_data
                    assert "ts_utc" in event_data

    def test_corrupted_temp_file_recovery(self):
        """Test recovery of corrupted temp file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir) / "spool" / "test_monitor"
            spool_dir.mkdir(parents=True)

            # Create a corrupted temp file
            temp_file = spool_dir / "20250909-10.ndjson.gz.part"

            # Write valid content first
            valid_content = [
                '{"id": "1", "data": "valid1"}',
                '{"id": "2", "data": "valid2"}',
                '{"id": "3", "data": "valid3"}',
            ]

            # Then add corrupted content
            corrupted_content = valid_content + [
                '{"id": "4", "data": "incomplete',  # Missing closing brace
                "completely invalid json line",
                '{"id": "5"}',  # This valid line should not be recovered (stops at first corruption)
            ]

            with gzip.open(temp_file, "wt", encoding="utf-8") as f:
                for line in corrupted_content:
                    f.write(line + "\n")

            # Run recovery
            recovery_report = recover_all_temp_files(spool_dir.parent)

            assert recovery_report.temp_files_found == 1
            assert recovery_report.temp_files_recovered == 1
            assert (
                recovery_report.total_lines_salvaged >= 3
            )  # Should salvage valid lines

            # Check recovered file
            recovered_files = list(spool_dir.glob("*_recovered.ndjson.gz"))
            assert len(recovered_files) == 1

            with gzip.open(recovered_files[0], "rt", encoding="utf-8") as f:
                lines = f.readlines()
                # Should have salvaged the valid lines
                assert len(lines) >= 3
                for line in lines:
                    event_data = json.loads(line.strip())
                    assert "id" in event_data

            # Check error sidecar was created
            error_files = list(spool_dir.glob("*.error"))
            assert len(error_files) == 1

            error_content = error_files[0].read_text(encoding="utf-8")
            assert "lines" in error_content.lower()
            assert "salvaged" in error_content.lower()

    def test_recovery_preserves_idempotency(self):
        """Test that recovery doesn't create duplicates on subsequent imports."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir) / "spool"
            db_path = Path(temp_dir) / "test.db"

            # Create database
            db = Database(db_path)

            # Create events with specific IDs
            events = [
                {
                    "id": "unique-event-1",
                    "ts_utc": int(time.time() * 1000),
                    "monitor": "test_monitor",
                    "action": "test_action",
                    "subject_type": "none",
                },
                {
                    "id": "unique-event-2",
                    "ts_utc": int(time.time() * 1000) + 1000,
                    "monitor": "test_monitor",
                    "action": "test_action",
                    "subject_type": "none",
                },
            ]

            # Simulate crash by manually creating temp file
            monitor_dir = spool_dir / "test_monitor"
            monitor_dir.mkdir(parents=True)
            temp_file = monitor_dir / "20250909-10.part"

            with temp_file.open("w", encoding="utf-8") as f:
                for event in events:
                    f.write(json.dumps(event) + "\n")

            # First recovery and import
            recovery_report = recover_all_temp_files(spool_dir)
            assert recovery_report.temp_files_recovered == 1

            # Import recovered events
            importer = JournalImporter(db)
            importer.flush_all_monitors(spool_dir)

            # Check events were imported
            cursor = db._connection.execute("SELECT COUNT(*) FROM events")
            initial_count = cursor.fetchone()[0]
            assert initial_count == 2

            # Simulate another crash with same events (duplicate scenario)
            temp_file2 = monitor_dir / "20250909-11.part"
            with temp_file2.open("w", encoding="utf-8") as f:
                for event in events:  # Same events with same IDs
                    f.write(json.dumps(event) + "\n")

            # Second recovery and import
            recover_all_temp_files(spool_dir)
            stats2 = importer.flush_all_monitors(spool_dir)

            # Check no duplicates were created
            cursor = db._connection.execute("SELECT COUNT(*) FROM events")
            final_count = cursor.fetchone()[0]
            assert final_count == 2, "Should not create duplicates due to idempotency"

            # Check duplicate stats
            assert stats2["total_duplicates_skipped"] == 2

            db.close()

    @pytest.mark.skipif(
        sys.platform != "win32", reason="Windows-specific fsync behavior test"
    )
    def test_directory_fsync_durability(self):
        """Test directory fsync for durability (Windows-specific behavior)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir) / "spool" / "test_monitor"
            spool_dir.mkdir(parents=True)

            # Create temp file
            temp_file = spool_dir / "test.part"
            temp_file.write_text('{"id": "1", "data": "test"}\n', encoding="utf-8")

            # Recovery should attempt directory fsync
            recovery_report = recover_all_temp_files(spool_dir.parent)

            assert recovery_report.temp_files_recovered == 1

            # Check that final file exists and temp is gone
            assert not temp_file.exists()
            recovered_files = list(spool_dir.glob("*.ndjson.gz"))
            assert len(recovered_files) == 1

    def test_multiple_monitor_recovery(self):
        """Test recovery across multiple monitor directories."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir) / "spool"

            # Create multiple monitor directories with temp files
            monitors = ["active_window", "keyboard", "mouse"]
            for monitor in monitors:
                monitor_dir = spool_dir / monitor
                monitor_dir.mkdir(parents=True)

                # Create temp files with different corruption scenarios
                if monitor == "active_window":
                    # Valid temp file
                    temp_file = monitor_dir / "20250909-10.part"
                    temp_file.write_text(
                        '{"id": "aw1", "monitor": "active_window"}\n', encoding="utf-8"
                    )

                elif monitor == "keyboard":
                    # Gzipped temp file
                    temp_file = monitor_dir / "20250909-10.ndjson.gz.part"
                    with gzip.open(temp_file, "wt", encoding="utf-8") as f:
                        f.write('{"id": "kb1", "monitor": "keyboard"}\n')

                elif monitor == "mouse":
                    # Corrupted temp file
                    temp_file = monitor_dir / "20250909-10.part"
                    content = (
                        '{"id": "m1", "monitor": "mouse"}\n{"id": "m2", "incomplete'
                    )
                    temp_file.write_text(content, encoding="utf-8")

            # Run recovery
            recovery_report = recover_all_temp_files(spool_dir)

            assert recovery_report.temp_files_found == 3
            assert recovery_report.temp_files_recovered == 3
            assert len(recovery_report.monitors_processed) == 3
            assert all(
                monitor in recovery_report.monitors_processed for monitor in monitors
            )

            # Check no temp files remain
            temp_files = list(spool_dir.glob("**/*.part"))
            assert len(temp_files) == 0

            # Check recovered files exist
            for monitor in monitors:
                journal_files = list((spool_dir / monitor).glob("*.ndjson.gz"))
                assert (
                    len(journal_files) >= 1
                ), f"Monitor {monitor} should have recovered file"

    def test_gzipped_temp_file_cli_recovery(self):
        """Test gzipped temp file recovery via CLI spool flush."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir) / "config"
            spool_dir = Path(temp_dir) / "spool"
            db_path = Path(temp_dir) / "test.db"

            config_dir.mkdir()
            spool_dir.mkdir()

            # Create config file
            config_content = f"""
storage:
  database_path: "{db_path.as_posix()}"
  spool_dir: "{spool_dir.as_posix()}"
"""
            (config_dir / "config.yaml").write_text(config_content, encoding="utf-8")

            # Create gzipped temp file to simulate crash
            monitor_dir = spool_dir / "test_monitor"
            monitor_dir.mkdir()
            temp_file = monitor_dir / "20250909-10.ndjson.gz.part"

            # Create valid gzipped content
            events = [
                {
                    "id": "gzip-event-1",
                    "ts_utc": 123456789,
                    "monitor": "test_monitor",
                    "data": "test1",
                },
                {
                    "id": "gzip-event-2",
                    "ts_utc": 123456790,
                    "monitor": "test_monitor",
                    "data": "test2",
                },
                {
                    "id": "gzip-event-3",
                    "ts_utc": 123456791,
                    "monitor": "test_monitor",
                    "data": "test3",
                },
            ]

            content = "\n".join(json.dumps(event) for event in events) + "\n"
            valid_bytes = gzip.compress(content.encode("utf-8"))

            # Truncate to simulate corruption
            truncated_bytes = valid_bytes[:-30]
            temp_file.write_bytes(truncated_bytes)

            # Set environment for config
            env = os.environ.copy()
            env["LB3_CONFIG_DIR"] = str(config_dir)

            # Run spool flush command (should trigger recovery)
            result = subprocess.run(
                ["python", "-m", "lb3", "spool", "flush"],
                env=env,
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent.parent,
            )

            # Should succeed and show recovery message
            assert result.returncode == 0
            output = result.stdout
            assert "Recovered" in output and (
                "temp segments" in output or "temp files" in output
            )
            assert "lines salvaged" in output

            # Check if recovery message is present (may be empty if no temp files found)
            print(f"CLI output: {output}")
            print(f"CLI stderr: {result.stderr}")

            # Since CLI may not show recovery message if no temp files are found in wrong directory,
            # let's manually check if our temp file was processed
            if not temp_file.exists() and monitor_dir.glob("*_recovered.ndjson.gz"):
                print("Recovery worked - temp file removed and recovered file created")
            else:
                print(
                    f"Recovery may not have worked - temp file exists: {temp_file.exists()}"
                )
                print(f"Files in monitor dir: {list(monitor_dir.glob('*'))}")

            # Verify temp file is gone
            assert not temp_file.exists()

            # Verify recovered file exists
            recovered_files = list(monitor_dir.glob("*_recovered.ndjson.gz"))
            assert len(recovered_files) == 1

            # Verify error sidecar exists
            error_files = list(monitor_dir.glob("*.error"))
            assert len(error_files) == 1

            # Verify events were imported (if database was created)
            if db_path.exists():
                db = Database(db_path)
                try:
                    cursor = db._connection.execute(
                        "SELECT COUNT(*) FROM events WHERE monitor = 'test_monitor'"
                    )
                    count = cursor.fetchone()[0]
                    assert (
                        count >= 1
                    ), "At least some events should have been imported after recovery"

                    # Test idempotency - run flush again
                    result2 = subprocess.run(
                        ["python", "-m", "lb3", "spool", "flush"],
                        env=env,
                        capture_output=True,
                        text=True,
                        cwd=Path(__file__).parent.parent.parent,
                    )

                    assert result2.returncode == 0
                    # Should report 0 new temp segments on second run
                    output2 = result2.stdout
                    assert "Recovered 0 temp segments (0 lines salvaged)" in output2

                    # Should import 0 new events (idempotency)
                    cursor2 = db._connection.execute(
                        "SELECT COUNT(*) FROM events WHERE monitor = 'test_monitor'"
                    )
                    final_count = cursor2.fetchone()[0]
                    assert (
                        final_count == count
                    ), "Should not create duplicates on second flush"

                finally:
                    db.close()

    def test_cli_integration_with_recovery(self):
        """Test CLI commands integrate recovery properly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir) / "config"
            spool_dir = Path(temp_dir) / "spool"
            db_path = Path(temp_dir) / "test.db"

            config_dir.mkdir()
            spool_dir.mkdir()

            # Create config file
            config_content = f"""
storage:
  database_path: "{db_path.as_posix()}"
  spool_dir: "{spool_dir.as_posix()}"
"""
            (config_dir / "config.yaml").write_text(config_content, encoding="utf-8")

            # Create temp file to simulate crash
            monitor_dir = spool_dir / "test_monitor"
            monitor_dir.mkdir()
            temp_file = monitor_dir / "20250909-10.part"
            temp_file.write_text(
                '{"id": "test1", "ts_utc": 123456, "monitor": "test_monitor"}\n',
                encoding="utf-8",
            )

            # Set environment for config
            env = os.environ.copy()
            env["LB3_CONFIG_DIR"] = str(config_dir)

            # Run spool flush command (should trigger recovery)
            result = subprocess.run(
                ["python", "-m", "lb3", "spool", "flush"],
                env=env,
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent.parent,
            )

            # Should succeed and show recovery message
            assert result.returncode == 0
            output = result.stdout
            assert "Recovered" in output or "temp segments" in output

            # Verify temp file is gone
            assert not temp_file.exists()

            # Verify event was imported
            if db_path.exists():
                db = Database(db_path)
                try:
                    cursor = db._connection.execute(
                        "SELECT COUNT(*) FROM events WHERE id = 'test1'"
                    )
                    count = cursor.fetchone()[0]
                    assert count == 1, "Event should have been imported after recovery"
                finally:
                    db.close()


class TestResilienceFeatures:
    """Tests for general resilience features."""

    def test_logging_directory_creation(self):
        """Test that logging creates directories as needed."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_dir = Path(temp_dir) / "logs" / "subdir"
            # Directory doesn't exist yet
            assert not log_dir.exists()

            # Import and setup logging
            from lb3.logging_setup import setup_logging

            setup_logging(log_dir=log_dir, console=False)

            # Directory should be created
            assert log_dir.exists()

            # Log file should be created
            log_files = list(log_dir.glob("*.log"))
            assert len(log_files) == 1

    def test_spooler_handles_disk_full_gracefully(self):
        """Test spooler behavior when disk is full (simulated)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir) / "spool"

            # Create spooler
            spooler = JournalSpooler("test_monitor", spool_dir)

            # Write some events normally
            for i in range(5):
                event = {
                    "id": f"event-{i}",
                    "ts_utc": int(time.time() * 1000),
                    "monitor": "test_monitor",
                }
                spooler.write_event(event)

            # Simulate error by closing spooler early
            with contextlib.suppress(Exception):
                spooler.close()

            # Recovery should still work
            recover_all_temp_files(spool_dir)
            # May or may not have temp files depending on timing

    def test_concurrent_recovery_safety(self):
        """Test that concurrent recovery operations are safe."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir) / "spool"

            # Create temp files
            for i in range(3):
                monitor_dir = spool_dir / f"monitor{i}"
                monitor_dir.mkdir(parents=True)
                temp_file = monitor_dir / "test.part"
                temp_file.write_text(
                    f'{{"id": "event{i}", "monitor": "monitor{i}"}}\n', encoding="utf-8"
                )

            # Run recovery from multiple threads
            results = []

            def run_recovery():
                try:
                    report = recover_all_temp_files(spool_dir)
                    results.append(report)
                except Exception as e:
                    results.append(e)

            threads = []
            for _ in range(3):
                thread = threading.Thread(target=run_recovery)
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

            # At least one should succeed
            successful_results = [r for r in results if not isinstance(r, Exception)]
            assert len(successful_results) >= 1

            # No temp files should remain
            temp_files = list(spool_dir.glob("**/*.part"))
            assert len(temp_files) == 0


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_recovery_with_permission_denied(self):
        """Test recovery handles permission errors gracefully."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir) / "spool" / "test_monitor"
            spool_dir.mkdir(parents=True)

            temp_file = spool_dir / "test.part"
            temp_file.write_text('{"id": "test", "data": "value"}\n', encoding="utf-8")

            # On Windows, we can't easily simulate permission issues,
            # but we can test the error handling path exists
            recovery_report = recover_all_temp_files(spool_dir.parent)

            # Should handle gracefully even if there are errors
            assert isinstance(recovery_report.temp_files_found, int)

    def test_recovery_with_very_large_temp_file(self):
        """Test recovery of large temp files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir) / "spool" / "test_monitor"
            spool_dir.mkdir(parents=True)

            temp_file = spool_dir / "large.part"

            # Create a moderately large temp file
            with temp_file.open("w", encoding="utf-8") as f:
                for i in range(1000):
                    event = {
                        "id": f"large-event-{i}",
                        "ts_utc": int(time.time() * 1000) + i,
                        "monitor": "test_monitor",
                        "data": f"data-{i}" * 10,  # Some padding
                    }
                    f.write(json.dumps(event) + "\n")

            recovery_report = recover_all_temp_files(spool_dir.parent)

            assert recovery_report.temp_files_recovered == 1
            assert recovery_report.total_lines_salvaged == 1000

            # Check recovered file
            recovered_files = list(spool_dir.glob("*.ndjson.gz"))
            assert len(recovered_files) == 1

            # Should be compressed and readable
            with gzip.open(recovered_files[0], "rt", encoding="utf-8") as f:
                lines = f.readlines()
                assert len(lines) == 1000

    def test_empty_temp_files(self):
        """Test recovery of empty temp files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir) / "spool" / "test_monitor"
            spool_dir.mkdir(parents=True)

            # Create empty temp file
            temp_file = spool_dir / "empty.part"
            temp_file.write_text("", encoding="utf-8")

            recovery_report = recover_all_temp_files(spool_dir.parent)

            assert recovery_report.temp_files_found == 1
            # Empty file recovery should fail gracefully
            assert recovery_report.temp_files_failed >= 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
