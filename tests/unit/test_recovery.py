"""Unit tests for crash recovery functionality."""

import gzip
import json
import tempfile
from pathlib import Path

import pytest

from lb3.recovery import (
    RecoveryReport,
    recover_all_temp_files,
    recover_monitor_temp_files,
    salvage_gzipped_ndjson,
    salvage_plain_ndjson,
)


class TestSalvagePlainNdjson:
    """Tests for plain NDJSON salvage functionality."""

    def test_salvage_valid_file(self):
        """Test salvaging a file with all valid JSON lines."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            temp_file = temp_dir / "test.part"

            # Create valid NDJSON content
            valid_lines = [
                '{"id": "1", "data": "test1"}',
                '{"id": "2", "data": "test2"}',
                '{"id": "3", "data": "test3"}',
            ]
            temp_file.write_text("\n".join(valid_lines) + "\n", encoding="utf-8")

            stats = salvage_plain_ndjson(temp_file)

            assert stats.success
            assert stats.lines_total == 3
            assert stats.lines_salvaged == 3
            assert stats.lines_corrupted == 0
            assert stats.recovered_path is not None
            assert stats.error_path is None
            assert not temp_file.exists()  # Temp file should be removed

            # Check recovered file content
            with gzip.open(stats.recovered_path, "rt", encoding="utf-8") as f:
                content = f.read()
                lines = content.strip().split("\n")
                assert len(lines) == 3
                for line in lines:
                    json.loads(line)  # Should parse without error

    def test_salvage_corrupted_file(self):
        """Test salvaging a file with some corrupted lines."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            temp_file = temp_dir / "test.part"

            # Create mixed valid/invalid content
            content_lines = [
                '{"id": "1", "data": "test1"}',  # valid
                '{"id": "2", "data": "test2"}',  # valid
                '{"id": "3", "data": "test',  # corrupted - missing closing brace
                "invalid json line",  # corrupted - not JSON
                '{"id": "4", "data": "test4"}',  # this should not be recovered
            ]
            temp_file.write_text("\n".join(content_lines) + "\n", encoding="utf-8")

            stats = salvage_plain_ndjson(temp_file)

            assert stats.success
            assert stats.lines_total == 5
            assert stats.lines_salvaged == 2
            assert stats.lines_corrupted == 1  # stops at first corruption
            assert stats.recovered_path is not None
            assert stats.error_path is not None
            assert not temp_file.exists()

            # Check recovered file content
            with gzip.open(stats.recovered_path, "rt", encoding="utf-8") as f:
                content = f.read()
                lines = content.strip().split("\n")
                assert len(lines) == 2

            # Check error sidecar
            error_content = stats.error_path.read_text(encoding="utf-8")
            assert "2 valid lines" in error_content
            assert "1 corrupted lines discarded" in error_content

    def test_salvage_empty_file(self):
        """Test salvaging an empty file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            temp_file = temp_dir / "empty.part"
            temp_file.write_text("", encoding="utf-8")

            stats = salvage_plain_ndjson(temp_file)

            assert not stats.success
            assert stats.lines_total == 0
            assert stats.lines_salvaged == 0
            assert stats.recovered_path is None

    def test_salvage_only_whitespace(self):
        """Test salvaging a file with only whitespace."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            temp_file = temp_dir / "whitespace.part"
            temp_file.write_text("\n\n   \n\t\n\n", encoding="utf-8")

            stats = salvage_plain_ndjson(temp_file)

            assert not stats.success
            assert stats.lines_salvaged == 0


class TestSalvageGzippedNdjson:
    """Tests for gzipped NDJSON salvage functionality."""

    def test_salvage_valid_gzipped_file(self):
        """Test salvaging a valid gzipped file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            temp_file = temp_dir / "test.ndjson.gz.part"

            # Create valid gzipped NDJSON content
            valid_lines = [
                '{"id": "1", "data": "test1"}',
                '{"id": "2", "data": "test2"}',
                '{"id": "3", "data": "test3"}',
            ]
            with gzip.open(temp_file, "wt", encoding="utf-8") as f:
                for line in valid_lines:
                    f.write(line + "\n")

            stats = salvage_gzipped_ndjson(temp_file)

            assert stats.success
            assert stats.lines_total == 3
            assert stats.lines_salvaged == 3
            assert stats.lines_corrupted == 0
            assert stats.recovered_path is not None
            assert stats.error_path is not None  # Always created for gzipped files
            assert not temp_file.exists()

    def test_salvage_corrupted_gzipped_file(self):
        """Test salvaging a file with corrupted gzip content."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            temp_file = temp_dir / "20250909-15.ndjson.gz.part"

            # Create valid gzipped content first
            valid_lines = [
                '{"id": "test1", "data": "value1", "ts": 123}',
                '{"id": "test2", "data": "value2", "ts": 124}',
                '{"id": "test3", "data": "value3", "ts": 125}',
            ]
            valid_content = "\n".join(valid_lines) + "\n"
            valid_bytes = gzip.compress(valid_content.encode("utf-8"))

            # Truncate last 30 bytes to simulate corruption
            truncated_bytes = valid_bytes[:-30]
            temp_file.write_bytes(truncated_bytes)

            stats = salvage_gzipped_ndjson(temp_file)

            # Should successfully recover at least some lines
            assert stats.success
            assert stats.recovered_path is not None
            assert stats.error_path is not None
            assert stats.lines_salvaged >= 1  # Should recover at least some lines
            assert not temp_file.exists()  # Temp file should be removed

            # Check recovered filename follows convention
            assert "_recovered.ndjson.gz" in stats.recovered_path.name
            assert stats.recovered_path.name == "20250909-15_recovered.ndjson.gz"

            # Verify recovered file is valid gzip and readable
            with gzip.open(stats.recovered_path, "rt", encoding="utf-8") as f:
                recovered_content = f.read()
                recovered_lines = recovered_content.strip().split("\n")
                assert len(recovered_lines) == stats.lines_salvaged

                # All recovered lines should be valid JSON
                for line in recovered_lines:
                    json.loads(line)  # Should not raise

            # Check error sidecar format
            error_content = stats.error_path.read_text(encoding="utf-8")
            assert "bytes_read=" in error_content
            assert "lines_salvaged=" in error_content
            assert "reason=" in error_content
            # May say "complete file" if truncation didn't cause zlib error, or "truncated gzip" if it did
            assert ("complete file" in error_content) or (
                "truncated gzip" in error_content
            )


class TestRecoverMonitorTempFiles:
    """Tests for monitor temp file recovery."""

    def test_recover_no_temp_files(self):
        """Test recovery when no temp files exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            monitor_dir = Path(temp_dir) / "test_monitor"
            monitor_dir.mkdir()

            results = recover_monitor_temp_files(monitor_dir)
            assert len(results) == 0

    def test_recover_multiple_temp_files(self):
        """Test recovery of multiple temp files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            monitor_dir = Path(temp_dir) / "test_monitor"
            monitor_dir.mkdir()

            # Create multiple temp files
            temp1 = monitor_dir / "file1.part"
            temp2 = monitor_dir / "file2.ndjson.gz.part"

            temp1.write_text('{"id": "1", "data": "test1"}\n', encoding="utf-8")

            with gzip.open(temp2, "wt", encoding="utf-8") as f:
                f.write('{"id": "2", "data": "test2"}\n')

            results = recover_monitor_temp_files(monitor_dir)

            assert len(results) == 2
            assert all(stat.success for stat in results)
            assert not temp1.exists()
            assert not temp2.exists()

    def test_recover_nonexistent_directory(self):
        """Test recovery on non-existent directory."""
        nonexistent_dir = Path("/nonexistent/monitor/dir")
        results = recover_monitor_temp_files(nonexistent_dir)
        assert len(results) == 0


class TestRecoverAllTempFiles:
    """Tests for recovery across all monitors."""

    def test_recover_all_empty_spool(self):
        """Test recovery on empty spool directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)

            report = recover_all_temp_files(spool_dir)

            assert isinstance(report, RecoveryReport)
            assert report.temp_files_found == 0
            assert report.temp_files_recovered == 0
            assert report.total_lines_salvaged == 0
            assert len(report.salvage_stats) == 0

    def test_recover_all_with_monitors(self):
        """Test recovery across multiple monitor directories."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)

            # Create monitor directories with temp files
            monitor1_dir = spool_dir / "monitor1"
            monitor2_dir = spool_dir / "monitor2"
            monitor1_dir.mkdir()
            monitor2_dir.mkdir()

            # Add temp files
            temp1 = monitor1_dir / "file1.part"
            temp2 = monitor2_dir / "file2.part"

            temp1.write_text('{"id": "1", "data": "test1"}\n', encoding="utf-8")
            temp2.write_text(
                '{"id": "2", "data": "test2"}\n{"id": "3", "data": "test3"}\n',
                encoding="utf-8",
            )

            report = recover_all_temp_files(spool_dir)

            assert report.temp_files_found == 2
            assert report.temp_files_recovered == 2
            assert report.temp_files_failed == 0
            assert report.total_lines_salvaged == 3
            assert len(report.salvage_stats) == 2
            assert "monitor1" in report.monitors_processed
            assert "monitor2" in report.monitors_processed

    def test_recover_specific_monitors(self):
        """Test recovery limited to specific monitors."""
        with tempfile.TemporaryDirectory() as temp_dir:
            spool_dir = Path(temp_dir)

            # Create multiple monitor directories
            for monitor in ["monitor1", "monitor2", "monitor3"]:
                monitor_dir = spool_dir / monitor
                monitor_dir.mkdir()
                temp_file = monitor_dir / f"{monitor}.part"
                temp_file.write_text('{"id": "1", "data": "test"}\n', encoding="utf-8")

            # Recover only specific monitors
            report = recover_all_temp_files(
                spool_dir, monitors=["monitor1", "monitor3"]
            )

            assert report.temp_files_found == 2
            assert "monitor1" in report.monitors_processed
            assert "monitor3" in report.monitors_processed
            assert "monitor2" not in report.monitors_processed

            # Monitor2 temp file should still exist
            assert (spool_dir / "monitor2" / "monitor2.part").exists()

    def test_nonexistent_spool_directory(self):
        """Test recovery on non-existent spool directory."""
        nonexistent_spool = Path("/nonexistent/spool")
        report = recover_all_temp_files(nonexistent_spool)

        assert report.temp_files_found == 0
        assert len(report.monitors_processed) == 0


class TestRecoveryReport:
    """Tests for RecoveryReport functionality."""

    def test_summary_line_no_files(self):
        """Test summary line when no temp files found."""
        report = RecoveryReport(
            monitors_processed=[],
            temp_files_found=0,
            temp_files_recovered=0,
            temp_files_failed=0,
            total_lines_salvaged=0,
            salvage_stats=[],
        )

        assert report.summary_line() == "Recovery sweep: no temp files found"

    def test_summary_line_with_files(self):
        """Test summary line with recovered files."""
        report = RecoveryReport(
            monitors_processed=["monitor1"],
            temp_files_found=2,
            temp_files_recovered=1,
            temp_files_failed=1,
            total_lines_salvaged=10,
            salvage_stats=[],
        )

        summary = report.summary_line()
        assert "1/2 temp files recovered" in summary
        assert "10 lines salvaged" in summary


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
