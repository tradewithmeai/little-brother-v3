"""Unit tests for Database.upsert_file_record method."""

import tempfile
from pathlib import Path

import pytest

from lb3.database import Database


@pytest.fixture
def temp_database():
    """Provide a temporary database that gets properly cleaned up."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test.db"
        db = Database(db_path)
        try:
            yield db
        finally:
            db.close()


def test_upsert_file_record_first_insert(temp_database):
    """Test first insert returns ULID and sets first_seen_utc == last_seen_utc."""
    db = temp_database

    path_hash = "test_path_hash_123"
    ext = "pdf"
    ts_ms = 1609459200000  # 2021-01-01 00:00:00 UTC

    # First insert
    file_id = db.upsert_file_record(path_hash, ext, ts_ms)

    # Verify return value is ULID
    assert isinstance(file_id, str)
    assert len(file_id) == 26  # ULID length

    # Verify database record
    with db._get_connection() as conn:
        cursor = conn.execute(
            "SELECT id, path_hash, ext, first_seen_utc, last_seen_utc FROM files WHERE path_hash = ?",
            (path_hash,),
        )
        result = cursor.fetchone()

    assert result is not None
    assert result[0] == file_id
    assert result[1] == path_hash
    assert result[2] == ext
    assert result[3] == ts_ms  # first_seen_utc
    assert result[4] == ts_ms  # last_seen_utc
    assert result[3] == result[4]  # first_seen_utc == last_seen_utc


def test_upsert_file_record_second_call_same_path(temp_database):
    """Test second call with same path_hash updates last_seen_utc and returns same id."""
    db = temp_database

    path_hash = "test_path_hash_456"
    ext = "txt"
    ts_ms_1 = 1609459200000  # First timestamp
    ts_ms_2 = 1609459260000  # Later timestamp

    # First call
    file_id_1 = db.upsert_file_record(path_hash, ext, ts_ms_1)

    # Second call with later timestamp
    file_id_2 = db.upsert_file_record(path_hash, ext, ts_ms_2)

    # Should return same ID
    assert file_id_1 == file_id_2

    # Verify last_seen_utc was updated
    with db._get_connection() as conn:
        cursor = conn.execute(
            "SELECT first_seen_utc, last_seen_utc FROM files WHERE id = ?", (file_id_1,)
        )
        result = cursor.fetchone()

    assert result is not None
    assert result[0] == ts_ms_1  # first_seen_utc unchanged
    assert result[1] == ts_ms_2  # last_seen_utc updated
    assert result[1] >= result[0]  # last_seen_utc >= first_seen_utc


def test_upsert_file_record_ext_handling(temp_database):
    """Test extension handling with COALESCE behavior."""
    db = temp_database

    path_hash = "test_path_hash_ext"
    ts_ms = 1609459200000

    # Case 1: DB has NULL ext, call provides "pdf" -> ext becomes "pdf"
    file_id = db.upsert_file_record(path_hash, "", ts_ms)

    # Verify ext is empty initially
    with db._get_connection() as conn:
        cursor = conn.execute("SELECT ext FROM files WHERE id = ?", (file_id,))
        result = cursor.fetchone()
    assert result[0] == ""

    # Update with non-empty extension
    db.upsert_file_record(path_hash, "pdf", ts_ms + 1000)

    # Verify ext became "pdf"
    with db._get_connection() as conn:
        cursor = conn.execute("SELECT ext FROM files WHERE id = ?", (file_id,))
        result = cursor.fetchone()
    assert result[0] == "pdf"

    # Case 2: DB has non-empty ext, calling with "" doesn't overwrite it
    db.upsert_file_record(path_hash, "", ts_ms + 2000)

    # Verify ext is still "pdf" (not overwritten by empty string)
    with db._get_connection() as conn:
        cursor = conn.execute("SELECT ext FROM files WHERE id = ?", (file_id,))
        result = cursor.fetchone()
    assert result[0] == "pdf"


def test_upsert_file_record_different_paths(temp_database):
    """Test different path_hashes get different IDs."""
    db = temp_database

    path_hash_1 = "path_hash_1"
    path_hash_2 = "path_hash_2"
    ext = "log"
    ts_ms = 1609459200000

    file_id_1 = db.upsert_file_record(path_hash_1, ext, ts_ms)
    file_id_2 = db.upsert_file_record(path_hash_2, ext, ts_ms)

    # Different paths should get different IDs
    assert file_id_1 != file_id_2

    # Both records should exist
    with db._get_connection() as conn:
        cursor = conn.execute("SELECT COUNT(*) FROM files")
        count = cursor.fetchone()[0]
    assert count == 2


def test_upsert_file_record_empty_extension(temp_database):
    """Test handling of empty extension."""
    db = temp_database

    path_hash = "test_empty_ext"
    ts_ms = 1609459200000

    # Insert with empty extension
    file_id = db.upsert_file_record(path_hash, "", ts_ms)

    # Verify record exists with empty extension
    with db._get_connection() as conn:
        cursor = conn.execute("SELECT ext FROM files WHERE id = ?", (file_id,))
        result = cursor.fetchone()
    assert result[0] == ""


def test_upsert_file_record_basic_functionality():
    """Test basic functionality with explicit cleanup."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test.db"
        db = Database(db_path)

        try:
            # Test with very long path_hash
            long_path_hash = "x" * 1000
            file_id = db.upsert_file_record(long_path_hash, "txt", 1609459200000)
            assert isinstance(file_id, str)
            assert len(file_id) == 26

            # Test with zero timestamp
            file_id_zero = db.upsert_file_record("zero_time", "log", 0)
            assert isinstance(file_id_zero, str)

            # Test with negative timestamp
            file_id_neg = db.upsert_file_record("neg_time", "tmp", -1000)
            assert isinstance(file_id_neg, str)

        finally:
            db.close()
