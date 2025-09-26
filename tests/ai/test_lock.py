"""Test AI advisory lock functionality."""

import tempfile
import time
from pathlib import Path

from lb3.ai.lock import acquire_lock, lock_status, release_lock, renew_lock
from lb3.database import Database


def close_db_connections(db: Database):
    """Ensure all database connections are properly closed."""
    try:
        # Close any active connections
        if hasattr(db, '_connection') and db._connection:
            db._connection.close()
        # Force garbage collection of any remaining connections
        import gc
        gc.collect()
    except Exception:
        pass


def test_acquire_lock_success():
    """Test successful lock acquisition."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_acquire.db"
        db = Database(db_path)

        try:
            # Acquire lock
            result = acquire_lock(db, "test_lock", 300)

            assert result["success"] is True
            assert "owner_token" in result
            assert "expires_utc_ms" in result
            assert len(result["owner_token"]) == 32  # 16 bytes hex = 32 chars
            assert result["expires_utc_ms"] > int(time.time() * 1000)
        finally:
            close_db_connections(db)


def test_acquire_lock_already_held():
    """Test lock acquisition when already held."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_acquire_held.db"
        db = Database(db_path)

        try:
            # First acquisition
            result1 = acquire_lock(db, "test_lock", 300)
            assert result1["success"] is True

            # Second acquisition should fail
            result2 = acquire_lock(db, "test_lock", 300)
            assert result2["success"] is False
            assert result2["reason"] == "lock_held"
            assert result2["held_by"] == result1["owner_token"]
            assert "expires_utc_ms" in result2
        finally:
            close_db_connections(db)


def test_renew_lock_success():
    """Test successful lock renewal."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_renew.db"
        db = Database(db_path)

        try:
            # Acquire lock
            result1 = acquire_lock(db, "test_lock", 300)
            assert result1["success"] is True

            # Renew lock
            result2 = renew_lock(db, "test_lock", result1["owner_token"], 600)
            assert result2["success"] is True
            assert result2["expires_utc_ms"] > result1["expires_utc_ms"]
        finally:
            close_db_connections(db)


def test_renew_lock_not_owner():
    """Test lock renewal with wrong owner token."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_renew_not_owner.db"
        db = Database(db_path)

        try:
            # Acquire lock
            result1 = acquire_lock(db, "test_lock", 300)
            assert result1["success"] is True

            # Try to renew with wrong token
            result2 = renew_lock(db, "test_lock", "wrong_token", 600)
            assert result2["success"] is False
            assert result2["reason"] == "not_owner"
        finally:
            close_db_connections(db)


def test_renew_lock_not_found():
    """Test lock renewal when lock doesn't exist."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_renew_not_found.db"
        db = Database(db_path)

        try:
            # Try to renew non-existent lock
            result = renew_lock(db, "nonexistent_lock", "any_token", 600)
            assert result["success"] is False
            assert result["reason"] == "not_found"
        finally:
            close_db_connections(db)


def test_release_lock_success():
    """Test successful lock release."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_release.db"
        db = Database(db_path)

        try:
            # Acquire lock
            result1 = acquire_lock(db, "test_lock", 300)
            assert result1["success"] is True

            # Release lock
            result2 = release_lock(db, "test_lock", result1["owner_token"])
            assert result2["success"] is True
        finally:
            close_db_connections(db)


def test_release_lock_not_owner():
    """Test lock release with wrong owner token."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_release_not_owner.db"
        db = Database(db_path)

        try:
            # Acquire lock
            result1 = acquire_lock(db, "test_lock", 300)
            assert result1["success"] is True

            # Try to release with wrong token
            result2 = release_lock(db, "test_lock", "wrong_token")
            assert result2["success"] is False
            assert result2["reason"] == "not_owner"
        finally:
            close_db_connections(db)


def test_release_lock_not_found():
    """Test lock release when lock doesn't exist."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_release_not_found.db"
        db = Database(db_path)

        try:
            # Try to release non-existent lock
            result = release_lock(db, "nonexistent_lock", "any_token")
            assert result["success"] is False
            assert result["reason"] == "not_found"
        finally:
            close_db_connections(db)


def test_lock_status_exists():
    """Test lock status when lock exists."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_status_exists.db"
        db = Database(db_path)

        try:
            # Acquire lock
            acquire_result = acquire_lock(db, "test_lock", 300)
            assert acquire_result["success"] is True

            # Check status
            status_result = lock_status(db, "test_lock")
            assert status_result["exists"] is True
            assert status_result["owner_token"] == acquire_result["owner_token"]
            assert status_result["expires_utc_ms"] == acquire_result["expires_utc_ms"]
            assert "acquired_utc_ms" in status_result
        finally:
            close_db_connections(db)


def test_lock_status_not_exists():
    """Test lock status when lock doesn't exist."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_status_not_exists.db"
        db = Database(db_path)

        try:
            # Check status of non-existent lock
            result = lock_status(db, "nonexistent_lock")
            assert result["exists"] is False
            assert len(result) == 1  # Only "exists" key
        finally:
            close_db_connections(db)


def test_expired_lock_cleanup():
    """Test that expired locks are automatically cleaned up."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_expired.db"
        db = Database(db_path)

        try:
            # Acquire lock with very short TTL
            result1 = acquire_lock(db, "test_lock", 1)  # 1 second
            assert result1["success"] is True

            # Wait for lock to expire
            time.sleep(1.1)

            # Try to acquire again - should succeed because expired lock was cleaned up
            result2 = acquire_lock(db, "test_lock", 300)
            assert result2["success"] is True
            assert result2["owner_token"] != result1["owner_token"]
        finally:
            close_db_connections(db)


def test_lock_after_release():
    """Test acquiring lock after it's been released."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_after_release.db"
        db = Database(db_path)

        try:
            # Acquire lock
            result1 = acquire_lock(db, "test_lock", 300)
            assert result1["success"] is True

            # Release lock
            release_result = release_lock(db, "test_lock", result1["owner_token"])
            assert release_result["success"] is True

            # Acquire again - should succeed with different token
            result2 = acquire_lock(db, "test_lock", 300)
            assert result2["success"] is True
            assert result2["owner_token"] != result1["owner_token"]
        finally:
            close_db_connections(db)


def test_multiple_locks():
    """Test managing multiple different locks simultaneously."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_multiple.db"
        db = Database(db_path)

        try:
            # Acquire multiple locks
            result1 = acquire_lock(db, "lock1", 300)
            result2 = acquire_lock(db, "lock2", 300)
            result3 = acquire_lock(db, "lock3", 300)

            assert result1["success"] is True
            assert result2["success"] is True
            assert result3["success"] is True

            # All should have different tokens
            tokens = {
                result1["owner_token"],
                result2["owner_token"],
                result3["owner_token"],
            }
            assert len(tokens) == 3

            # Check status of all locks
            status1 = lock_status(db, "lock1")
            status2 = lock_status(db, "lock2")
            status3 = lock_status(db, "lock3")

            assert status1["exists"] is True
            assert status2["exists"] is True
            assert status3["exists"] is True
        finally:
            close_db_connections(db)
