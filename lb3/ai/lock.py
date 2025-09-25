"""Advisory lock management for AI analysis."""

import secrets
import time
from typing import Any

from ..database import Database


def now_ms() -> int:
    """Get current UTC milliseconds timestamp.

    Returns:
        Current UTC timestamp in milliseconds
    """
    return int(time.time() * 1000)


def acquire_lock(db: Database, lock_name: str, ttl_sec: int) -> dict[str, Any]:
    """Acquire an advisory lock.

    Args:
        db: Database instance
        lock_name: Name of the lock to acquire
        ttl_sec: Time-to-live in seconds

    Returns:
        Dictionary with success/failure status:
        - success=True: {"success": True, "owner_token": str, "expires_utc_ms": int}
        - success=False: {"success": False, "reason": str, "held_by": str, "expires_utc_ms": int}
    """
    current_time = now_ms()
    expires_utc_ms = current_time + (ttl_sec * 1000)

    with db._get_connection() as conn:
        # Clean up expired locks first
        conn.execute("DELETE FROM ai_lock WHERE expires_utc_ms <= ?", (current_time,))

        # Check if lock already exists and is valid
        existing = conn.execute(
            "SELECT owner_token, expires_utc_ms FROM ai_lock WHERE lock_name = ?",
            (lock_name,),
        ).fetchone()

        if existing:
            return {
                "success": False,
                "reason": "lock_held",
                "held_by": existing[0],
                "expires_utc_ms": existing[1],
            }

        # Acquire the lock
        owner_token = secrets.token_hex(16)
        conn.execute(
            "INSERT INTO ai_lock (lock_name, owner_token, acquired_utc_ms, expires_utc_ms) VALUES (?, ?, ?, ?)",
            (lock_name, owner_token, current_time, expires_utc_ms),
        )
        conn.commit()

        return {
            "success": True,
            "owner_token": owner_token,
            "expires_utc_ms": expires_utc_ms,
        }


def renew_lock(
    db: Database, lock_name: str, owner_token: str, ttl_sec: int
) -> dict[str, Any]:
    """Renew an existing advisory lock.

    Args:
        db: Database instance
        lock_name: Name of the lock to renew
        owner_token: Token proving ownership
        ttl_sec: New time-to-live in seconds

    Returns:
        Dictionary with success/failure status:
        - success=True: {"success": True, "expires_utc_ms": int}
        - success=False: {"success": False, "reason": str}
    """
    current_time = now_ms()
    new_expires = current_time + (ttl_sec * 1000)

    with db._get_connection() as conn:
        # Clean up expired locks first
        conn.execute("DELETE FROM ai_lock WHERE expires_utc_ms <= ?", (current_time,))

        # Try to renew the lock
        cursor = conn.execute(
            "UPDATE ai_lock SET expires_utc_ms = ? WHERE lock_name = ? AND owner_token = ?",
            (new_expires, lock_name, owner_token),
        )

        if cursor.rowcount == 0:
            # Check if lock exists but with different owner
            existing = conn.execute(
                "SELECT owner_token FROM ai_lock WHERE lock_name = ?", (lock_name,)
            ).fetchone()

            if existing:
                return {"success": False, "reason": "not_owner"}
            else:
                return {"success": False, "reason": "not_found"}

        conn.commit()
        return {"success": True, "expires_utc_ms": new_expires}


def release_lock(db: Database, lock_name: str, owner_token: str) -> dict[str, Any]:
    """Release an advisory lock.

    Args:
        db: Database instance
        lock_name: Name of the lock to release
        owner_token: Token proving ownership

    Returns:
        Dictionary with success/failure status:
        - success=True: {"success": True}
        - success=False: {"success": False, "reason": str}
    """
    with db._get_connection() as conn:
        cursor = conn.execute(
            "DELETE FROM ai_lock WHERE lock_name = ? AND owner_token = ?",
            (lock_name, owner_token),
        )

        if cursor.rowcount == 0:
            # Check if lock exists but with different owner
            existing = conn.execute(
                "SELECT owner_token FROM ai_lock WHERE lock_name = ?", (lock_name,)
            ).fetchone()

            if existing:
                return {"success": False, "reason": "not_owner"}
            else:
                return {"success": False, "reason": "not_found"}

        conn.commit()
        return {"success": True}


def lock_status(db: Database, lock_name: str) -> dict[str, Any]:
    """Get status of an advisory lock.

    Args:
        db: Database instance
        lock_name: Name of the lock to check

    Returns:
        Dictionary with lock status:
        - exists=True: {"exists": True, "owner_token": str, "acquired_utc_ms": int, "expires_utc_ms": int}
        - exists=False: {"exists": False}
    """
    current_time = now_ms()

    with db._get_connection() as conn:
        # Clean up expired locks first
        conn.execute("DELETE FROM ai_lock WHERE expires_utc_ms <= ?", (current_time,))

        # Check lock status
        result = conn.execute(
            "SELECT owner_token, acquired_utc_ms, expires_utc_ms FROM ai_lock WHERE lock_name = ?",
            (lock_name,),
        ).fetchone()

        if result:
            return {
                "exists": True,
                "owner_token": result[0],
                "acquired_utc_ms": result[1],
                "expires_utc_ms": result[2],
            }
        else:
            return {"exists": False}
