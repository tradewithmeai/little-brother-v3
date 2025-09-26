"""Foreground focus sessionisation utilities."""

from ..database import Database


def build_window_sessions(
    db: Database, since_ms: int, until_ms: int, idle_threshold_ms: int = 60000
) -> list[dict]:
    """Build foreground window sessions from active_window events.

    Args:
        db: Database instance
        since_ms: Start time in UTC milliseconds (inclusive)
        until_ms: End time in UTC milliseconds (exclusive)
        idle_threshold_ms: Maximum gap between events to maintain session

    Returns:
        List of session dicts with start_ms, end_ms, window_id, app_id
        Sessions are clamped to [since_ms, until_ms) and non-overlapping
    """
    with db._get_connection() as conn:
        # Get active_window events with window/app info
        events = conn.execute(
            """
            SELECT
                e.ts_utc,
                e.subject_id as window_id,
                w.app_id
            FROM events e
            LEFT JOIN windows w ON w.id = e.subject_id
            WHERE e.monitor = 'active_window'
            AND e.ts_utc >= ?
            AND e.ts_utc < ?
            ORDER BY e.ts_utc
            """,
            (since_ms, until_ms),
        ).fetchall()

    if not events:
        return []

    sessions = []

    # Process events in pairs to create sessions
    for i in range(len(events)):
        current_ts, current_window_id, current_app_id = events[i]

        # Determine session end time
        if i + 1 < len(events):
            next_ts, _, _ = events[i + 1]
            gap = next_ts - current_ts

            if gap > idle_threshold_ms:
                # Large gap - session ends immediately (no extension)
                # This creates a very brief session, effectively ending at current_ts
                session_end = current_ts + 1000  # Minimal duration
            else:
                # Normal gap - session ends when next event starts
                session_end = next_ts
        else:
            # Last event - session extends to until_ms
            session_end = until_ms

        # Only create session if it has reasonable duration
        if session_end > current_ts:
            # Clamp to bounds
            start_clamped = max(current_ts, since_ms)
            end_clamped = min(session_end, until_ms)

            if start_clamped < end_clamped:
                sessions.append(
                    {
                        "start_ms": start_clamped,
                        "end_ms": end_clamped,
                        "window_id": current_window_id,
                        "app_id": current_app_id,
                    }
                )

    return sorted(sessions, key=lambda s: s["start_ms"])


def count_context_switches(sessions: list[dict], hstart_ms: int, hend_ms: int) -> int:
    """Count context switches within an hour window.

    Args:
        sessions: List of session dicts from build_window_sessions
        hstart_ms: Hour start in UTC milliseconds
        hend_ms: Hour end in UTC milliseconds (exclusive)

    Returns:
        Number of session transitions within the hour window
    """
    # Find sessions that overlap with the hour window
    overlapping_sessions = []
    for session in sessions:
        start = max(session["start_ms"], hstart_ms)
        end = min(session["end_ms"], hend_ms)
        if start < end:
            overlapping_sessions.append(session)

    # Count transitions - each overlapping session after the first is a transition
    return max(0, len(overlapping_sessions) - 1)
