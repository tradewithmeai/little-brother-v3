"""Time utilities for AI analysis."""


def floor_hour_ms(ts_ms: int) -> int:
    """Floor timestamp to the start of its hour (UTC).

    Args:
        ts_ms: Timestamp in UTC milliseconds

    Returns:
        UTC milliseconds aligned to start of hour (:00)
    """
    # Convert to seconds, floor to hour, convert back to ms
    ts_sec = ts_ms // 1000
    hour_sec = (ts_sec // 3600) * 3600
    return hour_sec * 1000


def ceil_hour_ms(ts_ms: int) -> int:
    """Ceil timestamp to the start of the next hour (UTC).

    Args:
        ts_ms: Timestamp in UTC milliseconds

    Returns:
        UTC milliseconds aligned to start of next hour (:00)
    """
    # Convert to seconds, ceil to hour, convert back to ms
    ts_sec = ts_ms // 1000
    hour_sec = ((ts_sec + 3599) // 3600) * 3600
    return hour_sec * 1000


def iter_hours(since_utc_ms: int, until_utc_ms: int) -> list[tuple[int, int]]:
    """Generate half-open hour windows [hstart, hend) covering the range.

    Args:
        since_utc_ms: Start time in UTC milliseconds (will be floored)
        until_utc_ms: End time in UTC milliseconds (will be ceiled)

    Returns:
        List of (hstart_ms, hend_ms) tuples where:
        - hstart is aligned to :00
        - hend = hstart + 3600000 (1 hour)
        - Windows are half-open [hstart, hend)
        - Empty list if since >= until after alignment
    """
    start_hour = floor_hour_ms(since_utc_ms)
    end_hour = ceil_hour_ms(until_utc_ms)

    if start_hour >= end_hour:
        return []

    windows = []
    current_hour = start_hour

    while current_hour < end_hour:
        windows.append((current_hour, current_hour + 3600000))
        current_hour += 3600000

    return windows
