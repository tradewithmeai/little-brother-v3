"""Metric catalog seeding and management for AI analysis."""

from ..database import Database


def seed_metric_catalog(db: Database) -> dict[str, int]:
    """Seed the metric catalog with standard metrics.

    Args:
        db: Database instance

    Returns:
        Dict with 'inserted', 'updated', and 'total' counts
    """
    metrics = [
        {
            "metric_key": "focus_minutes",
            "description": "Total minutes of focused foreground activity within the period.",
            "unit": "minutes",
            "version": 1,
        },
        {
            "metric_key": "idle_minutes",
            "description": "Minutes without meaningful activity (derived from focus gaps).",
            "unit": "minutes",
            "version": 1,
        },
        {
            "metric_key": "keyboard_events",
            "description": "Number of keyboard input events observed.",
            "unit": "count",
            "version": 1,
        },
        {
            "metric_key": "mouse_events",
            "description": "Number of mouse input events observed.",
            "unit": "count",
            "version": 1,
        },
        {
            "metric_key": "context_switches",
            "description": "Foreground app/window switches in the period.",
            "unit": "count",
            "version": 1,
        },
        {
            "metric_key": "deep_focus_minutes",
            "description": "Longest continuous single-app focus block within the period.",
            "unit": "minutes",
            "version": 1,
        },
    ]

    inserted = 0
    updated = 0

    with db._get_connection() as conn:
        for metric in metrics:
            # Check if metric exists
            existing = conn.execute(
                "SELECT version FROM ai_metric_catalog WHERE metric_key = ?",
                (metric["metric_key"],),
            ).fetchone()

            if existing is None:
                # Insert new metric
                conn.execute(
                    """
                    INSERT INTO ai_metric_catalog (metric_key, description, unit, version)
                    VALUES (?, ?, ?, ?)
                """,
                    (
                        metric["metric_key"],
                        metric["description"],
                        metric["unit"],
                        metric["version"],
                    ),
                )
                inserted += 1
            elif existing[0] != metric["version"]:
                # Update if version changed
                conn.execute(
                    """
                    UPDATE ai_metric_catalog
                    SET description = ?, unit = ?, version = ?
                    WHERE metric_key = ?
                """,
                    (
                        metric["description"],
                        metric["unit"],
                        metric["version"],
                        metric["metric_key"],
                    ),
                )
                updated += 1

        conn.commit()

    total = len(metrics)
    return {"inserted": inserted, "updated": updated, "total": total}
