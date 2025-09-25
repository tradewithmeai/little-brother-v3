"""Test metric catalog seeding and management."""

import tempfile
from pathlib import Path

from lb3.ai.metrics import seed_metric_catalog
from lb3.database import Database


def test_seed_metric_catalog_idempotent():
    """Test that seeding metrics twice is idempotent."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_metrics.db"
        db = Database(db_path)

        # First run should insert all 6 metrics
        result1 = seed_metric_catalog(db)
        assert result1["inserted"] == 6
        assert result1["updated"] == 0
        assert result1["total"] == 6

        # Second run should be idempotent
        result2 = seed_metric_catalog(db)
        assert result2["inserted"] == 0
        assert result2["updated"] == 0
        assert result2["total"] == 6

        # Verify all 6 metrics exist
        with db._get_connection() as conn:
            count = conn.execute("SELECT COUNT(*) FROM ai_metric_catalog").fetchone()[0]
            assert count == 6

            keys = conn.execute(
                """
                SELECT metric_key FROM ai_metric_catalog ORDER BY metric_key
            """
            ).fetchall()
            key_list = [row[0] for row in keys]
            expected_keys = [
                "context_switches",
                "deep_focus_minutes",
                "focus_minutes",
                "idle_minutes",
                "keyboard_events",
                "mouse_events",
            ]
            assert key_list == expected_keys
