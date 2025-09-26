"""Tests for AI advice system."""

import json
import tempfile
import time
from pathlib import Path

import pytest

from lb3.ai.advice import (
    get_daily_advice,
    get_hourly_advice,
    upsert_daily_advice,
    upsert_hourly_advice,
)
from lb3.database import Database


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    db = None
    try:
        db = Database(db_path)
        yield db
    finally:
        if db:
            db.close()
        db_path.unlink(missing_ok=True)


def create_test_hourly_data(db: Database, hour_start_ms: int):
    """Create test hourly summary and evidence data."""
    run_id = "test-run-123"
    input_hash = "test-hour-hash-abc123"
    current_ms = int(time.time() * 1000)

    # Insert hourly summary metrics
    metrics = {
        "focus_minutes": 15.0,
        "idle_minutes": 45.0,
        "keyboard_minutes": 12.0,
        "mouse_minutes": 8.0,
        "switches": 15.0,
        "deep_focus_minutes": 35.0,
    }

    with db._get_connection() as conn:
        # Insert run record
        conn.execute(
            """
            INSERT OR IGNORE INTO ai_run (
                run_id, started_utc_ms, params_json, status
            ) VALUES (?, ?, ?, ?)
            """,
            (run_id, current_ms, "{}", "running"),
        )

        # Insert metric catalog entries
        for metric_key in metrics:
            conn.execute(
                """
                INSERT OR IGNORE INTO ai_metric_catalog (
                    metric_key, description, unit
                ) VALUES (?, ?, ?)
                """,
                (
                    metric_key,
                    f"Test {metric_key}",
                    "minutes" if "minutes" in metric_key else "count",
                ),
            )
        for metric_key, value in metrics.items():
            conn.execute(
                """
                INSERT INTO ai_hourly_summary (
                    hour_utc_start_ms, metric_key, value_num, input_row_count, coverage_ratio,
                    run_id, input_hash_hex, created_utc_ms, updated_utc_ms, computed_by_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    hour_start_ms,
                    metric_key,
                    value,
                    100,
                    0.75,
                    run_id,
                    input_hash,
                    current_ms,
                    current_ms,
                    1,
                ),
            )

        # Insert hourly evidence
        evidence_data = [
            {"app": "Code", "minutes": 8.5},
            {"app": "Browser", "minutes": 4.2},
            {"app": "Terminal", "minutes": 2.3},
        ]
        conn.execute(
            """
            INSERT INTO ai_hourly_evidence (
                hour_utc_start_ms, metric_key, evidence_json
            ) VALUES (?, ?, ?)
            """,
            (hour_start_ms, "top_app_minutes", json.dumps(evidence_data)),
        )
        conn.commit()


def create_test_daily_data(db: Database, day_start_ms: int):
    """Create test daily summary data."""
    run_id = "test-daily-run-456"
    input_hash = "test-day-hash-def456"
    current_ms = int(time.time() * 1000)

    # Insert daily summary metrics
    metrics = {
        "focus_minutes": 120.0,
        "deep_focus_minutes": 90.0,
        "switches": 180.0,
    }

    with db._get_connection() as conn:
        # Insert run record
        conn.execute(
            """
            INSERT OR IGNORE INTO ai_run (
                run_id, started_utc_ms, params_json, status
            ) VALUES (?, ?, ?, ?)
            """,
            (run_id, current_ms, "{}", "running"),
        )

        # Insert metric catalog entries
        for metric_key in metrics:
            conn.execute(
                """
                INSERT OR IGNORE INTO ai_metric_catalog (
                    metric_key, description, unit
                ) VALUES (?, ?, ?)
                """,
                (
                    metric_key,
                    f"Test {metric_key}",
                    "minutes" if "minutes" in metric_key else "count",
                ),
            )
        for metric_key, value in metrics.items():
            conn.execute(
                """
                INSERT INTO ai_daily_summary (
                    day_utc_start_ms, metric_key, value_num, hours_counted, low_conf_hours,
                    input_hash_hex, run_id, created_utc_ms, updated_utc_ms, computed_by_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    day_start_ms,
                    metric_key,
                    value,
                    8,
                    2,
                    input_hash,
                    run_id,
                    current_ms,
                    current_ms,
                    1,
                ),
            )
        conn.commit()


class TestHourlyAdvice:
    """Test hourly advice generation."""

    def test_hourly_advice_low_focus_trigger(self, temp_db):
        """Test low_focus rule triggers with focus < 25 minutes."""
        hour_start_ms = 1727380800000  # Fixed test timestamp
        hour_end_ms = hour_start_ms + 3600000
        run_id = "test-run"

        create_test_hourly_data(temp_db, hour_start_ms)

        advice_list = get_hourly_advice(temp_db, hour_start_ms, hour_end_ms, run_id)

        # Should trigger low_focus (15 < 25), high_switches (15 >= 12), deep_focus_positive (35 >= 30), and long_idle (45 >= 40)
        assert len(advice_list) == 4

        # Check low_focus advice
        low_focus = next(a for a in advice_list if a["rule_key"] == "low_focus")
        assert low_focus["severity"] == "warn"
        assert low_focus["rule_version"] == 1
        assert "15.0m; target ≥ 25m" in low_focus["advice_text"]
        assert 0.3 <= low_focus["score"] <= 0.9

        # Verify evidence and reason JSON format
        evidence = json.loads(low_focus["evidence_json"])
        assert evidence["focus_minutes"] == 15.0
        assert evidence["coverage_ratio"] == 0.75
        assert len(evidence["top_app_minutes"]) == 3

        reason = json.loads(low_focus["reason_json"])
        assert reason["focus_minutes_actual"] == 15.0
        assert reason["focus_minutes_threshold"] == 25.0

    def test_hourly_advice_high_switches_trigger(self, temp_db):
        """Test high_switches rule triggers with switches >= 12."""
        hour_start_ms = 1727380800000
        hour_end_ms = hour_start_ms + 3600000
        run_id = "test-run"

        create_test_hourly_data(temp_db, hour_start_ms)

        advice_list = get_hourly_advice(temp_db, hour_start_ms, hour_end_ms, run_id)

        high_switches = next(a for a in advice_list if a["rule_key"] == "high_switches")
        assert high_switches["severity"] == "warn"
        assert "15s" in high_switches["advice_text"]
        assert 0.3 <= high_switches["score"] <= 0.8

    def test_hourly_advice_deep_focus_positive_trigger(self, temp_db):
        """Test deep_focus_positive rule triggers with deep_focus >= 30."""
        hour_start_ms = 1727380800000
        hour_end_ms = hour_start_ms + 3600000
        run_id = "test-run"

        create_test_hourly_data(temp_db, hour_start_ms)

        advice_list = get_hourly_advice(temp_db, hour_start_ms, hour_end_ms, run_id)

        deep_focus = next(
            a for a in advice_list if a["rule_key"] == "deep_focus_positive"
        )
        assert deep_focus["severity"] == "good"
        assert "35.0m" in deep_focus["advice_text"]
        assert 0.4 <= deep_focus["score"] <= 0.9

    def test_hourly_advice_no_triggers_low_coverage(self, temp_db):
        """Test no advice generated when coverage_ratio < 0.60."""
        hour_start_ms = 1727380800000
        hour_end_ms = hour_start_ms + 3600000
        run_id = "test-run"
        current_ms = int(time.time() * 1000)

        # Create data with low coverage ratio
        with temp_db._get_connection() as conn:
            # Insert run and metric catalog
            conn.execute(
                "INSERT OR IGNORE INTO ai_run (run_id, started_utc_ms, params_json, status) VALUES (?, ?, ?, ?)",
                (run_id, current_ms, "{}", "running"),
            )
            conn.execute(
                "INSERT OR IGNORE INTO ai_metric_catalog (metric_key, description, unit) VALUES (?, ?, ?)",
                ("focus_minutes", "Test focus_minutes", "minutes"),
            )

            conn.execute(
                """
                INSERT INTO ai_hourly_summary (
                    hour_utc_start_ms, metric_key, value_num, input_row_count, coverage_ratio,
                    run_id, input_hash_hex, created_utc_ms, updated_utc_ms, computed_by_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    hour_start_ms,
                    "focus_minutes",
                    10.0,
                    100,
                    0.50,
                    run_id,
                    "hash",
                    current_ms,
                    current_ms,
                    1,
                ),  # Low coverage
            )
            conn.commit()

        advice_list = get_hourly_advice(temp_db, hour_start_ms, hour_end_ms, run_id)
        assert len(advice_list) == 0

    def test_hourly_advice_idempotency(self, temp_db):
        """Test idempotent upsert behavior."""
        hour_start_ms = 1727380800000
        run_id = "test-run"

        create_test_hourly_data(temp_db, hour_start_ms)

        # First upsert
        result1 = upsert_hourly_advice(
            temp_db,
            hour_start_ms,
            "low_focus",
            1,
            "warn",
            0.5,
            "Test advice",
            "hash123",
            '{"test": true}',
            '{"reason": "test"}',
            run_id,
        )
        assert result1["action"] == "inserted"

        # Second upsert with same data - should be unchanged
        result2 = upsert_hourly_advice(
            temp_db,
            hour_start_ms,
            "low_focus",
            1,
            "warn",
            0.5,
            "Test advice",
            "hash123",
            '{"test": true}',
            '{"reason": "test"}',
            run_id,
        )
        assert result2["action"] == "unchanged"

        # Third upsert with different score - should update
        result3 = upsert_hourly_advice(
            temp_db,
            hour_start_ms,
            "low_focus",
            1,
            "warn",
            0.7,
            "Test advice",
            "hash123",
            '{"test": true}',
            '{"reason": "test"}',
            run_id,
        )
        assert result3["action"] == "updated"


class TestDailyAdvice:
    """Test daily advice generation."""

    def test_daily_advice_low_daily_focus_trigger(self, temp_db):
        """Test low_daily_focus rule triggers with focus < 180 minutes."""
        day_start_ms = 1727308800000  # Fixed test timestamp
        run_id = "test-daily-run"

        create_test_daily_data(temp_db, day_start_ms)

        advice_list = get_daily_advice(temp_db, day_start_ms, run_id)

        # Should trigger low_daily_focus (120 < 180) and high_switch_day (180 >= 150)
        assert len(advice_list) == 2

        low_focus = next(a for a in advice_list if a["rule_key"] == "low_daily_focus")
        assert low_focus["severity"] == "warn"
        assert "120.0m; target ≥ 180m" in low_focus["advice_text"]
        assert 0.3 <= low_focus["score"] <= 0.8

    def test_daily_advice_positive_deep_focus_trigger(self, temp_db):
        """Test positive_deep_focus_day rule does not trigger with 90 < 120."""
        day_start_ms = 1727308800000
        run_id = "test-daily-run"

        create_test_daily_data(temp_db, day_start_ms)

        advice_list = get_daily_advice(temp_db, day_start_ms, run_id)

        # Should not trigger positive_deep_focus_day (90 < 120)
        deep_focus_advice = [
            a for a in advice_list if a["rule_key"] == "positive_deep_focus_day"
        ]
        assert len(deep_focus_advice) == 0

    def test_daily_advice_high_switch_day_trigger(self, temp_db):
        """Test high_switch_day rule triggers with switches >= 150."""
        day_start_ms = 1727308800000
        run_id = "test-daily-run"

        create_test_daily_data(temp_db, day_start_ms)

        advice_list = get_daily_advice(temp_db, day_start_ms, run_id)

        high_switches = next(
            a for a in advice_list if a["rule_key"] == "high_switch_day"
        )
        assert high_switches["severity"] == "warn"
        assert "180s" in high_switches["advice_text"]
        assert 0.3 <= high_switches["score"] <= 0.8

    def test_daily_advice_idempotency(self, temp_db):
        """Test daily advice idempotent upsert."""
        day_start_ms = 1727308800000
        run_id = "test-daily-run"

        # First upsert
        result1 = upsert_daily_advice(
            temp_db,
            day_start_ms,
            "low_daily_focus",
            1,
            "warn",
            0.6,
            "Test daily advice",
            "hash456",
            '{"daily": true}',
            '{"daily_reason": "test"}',
            run_id,
        )
        assert result1["action"] == "inserted"

        # Second upsert - should be unchanged
        result2 = upsert_daily_advice(
            temp_db,
            day_start_ms,
            "low_daily_focus",
            1,
            "warn",
            0.6,
            "Test daily advice",
            "hash456",
            '{"daily": true}',
            '{"daily_reason": "test"}',
            run_id,
        )
        assert result2["action"] == "unchanged"

        # Update with different hash - should update
        result3 = upsert_daily_advice(
            temp_db,
            day_start_ms,
            "low_daily_focus",
            1,
            "warn",
            0.6,
            "Test daily advice",
            "hash789",
            '{"daily": true}',
            '{"daily_reason": "test"}',
            run_id,
        )
        assert result3["action"] == "updated"


class TestAdviceMutation:
    """Test advice changes when metrics change."""

    def test_hourly_metric_threshold_crossing(self, temp_db):
        """Test advice updates when metric crosses threshold."""
        hour_start_ms = 1727380800000
        hour_end_ms = hour_start_ms + 3600000
        run_id = "test-mutation"
        current_ms = int(time.time() * 1000)

        # Initial data with focus_minutes = 30 (above threshold)
        with temp_db._get_connection() as conn:
            # Insert run and metric catalog
            conn.execute(
                "INSERT OR IGNORE INTO ai_run (run_id, started_utc_ms, params_json, status) VALUES (?, ?, ?, ?)",
                (run_id, current_ms, "{}", "running"),
            )
            conn.execute(
                "INSERT OR IGNORE INTO ai_metric_catalog (metric_key, description, unit) VALUES (?, ?, ?)",
                ("focus_minutes", "Test focus_minutes", "minutes"),
            )

            conn.execute(
                """
                INSERT INTO ai_hourly_summary (
                    hour_utc_start_ms, metric_key, value_num, input_row_count, coverage_ratio,
                    run_id, input_hash_hex, created_utc_ms, updated_utc_ms, computed_by_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    hour_start_ms,
                    "focus_minutes",
                    30.0,
                    100,
                    0.75,
                    run_id,
                    "hash1",
                    current_ms,
                    current_ms,
                    1,
                ),
            )
            conn.commit()

        # Should not trigger low_focus
        advice_list1 = get_hourly_advice(temp_db, hour_start_ms, hour_end_ms, run_id)
        low_focus_advice1 = [a for a in advice_list1 if a["rule_key"] == "low_focus"]
        assert len(low_focus_advice1) == 0

        # Update metric to cross threshold
        with temp_db._get_connection() as conn:
            conn.execute(
                """
                UPDATE ai_hourly_summary
                SET value_num = ?, input_hash_hex = ?
                WHERE hour_utc_start_ms = ? AND metric_key = ?
                """,
                (20.0, "hash2", hour_start_ms, "focus_minutes"),
            )
            conn.commit()

        # Should now trigger low_focus
        advice_list2 = get_hourly_advice(temp_db, hour_start_ms, hour_end_ms, run_id)
        low_focus_advice2 = [a for a in advice_list2 if a["rule_key"] == "low_focus"]
        assert len(low_focus_advice2) == 1
        assert low_focus_advice2[0]["input_hash_hex"] == "hash2"


class TestCLIIntegration:
    """Test CLI output format matches specification."""

    def test_advice_show_hour_format(self, temp_db):
        """Test ai advise show hour output format."""
        hour_start_ms = 1727380800000

        # Insert test advice directly
        with temp_db._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO ai_advice_hourly (
                    advice_id, hour_utc_start_ms, rule_key, rule_version, severity,
                    score, advice_text, input_hash_hex, evidence_json, reason_json, run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "test-advice-id",
                    hour_start_ms,
                    "low_focus",
                    1,
                    "warn",
                    0.6667,
                    "Low focused time this hour (15.0m; target ≥ 25m). Try reducing interruptions.",
                    "hash123",
                    "{}",
                    "{}",
                    "test-run",
                ),
            )
            conn.commit()

        # Test query matches CLI format
        with temp_db._get_connection() as conn:
            rows = conn.execute(
                """
                SELECT rule_key, rule_version, severity, score, advice_text, input_hash_hex
                FROM ai_advice_hourly
                WHERE hour_utc_start_ms = ?
                ORDER BY rule_key
                """,
                (hour_start_ms,),
            ).fetchall()

        assert len(rows) == 1
        rule_key, rule_version, severity, score, advice_text, input_hash_hex = rows[0]

        # Verify CLI output format
        expected_output = f'advice rule={rule_key},version={rule_version},severity={severity},score={score},text="{advice_text}",hash={input_hash_hex}'
        assert (
            expected_output
            == 'advice rule=low_focus,version=1,severity=warn,score=0.6667,text="Low focused time this hour (15.0m; target ≥ 25m). Try reducing interruptions.",hash=hash123'
        )

    def test_advice_rule_catalog_seeded(self, temp_db):
        """Test rule catalog is properly seeded."""
        with temp_db._get_connection() as conn:
            rows = conn.execute(
                """
                SELECT rule_key, version, title
                FROM ai_advice_rule_catalog
                ORDER BY rule_key, version
                """,
            ).fetchall()

        # Should have 8 rules seeded
        assert len(rows) == 8

        rule_keys = [row[0] for row in rows]
        expected_rules = [
            "deep_focus_positive",
            "high_switch_day",
            "high_switches",
            "long_idle",
            "low_daily_focus",
            "low_focus",
            "passive_input",
            "positive_deep_focus_day",
        ]
        assert sorted(rule_keys) == sorted(expected_rules)
