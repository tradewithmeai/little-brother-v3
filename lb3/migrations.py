"""Database migration framework for Little Brother v3."""

LATEST_SCHEMA_VERSION = 2

MIGRATIONS = [
    {
        "version": 2,
        "name": "ai_summaries_v1",
        "sql": """
        CREATE TABLE IF NOT EXISTS ai_metric_catalog(
            metric_key TEXT PRIMARY KEY,
            description TEXT NOT NULL,
            unit TEXT NOT NULL,
            version INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS ai_run(
            run_id TEXT PRIMARY KEY,
            started_utc_ms INTEGER NOT NULL,
            finished_utc_ms INTEGER,
            code_git_sha TEXT,
            params_json TEXT NOT NULL,
            status TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ai_hourly_summary(
            hour_utc_start_ms INTEGER NOT NULL,
            metric_key TEXT NOT NULL REFERENCES ai_metric_catalog(metric_key),
            value_num REAL NOT NULL,
            input_row_count INTEGER NOT NULL,
            coverage_ratio REAL NOT NULL,
            run_id TEXT NOT NULL REFERENCES ai_run(run_id),
            input_hash_hex TEXT NOT NULL,
            created_utc_ms INTEGER NOT NULL,
            updated_utc_ms INTEGER NOT NULL,
            computed_by_version INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (hour_utc_start_ms, metric_key)
        );

        CREATE TABLE IF NOT EXISTS ai_daily_summary(
            day_utc_start_ms INTEGER NOT NULL,
            metric_key TEXT NOT NULL REFERENCES ai_metric_catalog(metric_key),
            value_num REAL NOT NULL,
            hours_counted INTEGER NOT NULL,
            low_conf_hours INTEGER NOT NULL,
            run_id TEXT NOT NULL REFERENCES ai_run(run_id),
            input_hash_hex TEXT NOT NULL,
            created_utc_ms INTEGER NOT NULL,
            updated_utc_ms INTEGER NOT NULL,
            computed_by_version INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (day_utc_start_ms, metric_key)
        );

        CREATE TABLE IF NOT EXISTS ai_hourly_evidence(
            hour_utc_start_ms INTEGER NOT NULL,
            metric_key TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            PRIMARY KEY (hour_utc_start_ms, metric_key)
        );

        CREATE INDEX IF NOT EXISTS idx_ai_hourly_metric_hour ON ai_hourly_summary(metric_key, hour_utc_start_ms);

        CREATE INDEX IF NOT EXISTS idx_ai_daily_metric_day ON ai_daily_summary(metric_key, day_utc_start_ms);
        """
    }
]