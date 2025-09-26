"""Database migration framework for Little Brother v3."""

LATEST_SCHEMA_VERSION = 5

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
        """,
    },
    {
        "version": 3,
        "name": "advisory_locks_v1",
        "sql": """
        CREATE TABLE IF NOT EXISTS ai_lock(
            lock_name TEXT PRIMARY KEY,
            owner_token TEXT NOT NULL,
            acquired_utc_ms INTEGER NOT NULL,
            expires_utc_ms INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_ai_lock_expires ON ai_lock(expires_utc_ms);
        """,
    },
    {
        "version": 4,
        "name": "reporting_audit_v1",
        "sql": """
        CREATE TABLE IF NOT EXISTS ai_report(
            report_id TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            period_start_ms INTEGER NOT NULL,
            period_end_ms INTEGER NOT NULL,
            format TEXT NOT NULL,
            file_path TEXT NOT NULL,
            file_sha256 TEXT NOT NULL,
            generated_utc_ms INTEGER NOT NULL,
            run_id TEXT NOT NULL REFERENCES ai_run(run_id),
            input_hash_hex TEXT NOT NULL,
            UNIQUE(kind, period_start_ms, format)
        );

        CREATE INDEX IF NOT EXISTS idx_ai_report_period ON ai_report(kind, period_start_ms);
        """,
    },
    {
        "version": 5,
        "name": "advice_v1",
        "sql": """
        CREATE TABLE IF NOT EXISTS ai_advice_hourly(
            advice_id TEXT PRIMARY KEY,
            hour_utc_start_ms INTEGER NOT NULL,
            rule_key TEXT NOT NULL,
            rule_version INTEGER NOT NULL,
            severity TEXT NOT NULL,
            score REAL NOT NULL,
            advice_text TEXT NOT NULL,
            input_hash_hex TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            reason_json TEXT NOT NULL,
            run_id TEXT NOT NULL REFERENCES ai_run(run_id),
            UNIQUE(hour_utc_start_ms, rule_key, rule_version)
        );

        CREATE TABLE IF NOT EXISTS ai_advice_daily(
            advice_id TEXT PRIMARY KEY,
            day_utc_start_ms INTEGER NOT NULL,
            rule_key TEXT NOT NULL,
            rule_version INTEGER NOT NULL,
            severity TEXT NOT NULL,
            score REAL NOT NULL,
            advice_text TEXT NOT NULL,
            input_hash_hex TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            reason_json TEXT NOT NULL,
            run_id TEXT NOT NULL REFERENCES ai_run(run_id),
            UNIQUE(day_utc_start_ms, rule_key, rule_version)
        );

        CREATE TABLE IF NOT EXISTS ai_advice_rule_catalog(
            rule_key TEXT NOT NULL,
            version INTEGER NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            PRIMARY KEY(rule_key, version)
        );

        CREATE INDEX IF NOT EXISTS idx_ai_advice_hourly_hour ON ai_advice_hourly(hour_utc_start_ms);
        CREATE INDEX IF NOT EXISTS idx_ai_advice_daily_day ON ai_advice_daily(day_utc_start_ms);

        INSERT OR IGNORE INTO ai_advice_rule_catalog(rule_key, version, title, description) VALUES
        ('low_focus', 1, 'Low Focus Time', 'Warns when focused time drops below 25 minutes per hour'),
        ('high_switches', 1, 'High Context Switching', 'Warns when context switches exceed 12 per hour'),
        ('deep_focus_positive', 1, 'Strong Deep Focus', 'Celebrates extended deep focus blocks'),
        ('passive_input', 1, 'Passive Input Pattern', 'Notes periods of low input with active window time'),
        ('long_idle', 1, 'Extended Idle Time', 'Notes extended idle periods over 40 minutes'),
        ('low_daily_focus', 1, 'Low Daily Focus', 'Warns when daily focused time drops below 3 hours'),
        ('positive_deep_focus_day', 1, 'Strong Daily Deep Focus', 'Celebrates days with significant deep focus'),
        ('high_switch_day', 1, 'High Daily Switching', 'Warns when daily context switches exceed 150');
        """,
    },
]
