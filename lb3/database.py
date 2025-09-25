"""Database module for Little Brother v3."""

import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Optional

from .config import get_effective_config
from .ids import new_id
from .logging_setup import get_logger

logger = get_logger("database")


class Database:
    """SQLite database connection with WAL mode and schema management."""

    def __init__(self, db_path: Optional[Path] = None):
        """Initialize database connection.

        Args:
            db_path: Path to SQLite database file. If None, uses config path.
        """
        if db_path is None:
            config = get_effective_config()
            db_path = Path(config.storage.sqlite_path)

        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = threading.Lock()

        # Ensure directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Initialize database
        self._init_database()

        # Compatibility alias - temporary, to be removed in later cleanup
        self._connection = self._conn

    def _init_database(self) -> None:
        """Initialize database with schema and WAL mode."""
        with self._get_connection() as conn:
            # Enable WAL mode
            conn.execute("PRAGMA journal_mode=WAL")

            # Create schema
            self._create_schema(conn)

            # Create indexes
            self._create_indexes(conn)

            # Ensure schema_version row exists (v1 for existing schema)
            try:
                version_row = conn.execute(
                    "SELECT version FROM schema_version LIMIT 1"
                ).fetchone()
                if version_row is None:
                    conn.execute("INSERT INTO schema_version (version) VALUES (1)")
                    conn.commit()
            except sqlite3.OperationalError:
                # Table doesn't exist yet (should have been created by _create_schema)
                pass

            # Apply migrations to reach latest version
            self.apply_migrations(conn)

            logger.info(f"Database initialized at {self.db_path}")

    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection, creating if necessary."""
        if self._conn is None or self._conn.execute("SELECT 1").fetchone() is None:
            self._conn = sqlite3.connect(
                str(self.db_path), timeout=30.0, check_same_thread=False
            )
            self._conn.row_factory = sqlite3.Row
            # Update compatibility alias
            self._connection = self._conn

        return self._conn

    def _create_schema(self, conn: sqlite3.Connection) -> None:
        """Create database schema."""
        schema_sql = """
        CREATE TABLE IF NOT EXISTS sessions(
            id TEXT PRIMARY KEY,
            started_at_utc INTEGER NOT NULL,
            os TEXT,
            hostname TEXT,
            app_version TEXT
        );

        CREATE TABLE IF NOT EXISTS apps(
            id TEXT PRIMARY KEY,
            exe_name TEXT,
            exe_path_hash TEXT,
            first_seen_utc INTEGER,
            last_seen_utc INTEGER
        );

        CREATE TABLE IF NOT EXISTS windows(
            id TEXT PRIMARY KEY,
            app_id TEXT,
            title_hash TEXT,
            first_seen_utc INTEGER,
            last_seen_utc INTEGER
        );

        CREATE TABLE IF NOT EXISTS files(
            id TEXT PRIMARY KEY,
            path_hash TEXT,
            ext TEXT,
            first_seen_utc INTEGER,
            last_seen_utc INTEGER
        );

        CREATE TABLE IF NOT EXISTS urls(
            id TEXT PRIMARY KEY,
            url_hash TEXT,
            domain_hash TEXT,
            first_seen_utc INTEGER,
            last_seen_utc INTEGER
        );

        CREATE TABLE IF NOT EXISTS events(
            id TEXT PRIMARY KEY,
            ts_utc INTEGER NOT NULL,
            monitor TEXT NOT NULL CHECK(monitor IN (
                'active_window','context_snapshot','keyboard','mouse','browser','file'
            )),
            action TEXT NOT NULL,
            subject_type TEXT NOT NULL CHECK(subject_type IN ('app','window','file','url','none')),
            subject_id TEXT,
            session_id TEXT NOT NULL,
            batch_id TEXT,
            pid INTEGER,
            exe_name TEXT,
            exe_path_hash TEXT,
            window_title_hash TEXT,
            url_hash TEXT,
            file_path_hash TEXT,
            attrs_json TEXT
        );

        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER NOT NULL
        );
        """

        # Execute each statement separately
        for statement in schema_sql.strip().split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(statement)

        conn.commit()

    def _create_indexes(self, conn: sqlite3.Connection) -> None:
        """Create database indexes."""
        indexes_sql = """
        CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts_utc);
        CREATE INDEX IF NOT EXISTS idx_events_monitor_ts ON events(monitor, ts_utc);
        CREATE INDEX IF NOT EXISTS idx_events_subject ON events(subject_type, subject_id);
        CREATE INDEX IF NOT EXISTS idx_apps_exe ON apps(exe_name);
        CREATE INDEX IF NOT EXISTS idx_windows_app ON windows(app_id);
        """

        # Execute each index statement
        for statement in indexes_sql.strip().split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(statement)

        conn.commit()

    def apply_migrations(self, conn: sqlite3.Connection) -> None:
        """Apply database migrations to reach latest schema version."""
        from .migrations import LATEST_SCHEMA_VERSION, MIGRATIONS

        # Get current schema version
        try:
            current_version = conn.execute(
                "SELECT version FROM schema_version LIMIT 1"
            ).fetchone()
            if current_version is None:
                # Table exists but empty, treat as version 1
                conn.execute("INSERT INTO schema_version (version) VALUES (1)")
                conn.commit()
                current_version = 1
            else:
                current_version = current_version[0]
        except sqlite3.OperationalError:
            # Table doesn't exist, will be created by schema creation
            current_version = 1

        # Apply migrations
        for migration in MIGRATIONS:
            if migration["version"] > current_version:
                logger.info(
                    f"schema version {current_version} → {migration['version']}"
                )
                logger.info(f"applied migration: {migration['name']}")

                # Execute migration SQL in transaction
                try:
                    # Execute each statement separately
                    for statement in migration["sql"].strip().split(";"):
                        statement = statement.strip()
                        if statement:
                            conn.execute(statement)

                    # Update schema version
                    conn.execute(
                        "UPDATE schema_version SET version = ?", (migration["version"],)
                    )
                    conn.commit()
                    current_version = migration["version"]

                except Exception as e:
                    conn.rollback()
                    logger.error(f"Migration {migration['name']} failed: {e}")
                    raise

        if current_version == LATEST_SCHEMA_VERSION:
            logger.info(f"already at version {LATEST_SCHEMA_VERSION}")

    def health_check(self) -> dict[str, Any]:
        """Perform database health check and return status."""
        try:
            with self._get_connection() as conn:
                # Check WAL mode
                wal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]

                # Check tables exist
                tables_query = """
                SELECT name FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
                tables = [row[0] for row in conn.execute(tables_query)]
                expected_tables = [
                    "apps",
                    "events",
                    "files",
                    "sessions",
                    "urls",
                    "windows",
                ]

                # Check indexes exist
                indexes_query = """
                SELECT name FROM sqlite_master
                WHERE type='index' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
                indexes = [row[0] for row in conn.execute(indexes_query)]
                expected_indexes = [
                    "idx_apps_exe",
                    "idx_events_monitor_ts",
                    "idx_events_subject",
                    "idx_events_ts",
                    "idx_windows_app",
                ]

                # Get table counts
                counts = {}
                for table in expected_tables:
                    count_result = conn.execute(
                        f"SELECT COUNT(*) FROM {table}"
                    ).fetchone()
                    counts[table] = count_result[0] if count_result else 0

                # Test WAL checkpoint
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

                return {
                    "status": "healthy",
                    "db_path": str(self.db_path),
                    "wal_mode": wal_mode,
                    "tables_found": tables,
                    "tables_expected": expected_tables,
                    "indexes_found": indexes,
                    "indexes_expected": expected_indexes,
                    "table_counts": counts,
                    "tables_missing": set(expected_tables) - set(tables),
                    "indexes_missing": set(expected_indexes) - set(indexes),
                }

        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "db_path": str(self.db_path),
            }

    def get_table_counts(self) -> dict[str, int]:
        """Get count of records in each table."""
        counts = {}
        tables = ["sessions", "apps", "windows", "files", "urls", "events"]

        with self._get_connection() as conn:
            for table in tables:
                result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                counts[table] = result[0] if result else 0

        return counts

    def insert_session(self, session_data: dict[str, Any]) -> None:
        """Insert a session record."""
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO sessions (id, started_at_utc, os, hostname, app_version)
                VALUES (?, ?, ?, ?, ?)
            """,
                (
                    session_data["id"],
                    session_data["started_at_utc"],
                    session_data.get("os"),
                    session_data.get("hostname"),
                    session_data.get("app_version"),
                ),
            )
            conn.commit()

    def insert_event(self, event_data: dict[str, Any]) -> None:
        """Insert an event record."""
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO events (
                    id, ts_utc, monitor, action, subject_type, subject_id,
                    session_id, batch_id, pid, exe_name, exe_path_hash,
                    window_title_hash, url_hash, file_path_hash, attrs_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    event_data["id"],
                    event_data["ts_utc"],
                    event_data["monitor"],
                    event_data["action"],
                    event_data["subject_type"],
                    event_data.get("subject_id"),
                    event_data["session_id"],
                    event_data.get("batch_id"),
                    event_data.get("pid"),
                    event_data.get("exe_name"),
                    event_data.get("exe_path_hash"),
                    event_data.get("window_title_hash"),
                    event_data.get("url_hash"),
                    event_data.get("file_path_hash"),
                    event_data.get("attrs_json"),
                ),
            )
            conn.commit()

    def get_events_by_timerange(
        self, start_utc: int, end_utc: int, limit: int = 1000
    ) -> list[dict[str, Any]]:
        """Get events within a time range."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT * FROM events
                WHERE ts_utc >= ? AND ts_utc <= ?
                ORDER BY ts_utc DESC
                LIMIT ?
            """,
                (start_utc, end_utc, limit),
            )

            return [dict(row) for row in cursor.fetchall()]

    def upsert_file_record(self, path_hash: str, ext: str, ts_ms: int) -> str:
        """Upsert file record with thread-safe atomic operation.

        Args:
            path_hash: Hash of the file path for privacy
            ext: File extension (no dot, lowercased)
            ts_ms: Timestamp in UNIX epoch milliseconds UTC

        Returns:
            File ID (ULID string) - stable across calls for same path_hash

        Behavior:
            - If file exists: updates last_seen_utc, preserves non-null ext
            - If file doesn't exist: creates new record with new ULID
            - Uses COALESCE to avoid overwriting existing ext with empty value
        """
        with self._lock:
            with self._get_connection() as conn:
                # Lookup existing record
                cursor = conn.execute(
                    "SELECT id FROM files WHERE path_hash = ? LIMIT 1", (path_hash,)
                )
                result = cursor.fetchone()

                if result:
                    # Update existing record
                    file_id = result[0]
                    conn.execute(
                        "UPDATE files SET last_seen_utc = ?, ext = CASE WHEN (ext IS NULL OR ext = '') AND ? != '' THEN ? ELSE ext END WHERE id = ?",
                        (ts_ms, ext, ext, file_id),
                    )
                else:
                    # Insert new record
                    file_id = new_id()
                    conn.execute(
                        "INSERT INTO files (id, path_hash, ext, first_seen_utc, last_seen_utc) VALUES (?, ?, ?, ?, ?)",
                        (file_id, path_hash, ext, ts_ms, ts_ms),
                    )

                # Connection context manager handles commit
                return file_id

    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None


def get_database() -> Database:
    """Get database instance using config path."""
    return Database()


def create_test_event() -> dict[str, Any]:
    """Create a test event for integration testing."""
    from .hashutil import hash_str
    from .ids import new_id

    current_time_ms = int(time.time() * 1000)

    return {
        "id": new_id(),
        "ts_utc": current_time_ms,
        "monitor": "active_window",
        "action": "window_focus",
        "subject_type": "window",
        "subject_id": new_id(),
        "session_id": new_id(),
        "batch_id": new_id(),
        "pid": 1234,
        "exe_name": "notepad.exe",
        "exe_path_hash": hash_str(r"C:\Windows\System32\notepad.exe", "exe_path"),
        "window_title_hash": hash_str("Test Window", "window_title"),
        "url_hash": None,
        "file_path_hash": None,
        "attrs_json": '{"test": true}',
    }
