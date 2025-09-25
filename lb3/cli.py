"""Command line interface for Little Brother v3."""

import contextlib

import typer

from .config import Config, get_effective_config
from .version import __version__

app = typer.Typer(
    name="lb3", help="Little Brother v3 - System monitoring daemon and CLI"
)

# Config command group
config_app = typer.Typer(help="Configuration management commands")
app.add_typer(config_app, name="config")

# Database command group
db_app = typer.Typer(help="Database management commands")
app.add_typer(db_app, name="db")

# Spool command group
spool_app = typer.Typer(help="Journal spool management commands")
app.add_typer(spool_app, name="spool")

# Monitors command group
monitors_app = typer.Typer(help="Monitor management and diagnostics commands")
app.add_typer(monitors_app, name="monitors")

# AI command group
ai_app = typer.Typer(help="AI analysis commands")
app.add_typer(ai_app, name="ai")


@db_app.command("schema-version")
def db_schema_version() -> None:
    """Show current database schema version."""
    try:
        from .database import get_database

        db = get_database()
        with db._get_connection() as conn:
            version = conn.execute(
                "SELECT version FROM schema_version LIMIT 1"
            ).fetchone()[0]
            typer.echo(f"version={version}")
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e


@db_app.command("list-ai-objects")
def db_list_ai_objects() -> None:
    """List AI-related database tables and indexes."""
    try:
        from .database import get_database

        db = get_database()
        with db._get_connection() as conn:
            # Get AI tables
            ai_tables = conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table' AND name LIKE 'ai_%'
                ORDER BY name
            """
            ).fetchall()
            table_names = [row[0] for row in ai_tables]

            # Get AI indexes
            ai_indexes = conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='index' AND name LIKE 'idx_ai_%'
                ORDER BY name
            """
            ).fetchall()
            index_names = [row[0] for row in ai_indexes]

            typer.echo(f"ai_tables={','.join(table_names)}")
            typer.echo(f"ai_indexes={','.join(index_names)}")
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e


# AI Metrics commands
metrics_app = typer.Typer(help="AI metrics management commands")
ai_app.add_typer(metrics_app, name="metrics")


@metrics_app.command("list")
def ai_metrics_list() -> None:
    """List all metrics in the catalog."""
    try:
        from .database import get_database

        db = get_database()
        with db._get_connection() as conn:
            # Get all metrics
            metrics = conn.execute(
                """
                SELECT metric_key, unit, version
                FROM ai_metric_catalog
                ORDER BY metric_key
            """
            ).fetchall()

            for row in metrics:
                typer.echo(f"metric_key={row[0]},unit={row[1]},version={row[2]}")
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e


@metrics_app.command("seed")
def ai_metrics_seed() -> None:
    """Seed metrics catalog once."""
    try:
        from .ai.metrics import seed_metric_catalog
        from .database import get_database

        db = get_database()
        result = seed_metric_catalog(db)
        typer.echo(
            f"inserted={result['inserted']},updated={result['updated']},total={result['total']}"
        )
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e


@metrics_app.command("seed-twice")
def ai_metrics_seed_twice() -> None:
    """Seed metrics catalog twice to prove idempotency."""
    try:
        from .ai.metrics import seed_metric_catalog
        from .database import get_database

        db = get_database()

        # First run
        result1 = seed_metric_catalog(db)
        typer.echo(
            f"run1: inserted={result1['inserted']},updated={result1['updated']},total={result1['total']}"
        )

        # Second run
        result2 = seed_metric_catalog(db)
        typer.echo(
            f"run2: inserted={result2['inserted']},updated={result2['updated']},total={result2['total']}"
        )
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e


@app.command()
def version() -> None:
    """Show version information."""
    typer.echo(f"Little Brother v3 {__version__}")


@app.command()
def diag(
    json: bool = typer.Option(False, "--json", help="Output in JSON format"),
) -> None:
    """Run system diagnostics and show configuration."""
    try:
        import json as json_module
        import os
        import platform
        import sys
        from pathlib import Path

        from .config import get_effective_config
        from .database import get_database

        # Get system information
        system_info = {
            "platform": {
                "system": platform.system(),
                "release": platform.release(),
                "version": platform.version(),
                "machine": platform.machine(),
                "processor": platform.processor(),
            },
            "python": {
                "version": sys.version.split()[0],
                "executable": sys.executable,
                "prefix": sys.prefix,
            },
            "process": {
                "pid": os.getpid(),
                "cwd": os.getcwd(),
            },
        }

        # Get configuration
        config = get_effective_config()
        log_dir = Path("lb_data/logs")  # Default log directory
        config_info = {
            "storage": {
                "spool_dir": str(config.storage.spool_dir),
                "spool_dir_exists": Path(config.storage.spool_dir).exists(),
                "sqlite_path": str(config.storage.sqlite_path),
                "sqlite_exists": Path(config.storage.sqlite_path).exists(),
                "log_dir": str(log_dir),
                "log_dir_exists": log_dir.exists(),
            },
            "guardrails": {
                "no_global_text_keylogging": config.guardrails.no_global_text_keylogging,
            },
            "time_zone_handling": config.time_zone_handling,
        }

        # Check database health
        db_info = {"status": "unknown", "error": None, "table_counts": {}}
        try:
            db = get_database()
            health = db.health_check()
            db_info["status"] = health["status"]
            if health["status"] == "healthy":
                db_info["table_counts"] = db.get_table_counts()
            else:
                db_info["error"] = health.get("error")
            db.close()
        except Exception as e:
            db_info["status"] = "error"
            db_info["error"] = str(e)

        # Check spool directory status
        spool_info = {"status": "unknown", "monitor_dirs": {}, "total_pending_files": 0}
        try:
            spool_dir = Path(config.storage.spool_dir)
            if spool_dir.exists():
                from .importer import KNOWN_MONITORS

                spool_info["status"] = "exists"
                for monitor_dir in spool_dir.iterdir():
                    if monitor_dir.is_dir() and not monitor_dir.name.startswith("_"):
                        monitor_name = monitor_dir.name
                        is_known = monitor_name in KNOWN_MONITORS

                        # Count files
                        all_files = list(monitor_dir.glob("*"))
                        pending_files = [
                            f
                            for f in monitor_dir.glob("*.ndjson.gz")
                            if not f.name.endswith(".part")
                            and not f.name.endswith(".error")
                        ]

                        spool_info["monitor_dirs"][monitor_name] = {
                            "known_monitor": is_known,
                            "total_files": len(all_files),
                            "pending_files": len(pending_files),
                        }

                        if is_known:
                            spool_info["total_pending_files"] += len(pending_files)
            else:
                spool_info["status"] = "missing"
        except Exception as e:
            spool_info["status"] = "error"
            spool_info["error"] = str(e)

        # Get quota information
        from .spool_quota import get_quota_manager

        quota_manager = get_quota_manager()
        usage = quota_manager.get_spool_usage()
        largest_files = quota_manager.get_largest_done_files(5)

        # Compile diagnostics
        diagnostics = {
            "timestamp": __import__("datetime")
            .datetime.now(__import__("datetime").timezone.utc)
            .isoformat(),
            "version": __version__,
            "system": system_info,
            "config": config_info,
            "database": db_info,
            "spool": spool_info,
            "quota": {
                "quota_mb": usage.quota_bytes // (1024 * 1024),
                "used_mb": usage.used_bytes // (1024 * 1024),
                "soft_pct": config.storage.spool_soft_pct,
                "hard_pct": config.storage.spool_hard_pct,
                "state": usage.state.value,
                "dropped_batches": usage.dropped_batches,
                "largest_done_files": [
                    {
                        "monitor": monitor,
                        "filename": filename,
                        "size_mb": size // (1024 * 1024),
                    }
                    for monitor, filename, size in largest_files
                ],
            },
        }

        # Output results
        if json:
            typer.echo(json_module.dumps(diagnostics, indent=2))
        else:
            # Human-readable output
            typer.echo("Little Brother v3 Diagnostics")
            typer.echo(f"Version: {__version__}")
            typer.echo(
                f"Platform: {system_info['platform']['system']} {system_info['platform']['release']}"
            )
            typer.echo(f"Python: {system_info['python']['version']}")

            typer.echo("\nConfiguration:")
            typer.echo(
                f"  Spool dir: {config_info['storage']['spool_dir']} {'OK' if config_info['storage']['spool_dir_exists'] else 'MISSING'}"
            )
            typer.echo(
                f"  Database: {config_info['storage']['sqlite_path']} {'OK' if config_info['storage']['sqlite_exists'] else 'MISSING'}"
            )
            typer.echo(
                f"  Log dir: {config_info['storage']['log_dir']} {'OK' if config_info['storage']['log_dir_exists'] else 'MISSING'}"
            )
            typer.echo(
                f"  Text keylogging: {'disabled' if config_info['guardrails']['no_global_text_keylogging'] else 'enabled'}"
            )
            typer.echo(f"  Time zone handling: {config_info['time_zone_handling']}")

            typer.echo("\nDatabase:")
            typer.echo(f"  Status: {db_info['status']}")
            if db_info["status"] == "healthy" and db_info["table_counts"]:
                typer.echo(f"  Events: {db_info['table_counts'].get('events', 0)}")
                typer.echo(f"  Apps: {db_info['table_counts'].get('apps', 0)}")
                typer.echo(f"  Windows: {db_info['table_counts'].get('windows', 0)}")
            elif db_info.get("error"):
                typer.echo(f"  Error: {db_info['error']}")

            typer.echo("\nSpool:")
            typer.echo(f"  Status: {spool_info['status']}")
            typer.echo(f"  Pending files: {spool_info['total_pending_files']}")
            if spool_info["monitor_dirs"]:
                typer.echo("  Monitor directories:")
                for monitor, info in spool_info["monitor_dirs"].items():
                    status_icon = "OK" if info["known_monitor"] else "??"
                    typer.echo(
                        f"    {status_icon} {monitor}: {info['pending_files']} pending / {info['total_files']} total"
                    )

            typer.echo("\nQuota:")
            typer.echo(
                f"  Usage: {usage.used_bytes // (1024*1024)}MB / {usage.quota_bytes // (1024*1024)}MB ({usage.state.value})"
            )
            typer.echo(
                f"  Thresholds: {config.storage.spool_soft_pct}% soft, {config.storage.spool_hard_pct}% hard"
            )
            if usage.dropped_batches > 0:
                typer.echo(f"  Dropped batches: {usage.dropped_batches}")
            if largest_files:
                typer.echo("  Largest _done files:")
                for monitor, filename, size in largest_files:
                    typer.echo(f"    {monitor}/{filename}: {size // (1024*1024)}MB")

    except Exception as e:
        typer.echo(f"[ERROR] Failed to run diagnostics: {e}")
        raise typer.Exit(1) from e


@app.command()
def cleanup(
    days: int = typer.Option(30, "--days", help="Delete files older than N days"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be deleted without deleting"
    ),
    spool: bool = typer.Option(True, "--spool/--no-spool", help="Clean up spool files"),
    logs: bool = typer.Option(True, "--logs/--no-logs", help="Clean up log files"),
    json: bool = typer.Option(False, "--json", help="Output in JSON format"),
) -> None:
    """Clean up old spool and log files."""
    try:
        import json as json_module
        import time
        from pathlib import Path

        from .config import get_effective_config

        config = get_effective_config()
        cutoff_time = time.time() - (days * 24 * 60 * 60)  # Convert days to seconds
        log_dir = Path("lb_data/logs")  # Default log directory

        cleanup_results = {
            "cutoff_days": days,
            "cutoff_timestamp": cutoff_time,
            "dry_run": dry_run,
            "spool_cleanup": {
                "enabled": False,
                "files_deleted": 0,
                "bytes_freed": 0,
                "errors": [],
            },
            "log_cleanup": {
                "enabled": False,
                "files_deleted": 0,
                "bytes_freed": 0,
                "errors": [],
            },
        }

        # Clean up spool files
        if spool:
            cleanup_results["spool_cleanup"]["enabled"] = True
            spool_dir = Path(config.storage.spool_dir)

            if spool_dir.exists():
                # Clean up _done directory (processed files)
                done_dir = spool_dir / "_done"
                if done_dir.exists():
                    try:
                        for file_path in done_dir.rglob("*"):
                            if (
                                file_path.is_file()
                                and file_path.stat().st_mtime < cutoff_time
                            ):
                                try:
                                    file_size = file_path.stat().st_size
                                    if not dry_run:
                                        file_path.unlink()
                                    cleanup_results["spool_cleanup"][
                                        "files_deleted"
                                    ] += 1
                                    cleanup_results["spool_cleanup"][
                                        "bytes_freed"
                                    ] += file_size
                                except Exception as e:
                                    cleanup_results["spool_cleanup"]["errors"].append(
                                        f"Failed to delete {file_path}: {e}"
                                    )
                    except Exception as e:
                        cleanup_results["spool_cleanup"]["errors"].append(
                            f"Error scanning spool directory: {e}"
                        )

                # Clean up .error files from monitor directories
                try:
                    for monitor_dir in spool_dir.iterdir():
                        if monitor_dir.is_dir() and not monitor_dir.name.startswith(
                            "_"
                        ):
                            for error_file in monitor_dir.glob("*.error"):
                                try:
                                    if error_file.stat().st_mtime < cutoff_time:
                                        file_size = error_file.stat().st_size
                                        if not dry_run:
                                            error_file.unlink()
                                        cleanup_results["spool_cleanup"][
                                            "files_deleted"
                                        ] += 1
                                        cleanup_results["spool_cleanup"][
                                            "bytes_freed"
                                        ] += file_size
                                except Exception as e:
                                    cleanup_results["spool_cleanup"]["errors"].append(
                                        f"Failed to delete {error_file}: {e}"
                                    )
                except Exception as e:
                    cleanup_results["spool_cleanup"]["errors"].append(
                        f"Error scanning monitor directories: {e}"
                    )

        # Clean up log files
        if logs:
            cleanup_results["log_cleanup"]["enabled"] = True

            if log_dir.exists():
                try:
                    for log_file in log_dir.glob("*.log"):
                        try:
                            if log_file.stat().st_mtime < cutoff_time:
                                file_size = log_file.stat().st_size
                                if not dry_run:
                                    log_file.unlink()
                                cleanup_results["log_cleanup"]["files_deleted"] += 1
                                cleanup_results["log_cleanup"][
                                    "bytes_freed"
                                ] += file_size
                        except Exception as e:
                            cleanup_results["log_cleanup"]["errors"].append(
                                f"Failed to delete {log_file}: {e}"
                            )
                except Exception as e:
                    cleanup_results["log_cleanup"]["errors"].append(
                        f"Error scanning log directory: {e}"
                    )

        # Output results
        if json:
            typer.echo(json_module.dumps(cleanup_results, indent=2))
        else:
            action = "Would delete" if dry_run else "Deleted"
            total_files = (
                cleanup_results["spool_cleanup"]["files_deleted"]
                + cleanup_results["log_cleanup"]["files_deleted"]
            )
            total_bytes = (
                cleanup_results["spool_cleanup"]["bytes_freed"]
                + cleanup_results["log_cleanup"]["bytes_freed"]
            )

            typer.echo(f"Cleanup Summary (files older than {days} days):")

            if cleanup_results["spool_cleanup"]["enabled"]:
                typer.echo(
                    f"  Spool: {action} {cleanup_results['spool_cleanup']['files_deleted']} files "
                    f"({cleanup_results['spool_cleanup']['bytes_freed']:,} bytes)"
                )
                if cleanup_results["spool_cleanup"]["errors"]:
                    typer.echo(
                        f"    Errors: {len(cleanup_results['spool_cleanup']['errors'])}"
                    )

            if cleanup_results["log_cleanup"]["enabled"]:
                typer.echo(
                    f"  Logs: {action} {cleanup_results['log_cleanup']['files_deleted']} files "
                    f"({cleanup_results['log_cleanup']['bytes_freed']:,} bytes)"
                )
                if cleanup_results["log_cleanup"]["errors"]:
                    typer.echo(
                        f"    Errors: {len(cleanup_results['log_cleanup']['errors'])}"
                    )

            typer.echo(f"Total: {action} {total_files} files ({total_bytes:,} bytes)")

            # Show errors if any
            all_errors = (
                cleanup_results["spool_cleanup"]["errors"]
                + cleanup_results["log_cleanup"]["errors"]
            )
            if all_errors:
                typer.echo(f"\nErrors ({len(all_errors)}):")
                for error in all_errors[:5]:  # Show first 5 errors
                    typer.echo(f"  {error}")
                if len(all_errors) > 5:
                    typer.echo(f"  ... and {len(all_errors) - 5} more errors")

    except Exception as e:
        typer.echo(f"[ERROR] Failed to run cleanup: {e}")
        raise typer.Exit(1) from e


@app.command()
def status(
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show detailed status information"
    ),
    json: bool = typer.Option(False, "--json", help="Output in JSON format"),
) -> None:
    """Show basic health and monitor status."""
    try:
        import json as json_module
        from datetime import datetime, timezone
        from pathlib import Path

        from .database import get_database

        # Get database connection
        db = get_database()

        try:
            # Check database health
            health = db.health_check()
            if health["status"] != "healthy":
                typer.echo(
                    f"[ERROR] Database unhealthy: {health.get('error', 'unknown error')}"
                )
                raise typer.Exit(1)

            # Get last event time per monitor
            conn = db._get_connection()
            cursor = conn.execute(
                """
                SELECT monitor, MAX(ts_utc) as last_ts_utc, COUNT(*) as event_count
                FROM events
                GROUP BY monitor
                ORDER BY last_ts_utc DESC
            """
            )

            monitor_data = cursor.fetchall()
            current_time = datetime.now(timezone.utc)
            current_time_ms = int(current_time.timestamp() * 1000)

            # Build structured data for both output modes
            monitors_info = []

            if not monitor_data:
                status_summary = "No events found in database"
            else:
                for row in monitor_data:
                    monitor, last_ts_utc, event_count = row
                    monitor_info = {
                        "monitor": monitor,
                        "event_count": event_count,
                        "last_event_utc": None,
                        "age_seconds": None,
                        "age_str": "no events",
                    }

                    if last_ts_utc:
                        # Convert UTC milliseconds to datetime
                        last_event_time = datetime.fromtimestamp(
                            last_ts_utc / 1000, timezone.utc
                        )
                        monitor_info["last_event_utc"] = last_event_time.isoformat()

                        age_ms = current_time_ms - last_ts_utc
                        age_seconds = age_ms // 1000
                        monitor_info["age_seconds"] = age_seconds

                        # Format age nicely
                        if age_seconds < 60:
                            age_str = f"{age_seconds}s ago"
                        elif age_seconds < 3600:
                            age_str = f"{age_seconds // 60}m ago"
                        elif age_seconds < 86400:
                            age_str = f"{age_seconds // 3600}h ago"
                        else:
                            age_str = f"{age_seconds // 86400}d ago"
                        monitor_info["age_str"] = age_str

                    monitors_info.append(monitor_info)

            # Check for pending spool files
            from .config import get_effective_config

            config = get_effective_config()
            spool_dir = Path(config.storage.spool_dir)

            pending_files = {}
            total_pending = 0

            if spool_dir.exists():
                # Known monitors to include in count
                from .importer import KNOWN_MONITORS

                for monitor_dir in spool_dir.iterdir():
                    if monitor_dir.is_dir() and not monitor_dir.name.startswith("_"):
                        monitor_name = monitor_dir.name
                        # Only count known monitors
                        if monitor_name in KNOWN_MONITORS:
                            # Count .ndjson.gz files (excluding .part and .error files)
                            monitor_files = [
                                f
                                for f in monitor_dir.glob("*.ndjson.gz")
                                if not f.name.endswith(".part")
                                and not f.name.endswith(".error")
                            ]
                            if monitor_files:
                                pending_files[monitor_name] = len(monitor_files)
                                total_pending += len(monitor_files)

            # Get database stats
            table_counts = db.get_table_counts()

            # Get quota information
            from .spool_quota import get_quota_manager

            quota_manager = get_quota_manager()
            usage = quota_manager.get_spool_usage()

            # Prepare output data
            status_data = {
                "timestamp_utc": current_time.isoformat(),
                "database_health": health["status"],
                "monitors": monitors_info,
                "pending_files": {"total": total_pending, "by_monitor": pending_files},
                "database_stats": {"total_events": table_counts.get("events", 0)},
                "spool": {
                    "quota_mb": usage.quota_bytes // (1024 * 1024),
                    "used_mb": usage.used_bytes // (1024 * 1024),
                    "soft_pct": config.storage.spool_soft_pct,
                    "hard_pct": config.storage.spool_hard_pct,
                    "state": usage.state.value,
                    "dropped_batches": usage.dropped_batches,
                },
            }

            # Output in requested format
            if json:
                typer.echo(json_module.dumps(status_data, indent=2))
            else:
                # Human-readable output
                if not monitor_data:
                    typer.echo("No events found in database")
                else:
                    typer.echo("Monitor status:")
                    for monitor_info in monitors_info:
                        if verbose:
                            typer.echo(
                                f"  {monitor_info['monitor']}: last event {monitor_info['age_str']} "
                                f"({monitor_info['event_count']} total events)"
                            )
                        else:
                            typer.echo(
                                f"  {monitor_info['monitor']}: {monitor_info['age_str']}"
                            )

                # Show pending files
                if total_pending > 0:
                    if verbose:
                        typer.echo(f"\nPending import files: {total_pending}")
                        for monitor, count in pending_files.items():
                            typer.echo(f"  {monitor}: {count} files")
                    else:
                        typer.echo(f"\nPending imports: {total_pending} files")
                else:
                    if verbose:
                        typer.echo("\nNo pending import files")
                    else:
                        typer.echo("\nPending imports: 0 files")

                # Show database stats if verbose
                if verbose:
                    typer.echo(
                        f"\nDatabase: {table_counts.get('events', 0)} events total"
                    )

                # Show quota information
                typer.echo(
                    f"\nSpool quota: {usage.used_bytes // (1024*1024)}MB / {usage.quota_bytes // (1024*1024)}MB ({usage.state.value})"
                )
                if usage.dropped_batches > 0:
                    typer.echo(f"Dropped batches: {usage.dropped_batches}")

        finally:
            db.close()

    except Exception as e:
        typer.echo(f"[ERROR] Status check failed: {e}", err=True)
        raise typer.Exit(1) from e


@app.command()
def daemon(
    action: str = typer.Argument(..., help="Action: start, stop, status"),
) -> None:
    """Manage the monitoring daemon."""
    typer.echo(f"Daemon {action} - coming soon")


@app.command()
def run(
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print events to console instead of writing files"
    ),
    duration: int = typer.Option(
        10, "--duration", "-d", help="Duration in seconds (dry-run only)"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Print detailed status messages"
    ),
) -> None:
    """Start all monitors with graceful shutdown on Ctrl+C."""
    try:
        from pathlib import Path

        from .config import get_effective_config
        from .ids import new_id
        from .logging_setup import set_session_id, setup_logging
        from .recovery import recover_all_temp_files
        from .supervisor import create_standard_supervisor

        # Generate session ID and initialize logging
        session_id = new_id()
        set_session_id(session_id)

        # Setup logging with appropriate levels
        console_level = "DEBUG" if verbose else "INFO"
        logger = setup_logging(
            console_level=console_level,
            file_level="DEBUG",
            session_id=session_id,
            console=True,
        )

        logger.info("Starting Little Brother v3 monitoring system")

        # Run recovery sweep before starting monitors
        config = get_effective_config()
        spool_dir = Path(config.storage.spool_dir)

        recovery_report = recover_all_temp_files(spool_dir)
        if recovery_report.temp_files_found > 0:
            # Log summary line as requested
            logger.info(recovery_report.summary_line())
            if not verbose:
                typer.echo(
                    f"Recovered {recovery_report.temp_files_recovered} temp segments "
                    f"({recovery_report.total_lines_salvaged} lines salvaged)"
                )

        # Create supervisor with appropriate settings
        supervisor = create_standard_supervisor(
            dry_run=dry_run, verbose=verbose, duration=duration if dry_run else 0
        )

        if dry_run:
            if not verbose:
                typer.echo(f"[DRY-RUN] Starting monitors for {duration} seconds...")
                typer.echo("[DRY-RUN] Events will be printed to console")
        else:
            if not verbose:
                typer.echo("Starting monitoring system...")

        # Start all monitors
        results = supervisor.start_all()

        # Wait for shutdown (Ctrl+C or natural completion in dry-run)
        with contextlib.suppress(KeyboardInterrupt):
            supervisor.wait_until_shutdown()

        # Graceful shutdown
        supervisor.stop_all()

        # Check if any monitors failed to start
        failed_monitors = [name for name, success in results.items() if not success]
        if failed_monitors and verbose:
            typer.echo(
                f"[INFO] Some monitors failed to start: {', '.join(failed_monitors)}"
            )

    except Exception as e:
        typer.echo(f"[ERROR] Failed to run monitors: {e}", err=True)
        raise typer.Exit(1) from e


@config_app.command("show")
def config_show() -> None:
    """Show the effective configuration."""
    try:
        config = get_effective_config()
        typer.echo(config.to_yaml())
    except Exception as e:
        typer.echo(f"Error loading configuration: {e}", err=True)
        raise typer.Exit(1) from e


@config_app.command("path")
def config_path() -> None:
    """Show the absolute path to the configuration file."""
    config_file = Config.get_config_path()
    typer.echo(str(config_file))


@db_app.command("check")
def db_check() -> None:
    """Perform database health check."""
    try:
        from .database import get_database

        db = get_database()
        health = db.health_check()

        if health["status"] == "healthy":
            typer.echo("[OK] Database health check: HEALTHY")
            typer.echo(f"Database path: {health['db_path']}")
            typer.echo(f"WAL mode: {health['wal_mode']}")

            # Show table counts
            typer.echo("\nTable counts:")
            for table, count in health["table_counts"].items():
                typer.echo(f"  {table}: {count:,}")

            # Check for missing components
            if health["tables_missing"]:
                typer.echo(
                    f"\n[WARN] Missing tables: {', '.join(health['tables_missing'])}"
                )

            if health["indexes_missing"]:
                typer.echo(
                    f"[WARN] Missing indexes: {', '.join(health['indexes_missing'])}"
                )

            if not health["tables_missing"] and not health["indexes_missing"]:
                typer.echo("\n[OK] All tables and indexes present")

        else:
            typer.echo("[ERROR] Database health check: ERROR")
            typer.echo(f"Database path: {health['db_path']}")
            typer.echo(f"Error: {health['error']}")
            raise typer.Exit(1)

    except Exception as e:
        typer.echo(f"[ERROR] Database health check failed: {e}", err=True)
        raise typer.Exit(1) from e


@spool_app.command("flush")
def spool_flush(
    monitor: str = typer.Option(
        "all", "--monitor", "-m", help="Monitor to flush (or 'all')"
    ),
) -> None:
    """Flush journal files to database."""
    try:
        from pathlib import Path

        from .config import get_effective_config
        from .importer import get_importer
        from .recovery import recover_all_temp_files

        # Run recovery sweep first
        config = get_effective_config()
        spool_dir = Path(config.storage.spool_dir)

        recovery_report = recover_all_temp_files(
            spool_dir, [monitor] if monitor != "all" else None
        )

        if recovery_report.temp_files_found > 0:
            typer.echo(
                f"Recovered {recovery_report.temp_files_recovered} temp segments "
                f"({recovery_report.total_lines_salvaged} lines salvaged)."
            )

        importer = get_importer()

        if monitor == "all":
            typer.echo("Flushing all monitor journals...")
            stats = importer.flush_all_monitors()

            # Concise summary as requested
            typer.echo("\n[OK] Import completed:")
            typer.echo(f"Files processed: {stats['total_files_processed']}")
            typer.echo(f"Events imported: {stats['total_events_imported']}")
            typer.echo(f"Duplicates skipped: {stats['total_duplicates_skipped']}")
            typer.echo(f"Invalid events: {stats['total_invalid_events']}")
            typer.echo(f"Duration: {stats['total_duration_seconds']:.2f}s")

            if stats["overall_events_per_minute"] > 0:
                typer.echo(
                    f"Throughput: {stats['overall_events_per_minute']:.0f} events/min"
                )

            if stats["total_files_with_errors"] > 0:
                typer.echo(
                    f"[WARN] Files with errors: {stats['total_files_with_errors']}"
                )

            # Show per-monitor breakdown
            if stats["monitor_stats"]:
                typer.echo("\nPer-monitor breakdown:")
                for mon, mon_stats in stats["monitor_stats"].items():
                    if mon_stats["files_processed"] > 0:
                        typer.echo(
                            f"  {mon}: {mon_stats['files_processed']} files, "
                            f"{mon_stats['events_imported']} events, "
                            f"{mon_stats['events_per_minute']:.0f} events/min"
                        )
                        if mon_stats["files_with_errors"] > 0:
                            typer.echo(
                                f"    [WARN] {mon_stats['files_with_errors']} files with errors"
                            )

        else:
            typer.echo(f"Flushing journal for monitor: {monitor}")
            stats = importer.flush_monitor(monitor)

            typer.echo(f"\n[OK] Import completed for {monitor}:")
            typer.echo(f"Files processed: {stats['files_processed']}")
            typer.echo(f"Events imported: {stats['events_imported']}")
            typer.echo(f"Duplicates skipped: {stats['duplicates_skipped']}")
            typer.echo(f"Invalid events: {stats['invalid_events']}")
            typer.echo(f"Duration: {stats['duration_seconds']:.2f}s")

            if stats["events_per_minute"] > 0:
                typer.echo(f"Throughput: {stats['events_per_minute']:.0f} events/min")

            if stats["files_with_errors"] > 0:
                typer.echo(f"[WARN] Files with errors: {stats['files_with_errors']}")
                for error in stats["errors"]:
                    typer.echo(f"  {error}")

    except Exception as e:
        typer.echo(f"[ERROR] Spool flush failed: {e}", err=True)
        raise typer.Exit(1) from e


@spool_app.command("generate")
def spool_generate(
    monitor: str = typer.Argument(..., help="Monitor to generate sample events for"),
    count: int = typer.Option(10, "--count", "-c", help="Number of events to generate"),
) -> None:
    """Generate sample events in journal format."""
    try:
        from .spooler import SpoolerManager, create_sample_event

        valid_monitors = [
            "active_window",
            "context_snapshot",
            "keyboard",
            "mouse",
            "browser",
            "file",
        ]
        if monitor not in valid_monitors:
            typer.echo(
                f"[ERROR] Invalid monitor. Valid options: {', '.join(valid_monitors)}"
            )
            raise typer.Exit(1)

        typer.echo(f"Generating {count} sample events for {monitor}...")

        manager = SpoolerManager()
        for _ in range(count):
            event = create_sample_event(monitor)
            manager.write_event(monitor, event)

        # Close all spoolers to ensure files are finalized
        manager.close_all()

        typer.echo(f"[OK] Generated {count} events for {monitor}")
        typer.echo(f"Journal files created in: ./lb_data/spool/{monitor}/")

    except Exception as e:
        typer.echo(f"[ERROR] Sample generation failed: {e}", err=True)
        raise typer.Exit(1) from e


@monitors_app.command("status")
def monitors_status(
    json: bool = typer.Option(False, "--json", help="Output in JSON format"),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show detailed monitor information"
    ),
) -> None:
    """Show configuration status of all monitors (non-invasive)."""
    import json as json_module
    import threading
    import time

    from .config import get_effective_config

    try:
        # Get configuration without starting any monitors
        config = get_effective_config()

        # Check if supervisor is already running by looking for active threads
        active_threads = [
            t.name for t in threading.enumerate() if t.name.startswith("Monitor-")
        ]
        is_running = len(active_threads) > 0

        # Define standard monitors and their config status
        monitor_configs = {
            "heartbeat": {
                "configured": True,  # Always configured
                "enabled": True,  # Always enabled
                "note": "Core monitor (always active)",
            },
            "keyboard": {
                "configured": True,  # Always configured
                "enabled": True,  # Always enabled
                "note": "Input monitor",
            },
            "mouse": {
                "configured": True,  # Always configured
                "enabled": True,  # Always enabled
                "note": "Input monitor",
            },
            "active_window": {
                "configured": True,  # Configured on Windows
                "enabled": True,  # Enabled on Windows
                "note": "Window tracking (Windows only)",
            },
            "file": {
                "configured": True,  # Always configured
                "enabled": True,  # Always enabled
                "note": "File system monitor",
            },
            "browser": {
                "configured": True,  # Always configured
                "enabled": not config.browser.integration.disabled_by_default,  # Based on config
                "note": "CDP browser integration",
            },
            "context_snapshot": {
                "configured": hasattr(config, "monitors"),  # Has monitors config
                "enabled": False,  # REMOVED FROM RUNTIME
                "note": "REMOVED from runtime (use 'lb3 probe context' for manual testing)",
            },
        }

        # Add quiescence status if context_snapshot has config
        if hasattr(config, "monitors") and hasattr(config.monitors, "context_snapshot"):
            quiescence_info = {
                "configured": True,
                "enabled": config.monitors.context_snapshot.quiescence.enabled,
                "interval": config.monitors.context_snapshot.quiescence.interval,
                "note": "Timer-based context snapshots (optional)",
            }
            monitor_configs["quiescence"] = quiescence_info

        result = {
            "supervisor_running": is_running,
            "monitors": monitor_configs,
            "timestamp": time.time(),
            "warning": "This command is non-invasive and shows configuration only",
        }

        if json:
            typer.echo(json_module.dumps(result, indent=2))
        else:
            if is_running:
                typer.echo("Supervisor: RUNNING (monitor threads detected)")
            else:
                typer.echo("Supervisor: NOT RUNNING (no active monitor threads)")

            typer.echo()
            typer.echo("Monitor Configuration Status:")

            for monitor_name, info in monitor_configs.items():
                status_parts = []

                if info["configured"]:
                    status_parts.append("configured")
                else:
                    status_parts.append("NOT configured")

                if info["enabled"]:
                    status_parts.append("enabled")
                else:
                    status_parts.append("disabled")

                status_text = ", ".join(status_parts)
                typer.echo(f"  {monitor_name}: {status_text}")

                if verbose and "note" in info:
                    typer.echo(f"    note: {info['note']}")

                if verbose and "interval" in info:
                    typer.echo(f"    interval: {info['interval']}")

            if not verbose:
                typer.echo()
                typer.echo("Use -v/--verbose for detailed information")
                typer.echo("Note: context_snapshot completely removed from runtime")

    except Exception as e:
        typer.echo(f"[ERROR] Failed to get monitor status: {e}", err=True)
        raise typer.Exit(1) from e


@app.command()
def probe(
    target: str = typer.Argument(
        "context", help="Target to probe (currently only 'context' supported)"
    ),
) -> None:
    """Probe specific monitor functionality in complete isolation."""
    if target != "context":
        typer.echo(
            f"[ERROR] Unsupported probe target: {target}. Only 'context' is supported."
        )
        raise typer.Exit(1)

    import time
    from pathlib import Path

    from .events import SpoolerSink, get_event_bus
    from .importer import JournalImporter
    from .monitors.context_snapshot import ContextSnapshotMonitor
    from .spooler import get_spooler_manager

    # Initialize cleanup variables
    bus = None
    sink = None
    monitor = None

    try:
        # Start isolated event bus and spooler sink
        bus = get_event_bus()
        sink = SpoolerSink()
        bus.subscribe(sink)
        bus.start()

        # Create and start context snapshot monitor in isolation
        monitor = ContextSnapshotMonitor(dry_run=False)
        monitor.start()

        # Wait for initialization
        time.sleep(0.5)

        # Force emit a snapshot
        monitor.force_emit(trigger="probe")

        # Wait for async processing
        time.sleep(3.0)

        # Stop monitor first
        monitor.stop()

        # Flush and close spooler
        spooler_manager = get_spooler_manager()
        spooler_manager.flush_idle_spoolers()
        sink.close()
        bus.stop()

        # Clear references for cleanup
        monitor = None
        sink = None
        bus = None

        # Import phase
        from .config import get_effective_config

        config = get_effective_config()
        spool_dir = Path(config.storage.spool_dir)
        importer = JournalImporter(spool_dir)

        # Import context_snapshot files specifically
        context_spool = spool_dir / "context_snapshot"
        imported_count = 0
        latest_file = None

        if context_spool.exists():
            ndjson_files = list(context_spool.glob("*.ndjson.gz"))
            if ndjson_files:
                # Sort by modification time, get latest
                ndjson_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
                latest_file = ndjson_files[0]

                # Import
                result = importer.flush_monitor("context_snapshot")
                imported_count = result.get("events_imported", 0)

        # Single-line output as required
        latest_file_name = latest_file.name if latest_file else "<none>"
        typer.echo(f"probe: imported={imported_count}, latest_file={latest_file_name}")

        # Exit with appropriate code based on success
        if imported_count > 0:
            raise typer.Exit(0)  # Success
        else:
            raise typer.Exit(1)  # Failure

    except typer.Exit:
        # Re-raise typer.Exit without modification to preserve exit codes
        raise
    except Exception as e:
        # Ensure proper cleanup even on error
        try:
            if monitor:
                monitor.stop()
            if sink:
                sink.close()
            if bus:
                bus.stop()
        except Exception:
            pass  # Ignore cleanup errors

        typer.echo(f"[ERROR] Probe failed: {e}", err=True)
        raise typer.Exit(1) from e


@spool_app.command()
def stats(
    reset: bool = typer.Option(
        False, "--reset", help="Reset counters after displaying"
    ),
) -> None:
    """Show spooler write and finalization statistics."""

    from .spooler import get_spooler_manager

    try:
        manager = get_spooler_manager()

        if reset:
            stats_data = manager.reset_stats()
            typer.echo("Spooler stats (before reset):")
        else:
            stats_data = manager.get_stats()
            typer.echo("Current spooler stats:")

        # Display written events
        written = stats_data.get("written_by_monitor", {})
        if written:
            typer.echo("\nEvents written by monitor:")
            for monitor, count in sorted(written.items()):
                typer.echo(f"  {monitor}: {count}")
        else:
            typer.echo("\nNo events written yet")

        # Display finalized files
        finalized = stats_data.get("finalised_files_by_monitor", {})
        if finalized:
            typer.echo("\nFiles finalized by monitor:")
            for monitor, count in sorted(finalized.items()):
                typer.echo(f"  {monitor}: {count}")
        else:
            typer.echo("\nNo files finalized yet")

        if reset:
            typer.echo("\nCounters have been reset to zero.")

    except Exception as e:
        typer.echo(f"[ERROR] Failed to get spooler stats: {e}", err=True)
        raise typer.Exit(1) from e


def main() -> None:
    """Main entry point for python -m lb3."""
    typer.echo("Little Brother v3 CLI coming soon")


if __name__ == "__main__":
    app()
