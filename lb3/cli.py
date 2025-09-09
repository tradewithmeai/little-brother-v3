"""Command line interface for Little Brother v3."""

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


@app.command()
def version() -> None:
    """Show version information."""
    typer.echo(f"Little Brother v3 {__version__}")


@app.command()
def status(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed status information")
) -> None:
    """Show basic health and monitor status."""
    try:
        import time
        from pathlib import Path

        from .database import get_database
        
        # Get database connection
        db = get_database()
        
        try:
            # Check database health
            health = db.health_check()
            if health["status"] != "healthy":
                typer.echo(f"[ERROR] Database unhealthy: {health.get('error', 'unknown error')}")
                raise typer.Exit(1)
            
            # Get last event time per monitor
            conn = db._get_connection()
            cursor = conn.execute("""
                SELECT monitor, MAX(ts_utc) as last_ts_utc, COUNT(*) as event_count
                FROM events 
                GROUP BY monitor 
                ORDER BY last_ts_utc DESC
            """)
            
            monitor_data = cursor.fetchall()
            current_time_ms = int(time.time() * 1000)
            
            if not monitor_data:
                typer.echo("No events found in database")
            else:
                typer.echo("Monitor status:")
                
                for row in monitor_data:
                    monitor, last_ts_utc, event_count = row
                    if last_ts_utc:
                        age_ms = current_time_ms - last_ts_utc
                        age_seconds = age_ms // 1000
                        
                        # Format age nicely
                        if age_seconds < 60:
                            age_str = f"{age_seconds}s ago"
                        elif age_seconds < 3600:
                            age_str = f"{age_seconds // 60}m ago"
                        elif age_seconds < 86400:
                            age_str = f"{age_seconds // 3600}h ago"
                        else:
                            age_str = f"{age_seconds // 86400}d ago"
                        
                        if verbose:
                            typer.echo(f"  {monitor}: last event {age_str} ({event_count} total events)")
                        else:
                            typer.echo(f"  {monitor}: {age_str}")
                    else:
                        if verbose:
                            typer.echo(f"  {monitor}: no events ({event_count} total events)")
                        else:
                            typer.echo(f"  {monitor}: no events")
            
            # Check for pending spool files
            from .config import get_effective_config
            config = get_effective_config()
            spool_dir = Path(config.storage.spool_dir)
            
            if spool_dir.exists():
                pending_files = {}
                total_pending = 0
                
                for monitor_dir in spool_dir.iterdir():
                    if monitor_dir.is_dir() and not monitor_dir.name.startswith("_"):
                        # Count .ndjson.gz files (excluding .part files)
                        monitor_files = [
                            f for f in monitor_dir.glob("*.ndjson.gz") 
                            if not f.name.endswith(".part")
                        ]
                        if monitor_files:
                            pending_files[monitor_dir.name] = len(monitor_files)
                            total_pending += len(monitor_files)
                
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
            
            # Show basic database stats if verbose
            if verbose:
                table_counts = db.get_table_counts()
                typer.echo(f"\nDatabase: {table_counts['events']} events total")
                
        finally:
            db.close()
        
    except Exception as e:
        typer.echo(f"[ERROR] Status check failed: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def daemon(
    action: str = typer.Argument(..., help="Action: start, stop, status"),
) -> None:
    """Manage the monitoring daemon."""
    typer.echo(f"Daemon {action} - coming soon")


@app.command()
def run(
    dry_run: bool = typer.Option(False, "--dry-run", help="Print events to console instead of writing files"),
    duration: int = typer.Option(10, "--duration", "-d", help="Duration in seconds (dry-run only)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Print detailed status messages")
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
            console=True
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
                typer.echo(f"Recovered {recovery_report.temp_files_recovered} temp segments ({recovery_report.total_lines_salvaged} lines salvaged)")
        
        # Create supervisor with appropriate settings
        supervisor = create_standard_supervisor(
            dry_run=dry_run, 
            verbose=verbose,
            duration=duration if dry_run else 0
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
        try:
            supervisor.wait_until_shutdown()
        except KeyboardInterrupt:
            # This should be handled by signal handlers in supervisor
            pass
        
        # Graceful shutdown
        supervisor.stop_all()
        
        # Check if any monitors failed to start
        failed_monitors = [name for name, success in results.items() if not success]
        if failed_monitors and verbose:
            typer.echo(f"[INFO] Some monitors failed to start: {', '.join(failed_monitors)}")
        
    except Exception as e:
        typer.echo(f"[ERROR] Failed to run monitors: {e}", err=True)
        raise typer.Exit(1)


@config_app.command("show")
def config_show() -> None:
    """Show the effective configuration."""
    try:
        config = get_effective_config()
        typer.echo(config.to_yaml())
    except Exception as e:
        typer.echo(f"Error loading configuration: {e}", err=True)
        raise typer.Exit(1)


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
                typer.echo(f"\n[WARN] Missing tables: {', '.join(health['tables_missing'])}")
            
            if health["indexes_missing"]:
                typer.echo(f"[WARN] Missing indexes: {', '.join(health['indexes_missing'])}")
            
            if not health["tables_missing"] and not health["indexes_missing"]:
                typer.echo("\n[OK] All tables and indexes present")
        
        else:
            typer.echo("[ERROR] Database health check: ERROR")
            typer.echo(f"Database path: {health['db_path']}")
            typer.echo(f"Error: {health['error']}")
            raise typer.Exit(1)
            
    except Exception as e:
        typer.echo(f"[ERROR] Database health check failed: {e}", err=True)
        raise typer.Exit(1)


@spool_app.command("flush")
def spool_flush(
    monitor: str = typer.Option("all", "--monitor", "-m", help="Monitor to flush (or 'all')")
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
        
        recovery_report = recover_all_temp_files(spool_dir, 
                                               [monitor] if monitor != "all" else None)
        
        if recovery_report.temp_files_found > 0:
            typer.echo(f"Recovered {recovery_report.temp_files_recovered} temp segments ({recovery_report.total_lines_salvaged} lines salvaged).")
        
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
            
            if stats['overall_events_per_minute'] > 0:
                typer.echo(f"Throughput: {stats['overall_events_per_minute']:.0f} events/min")
            
            if stats['total_files_with_errors'] > 0:
                typer.echo(f"[WARN] Files with errors: {stats['total_files_with_errors']}")
            
            # Show per-monitor breakdown
            if stats['monitor_stats']:
                typer.echo("\nPer-monitor breakdown:")
                for mon, mon_stats in stats['monitor_stats'].items():
                    if mon_stats['files_processed'] > 0:
                        typer.echo(f"  {mon}: {mon_stats['files_processed']} files, {mon_stats['events_imported']} events, {mon_stats['events_per_minute']:.0f} events/min")
                        if mon_stats['files_with_errors'] > 0:
                            typer.echo(f"    [WARN] {mon_stats['files_with_errors']} files with errors")
        
        else:
            typer.echo(f"Flushing journal for monitor: {monitor}")
            stats = importer.flush_monitor(monitor)
            
            typer.echo(f"\n[OK] Import completed for {monitor}:")
            typer.echo(f"Files processed: {stats['files_processed']}")
            typer.echo(f"Events imported: {stats['events_imported']}")
            typer.echo(f"Duplicates skipped: {stats['duplicates_skipped']}")
            typer.echo(f"Invalid events: {stats['invalid_events']}")
            typer.echo(f"Duration: {stats['duration_seconds']:.2f}s")
            
            if stats['events_per_minute'] > 0:
                typer.echo(f"Throughput: {stats['events_per_minute']:.0f} events/min")
            
            if stats['files_with_errors'] > 0:
                typer.echo(f"[WARN] Files with errors: {stats['files_with_errors']}")
                for error in stats['errors']:
                    typer.echo(f"  {error}")
    
    except Exception as e:
        typer.echo(f"[ERROR] Spool flush failed: {e}", err=True)
        raise typer.Exit(1)


@spool_app.command("generate")
def spool_generate(
    monitor: str = typer.Argument(..., help="Monitor to generate sample events for"),
    count: int = typer.Option(10, "--count", "-c", help="Number of events to generate")
) -> None:
    """Generate sample events in journal format."""
    try:
        from .spooler import SpoolerManager, create_sample_event
        
        valid_monitors = ['active_window', 'context_snapshot', 'keyboard', 'mouse', 'browser', 'file']
        if monitor not in valid_monitors:
            typer.echo(f"[ERROR] Invalid monitor. Valid options: {', '.join(valid_monitors)}")
            raise typer.Exit(1)
        
        typer.echo(f"Generating {count} sample events for {monitor}...")
        
        manager = SpoolerManager()
        for i in range(count):
            event = create_sample_event(monitor)
            manager.write_event(monitor, event)
        
        # Close all spoolers to ensure files are finalized
        manager.close_all()
        
        typer.echo(f"[OK] Generated {count} events for {monitor}")
        typer.echo(f"Journal files created in: ./lb_data/spool/{monitor}/")
    
    except Exception as e:
        typer.echo(f"[ERROR] Sample generation failed: {e}", err=True)
        raise typer.Exit(1)


def main() -> None:
    """Main entry point for python -m lb3."""
    typer.echo("Little Brother v3 CLI coming soon")


if __name__ == "__main__":
    app()
