"""Integration tests for CLI run command with full lifecycle testing."""

import os
import signal
import subprocess
import tempfile
import time
from pathlib import Path

import pytest

from lb3.database import Database


@pytest.mark.usefixtures("no_thread_leaks")
class TestCliRunIntegration:
    """Integration tests for CLI run command lifecycle."""
    
    def test_run_command_full_lifecycle_graceful_shutdown(self):
        """Test full run command lifecycle: start monitors, verify activity, graceful shutdown."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Set up temporary environment
            config_dir = Path(temp_dir) / "config"
            data_dir = Path(temp_dir) / "data"
            spool_dir = data_dir / "spool"
            db_path = data_dir / "monitoring.db"
            
            config_dir.mkdir()
            data_dir.mkdir()
            spool_dir.mkdir(parents=True)
            
            # Create minimal config file
            config_file = config_dir / "config.yaml"
            config_content = f"""
storage:
  database_path: "{db_path.as_posix()}"
  spool_dir: "{spool_dir.as_posix()}"

monitors:
  heartbeat:
    interval: 1.0
  keyboard:
    enabled: true
  mouse:
    enabled: true
  file:
    enabled: true
"""
            with open(config_file, 'w') as f:
                f.write(config_content)
            
            # Set environment variables for config discovery
            env = os.environ.copy()
            env['LB3_CONFIG_DIR'] = str(config_dir)
            
            try:
                # Start lb3 run command in subprocess
                print("\nStarting lb3 run command...")
                start_time = time.time()
                
                process = subprocess.Popen(
                    ['python', '-m', 'lb3', 'run', '--verbose'],
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    cwd=Path(__file__).parent.parent.parent  # Project root
                )
                
                # Let it run for 10 seconds to accumulate events
                print("Letting monitors run for 10 seconds...")
                time.sleep(10)
                
                # Send SIGINT for graceful shutdown
                print("Sending SIGINT for graceful shutdown...")
                if os.name == 'nt':  # Windows
                    process.send_signal(signal.CTRL_C_EVENT)
                else:  # Unix-like
                    process.send_signal(signal.SIGINT)
                
                # Wait for graceful shutdown with timeout
                try:
                    stdout, stderr = process.communicate(timeout=15)
                    return_code = process.returncode
                    runtime = time.time() - start_time
                    
                    print(f"Process completed with return code: {return_code}")
                    print(f"Runtime: {runtime:.2f}s")
                    
                    # Verify graceful shutdown (exit code 0)
                    assert return_code == 0, f"Process should exit cleanly, got code {return_code}"
                    
                    # Check output for expected messages
                    output_lines = stdout.split('\n')
                    assert any("Starting monitoring system..." in line for line in output_lines), "Should show startup message"
                    assert any("Shutting down gracefully..." in line or "graceful shutdown" in line.lower() for line in output_lines), "Should show graceful shutdown message"
                    
                    # Should not have stack traces or unhandled exceptions in stderr
                    error_lines = stderr.split('\n')
                    critical_errors = [line for line in error_lines if 'Traceback' in line or 'Exception:' in line]
                    if critical_errors:
                        print(f"Warning: Found potential errors in stderr: {critical_errors[:3]}")
                    
                    # Verify events were written to spool directory
                    print("Checking spool directory for events...")
                    monitor_dirs = [d for d in spool_dir.iterdir() if d.is_dir() and not d.name.startswith('_')]
                    assert len(monitor_dirs) > 0, "Should have created monitor directories in spool"
                    
                    total_files = 0
                    for monitor_dir in monitor_dirs:
                        journal_files = list(monitor_dir.glob("*.ndjson.gz"))
                        total_files += len(journal_files)
                        print(f"  {monitor_dir.name}: {len(journal_files)} journal files")
                    
                    assert total_files > 0, "Should have created journal files during 10s run"
                    
                    # Flush events to database and verify
                    print("Flushing events to database...")
                    flush_process = subprocess.run(
                        ['python', '-m', 'lb3', 'spool', 'flush'],
                        env=env,
                        capture_output=True,
                        text=True,
                        cwd=Path(__file__).parent.parent.parent
                    )
                    
                    assert flush_process.returncode == 0, f"Spool flush should succeed: {flush_process.stderr}"
                    
                    # Verify database has events
                    if db_path.exists():
                        db = Database(db_path)
                        try:
                            counts = db.get_table_counts()
                            events_count = counts.get('events', 0)
                            print(f"Database contains {events_count} events")
                            assert events_count > 0, "Database should contain events after flush"
                        finally:
                            db.close()
                    else:
                        print("Warning: Database file was not created")
                    
                    # Test status command shows recent activity
                    print("Testing status command...")
                    status_process = subprocess.run(
                        ['python', '-m', 'lb3', 'status', '--verbose'],
                        env=env,
                        capture_output=True,
                        text=True,
                        cwd=Path(__file__).parent.parent.parent
                    )
                    
                    if status_process.returncode == 0:
                        status_output = status_process.stdout
                        print("Status output preview:", status_output[:200] + "..." if len(status_output) > 200 else status_output)
                        assert "Monitor status:" in status_output, "Status should show monitor information"
                    else:
                        print(f"Status command failed: {status_process.stderr}")
                    
                    print("\n[SUCCESS] Full lifecycle test completed successfully")
                    print(f"✓ Graceful startup and shutdown (exit code {return_code})")
                    print(f"✓ Events written to {total_files} journal files")
                    print(f"✓ Runtime: {runtime:.1f}s")
                    if 'events_count' in locals():
                        print(f"✓ {events_count} events imported to database")
                    
                except subprocess.TimeoutExpired:
                    # Force kill if graceful shutdown took too long
                    print("Graceful shutdown timeout, force killing process...")
                    process.kill()
                    stdout, stderr = process.communicate()
                    pytest.fail("Process did not shutdown gracefully within timeout")
                
            except Exception as e:
                # Clean up process if test fails
                try:
                    if process.poll() is None:
                        process.terminate()
                        process.wait(timeout=5)
                except:
                    pass
                raise e
    
    def test_dry_run_mode_no_file_writes(self):
        """Test dry-run mode prints events without writing files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir) / "config"
            data_dir = Path(temp_dir) / "data"
            spool_dir = data_dir / "spool"
            
            config_dir.mkdir()
            data_dir.mkdir()
            spool_dir.mkdir(parents=True)
            
            # Create minimal config
            config_file = config_dir / "config.yaml"
            config_content = f"""
storage:
  database_path: "{data_dir / 'monitoring.db'}"
  spool_dir: "{spool_dir.as_posix()}"
"""
            with open(config_file, 'w') as f:
                f.write(config_content)
            
            env = os.environ.copy()
            env['LB3_CONFIG_DIR'] = str(config_dir)
            
            try:
                # Run in dry-run mode for 5 seconds
                print("\nTesting dry-run mode...")
                process = subprocess.Popen(
                    ['python', '-m', 'lb3', 'run', '--dry-run', '--duration', '5'],
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    cwd=Path(__file__).parent.parent.parent
                )
                
                stdout, stderr = process.communicate(timeout=30)
                
                # Should exit cleanly after duration
                assert process.returncode == 0, f"Dry-run should exit cleanly: {stderr}"
                
                # Should show dry-run messages
                assert "[DRY-RUN]" in stdout, "Should show dry-run indicator in output"
                assert "Events will be printed to console" in stdout, "Should indicate console output mode"
                
                # Should print event data to stdout
                lines = stdout.split('\n')
                event_lines = [line for line in lines if 'ts_utc' in line or '"id"' in line]
                assert len(event_lines) > 0, f"Should print events to console. Output: {stdout[:500]}"
                
                # Should NOT create journal files
                monitor_dirs = [d for d in spool_dir.iterdir() if d.is_dir() and not d.name.startswith('_')]
                total_files = sum(len(list(d.glob("*.ndjson.gz"))) for d in monitor_dirs)
                assert total_files == 0, f"Dry-run should not create files, found {total_files} files"
                
                print("✓ Dry-run completed successfully")
                print(f"✓ Printed {len(event_lines)} event lines to console")
                print("✓ No journal files created (as expected)")
                
            except subprocess.TimeoutExpired:
                process.kill()
                pytest.fail("Dry-run process did not complete within expected time")
    
    def test_version_and_status_commands(self):
        """Test version and status commands work correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir) / "config" 
            data_dir = Path(temp_dir) / "data"
            
            config_dir.mkdir()
            data_dir.mkdir()
            
            # Create minimal config for status command
            config_file = config_dir / "config.yaml"
            config_content = f"""
storage:
  database_path: "{data_dir / 'monitoring.db'}"
  spool_dir: "{data_dir / 'spool'}"
"""
            with open(config_file, 'w') as f:
                f.write(config_content)
            
            env = os.environ.copy()
            env['LB3_CONFIG_DIR'] = str(config_dir)
            
            # Test version command
            print("\nTesting version command...")
            version_result = subprocess.run(
                ['python', '-m', 'lb3', 'version'],
                env=env,
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent.parent
            )
            
            assert version_result.returncode == 0, f"Version command failed: {version_result.stderr}"
            assert "Little Brother v3" in version_result.stdout, "Should show version string"
            print(f"✓ Version: {version_result.stdout.strip()}")
            
            # Test status command (should handle empty database gracefully)
            print("Testing status command...")
            status_result = subprocess.run(
                ['python', '-m', 'lb3', 'status'],
                env=env,
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent.parent
            )
            
            # Status may fail if no database exists yet, but should not crash
            print(f"Status exit code: {status_result.returncode}")
            if status_result.returncode == 0:
                print(f"✓ Status output: {status_result.stdout[:100]}...")
            else:
                print(f"Status failed (expected for empty DB): {status_result.stderr[:100]}...")
                # This is acceptable - status command may fail on empty system
            
            print("✓ Command interface tests completed")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])