"""Test configuration and fixtures for Little Brother v3."""

import os
import threading
import time

import pytest


@pytest.fixture(autouse=True)
def test_mode():
    """Enable test mode for all tests."""
    os.environ['LB3_TEST_MODE'] = '1'
    yield
    # Cleanup handled by os.environ teardown


@pytest.fixture
def no_thread_leaks():
    """Fixture to detect thread leaks during tests."""
    # Get initial thread set
    initial_threads = set(threading.enumerate())
    initial_daemon_threads = {t for t in initial_threads if t.daemon}
    initial_non_daemon_threads = {t for t in initial_threads if not t.daemon}
    
    yield
    
    # Wait briefly for threads to cleanup
    time.sleep(0.1)
    
    # Get final thread set
    final_threads = set(threading.enumerate())
    final_non_daemon_threads = {t for t in final_threads if not t.daemon}
    
    # Check for new non-daemon threads
    leaked_threads = final_non_daemon_threads - initial_non_daemon_threads
    if leaked_threads:
        thread_names = [t.name for t in leaked_threads]
        pytest.fail(f"Test leaked non-daemon threads: {thread_names}")


@pytest.fixture
def fake_clock():
    """Fixture providing a fake clock for deterministic timing."""
    clock_time = [0.0]
    
    def get_time():
        return clock_time[0]
    
    def advance(dt_s: float):
        clock_time[0] += dt_s
        return clock_time[0]
    
    get_time.advance = advance
    return get_time


@pytest.fixture 
def manual_scheduler(fake_clock):
    """Fixture providing a manual scheduler for deterministic tests."""
    from lb3.utils.scheduler import ManualScheduler
    return ManualScheduler(start_time=0.0, clock=fake_clock)