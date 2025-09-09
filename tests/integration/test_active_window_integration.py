"""Integration tests for active window monitor."""

import ctypes
import subprocess
import sys
import time
from unittest import skipUnless

import pytest

from lb3.database import get_database
from lb3.events import get_event_bus
from lb3.monitors.active_window import ActiveWindowMonitor


@skipUnless(sys.platform == "win32", "ActiveWindowMonitor integration tests only run on Windows")
class TestActiveWindowIntegration:
    """Integration tests for ActiveWindowMonitor with real Windows processes."""
    
    def setup_method(self):
        """Set up test environment."""
        self.launched_processes = []
        self.monitor = None
        self.event_bus = None
        self.received_events = []
        
    def teardown_method(self):
        """Clean up test environment."""
        # Stop monitor
        if self.monitor:
            try:
                self.monitor.stop()
            except:
                pass
        
        # Stop event bus
        if self.event_bus:
            try:
                self.event_bus.stop()
            except:
                pass
        
        # Kill launched processes
        for process in self.launched_processes:
            try:
                process.terminate()
                process.wait(timeout=5)
            except:
                try:
                    process.kill()
                except:
                    pass
    
    def _launch_process(self, executable, args=None):
        """Launch a process and track it for cleanup."""
        cmd = [executable]
        if args:
            cmd.extend(args)
        
        try:
            process = subprocess.Popen(cmd, 
                                     creationflags=subprocess.CREATE_NEW_CONSOLE if sys.platform == "win32" else 0)
            self.launched_processes.append(process)
            return process
        except FileNotFoundError:
            pytest.skip(f"Executable {executable} not found")
    
    def _set_foreground_window_by_process(self, process, max_attempts=10):
        """Set foreground window for a process by finding its main window."""
        from ctypes import wintypes
        
        user32 = ctypes.windll.user32
        
        def enum_windows_proc(hwnd, lParam):
            """Callback for EnumWindows."""
            pid = wintypes.DWORD()
            user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
            
            if pid.value == process.pid:
                # Check if this is a main window (has title and is visible)
                if user32.IsWindowVisible(hwnd):
                    length = user32.GetWindowTextLengthW(hwnd)
                    if length > 0:
                        # Set this window as foreground
                        user32.SetForegroundWindow(hwnd)
                        return False  # Stop enumeration
            return True  # Continue enumeration
        
        # Define callback type
        EnumWindowsProc = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
        enum_func = EnumWindowsProc(enum_windows_proc)
        
        # Try multiple times as window might not be ready immediately
        for attempt in range(max_attempts):
            user32.EnumWindows(enum_func, 0)
            time.sleep(0.2)  # Give time for window to become active
            
            # Check if we successfully set foreground
            foreground_hwnd = user32.GetForegroundWindow()
            if foreground_hwnd:
                pid = wintypes.DWORD()
                user32.GetWindowThreadProcessId(foreground_hwnd, ctypes.byref(pid))
                if pid.value == process.pid:
                    return True
        
        return False
    
    def _setup_event_collection(self):
        """Set up event bus and collection."""
        self.event_bus = get_event_bus()
        
        def event_collector(event):
            self.received_events.append(event)
        
        self.event_bus.subscribe(event_collector)
        self.event_bus.start()
    
    def test_notepad_explorer_window_switching(self):
        """Test window switching between Notepad and Explorer."""
        # Setup event collection
        self._setup_event_collection()
        
        # Create monitor
        self.monitor = ActiveWindowMonitor(dry_run=False)
        
        # Start monitor
        self.monitor.start()
        
        # Give monitor time to initialize
        time.sleep(0.5)
        
        # Clear any initial events
        self.received_events.clear()
        
        # Launch Notepad
        notepad = self._launch_process("notepad.exe")
        time.sleep(1.0)  # Wait for process to start
        
        # Set Notepad as foreground
        success = self._set_foreground_window_by_process(notepad)
        if not success:
            pytest.skip("Could not set Notepad as foreground window")
        
        time.sleep(1.5)  # Wait for monitor to detect change
        
        # Check that we got a window change event for Notepad
        notepad_events = [e for e in self.received_events 
                         if hasattr(e, 'exe_name') and e.exe_name and 'notepad' in e.exe_name.lower()]
        assert len(notepad_events) >= 1, f"Expected Notepad event, got: {[getattr(e, 'exe_name', 'unknown') for e in self.received_events]}"
        
        notepad_event = notepad_events[0]
        assert notepad_event.monitor == "active_window"
        assert notepad_event.action == "window_change"
        assert notepad_event.subject_type == "window"
        assert notepad_event.pid == notepad.pid
        assert notepad_event.exe_name.lower() == "notepad.exe"
        assert notepad_event.exe_path_hash is not None
        assert notepad_event.window_title_hash is not None
        
        # Check attrs_json
        import json
        attrs = json.loads(notepad_event.attrs_json)
        assert attrs['source'] == 'win32+poll'
        assert 'hwnd' in attrs
        assert 'app_id' in attrs
        
        # Clear events
        self.received_events.clear()
        
        # Launch Explorer
        explorer = self._launch_process("explorer.exe")
        time.sleep(1.0)
        
        # Set Explorer as foreground
        success = self._set_foreground_window_by_process(explorer)
        if not success:
            pytest.skip("Could not set Explorer as foreground window")
        
        time.sleep(1.5)  # Wait for monitor to detect change
        
        # Check that we got a window change event for Explorer
        explorer_events = [e for e in self.received_events 
                          if hasattr(e, 'exe_name') and e.exe_name and 'explorer' in e.exe_name.lower()]
        assert len(explorer_events) >= 1, f"Expected Explorer event, got: {[getattr(e, 'exe_name', 'unknown') for e in self.received_events]}"
        
        explorer_event = explorer_events[0]
        assert explorer_event.monitor == "active_window"
        assert explorer_event.action == "window_change"
        assert explorer_event.subject_type == "window"
        assert explorer_event.pid == explorer.pid
        assert explorer_event.exe_name.lower() == "explorer.exe"
        assert explorer_event.exe_path_hash is not None
        assert explorer_event.window_title_hash is not None
        
        # Verify hashed fields are different (different apps)
        assert notepad_event.exe_path_hash != explorer_event.exe_path_hash
        assert notepad_event.window_title_hash != explorer_event.window_title_hash
        assert notepad_event.subject_id != explorer_event.subject_id
        
        # Switch back to Notepad
        self.received_events.clear()
        self._set_foreground_window_by_process(notepad)
        time.sleep(1.5)
        
        # Should get another event for switching back
        final_events = [e for e in self.received_events 
                       if hasattr(e, 'exe_name') and e.exe_name and 'notepad' in e.exe_name.lower()]
        assert len(final_events) >= 1, "Expected event when switching back to Notepad"
    
    def test_no_events_when_stationary(self):
        """Test that no extra events are generated when window stays the same."""
        # Setup event collection
        self._setup_event_collection()
        
        # Create monitor
        self.monitor = ActiveWindowMonitor(dry_run=False)
        self.monitor.start()
        
        # Wait for initialization
        time.sleep(0.5)
        
        # Launch Notepad and set as foreground
        notepad = self._launch_process("notepad.exe")
        time.sleep(1.0)
        success = self._set_foreground_window_by_process(notepad)
        
        if not success:
            pytest.skip("Could not set Notepad as foreground window")
        
        time.sleep(1.5)  # Wait for initial detection
        
        # Count events so far
        initial_count = len([e for e in self.received_events 
                           if hasattr(e, 'exe_name') and e.exe_name and 'notepad' in e.exe_name.lower()])
        
        assert initial_count >= 1, "Should have initial event"
        
        # Clear events and wait longer with no changes
        self.received_events.clear()
        time.sleep(3.0)  # Wait multiple poll cycles
        
        # Should not have any new events since window didn't change
        new_events = [e for e in self.received_events 
                     if hasattr(e, 'exe_name') and e.exe_name and 'notepad' in e.exe_name.lower()]
        
        assert len(new_events) == 0, f"Expected no new events during idle period, got: {len(new_events)}"
    
    def test_apps_windows_upsert(self):
        """Test that apps and windows dimension tables are updated."""
        # Get database
        db = get_database()
        
        # Clear existing test data  
        with db._get_connection() as conn:
            conn.execute("DELETE FROM windows WHERE id LIKE 'test_%'")
            conn.execute("DELETE FROM apps WHERE id LIKE 'test_%'") 
        
        # Create monitor (dry_run=False to use database)
        self.monitor = ActiveWindowMonitor(dry_run=False)
        self.monitor.start()
        
        # Wait for initialization
        time.sleep(0.5)
        
        # Launch Notepad
        notepad = self._launch_process("notepad.exe")
        time.sleep(1.0)
        success = self._set_foreground_window_by_process(notepad)
        
        if not success:
            pytest.skip("Could not set Notepad as foreground window")
        
        time.sleep(2.0)  # Wait for detection and database update
        
        # Check that app record was created
        with db._get_connection() as conn:
            cursor = conn.execute("SELECT id, exe_name, exe_path_hash FROM apps WHERE exe_name = 'notepad.exe'")
            app_records = cursor.fetchall()
        
        assert len(app_records) >= 1, "App record should be created"
        
        app_record = app_records[0]
        app_id, exe_name, exe_path_hash = app_record
        
        assert exe_name == "notepad.exe"
        assert exe_path_hash is not None
        assert len(exe_path_hash) > 0
        
        # Check that window record was created
        with db._get_connection() as conn:
            cursor = conn.execute("SELECT id, app_id, title_hash FROM windows WHERE app_id = ?", (app_id,))
            window_records = cursor.fetchall()
        
        assert len(window_records) >= 1, "Window record should be created"
        
        window_record = window_records[0]
        window_id, window_app_id, title_hash = window_record
        
        assert window_app_id == app_id
        assert title_hash is not None
        assert len(title_hash) > 0
    
    def test_stable_subject_ids(self):
        """Test that subject IDs are stable across monitor restarts."""
        # Setup event collection
        self._setup_event_collection()
        
        # Create first monitor instance
        monitor1 = ActiveWindowMonitor(dry_run=False)
        monitor1.start()
        time.sleep(0.5)
        
        # Launch Notepad
        notepad = self._launch_process("notepad.exe")
        time.sleep(1.0)
        success = self._set_foreground_window_by_process(notepad)
        
        if not success:
            pytest.skip("Could not set Notepad as foreground window")
        
        time.sleep(1.5)
        
        # Get first event
        notepad_events = [e for e in self.received_events 
                         if hasattr(e, 'exe_name') and e.exe_name and 'notepad' in e.exe_name.lower()]
        assert len(notepad_events) >= 1
        
        first_event = notepad_events[0]
        first_subject_id = first_event.subject_id
        
        # Stop first monitor
        monitor1.stop()
        
        # Clear events and create second monitor instance
        self.received_events.clear()
        monitor2 = ActiveWindowMonitor(dry_run=False)
        monitor2.start()
        time.sleep(0.5)
        
        # Trigger window detection again
        self._set_foreground_window_by_process(notepad)
        time.sleep(1.5)
        
        # Get second event
        second_events = [e for e in self.received_events 
                        if hasattr(e, 'exe_name') and e.exe_name and 'notepad' in e.exe_name.lower()]
        assert len(second_events) >= 1
        
        second_event = second_events[0]
        second_subject_id = second_event.subject_id
        
        # Subject IDs should be the same (stable)
        assert first_subject_id == second_subject_id, "Subject IDs should be stable across monitor restarts"
        
        monitor2.stop()
        self.monitor = None  # Prevent duplicate cleanup