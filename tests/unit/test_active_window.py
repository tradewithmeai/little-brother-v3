"""Unit tests for active window monitor."""

import sys
import time
from unittest import skipUnless
from unittest.mock import MagicMock, patch

import pytest

from lb3.monitors.active_window import ActiveWindowMonitor, WindowInfo


@skipUnless(sys.platform == "win32", "ActiveWindowMonitor only supports Windows")
class TestActiveWindowMonitor:
    """Test ActiveWindowMonitor functionality."""
    
    @patch('lb3.monitors.active_window.get_database')
    def test_monitor_initialization(self, mock_get_database):
        """Test monitor initialization."""
        monitor = ActiveWindowMonitor(dry_run=True)
        
        assert monitor.name == "active_window"
        assert monitor.dry_run is True
        assert monitor.poll_interval_s == 1.2
        assert monitor._last_window_info is None
        assert monitor._hook_handle is None
        assert monitor._app_cache == {}
        assert monitor._window_cache == {}
    
    def test_monitor_initialization_non_windows(self):
        """Test that monitor raises error on non-Windows platforms."""
        with patch('sys.platform', 'linux'):
            with pytest.raises(RuntimeError, match="only supports Windows"):
                ActiveWindowMonitor()
    
    @patch('lb3.monitors.active_window.get_database')
    @patch('lb3.monitors.active_window.GetForegroundWindow')
    @patch('lb3.monitors.active_window.GetWindowTextLengthW')
    @patch('lb3.monitors.active_window.GetWindowTextW')
    @patch('lb3.monitors.active_window.GetWindowThreadProcessId')
    @patch('lb3.monitors.active_window.psutil.Process')
    @patch('lb3.monitors.active_window.hash_str')
    @patch('lb3.monitors.active_window.new_id')
    @patch('ctypes.byref')
    @patch('ctypes.wintypes.DWORD')
    def test_get_current_window_info(self, mock_dword_class, mock_byref, mock_new_id, mock_hash_str, mock_process_class,
                                   mock_get_thread_pid, mock_get_window_text, 
                                   mock_get_text_length, mock_get_foreground, mock_get_database):
        """Test getting current window information."""
        # Mock ctypes objects
        mock_pid_buffer = MagicMock()
        mock_pid_buffer.value = 1234
        mock_dword_class.return_value = mock_pid_buffer
        mock_byref.return_value = mock_pid_buffer
        
        # Mock ctypes functions
        mock_get_foreground.return_value = 123456  # Mock HWND
        mock_get_text_length.return_value = 10
        
        # Mock GetWindowTextW by setting buffer value
        def mock_text_func(hwnd, buffer, length):
            buffer.value = "Test Window"
            return 11
        mock_get_window_text.side_effect = mock_text_func
        
        # Mock GetWindowThreadProcessId
        def mock_pid_func(hwnd, pid_buffer):
            # Mock the DWORD object with a value attribute
            pid_buffer.value = 1234
            return 0
        mock_get_thread_pid.side_effect = mock_pid_func
        
        # Mock psutil.Process
        mock_process = MagicMock()
        mock_process.name.return_value = "notepad.exe"
        mock_process.exe.return_value = "C:\\Windows\\notepad.exe"
        mock_process_class.return_value = mock_process
        
        # Mock hash_str
        mock_hash_str.side_effect = lambda text, purpose: f"hash_{purpose}_{text}"
        
        # Mock new_id
        mock_new_id.side_effect = ["app_id_123", "window_id_456"]
        
        # Mock database
        mock_db = MagicMock()
        mock_db.execute_query.return_value = None  # No existing records
        mock_get_database.return_value = mock_db
        
        monitor = ActiveWindowMonitor(dry_run=True)
        window_info = monitor._get_current_window_info()
        
        assert window_info is not None
        assert window_info.hwnd == 123456
        assert window_info.title == "Test Window"
        assert window_info.title_hash == "hash_window_title_Test Window"
        assert window_info.exe_name == "notepad.exe"
        assert window_info.exe_path == "C:\\Windows\\notepad.exe"
        assert window_info.exe_path_hash == "hash_exe_path_C:\\Windows\\notepad.exe"
        assert window_info.pid == 1234
        assert window_info.app_id == "app_id_123"
        assert window_info.window_id == "window_id_456"
    
    @patch('lb3.monitors.active_window.get_database')
    @patch('lb3.monitors.active_window.GetForegroundWindow')
    def test_get_current_window_info_no_window(self, mock_get_foreground, mock_get_database):
        """Test getting window info when no foreground window."""
        mock_get_foreground.return_value = 0  # No window
        
        monitor = ActiveWindowMonitor(dry_run=True)
        window_info = monitor._get_current_window_info()
        
        assert window_info is None
    
    def test_has_window_changed_no_previous(self):
        """Test window change detection with no previous window."""
        monitor = ActiveWindowMonitor(dry_run=True)
        
        current = WindowInfo(
            hwnd=123, title="Test", title_hash="hash1", 
            exe_name="test.exe", exe_path="/test", exe_path_hash="hash2",
            pid=1234, app_id="app1", window_id="win1", timestamp=time.time()
        )
        
        assert monitor._has_window_changed(current) is True
    
    def test_has_window_changed_same_window(self):
        """Test window change detection with same window."""
        monitor = ActiveWindowMonitor(dry_run=True)
        
        window_info = WindowInfo(
            hwnd=123, title="Test", title_hash="hash1", 
            exe_name="test.exe", exe_path="/test", exe_path_hash="hash2",
            pid=1234, app_id="app1", window_id="win1", timestamp=time.time()
        )
        
        monitor._last_window_info = window_info
        
        # Same window info
        current = WindowInfo(
            hwnd=123, title="Test", title_hash="hash1", 
            exe_name="test.exe", exe_path="/test", exe_path_hash="hash2",
            pid=1234, app_id="app1", window_id="win1", timestamp=time.time()
        )
        
        assert monitor._has_window_changed(current) is False
    
    def test_has_window_changed_different_window(self):
        """Test window change detection with different window."""
        monitor = ActiveWindowMonitor(dry_run=True)
        
        monitor._last_window_info = WindowInfo(
            hwnd=123, title="Test1", title_hash="hash1", 
            exe_name="test1.exe", exe_path="/test1", exe_path_hash="hash2",
            pid=1234, app_id="app1", window_id="win1", timestamp=time.time()
        )
        
        # Different window
        current = WindowInfo(
            hwnd=456, title="Test2", title_hash="hash3", 
            exe_name="test2.exe", exe_path="/test2", exe_path_hash="hash4",
            pid=5678, app_id="app2", window_id="win2", timestamp=time.time()
        )
        
        assert monitor._has_window_changed(current) is True
    
    @patch('lb3.monitors.active_window.get_database')
    @patch('lb3.monitors.active_window.new_id')
    def test_get_or_create_app_id_new(self, mock_new_id, mock_get_database):
        """Test creating new app ID."""
        mock_new_id.return_value = "new_app_id"
        
        # Mock database with no existing record
        mock_db = MagicMock()
        mock_db.execute_query.return_value = None
        mock_get_database.return_value = mock_db
        
        monitor = ActiveWindowMonitor(dry_run=True)
        app_id = monitor._get_or_create_app_id("test_hash")
        
        assert app_id == "new_app_id"
        assert monitor._app_cache["test_hash"] == "new_app_id"
        
        mock_db.execute_query.assert_called_once_with(
            "SELECT id FROM apps WHERE exe_path_hash = ?",
            ("test_hash",)
        )
    
    @patch('lb3.monitors.active_window.get_database')
    def test_get_or_create_app_id_existing(self, mock_get_database):
        """Test getting existing app ID."""
        # Mock database with existing record
        mock_db = MagicMock()
        mock_db.execute_query.return_value = [("existing_app_id",)]
        mock_get_database.return_value = mock_db
        
        monitor = ActiveWindowMonitor(dry_run=True)
        app_id = monitor._get_or_create_app_id("test_hash")
        
        assert app_id == "existing_app_id"
        assert monitor._app_cache["test_hash"] == "existing_app_id"
    
    @patch('lb3.monitors.active_window.get_database')
    def test_get_or_create_app_id_cached(self, mock_get_database):
        """Test getting cached app ID."""
        monitor = ActiveWindowMonitor(dry_run=True)
        monitor._app_cache["test_hash"] = "cached_app_id"
        
        app_id = monitor._get_or_create_app_id("test_hash")
        
        assert app_id == "cached_app_id"
        # Database should not be queried for cached values
        mock_get_database.assert_not_called()
    
    @patch('lb3.monitors.active_window.get_database')
    @patch('lb3.monitors.active_window.new_id')
    def test_get_or_create_window_id(self, mock_new_id, mock_get_database):
        """Test creating window ID."""
        mock_new_id.side_effect = ["new_app_id", "new_window_id"]
        
        # Mock database with no existing records
        mock_db = MagicMock()
        mock_db.execute_query.return_value = None
        mock_get_database.return_value = mock_db
        
        monitor = ActiveWindowMonitor(dry_run=True)
        window_id = monitor._get_or_create_window_id("exe_hash", "title_hash")
        
        assert window_id == "new_window_id"
        assert monitor._window_cache[("exe_hash", "title_hash")] == "new_window_id"
        assert monitor._app_cache["exe_hash"] == "new_app_id"
    
    @patch('lb3.monitors.active_window.get_database')
    @patch('lb3.monitors.active_window.time.time')
    def test_upsert_app_and_window(self, mock_time, mock_get_database):
        """Test upserting app and window records."""
        mock_time.return_value = 1234567890.5  # Mock timestamp
        
        mock_db = MagicMock()
        mock_get_database.return_value = mock_db
        
        window_info = WindowInfo(
            hwnd=123, title="Test", title_hash="title_hash", 
            exe_name="test.exe", exe_path="/test", exe_path_hash="exe_hash",
            pid=1234, app_id="app_id", window_id="window_id", timestamp=time.time()
        )
        
        monitor = ActiveWindowMonitor(dry_run=True)
        monitor._upsert_app_and_window(window_info)
        
        # Check that both upsert queries were called
        assert mock_db.execute_query.call_count == 2
        
        # Check app upsert
        app_call = mock_db.execute_query.call_args_list[0]
        assert "INSERT INTO apps" in app_call[0][0]
        assert app_call[0][1] == ("app_id", "test.exe", "exe_hash", 1234567890500, 1234567890500)
        
        # Check window upsert
        window_call = mock_db.execute_query.call_args_list[1]
        assert "INSERT INTO windows" in window_call[0][0]
        assert window_call[0][1] == ("window_id", "app_id", "title_hash", 1234567890500, 1234567890500)
    
    @patch('lb3.monitors.active_window.get_database')
    def test_emit_window_change_event(self, mock_get_database):
        """Test emitting window change event."""
        monitor = ActiveWindowMonitor(dry_run=True)
        
        # Mock emit method
        emitted_events = []
        def mock_emit(event_data):
            emitted_events.append(event_data)
        monitor.emit = mock_emit
        
        window_info = WindowInfo(
            hwnd=123, title="Test", title_hash="title_hash", 
            exe_name="test.exe", exe_path="/test", exe_path_hash="exe_hash",
            pid=1234, app_id="app_id", window_id="window_id", timestamp=time.time()
        )
        
        monitor._emit_window_change_event(window_info)
        
        assert len(emitted_events) == 1
        event = emitted_events[0]
        
        assert event['action'] == 'window_change'
        assert event['subject_type'] == 'window'
        assert event['subject_id'] == 'window_id'
        assert event['pid'] == 1234
        assert event['exe_name'] == 'test.exe'
        assert event['exe_path_hash'] == 'exe_hash'
        assert event['window_title_hash'] == 'title_hash'
        # Check attrs_json structure matches spec: {"source":"win32+poll","hwnd":<int or null>}
        assert event['attrs']['source'] == 'win32+poll'
        assert event['attrs']['hwnd'] == 123  # hwnd from window_info
        assert event['attrs']['app_id'] == 'app_id'
    
    @patch('lb3.monitors.active_window.get_database')
    @patch('lb3.monitors.active_window.HOOKS_AVAILABLE', True)
    @patch('lb3.monitors.active_window.SetWinEventHook')
    def test_setup_win32_hook(self, mock_set_hook, mock_get_database):
        """Test setting up Win32 event hook."""
        mock_set_hook.return_value = 12345  # Mock hook handle
        
        monitor = ActiveWindowMonitor(dry_run=True)
        monitor._setup_win32_hook()
        
        assert monitor._hook_handle == 12345
        assert monitor._hook_callback is not None
        
        # Verify hook was set with correct parameters
        mock_set_hook.assert_called_once()
        args = mock_set_hook.call_args[0]
        assert args[0] == 0x0003  # EVENT_SYSTEM_FOREGROUND
        assert args[1] == 0x0003  # EVENT_SYSTEM_FOREGROUND
    
    @patch('lb3.monitors.active_window.get_database')
    @patch('lb3.monitors.active_window.HOOKS_AVAILABLE', True)
    @patch('lb3.monitors.active_window.UnhookWinEvent')
    def test_cleanup_win32_hook(self, mock_unhook, mock_get_database):
        """Test cleaning up Win32 event hook."""
        monitor = ActiveWindowMonitor(dry_run=True)
        monitor._hook_handle = 12345
        monitor._hook_callback = MagicMock()
        
        monitor._cleanup_win32_hook()
        
        mock_unhook.assert_called_once_with(12345)
        assert monitor._hook_handle is None
        assert monitor._hook_callback is None
    
    @patch('lb3.monitors.active_window.get_database')
    @patch('lb3.monitors.active_window.hash_str')
    def test_hashing_is_invoked(self, mock_hash_str, mock_get_database):
        """Test that hashing is invoked for sensitive data."""
        mock_hash_str.side_effect = lambda text, purpose: f"hashed_{purpose}_{len(text)}"
        
        # Mock other dependencies
        with patch('lb3.monitors.active_window.GetForegroundWindow', return_value=123), \
             patch('lb3.monitors.active_window.GetWindowTextLengthW', return_value=5), \
             patch('lb3.monitors.active_window.GetWindowTextW') as mock_get_text, \
             patch('lb3.monitors.active_window.GetWindowThreadProcessId') as mock_get_pid, \
             patch('lb3.monitors.active_window.psutil.Process') as mock_process, \
             patch('lb3.monitors.active_window.new_id', side_effect=["app_id", "window_id"]), \
             patch('ctypes.wintypes.DWORD') as mock_dword, \
             patch('ctypes.byref') as mock_byref:
            
            # Mock ctypes objects
            mock_pid_buffer = MagicMock()
            mock_pid_buffer.value = 1234
            mock_dword.return_value = mock_pid_buffer
            mock_byref.return_value = mock_pid_buffer
            
            # Mock GetWindowTextW
            def mock_text_func(hwnd, buffer, length):
                buffer.value = "Title"
                return 5
            mock_get_text.side_effect = mock_text_func
            
            # Mock GetWindowThreadProcessId
            def mock_pid_func(hwnd, pid_buffer):
                pid_buffer.value = 1234
                return 0
            mock_get_pid.side_effect = mock_pid_func
            
            # Mock psutil.Process
            mock_proc = MagicMock()
            mock_proc.name.return_value = "test.exe"
            mock_proc.exe.return_value = "/path/to/test.exe"
            mock_process.return_value = mock_proc
            
            # Mock database
            mock_db = MagicMock()
            mock_db.execute_query.return_value = None
            mock_get_database.return_value = mock_db
            
            monitor = ActiveWindowMonitor(dry_run=True)
            window_info = monitor._get_current_window_info()
            
            # Verify hashing was called for both purposes
            hash_calls = mock_hash_str.call_args_list
            purposes = [call[1]['purpose'] for call in hash_calls]
            
            assert 'exe_path' in purposes
            assert 'window_title' in purposes
            
            # Verify hash results are used
            assert window_info.exe_path_hash == "hashed_exe_path_17"  # len("/path/to/test.exe")
            assert window_info.title_hash == "hashed_window_title_5"  # len("Title")


class TestWindowInfo:
    """Test WindowInfo dataclass."""
    
    def test_window_info_creation(self):
        """Test creating WindowInfo object."""
        info = WindowInfo(
            hwnd=123,
            title="Test Window",
            title_hash="hashed_title",
            exe_name="test.exe",
            exe_path="/path/test.exe",
            exe_path_hash="hashed_path",
            pid=1234,
            app_id="app_123",
            window_id="window_456",
            timestamp=time.time()
        )
        
        assert info.hwnd == 123
        assert info.title == "Test Window"
        assert info.title_hash == "hashed_title"
        assert info.exe_name == "test.exe"
        assert info.exe_path == "/path/test.exe"
        assert info.exe_path_hash == "hashed_path"
        assert info.pid == 1234
        assert info.app_id == "app_123"
        assert info.window_id == "window_456"