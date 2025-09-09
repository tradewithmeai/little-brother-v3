"""Active window monitoring for Little Brother v3."""

import ctypes
import ctypes.wintypes
import sys
import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional, Set

import psutil

from ..database import get_database
from ..hashutil import hash_str
from ..ids import new_id
from ..logging_setup import get_logger
from .base import MonitorBase

logger = get_logger("active_window")

# Windows constants
WM_WININICHANGE = 0x001A
WINEVENT_OUTOFCONTEXT = 0x0000
WINEVENT_SKIPOWNPROCESS = 0x0002
EVENT_SYSTEM_FOREGROUND = 0x0003

# Windows API types
user32 = ctypes.windll.user32
kernel32 = ctypes.windll.kernel32

# Function prototypes
GetForegroundWindow = user32.GetForegroundWindow
GetForegroundWindow.restype = ctypes.wintypes.HWND

GetWindowTextW = user32.GetWindowTextW
GetWindowTextW.argtypes = [ctypes.wintypes.HWND, ctypes.wintypes.LPWSTR, ctypes.c_int]
GetWindowTextW.restype = ctypes.c_int

GetWindowTextLengthW = user32.GetWindowTextLengthW
GetWindowTextLengthW.argtypes = [ctypes.wintypes.HWND]
GetWindowTextLengthW.restype = ctypes.c_int

GetWindowThreadProcessId = user32.GetWindowThreadProcessId
GetWindowThreadProcessId.argtypes = [ctypes.wintypes.HWND, ctypes.POINTER(ctypes.wintypes.DWORD)]
GetWindowThreadProcessId.restype = ctypes.wintypes.DWORD

# Event hook function prototype
WinEventProc = ctypes.WINFUNCTYPE(
    None,
    ctypes.wintypes.HANDLE,  # hWinEventHook
    ctypes.wintypes.DWORD,   # event
    ctypes.wintypes.HWND,    # hwnd
    ctypes.wintypes.LONG,    # idObject
    ctypes.wintypes.LONG,    # idChild
    ctypes.wintypes.DWORD,   # idEventThread
    ctypes.wintypes.DWORD    # dwmsEventTime
)

# Try to get SetWinEventHook - may not be available on all systems
try:
    SetWinEventHook = ctypes.windll.user32.SetWinEventHookA
    SetWinEventHook.argtypes = [
        ctypes.wintypes.DWORD,      # eventMin
        ctypes.wintypes.DWORD,      # eventMax
        ctypes.wintypes.HMODULE,    # hmodWinEventProc
        WinEventProc,               # lpfnWinEventProc
        ctypes.wintypes.DWORD,      # idProcess
        ctypes.wintypes.DWORD,      # idThread
        ctypes.wintypes.DWORD       # dwFlags
    ]
    SetWinEventHook.restype = ctypes.wintypes.HANDLE

    UnhookWinEvent = ctypes.windll.user32.UnhookWinEvent
    UnhookWinEvent.argtypes = [ctypes.wintypes.HANDLE]
    UnhookWinEvent.restype = ctypes.wintypes.BOOL
    
    HOOKS_AVAILABLE = True
except (AttributeError, OSError):
    SetWinEventHook = None
    UnhookWinEvent = None
    HOOKS_AVAILABLE = False


@dataclass
class WindowInfo:
    """Information about an active window."""
    hwnd: Optional[int]
    title: str
    title_hash: str
    exe_name: Optional[str]
    exe_path: Optional[str]
    exe_path_hash: Optional[str]
    pid: Optional[int]
    app_id: str
    window_id: str
    timestamp: float


class ActiveWindowMonitor(MonitorBase):
    """Monitor active window changes with Win32 event hooks and polling confirmation."""

    def __init__(self, dry_run: bool = False):
        """Initialize active window monitor."""
        super().__init__(dry_run)
        
        # Check if we're on Windows
        if sys.platform != "win32":
            raise RuntimeError("ActiveWindowMonitor only supports Windows")
        
        self._last_window_info: Optional[WindowInfo] = None
        self._hook_handle: Optional[ctypes.wintypes.HANDLE] = None
        self._hook_callback = None
        self._pending_changes: Set[int] = set()  # HWNDs that changed
        self._hook_lock = threading.Lock()
        
        # Window/app ID mappings for stable subject_ids
        self._app_cache: Dict[str, str] = {}  # exe_path_hash -> app_id
        self._window_cache: Dict[tuple, str] = {}  # (exe_path_hash, title_hash) -> window_id
    
    @property
    def name(self) -> str:
        """Monitor name."""
        return "active_window"
    
    @property 
    def poll_interval_s(self) -> float:
        """Poll interval for confirmation."""
        return 1.2  # 1.0-1.5s as specified
    
    def start_monitoring(self) -> None:
        """Start active window monitoring with Win32 hooks."""
        try:
            # Set up Win32 event hook for foreground changes
            self._setup_win32_hook()
            
            # Get initial window state
            self._check_active_window()
            
            self.logger.info("Active window monitoring started with Win32 hooks")
            
        except Exception as e:
            self.logger.error(f"Failed to start active window monitoring: {e}")
            raise
    
    def stop_monitoring(self) -> None:
        """Stop active window monitoring."""
        self._cleanup_win32_hook()
        self.logger.info("Active window monitoring stopped")
    
    def _setup_win32_hook(self) -> None:
        """Set up Win32 event hook for foreground window changes."""
        if not HOOKS_AVAILABLE:
            self.logger.warning("Win32 hooks not available, using polling only")
            return
            
        try:
            # Create callback function
            def win_event_proc(hWinEventHook, event, hwnd, idObject, idChild, idEventThread, dwmsEventTime):
                """Win32 event callback for foreground changes."""
                if event == EVENT_SYSTEM_FOREGROUND and hwnd:
                    with self._hook_lock:
                        self._pending_changes.add(hwnd)
            
            self._hook_callback = WinEventProc(win_event_proc)
            
            # Set up the hook
            self._hook_handle = SetWinEventHook(
                EVENT_SYSTEM_FOREGROUND,  # eventMin
                EVENT_SYSTEM_FOREGROUND,  # eventMax  
                None,                     # hmodWinEventProc
                self._hook_callback,      # lpfnWinEventProc
                0,                        # idProcess (0 = all processes)
                0,                        # idThread (0 = all threads)
                WINEVENT_OUTOFCONTEXT | WINEVENT_SKIPOWNPROCESS
            )
            
            if not self._hook_handle:
                self.logger.warning("Failed to set Win32 event hook, using polling only")
                return
                
            self.logger.debug("Win32 foreground event hook installed")
            
        except Exception as e:
            self.logger.warning(f"Failed to setup Win32 hook, using polling only: {e}")
    
    def _cleanup_win32_hook(self) -> None:
        """Clean up Win32 event hook."""
        if not HOOKS_AVAILABLE or not UnhookWinEvent:
            return
            
        try:
            if self._hook_handle:
                UnhookWinEvent(self._hook_handle)
                self._hook_handle = None
            
            self._hook_callback = None
            
        except Exception as e:
            self.logger.warning(f"Error cleaning up Win32 hook: {e}")
    
    def _run_loop(self) -> None:
        """Override run loop to handle Win32 event confirmation."""
        try:
            self.start_monitoring()
            
            while not self.should_stop():
                # Check for pending changes from Win32 hooks
                pending_hwnds = set()
                with self._hook_lock:
                    pending_hwnds = self._pending_changes.copy()
                    self._pending_changes.clear()
                
                # If we have pending changes or it's time for regular check
                if pending_hwnds or True:  # Always check to confirm current state
                    self._check_active_window()
                
                # Wait for next poll or stop signal
                if self.wait_or_stop(self.poll_interval_s):
                    break
                    
        except Exception as e:
            self.logger.error(f"Error in active window monitor: {e}", exc_info=True)
        finally:
            try:
                self.stop_monitoring()
            except Exception as e:
                self.logger.error(f"Error stopping active window monitor: {e}", exc_info=True)
    
    def _check_active_window(self) -> None:
        """Check current active window and emit event if changed."""
        try:
            current_window = self._get_current_window_info()
            
            if current_window and self._has_window_changed(current_window):
                # Upsert into dimension tables
                self._upsert_app_and_window(current_window)
                
                # Emit window change event
                self._emit_window_change_event(current_window)
                
                self._last_window_info = current_window
                
        except Exception as e:
            self.logger.error(f"Error checking active window: {e}")
    
    def _get_current_window_info(self) -> Optional[WindowInfo]:
        """Get information about the currently active window."""
        try:
            # Get foreground window handle
            hwnd = GetForegroundWindow()
            if not hwnd:
                return None
            
            # Get window title
            title_length = GetWindowTextLengthW(hwnd)
            if title_length <= 0:
                title = ""
            else:
                buffer = ctypes.create_unicode_buffer(title_length + 1)
                GetWindowTextW(hwnd, buffer, title_length + 1)
                title = buffer.value or ""
            
            # Get process ID
            pid_buffer = ctypes.wintypes.DWORD()
            GetWindowThreadProcessId(hwnd, ctypes.byref(pid_buffer))
            pid = pid_buffer.value
            
            # Get process information
            exe_name = None
            exe_path = None
            exe_path_hash = None
            
            try:
                process = psutil.Process(pid)
                exe_name = process.name()
                exe_path = process.exe()
                if exe_path:
                    exe_path_hash = hash_str(exe_path, purpose="exe_path")
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
            
            # Create hashed title
            title_hash = hash_str(title, purpose="window_title") if title else ""
            
            # Get stable app and window IDs
            app_id = self._get_or_create_app_id(exe_path_hash)
            window_id = self._get_or_create_window_id(exe_path_hash, title_hash)
            
            return WindowInfo(
                hwnd=hwnd,
                title=title,
                title_hash=title_hash,
                exe_name=exe_name,
                exe_path=exe_path,
                exe_path_hash=exe_path_hash,
                pid=pid,
                app_id=app_id,
                window_id=window_id,
                timestamp=time.time()
            )
            
        except Exception as e:
            self.logger.debug(f"Error getting window info: {e}")
            return None
    
    def _has_window_changed(self, current: WindowInfo) -> bool:
        """Check if the active window has actually changed."""
        if not self._last_window_info:
            return True
        
        # Compare key identifying fields
        last = self._last_window_info
        return (
            current.hwnd != last.hwnd or
            current.title_hash != last.title_hash or 
            current.exe_path_hash != last.exe_path_hash or
            current.pid != last.pid
        )
    
    def _get_or_create_app_id(self, exe_path_hash: Optional[str]) -> str:
        """Get or create stable app ID for an executable path hash."""
        if not exe_path_hash:
            return new_id()  # Fallback for unknown apps
        
        # Check cache first
        if exe_path_hash in self._app_cache:
            return self._app_cache[exe_path_hash]
        
        # Check database
        db = get_database()
        with db._get_connection() as conn:
            cursor = conn.execute("SELECT id FROM apps WHERE exe_path_hash = ?", (exe_path_hash,))
            result = cursor.fetchall()
        
        if result:
            app_id = result[0][0]
        else:
            app_id = new_id()
        
        # Cache the result
        self._app_cache[exe_path_hash] = app_id
        return app_id
    
    def _get_or_create_window_id(self, exe_path_hash: Optional[str], title_hash: str) -> str:
        """Get or create stable window ID for a window (exe_path_hash, title_hash) pair."""
        cache_key = (exe_path_hash or "", title_hash)
        
        # Check cache first
        if cache_key in self._window_cache:
            return self._window_cache[cache_key]
        
        # Check database
        db = get_database()
        
        # First find the app_id
        app_id = self._get_or_create_app_id(exe_path_hash)
        
        # Look for existing window
        with db._get_connection() as conn:
            cursor = conn.execute("SELECT id FROM windows WHERE app_id = ? AND title_hash = ?", (app_id, title_hash))
            result = cursor.fetchall()
        
        if result:
            window_id = result[0][0]
        else:
            window_id = new_id()
        
        # Cache the result
        self._window_cache[cache_key] = window_id
        return window_id
    
    def _upsert_app_and_window(self, window_info: WindowInfo) -> None:
        """Upsert app and window records into dimension tables."""
        try:
            db = get_database()
            current_time = int(time.time() * 1000)  # milliseconds
            
            # Upsert records using database connection
            with db._get_connection() as conn:
                # Upsert app record
                if window_info.exe_path_hash:
                    conn.execute("""
                        INSERT INTO apps (id, exe_name, exe_path_hash, first_seen_utc, last_seen_utc)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(id) DO UPDATE SET
                            exe_name = COALESCE(excluded.exe_name, apps.exe_name),
                            last_seen_utc = excluded.last_seen_utc
                    """, (
                        window_info.app_id,
                        window_info.exe_name,
                        window_info.exe_path_hash,
                        current_time,
                        current_time
                    ))
                
                # Upsert window record
                conn.execute("""
                    INSERT INTO windows (id, app_id, title_hash, first_seen_utc, last_seen_utc)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        last_seen_utc = excluded.last_seen_utc
                """, (
                    window_info.window_id,
                    window_info.app_id,
                    window_info.title_hash,
                    current_time,
                    current_time
                ))
                
                # Ensure changes are committed
                conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error upserting app/window records: {e}")
    
    def _emit_window_change_event(self, window_info: WindowInfo) -> None:
        """Emit active window change event."""
        try:
            event_data = {
                'action': 'window_change',
                'subject_type': 'window',
                'subject_id': window_info.window_id,
                'pid': window_info.pid,
                'exe_name': window_info.exe_name,
                'exe_path_hash': window_info.exe_path_hash,
                'window_title_hash': window_info.title_hash,
                'attrs': {
                    'source': 'win32+poll',
                    'hwnd': window_info.hwnd,
                    'app_id': window_info.app_id
                }
            }
            
            self.emit(event_data)
            self.logger.debug(f"Emitted window change event for: {window_info.exe_name or 'unknown'}")
            
        except Exception as e:
            self.logger.error(f"Error emitting window change event: {e}")


# Legacy BaseMonitor-based class for backward compatibility
class LegacyActiveWindowMonitor:
    """Legacy active window monitor using BaseMonitor (preserved for reference)."""
    pass