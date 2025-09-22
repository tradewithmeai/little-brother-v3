"""Browser activity monitor for Little Brother v3."""

from pathlib import Path
from typing import Any, Optional

from ..config import get_effective_config
from ..hashutil import hash_str
from ..logging_setup import get_logger
from ..utils.scheduler import Scheduler
from .base import BatchConfig, MonitorBase

logger = get_logger("browser")


class BrowserMonitor(MonitorBase):
    """Browser activity monitor with CDP and fallback modes."""

    def __init__(
        self,
        dry_run: bool = False,
        batch_config: Optional[BatchConfig] = None,
        scheduler: Optional[Scheduler] = None,
    ):
        """Initialize browser monitor.

        Args:
            dry_run: Print events instead of emitting
            batch_config: Override batch configuration
            scheduler: Scheduler for deterministic timing
        """
        super().__init__(dry_run, scheduler)

        # Browser monitor defaults: flush every 2.0s from config
        if batch_config:
            self.batch_config = batch_config
        else:
            config = get_effective_config()
            # Parse browser poll interval
            browser_interval = config.heartbeat.poll_intervals.browser
            interval_seconds = self._parse_interval(browser_interval)
            self.batch_config = BatchConfig(max_size=50, max_time_s=interval_seconds)

        # CDP plugin
        self._cdp_plugin = None
        self._try_load_cdp_plugin()

        # Fallback mode state
        self._last_active_window = None
        self._known_browser_exes = {
            "chrome.exe",
            "msedge.exe",
            "brave.exe",
            "firefox.exe",
            "opera.exe",
            "vivaldi.exe",
            "safari.exe",
            "iexplore.exe",
        }

        # Track recent window titles to avoid spam
        self._recent_titles: dict[str, float] = {}
        self._title_dedupe_window = 5.0  # 5 seconds

    @property
    def name(self) -> str:
        """Monitor name."""
        return "browser"

    def _parse_interval(self, interval_str: str) -> float:
        """Parse interval string like '2.0s' to float seconds."""
        try:
            if interval_str.endswith("s"):
                return float(interval_str[:-1])
            else:
                return float(interval_str)
        except ValueError:
            return 2.0  # Default fallback

    def _try_load_cdp_plugin(self) -> None:
        """Try to load and initialize CDP plugin."""
        try:
            config = get_effective_config()

            # Check if CDP is enabled
            if (
                config.browser.integration.chrome_remote_debug_port > 0
                and "browser_cdp" in config.plugins.enabled
            ):
                from ..plugins.browser_cdp import BrowserCDPPlugin

                self._cdp_plugin = BrowserCDPPlugin(
                    dry_run=self.dry_run, scheduler=self.scheduler
                )

                if self._cdp_plugin.is_available():
                    logger.info("CDP plugin loaded and available")
                else:
                    logger.info(
                        "CDP plugin loaded but not available (Chrome not running with debug port)"
                    )
                    self._cdp_plugin = None
            else:
                logger.info("CDP plugin disabled by configuration")

        except Exception as e:
            logger.warning(f"Failed to load CDP plugin: {e}")
            self._cdp_plugin = None

    def start_monitoring(self) -> None:
        """Start browser monitoring."""
        # Try CDP mode first
        if self._cdp_plugin:
            try:
                self._cdp_plugin.start()
                if self._cdp_plugin.is_available():
                    logger.info("Browser monitoring started in CDP mode")
                    return
            except Exception as e:
                logger.warning(f"CDP plugin failed to start: {e}")
                self._cdp_plugin = None

        # Fall back to window-based monitoring
        logger.info("Browser monitoring started in fallback mode")

    def stop_monitoring(self) -> None:
        """Stop browser monitoring."""
        if self._cdp_plugin:
            try:
                self._cdp_plugin.stop()
                logger.info("CDP plugin stopped")
            except Exception as e:
                logger.warning(f"Error stopping CDP plugin: {e}")

        logger.info("Browser monitoring stopped")

    def run_monitor_cycle(self) -> None:
        """Run one monitoring cycle."""
        # If CDP is active, collect its events
        if self._cdp_plugin and self._cdp_plugin._running:
            self._collect_cdp_events()
        else:
            # Run fallback monitoring
            self._check_active_window()

    def _collect_cdp_events(self) -> None:
        """Collect events from CDP plugin."""
        try:
            # CDP plugin handles its own event emission through MonitorBase
            # We just need to run its monitor cycle
            if hasattr(self._cdp_plugin, "run_monitor_cycle"):
                self._cdp_plugin.run_monitor_cycle()
        except Exception as e:
            logger.debug(f"Error collecting CDP events: {e}")

    def _check_active_window(self) -> None:
        """Check active window for browser activity (fallback mode)."""
        try:
            # Get current active window info
            current_window = self._get_active_window_info()

            if not current_window:
                return

            exe_name = current_window.get("exe_name", "").lower()
            window_title = current_window.get("window_title", "")

            # Check if it's a browser window
            if exe_name not in self._known_browser_exes:
                return

            # Check if window info changed
            window_key = f"{exe_name}:{window_title}"
            if self._last_active_window == window_key:
                return

            # Check title deduplication
            current_time = self.scheduler.now()
            title_hash = hash_str(window_title, "window_title")

            if title_hash in self._recent_titles:
                last_seen = self._recent_titles[title_hash]
                if current_time - last_seen < self._title_dedupe_window:
                    return  # Skip duplicate within window

            # Update state
            self._last_active_window = window_key
            self._recent_titles[title_hash] = current_time

            # Emit conservative browser event
            event_data = {
                "action": "tab_switch",  # Conservative action name
                "subject_type": "url",
                "subject_id": None,  # No URL in fallback mode
                "attrs": {
                    "source": "fallback",
                    "exe_name": exe_name,
                    "window_title_present": bool(window_title.strip()),
                },
            }

            # Set window_title_hash in event
            if window_title.strip():
                self._set_event_window_title_hash(event_data, window_title)

            self.emit(event_data)
            logger.debug(f"Browser window change: {exe_name}")

        except Exception as e:
            logger.debug(f"Error checking active window: {e}")

    def _get_active_window_info(self) -> Optional[dict[str, Any]]:
        """Get active window information."""
        try:
            # Import platform-specific window detection
            import sys

            if sys.platform == "win32":
                return self._get_active_window_win32()
            else:
                logger.debug("Active window detection only supported on Windows")
                return None

        except Exception as e:
            logger.debug(f"Error getting active window: {e}")
            return None

    def _get_active_window_win32(self) -> Optional[dict[str, Any]]:
        """Get active window info on Windows."""
        try:
            import ctypes
            from ctypes import windll, wintypes

            # Get foreground window
            hwnd = windll.user32.GetForegroundWindow()
            if not hwnd:
                return None

            # Get window title
            title_length = windll.user32.GetWindowTextLengthW(hwnd)
            if title_length == 0:
                return None

            title_buffer = ctypes.create_unicode_buffer(title_length + 1)
            windll.user32.GetWindowTextW(hwnd, title_buffer, title_length + 1)
            window_title = title_buffer.value

            # Get process ID and executable name
            process_id = wintypes.DWORD()
            windll.user32.GetWindowThreadProcessId(hwnd, ctypes.byref(process_id))

            # Get process handle
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            process_handle = windll.kernel32.OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION, False, process_id.value
            )

            if not process_handle:
                return None

            try:
                # Get executable name
                exe_name_buffer = ctypes.create_unicode_buffer(512)
                buffer_size = wintypes.DWORD(512)

                success = windll.kernel32.QueryFullProcessImageNameW(
                    process_handle, 0, exe_name_buffer, ctypes.byref(buffer_size)
                )

                if success:
                    exe_path = exe_name_buffer.value
                    exe_name = Path(exe_path).name if exe_path else ""
                else:
                    exe_name = ""

                return {
                    "window_title": window_title,
                    "exe_name": exe_name,
                    "pid": process_id.value,
                }

            finally:
                windll.kernel32.CloseHandle(process_handle)

        except Exception as e:
            logger.debug(f"Error getting Win32 active window: {e}")
            return None

    def _set_event_window_title_hash(
        self, event_data: dict[str, Any], title: str
    ) -> None:
        """Set window title hash in event data (placeholder for MonitorBase integration)."""
        # This would be handled by MonitorBase when emitting the event
        # For now, we'll add it to attrs
        title_hash = hash_str(title, "window_title")
        event_data["attrs"]["window_title_hash"] = title_hash
