"""Monitor supervisor for lifecycle management."""

import signal
import sys
import time

from .events import SpoolerSink, get_event_bus
from .logging_setup import get_logger

logger = get_logger("supervisor")


class MonitorSupervisor:
    """Lightweight supervisor for monitor lifecycle management."""

    def __init__(self, dry_run: bool = False, verbose: bool = False):
        """Initialize supervisor.

        Args:
            dry_run: If True, monitors print events instead of writing files
            verbose: If True, print detailed status messages
        """
        self.dry_run = dry_run
        self.verbose = verbose
        self._monitors: list = []
        self._running = False
        self._shutdown_requested = False
        self._event_bus = None
        self._spooler_sink = None

        # Track monitor status
        self._monitor_status: dict[str, dict] = {}

        # Set up signal handlers for graceful shutdown
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Set up signal handlers for graceful shutdown."""

        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            if not self.verbose:
                print("\nShutting down gracefully...")
            self.request_shutdown()

        # Handle SIGINT (Ctrl+C) and SIGTERM
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Windows-specific: handle console control events
        if sys.platform == "win32":
            try:
                import ctypes
                from ctypes import wintypes

                def console_handler(ctrl_type):
                    if ctrl_type in (0, 2):  # CTRL_C_EVENT, CTRL_CLOSE_EVENT
                        logger.info(
                            f"Console control event {ctrl_type}, shutting down..."
                        )
                        if not self.verbose:
                            print("\nShutting down gracefully...")
                        self.request_shutdown()
                        return True
                    return False

                # Register console control handler
                kernel32 = ctypes.windll.kernel32
                handler_type = ctypes.WINFUNCTYPE(ctypes.c_bool, wintypes.DWORD)
                handler = handler_type(console_handler)
                kernel32.SetConsoleCtrlHandler(handler, True)

            except Exception as e:
                logger.debug(f"Could not set up Windows console handler: {e}")

    def add_monitor(self, monitor_class, monitor_name: str, **kwargs):
        """Add a monitor to be managed by the supervisor.

        Args:
            monitor_class: Monitor class to instantiate
            monitor_name: Name for the monitor
            **kwargs: Additional arguments for monitor initialization
        """
        try:
            monitor = monitor_class(dry_run=self.dry_run, **kwargs)
            self._monitors.append(monitor)

            self._monitor_status[monitor_name] = {
                "name": monitor_name,
                "monitor": monitor,
                "started": False,
                "error": None,
            }

            if self.verbose:
                logger.info(f"Added {monitor_name} monitor")

        except Exception as e:
            error_msg = f"Failed to create {monitor_name} monitor: {e}"
            logger.warning(error_msg)
            self._monitor_status[monitor_name] = {
                "name": monitor_name,
                "monitor": None,
                "started": False,
                "error": str(e),
            }

    def start_all(self) -> dict[str, bool]:
        """Start all monitors.

        Returns:
            Dict mapping monitor names to success status
        """
        if self._running:
            raise RuntimeError("Supervisor already running")

        self._running = True
        results = {}

        # Start event bus if not in dry-run mode
        if not self.dry_run:
            try:
                self._event_bus = get_event_bus()
                self._event_bus.start()

                self._spooler_sink = SpoolerSink()
                self._event_bus.subscribe(self._spooler_sink)

                if self.verbose:
                    logger.info("Started event bus and spooler sink")
            except Exception as e:
                logger.error(f"Failed to start event bus: {e}")
                self._running = False
                raise

        # Start all monitors
        started_count = 0
        for monitor_name, status in self._monitor_status.items():
            if status["monitor"] is None:
                results[monitor_name] = False
                if not self.verbose:
                    print(f"[WARN] {monitor_name}: {status['error']}")
                continue

            try:
                status["monitor"].start()
                status["started"] = True
                status["error"] = None
                results[monitor_name] = True
                started_count += 1

                if self.verbose:
                    logger.info(f"Started {monitor_name} monitor")

            except Exception as e:
                error_msg = f"Failed to start {monitor_name}: {e}"
                logger.warning(error_msg)
                status["error"] = str(e)
                results[monitor_name] = False

                if not self.verbose:
                    print(f"[WARN] {monitor_name}: {error_msg}")

        if not self.verbose:
            print(f"Started {started_count}/{len(self._monitor_status)} monitors")

        return results

    def stop_all(self, timeout_seconds: float = 10.0):
        """Stop all monitors gracefully.

        Args:
            timeout_seconds: Maximum time to wait for monitors to stop
        """
        if not self._running:
            return

        if self.verbose:
            logger.info("Stopping all monitors...")

        # Stop monitors in reverse order
        for monitor_name, status in reversed(list(self._monitor_status.items())):
            if status["started"] and status["monitor"]:
                try:
                    status["monitor"].stop()
                    status["started"] = False

                    if self.verbose:
                        logger.info(f"Stopped {monitor_name} monitor")

                except Exception as e:
                    logger.warning(f"Error stopping {monitor_name}: {e}")

        # Wait for monitor threads to finish with timeout
        self._wait_for_threads(timeout_seconds)

        # Stop event system
        if self._spooler_sink:
            try:
                self._spooler_sink.close()
                if self.verbose:
                    logger.info("Closed spooler sink")
            except Exception as e:
                logger.warning(f"Error closing spooler sink: {e}")

        if self._event_bus:
            try:
                self._event_bus.stop()
                if self.verbose:
                    logger.info("Stopped event bus")
            except Exception as e:
                logger.warning(f"Error stopping event bus: {e}")

        self._running = False

        if not self.verbose:
            print("Monitoring stopped")

    def _wait_for_threads(self, timeout_seconds: float):
        """Wait for monitor threads to finish with bounded timeout."""
        if self.verbose:
            logger.info(f"Waiting up to {timeout_seconds}s for threads to finish...")

        start_time = time.time()
        for monitor_name, status in self._monitor_status.items():
            if status["monitor"] and hasattr(status["monitor"], "_thread"):
                thread = status["monitor"]._thread
                if thread and thread.is_alive():
                    remaining_time = timeout_seconds - (time.time() - start_time)
                    if remaining_time > 0:
                        thread.join(remaining_time)
                        if thread.is_alive():
                            logger.warning(
                                f"{monitor_name} thread did not stop within timeout"
                            )
                    else:
                        logger.warning(
                            f"Timeout exceeded while waiting for {monitor_name}"
                        )

    def request_shutdown(self):
        """Request graceful shutdown."""
        self._shutdown_requested = True

    def is_shutdown_requested(self) -> bool:
        """Check if shutdown has been requested."""
        return self._shutdown_requested

    def wait_until_shutdown(self, check_interval: float = 0.5):
        """Wait until shutdown is requested.

        Args:
            check_interval: How often to check for shutdown request
        """
        while not self._shutdown_requested and self._running:
            # In dry-run mode, check if heartbeat monitor has finished (indicating duration elapsed)
            if self.dry_run:
                heartbeat_status = self._monitor_status.get("heartbeat")
                if (
                    heartbeat_status
                    and heartbeat_status["started"]
                    and heartbeat_status["monitor"]
                    and hasattr(heartbeat_status["monitor"], "_running")
                ):
                    # If heartbeat monitor has stopped, consider this natural completion
                    if not heartbeat_status["monitor"]._running:
                        logger.info(
                            "Heartbeat monitor finished, shutting down naturally"
                        )
                        break
            else:
                # In production mode, check if all monitors have finished naturally
                any_running = False
                for status in self._monitor_status.values():
                    if status["started"] and status["monitor"]:
                        if (
                            hasattr(status["monitor"], "_running")
                            and status["monitor"]._running
                        ):
                            any_running = True
                            break

                if not any_running:
                    # All monitors have finished naturally
                    break

            time.sleep(check_interval)

    def get_monitor_status(self) -> dict[str, dict]:
        """Get current status of all monitors."""
        return dict(self._monitor_status)

    def is_running(self) -> bool:
        """Check if supervisor is running."""
        return self._running


def create_standard_supervisor(
    dry_run: bool = False, verbose: bool = False, duration: int = 0
) -> MonitorSupervisor:
    """Create supervisor with standard monitor set.

    Args:
        dry_run: Print events instead of writing files
        verbose: Print detailed status messages
        duration: Duration for heartbeat monitor (0 = infinite)

    Returns:
        Configured MonitorSupervisor
    """
    supervisor = MonitorSupervisor(dry_run=dry_run, verbose=verbose)

    # Add standard monitors
    from .monitors.browser import BrowserMonitor
    from .monitors.context_snapshot import ContextSnapshotMonitor
    from .monitors.filewatch import FileWatchMonitor
    from .monitors.heartbeat import HeartbeatMonitor
    from .monitors.keyboard import KeyboardMonitor
    from .monitors.mouse import MouseMonitor

    # Heartbeat monitor
    heartbeat_beats = duration if dry_run and duration > 0 else 0
    supervisor.add_monitor(
        HeartbeatMonitor, "heartbeat", interval=1.0, total_beats=heartbeat_beats
    )

    # Context snapshot monitor
    supervisor.add_monitor(ContextSnapshotMonitor, "context")

    # Input monitors (may fail on some systems)
    supervisor.add_monitor(KeyboardMonitor, "keyboard")
    supervisor.add_monitor(MouseMonitor, "mouse")

    # Windows-only active window monitor
    if sys.platform == "win32":
        from .monitors.active_window import ActiveWindowMonitor

        supervisor.add_monitor(ActiveWindowMonitor, "window")

    # File system monitor
    supervisor.add_monitor(FileWatchMonitor, "file")

    # Browser monitor
    supervisor.add_monitor(BrowserMonitor, "browser")

    return supervisor
