"""Browser CDP (Chrome DevTools Protocol) plugin for Little Brother v3."""

import json
import threading
import time
from typing import Any, Dict, Optional

from ..config import get_effective_config
from ..database import get_database
from ..hashutil import extract_domain, hash_domain, hash_url
from ..ids import new_id
from ..logging_setup import get_logger
from ..monitors.base import BaseMonitor
from ..utils.scheduler import Scheduler

logger = get_logger("plugin.browser_cdp")


class BrowserCDPPlugin(BaseMonitor):
    """Chrome DevTools Protocol browser monitoring plugin."""
    
    def __init__(self, 
                 dry_run: bool = False,
                 scheduler: Optional[Scheduler] = None):
        """Initialize CDP browser plugin.
        
        Args:
            dry_run: Print events instead of emitting
            scheduler: Scheduler for deterministic timing
        """
        super().__init__(dry_run, scheduler)
        
        # Get CDP configuration
        config = get_effective_config()
        self._port = config.browser.integration.chrome_remote_debug_port
        self._base_url = f"http://127.0.0.1:{self._port}"
        
        # CDP connection state
        self._ws_connection = None
        self._targets = {}  # targetId -> target info
        self._session_lock = threading.Lock()
        
        # Event tracking
        self._last_events_flush = self.scheduler.now()
        
        # Try to import websocket library
        try:
            import websocket
            self._websocket = websocket
            logger.info("websocket library available for CDP")
        except ImportError:
            self._websocket = None
            logger.warning("websocket library not available, CDP disabled")
    
    @property
    def name(self) -> str:
        """Monitor name."""
        return "browser"
    
    def is_available(self) -> bool:
        """Check if CDP plugin can be used."""
        return (
            self._websocket is not None and 
            self._port > 0 and 
            self._check_debug_endpoint()
        )
    
    def _check_debug_endpoint(self) -> bool:
        """Check if Chrome debug endpoint is available."""
        try:
            import urllib.error
            import urllib.request
            
            # Check version endpoint
            version_url = f"{self._base_url}/json/version"
            with urllib.request.urlopen(version_url, timeout=2) as response:
                if response.status == 200:
                    version_data = json.loads(response.read().decode())
                    logger.info(f"Connected to browser: {version_data.get('Browser', 'Unknown')}")
                    return True
        except (urllib.error.URLError, Exception) as e:
            logger.debug(f"CDP endpoint not available: {e}")
        
        return False
    
    def start_monitoring(self) -> None:
        """Start CDP monitoring."""
        if not self.is_available():
            logger.warning("CDP not available, plugin disabled")
            return
        
        try:
            # Get list of targets
            self._discover_targets()
            
            # Connect to browser target for global events
            self._connect_to_browser()
            
            logger.info(f"CDP monitoring started on port {self._port}")
            
        except Exception as e:
            logger.error(f"Failed to start CDP monitoring: {e}")
            logger.info("Continuing without CDP monitoring (degraded mode)")
    
    def stop_monitoring(self) -> None:
        """Stop CDP monitoring."""
        try:
            if self._ws_connection:
                self._ws_connection.close()
                self._ws_connection = None
            logger.info("CDP monitoring stopped")
        except Exception as e:
            logger.warning(f"Error stopping CDP monitoring: {e}")
    
    def _discover_targets(self) -> None:
        """Discover available CDP targets."""
        try:
            import urllib.request
            
            targets_url = f"{self._base_url}/json"
            with urllib.request.urlopen(targets_url, timeout=5) as response:
                targets_data = json.loads(response.read().decode())
                
                with self._session_lock:
                    for target in targets_data:
                        target_id = target.get("id")
                        if target_id and target.get("type") == "page":
                            self._targets[target_id] = target
                            logger.debug(f"Discovered target: {target_id} - {target.get('title', 'Untitled')}")
                
        except Exception as e:
            logger.debug(f"Failed to discover targets: {e}")
    
    def _connect_to_browser(self) -> None:
        """Connect to browser-level CDP websocket."""
        try:
            import urllib.request
            
            # Get browser target websocket URL  
            version_url = f"{self._base_url}/json/version"
            with urllib.request.urlopen(version_url, timeout=5) as response:
                version_data = json.loads(response.read().decode())
                ws_url = version_data.get("webSocketDebuggerUrl")
                
                if ws_url:
                    self._ws_connection = self._websocket.WebSocketApp(
                        ws_url,
                        on_message=self._on_ws_message,
                        on_error=self._on_ws_error,
                        on_close=self._on_ws_close
                    )
                    
                    # Start websocket in separate thread
                    threading.Thread(
                        target=self._ws_connection.run_forever,
                        daemon=True
                    ).start()
                    
                    # Wait a moment for connection
                    time.sleep(0.5)
                    
                    # Enable target events
                    self._send_cdp_command("Target.setDiscoverTargets", {"discover": True})
                    
        except Exception as e:
            logger.error(f"Failed to connect to CDP websocket: {e}")
    
    def _send_cdp_command(self, method: str, params: Dict[str, Any] = None) -> None:
        """Send CDP command via websocket."""
        if not self._ws_connection:
            return
        
        try:
            message = {
                "id": int(time.time() * 1000),  # Simple ID generation
                "method": method,
                "params": params or {}
            }
            
            self._ws_connection.send(json.dumps(message))
            logger.debug(f"Sent CDP command: {method}")
            
        except Exception as e:
            logger.debug(f"Failed to send CDP command {method}: {e}")
    
    def _on_ws_message(self, ws, message: str) -> None:
        """Handle CDP websocket messages."""
        try:
            data = json.loads(message)
            
            # Handle events (no 'id' field)
            if "method" in data and "id" not in data:
                self._handle_cdp_event(data)
                
        except Exception as e:
            logger.debug(f"Error handling CDP message: {e}")
    
    def _on_ws_error(self, ws, error) -> None:
        """Handle CDP websocket errors."""
        logger.debug(f"CDP websocket error: {error}")
    
    def _on_ws_close(self, ws, close_status_code, close_msg) -> None:
        """Handle CDP websocket close."""
        logger.debug("CDP websocket connection closed")
    
    def _handle_cdp_event(self, event_data: Dict[str, Any]) -> None:
        """Handle CDP event and convert to browser event."""
        try:
            method = event_data.get("method", "")
            params = event_data.get("params", {})
            
            if method == "Target.targetCreated":
                self._handle_target_created(params)
            elif method == "Target.targetDestroyed":  
                self._handle_target_destroyed(params)
            elif method == "Target.targetInfoChanged":
                self._handle_target_changed(params)
                
        except Exception as e:
            logger.debug(f"Error handling CDP event {method}: {e}")
    
    def _handle_target_created(self, params: Dict[str, Any]) -> None:
        """Handle new tab/target creation."""
        target_info = params.get("targetInfo", {})
        target_id = target_info.get("targetId")
        target_type = target_info.get("type")
        url = target_info.get("url", "")
        title = target_info.get("title", "")
        
        if target_type != "page":
            return
        
        with self._session_lock:
            self._targets[target_id] = target_info
        
        # Only emit events for real pages (not about:blank, etc.)
        if url and not url.startswith(("about:", "chrome:", "edge:", "data:")):
            subject_id = self._get_or_create_url_record(url)
            
            event_data = {
                'action': 'tab_open',
                'subject_type': 'url',
                'subject_id': subject_id,
                'attrs': {
                    'source': 'cdp',
                    'targetId': target_id,
                    'tab_title_present': bool(title.strip())
                }
            }
            
            self.emit(event_data)
            logger.debug(f"Tab opened: {target_id}")
    
    def _handle_target_destroyed(self, params: Dict[str, Any]) -> None:
        """Handle tab/target destruction.""" 
        target_id = params.get("targetId")
        
        # Get target info before removing
        with self._session_lock:
            target_info = self._targets.pop(target_id, {})
        
        url = target_info.get("url", "")
        
        # Only emit for real pages
        if url and not url.startswith(("about:", "chrome:", "edge:", "data:")):
            subject_id = self._get_or_create_url_record(url)
            
            event_data = {
                'action': 'tab_close',
                'subject_type': 'url',
                'subject_id': subject_id,
                'attrs': {
                    'source': 'cdp',
                    'targetId': target_id,
                    'tab_title_present': False  # Tab is closing
                }
            }
            
            self.emit(event_data)
            logger.debug(f"Tab closed: {target_id}")
    
    def _handle_target_changed(self, params: Dict[str, Any]) -> None:
        """Handle tab/target changes (navigation, etc.)."""
        target_info = params.get("targetInfo", {})
        target_id = target_info.get("targetId")
        target_type = target_info.get("type")
        new_url = target_info.get("url", "")
        title = target_info.get("title", "")
        
        if target_type != "page":
            return
        
        # Get previous URL
        with self._session_lock:
            old_target_info = self._targets.get(target_id, {})
            old_url = old_target_info.get("url", "")
            self._targets[target_id] = target_info
        
        # Check if URL changed (navigation)
        if new_url != old_url and new_url and not new_url.startswith(("about:", "chrome:", "edge:", "data:")):
            subject_id = self._get_or_create_url_record(new_url)
            
            event_data = {
                'action': 'nav',
                'subject_type': 'url', 
                'subject_id': subject_id,
                'attrs': {
                    'source': 'cdp',
                    'targetId': target_id,
                    'tab_title_present': bool(title.strip())
                }
            }
            
            self.emit(event_data)
            logger.debug(f"Navigation: {target_id} -> {new_url[:50]}...")
    
    def _get_or_create_url_record(self, url: str) -> str:
        """Get existing URL record or create new one.
        
        Args:
            url: The URL to get/create record for
            
        Returns:
            URL record ID (ULID)
        """
        try:
            # Hash URL and extract domain
            url_hash = hash_url(url)
            domain = extract_domain(url)
            domain_hash = hash_domain(domain) if domain else ""
            
            db = get_database()
            
            # Check if URL record exists
            cursor = db._connection.cursor()
            cursor.execute(
                "SELECT id FROM urls WHERE url_hash = ?",
                (url_hash,)
            )
            result = cursor.fetchone()
            
            if result:
                # Update last_seen_utc
                url_id = result[0]
                now_utc = int(self.scheduler.now())
                cursor.execute(
                    "UPDATE urls SET last_seen_utc = ? WHERE id = ?",
                    (now_utc, url_id)
                )
                db._connection.commit()
                return url_id
            else:
                # Create new URL record
                url_id = new_id()
                now_utc = int(self.scheduler.now())
                cursor.execute(
                    "INSERT INTO urls (id, url_hash, domain_hash, first_seen_utc, last_seen_utc) VALUES (?, ?, ?, ?, ?)",
                    (url_id, url_hash, domain_hash, now_utc, now_utc)
                )
                db._connection.commit()
                return url_id
                
        except Exception as e:
            logger.error(f"Database error managing URL record: {e}")
            # Return a fallback ID
            return new_id()