"""Plugin system for Little Brother v3."""

from .browser_cdp import BrowserCDPPlugin
from .vscode_text_stub import VSCodeTextPlugin

__all__ = ["BrowserCDPPlugin", "VSCodeTextPlugin"]
