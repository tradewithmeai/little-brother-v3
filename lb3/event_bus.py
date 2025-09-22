"""Event bus system for Little Brother v3."""

import asyncio
import contextlib
import weakref
from collections.abc import Coroutine
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable


class EventType(Enum):
    """Event types."""

    MONITOR_START = "monitor.start"
    MONITOR_STOP = "monitor.stop"
    MONITOR_DATA = "monitor.data"
    MONITOR_ERROR = "monitor.error"

    SYSTEM_SHUTDOWN = "system.shutdown"
    SYSTEM_STARTUP = "system.startup"

    DATABASE_CONNECTED = "database.connected"
    DATABASE_ERROR = "database.error"


@dataclass
class Event:
    """Event data structure."""

    event_type: EventType
    source: str
    data: dict[str, Any]
    timestamp: float
    event_id: str


EventHandler = Callable[[Event], None]
AsyncEventHandler = Callable[[Event], Coroutine[Any, Any, None]]


class EventBus:
    """Simple event bus for decoupled communication."""

    def __init__(self) -> None:
        self._handlers: dict[EventType, set[EventHandler]] = {}
        self._async_handlers: dict[EventType, set[AsyncEventHandler]] = {}
        self._weak_handlers: dict[EventType, set[weakref.ref]] = {}

    def subscribe(self, event_type: EventType, handler: EventHandler) -> None:
        """Subscribe to an event type."""
        if event_type not in self._handlers:
            self._handlers[event_type] = set()

        self._handlers[event_type].add(handler)

    def subscribe_async(
        self, event_type: EventType, handler: AsyncEventHandler
    ) -> None:
        """Subscribe to an event type with async handler."""
        if event_type not in self._async_handlers:
            self._async_handlers[event_type] = set()

        self._async_handlers[event_type].add(handler)

    def subscribe_weak(self, event_type: EventType, handler: EventHandler) -> None:
        """Subscribe with weak reference to handler."""
        if event_type not in self._weak_handlers:
            self._weak_handlers[event_type] = set()

        self._weak_handlers[event_type].add(weakref.ref(handler))

    def unsubscribe(self, event_type: EventType, handler: EventHandler) -> None:
        """Unsubscribe from an event type."""
        if event_type in self._handlers:
            self._handlers[event_type].discard(handler)

        if event_type in self._async_handlers:
            self._async_handlers[event_type].discard(handler)

    def emit(self, event: Event) -> None:
        """Emit an event synchronously."""
        # Call sync handlers
        if event.event_type in self._handlers:
            for handler in list(self._handlers[event.event_type]):
                with contextlib.suppress(Exception):
                    handler(event)

        # Call weak reference handlers
        if event.event_type in self._weak_handlers:
            dead_refs = set()
            for handler_ref in list(self._weak_handlers[event.event_type]):
                handler = handler_ref()
                if handler is None:
                    dead_refs.add(handler_ref)
                else:
                    with contextlib.suppress(Exception):
                        handler(event)

            # Clean up dead references
            self._weak_handlers[event.event_type] -= dead_refs

    async def emit_async(self, event: Event) -> None:
        """Emit an event asynchronously."""
        # Emit sync first
        self.emit(event)

        # Call async handlers
        if event.event_type in self._async_handlers:
            tasks = []
            for handler in list(self._async_handlers[event.event_type]):
                try:
                    task = asyncio.create_task(handler(event))
                    tasks.append(task)
                except Exception:
                    # TODO: Log error
                    pass

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)


# Global event bus instance
event_bus = EventBus()
