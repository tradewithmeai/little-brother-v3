"""Scheduler infrastructure for deterministic timing in tests and production."""

import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Protocol

from ..logging_setup import get_logger

logger = get_logger("scheduler")


class Handle:
    """Handle for a scheduled task that can be cancelled."""
    
    def __init__(self, task_id: str):
        self.task_id = task_id
        self.cancelled = False
    
    def cancel(self) -> bool:
        """Cancel the task. Returns True if task was cancelled, False if already executed."""
        if not self.cancelled:
            self.cancelled = True
            return True
        return False


class Scheduler(Protocol):
    """Protocol for scheduling tasks with deterministic testing support."""
    
    def now(self) -> float:
        """Get current time in seconds."""
        ...
    
    def call_later(self, delay_s: float, fn: Callable[[], None]) -> Handle:
        """Schedule a function to be called after delay_s seconds."""
        ...
    
    def cancel(self, handle: Handle) -> bool:
        """Cancel a scheduled task."""
        ...
    
    def cancel_all(self) -> None:
        """Cancel all scheduled tasks."""
        ...


@dataclass
class ScheduledTask:
    """A task scheduled to run at a specific time."""
    handle: Handle
    due_time: float
    callback: Callable[[], None]
    
    def __post_init__(self):
        if self.callback is None:
            raise ValueError("callback cannot be None")


class RealScheduler:
    """Production scheduler using real time and threading.Timer."""
    
    def __init__(self):
        self._tasks: Dict[str, threading.Timer] = {}
        self._task_counter = 0
        self._lock = threading.Lock()
    
    def now(self) -> float:
        """Get current monotonic time."""
        return time.monotonic()
    
    def call_later(self, delay_s: float, fn: Callable[[], None]) -> Handle:
        """Schedule a function using threading.Timer."""
        with self._lock:
            self._task_counter += 1
            task_id = f"task_{self._task_counter}"
            handle = Handle(task_id)
            
            def wrapper():
                with self._lock:
                    # Remove from tracking when executed
                    self._tasks.pop(task_id, None)
                if not handle.cancelled:
                    try:
                        fn()
                    except Exception as e:
                        logger.error(f"Error in scheduled task {task_id}: {e}")
            
            timer = threading.Timer(delay_s, wrapper)
            self._tasks[task_id] = timer
            timer.start()
            
            return handle
    
    def cancel(self, handle: Handle) -> bool:
        """Cancel a specific task."""
        with self._lock:
            timer = self._tasks.pop(handle.task_id, None)
            if timer:
                timer.cancel()
                handle.cancel()
                return True
            return False
    
    def cancel_all(self) -> None:
        """Cancel all scheduled tasks."""
        with self._lock:
            for timer in self._tasks.values():
                timer.cancel()
            for handle in [Handle(task_id) for task_id in self._tasks.keys()]:
                handle.cancel()
            self._tasks.clear()


class ManualScheduler:
    """Test scheduler that requires manual time advancement."""
    
    def __init__(self, start_time: float = 0.0, clock: Optional[Callable[[], float]] = None):
        """Initialize scheduler with optional external clock.
        
        Args:
            start_time: Initial time value (used if no external clock)
            clock: Optional external clock function
        """
        self._current_time = start_time
        self._external_clock = clock
        self._tasks: List[ScheduledTask] = []
        self._task_counter = 0
        self._lock = threading.Lock()
    
    def now(self) -> float:
        """Get current simulated time."""
        if self._external_clock:
            return self._external_clock()
        with self._lock:
            return self._current_time
    
    def call_later(self, delay_s: float, fn: Callable[[], None]) -> Handle:
        """Schedule a function to be called after delay_s simulated seconds."""
        with self._lock:
            self._task_counter += 1
            task_id = f"manual_task_{self._task_counter}"
            handle = Handle(task_id)
            due_time = self.now() + delay_s
            
            task = ScheduledTask(handle=handle, due_time=due_time, callback=fn)
            self._tasks.append(task)
            self._tasks.sort(key=lambda t: t.due_time)  # Keep sorted by due time
            
            return handle
    
    def cancel(self, handle: Handle) -> bool:
        """Cancel a specific task."""
        with self._lock:
            for i, task in enumerate(self._tasks):
                if task.handle.task_id == handle.task_id:
                    self._tasks.pop(i)
                    handle.cancel()
                    return True
            return False
    
    def cancel_all(self) -> None:
        """Cancel all scheduled tasks."""
        with self._lock:
            for task in self._tasks:
                task.handle.cancel()
            self._tasks.clear()
    
    def advance(self, dt_s: float) -> int:
        """Advance simulated time by dt_s seconds and execute due tasks.
        
        Returns:
            Number of tasks executed.
        """
        executed_count = 0
        
        with self._lock:
            if not self._external_clock:
                self._current_time += dt_s
            
            # Execute all tasks that are now due
            due_tasks = []
            remaining_tasks = []
            
            current_time = self.now()
            for task in self._tasks:
                if not task.handle.cancelled and task.due_time <= current_time:
                    due_tasks.append(task)
                elif not task.handle.cancelled:
                    remaining_tasks.append(task)
            
            self._tasks = remaining_tasks
        
        # Execute due tasks outside the lock to avoid deadlock
        for task in due_tasks:
            if not task.handle.cancelled:
                try:
                    task.callback()
                    executed_count += 1
                except Exception as e:
                    logger.error(f"Error in scheduled task {task.handle.task_id}: {e}")
        
        return executed_count
    
    def pending_count(self) -> int:
        """Get count of pending (non-cancelled) tasks."""
        with self._lock:
            return len([t for t in self._tasks if not t.handle.cancelled])
    
    def next_due_time(self) -> Optional[float]:
        """Get the due time of the next task, or None if no tasks."""
        with self._lock:
            active_tasks = [t for t in self._tasks if not t.handle.cancelled]
            if active_tasks:
                return min(t.due_time for t in active_tasks)
            return None


def get_scheduler(test_mode: bool = False) -> Scheduler:
    """Get the appropriate scheduler based on mode."""
    if test_mode:
        return ManualScheduler()
    else:
        return RealScheduler()