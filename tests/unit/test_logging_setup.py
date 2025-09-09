"""Unit tests for centralized logging functionality."""

import logging
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from lb3.logging_setup import (
    ContextFormatter,
    MonitorLoggerAdapter,
    _logging_initialized,
    _logging_lock,
    get_logger,
    get_session_id,
    set_session_id,
    setup_logging,
)


class TestLoggingSetup:
    """Tests for logging setup functionality."""
    
    def setup_method(self):
        """Reset logging state before each test."""
        global _logging_initialized
        with _logging_lock:
            _logging_initialized = False
            # Close and clear any existing handlers
            root_logger = logging.getLogger("lb3")
            for handler in root_logger.handlers[:]:
                handler.close()
                root_logger.removeHandler(handler)
            root_logger.handlers.clear()
            set_session_id(None)
    
    def test_setup_logging_basic(self):
        """Test basic logging setup."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_dir = Path(temp_dir)
            session_id = "test-session-123"
            
            logger = setup_logging(
                console_level="INFO",
                file_level="DEBUG",
                session_id=session_id,
                log_dir=log_dir,
                console=False  # Disable console for testing
            )
            
            assert logger.name == "lb3"
            assert get_session_id() == session_id
            
            # Check log file was created
            log_files = list(log_dir.glob("*.log"))
            assert len(log_files) == 1
            
            log_file = log_files[0]
            # Filename should match pattern: YYYYMMDD_HHMMSS-PID.log
            assert "-" in log_file.stem
            assert log_file.suffix == ".log"
            
            # Close handlers to release file locks
            for handler in logger.handlers[:]:
                handler.close()
                logger.removeHandler(handler)
    
    def test_setup_logging_only_once(self):
        """Test that logging setup only happens once."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_dir = Path(temp_dir)
            
            # First setup
            logger1 = setup_logging(log_dir=log_dir, console=False)
            log_files_after_first = list(log_dir.glob("*.log"))
            
            # Second setup should return same logger, not create new file
            logger2 = setup_logging(log_dir=log_dir, console=False)
            log_files_after_second = list(log_dir.glob("*.log"))
            
            assert logger1 is logger2
            assert len(log_files_after_first) == len(log_files_after_second)
    
    def test_logging_levels(self):
        """Test different logging levels."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_dir = Path(temp_dir)
            
            # Setup with DEBUG console, INFO file
            logger = setup_logging(
                console_level="DEBUG",
                file_level="INFO", 
                log_dir=log_dir,
                console=False
            )
            
            # Get file handler
            file_handler = None
            for handler in logger.handlers:
                if isinstance(handler, logging.FileHandler):
                    file_handler = handler
                    break
            
            assert file_handler is not None
            assert file_handler.level == logging.INFO
    
    def test_session_id_management(self):
        """Test session ID getter/setter."""
        original_id = get_session_id()
        
        test_id = "test-session-456"
        set_session_id(test_id)
        assert get_session_id() == test_id
        
        # Reset
        set_session_id(original_id)


class TestContextFormatter:
    """Tests for the context formatter."""
    
    def test_formatter_adds_context(self):
        """Test that formatter adds required context fields."""
        formatter = ContextFormatter()
        
        # Create a log record
        record = logging.LogRecord(
            name="lb3.test",
            level=logging.INFO,
            pathname="",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None
        )
        record.created = time.time()
        record.msecs = 123
        
        # Set session ID
        set_session_id("test-session")
        
        formatted = formatter.format(record)
        
        # Check required fields are present
        assert "test-session" in formatted
        assert "MainThread" in formatted or "Thread-" in formatted  # Thread name
        assert "INFO" in formatted
        assert "Test message" in formatted
        # Should have UTC timestamp format
        assert "T" in formatted  # ISO format separator
    
    def test_formatter_extracts_monitor_from_logger_name(self):
        """Test that formatter extracts monitor from logger name."""
        formatter = ContextFormatter()
        
        record = logging.LogRecord(
            name="lb3.active_window",
            level=logging.INFO,
            pathname="",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None
        )
        record.created = time.time()
        record.msecs = 123
        
        formatted = formatter.format(record)
        assert "[active_window]" in formatted
    
    def test_formatter_handles_missing_session_id(self):
        """Test formatter handles missing session ID gracefully."""
        formatter = ContextFormatter()
        set_session_id(None)  # Clear session ID
        
        record = logging.LogRecord(
            name="lb3.test",
            level=logging.INFO,
            pathname="",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None
        )
        record.created = time.time()
        record.msecs = 123
        
        formatted = formatter.format(record)
        assert "[unknown]" in formatted  # Should use 'unknown' as fallback


class TestGetLogger:
    """Tests for get_logger functionality."""
    
    def setup_method(self):
        """Reset logging state."""
        global _logging_initialized
        with _logging_lock:
            _logging_initialized = False
            # Close and clear any existing handlers
            root_logger = logging.getLogger("lb3")
            for handler in root_logger.handlers[:]:
                handler.close()
                root_logger.removeHandler(handler)
            root_logger.handlers.clear()
            set_session_id(None)
    
    def test_get_logger_basic(self):
        """Test basic logger retrieval."""
        logger = get_logger("test")
        assert logger.name == "lb3.test"
        
        # Should auto-initialize logging
        assert _logging_initialized
    
    def test_get_logger_with_monitor(self):
        """Test logger with monitor context."""
        logger = get_logger("test", monitor="active_window")
        assert isinstance(logger, MonitorLoggerAdapter)
        assert logger.extra["monitor"] == "active_window"
    
    def test_multiple_loggers(self):
        """Test getting multiple loggers."""
        logger1 = get_logger("test1")
        logger2 = get_logger("test2")
        
        assert logger1.name == "lb3.test1"
        assert logger2.name == "lb3.test2"
        assert logger1 is not logger2


class TestMonitorLoggerAdapter:
    """Tests for MonitorLoggerAdapter."""
    
    def test_adapter_injects_monitor_context(self):
        """Test that adapter injects monitor context."""
        base_logger = logging.getLogger("lb3.test")
        adapter = MonitorLoggerAdapter(base_logger, {"monitor": "keyboard"})
        
        # Mock the makeRecord method to check extra data
        with patch.object(base_logger, 'makeRecord') as mock_make_record:
            mock_make_record.return_value = logging.LogRecord(
                name="lb3.test",
                level=logging.INFO,
                pathname="",
                lineno=1,
                msg="Test message",
                args=(),
                exc_info=None
            )
            
            # This should call makeRecord with monitor in extra
            adapter.info("Test message")
            
            # Check that makeRecord was called with monitor in extra
            call_args = mock_make_record.call_args
            extra = call_args[1].get('extra', {})
            assert extra.get('monitor') == 'keyboard'


class TestLoggingIntegration:
    """Integration tests for logging functionality."""
    
    def setup_method(self):
        """Reset logging state."""
        global _logging_initialized
        with _logging_lock:
            _logging_initialized = False
            # Close and clear any existing handlers
            root_logger = logging.getLogger("lb3")
            for handler in root_logger.handlers[:]:
                handler.close()
                root_logger.removeHandler(handler)
            root_logger.handlers.clear()
            set_session_id(None)
    
    def test_logging_to_file(self):
        """Test actual logging to file with proper format."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_dir = Path(temp_dir)
            session_id = "integration-test-session"
            
            # Setup logging
            setup_logging(
                console_level="DEBUG",
                file_level="DEBUG",
                session_id=session_id,
                log_dir=log_dir,
                console=False
            )
            
            # Get a logger and log some messages
            logger = get_logger("test_monitor", monitor="keyboard")
            logger.info("This is an info message")
            logger.debug("This is a debug message")
            logger.warning("This is a warning message")
            
            # Force flush
            for handler in logging.getLogger("lb3").handlers:
                handler.flush()
            
            # Read log file
            log_files = list(log_dir.glob("*.log"))
            assert len(log_files) == 1
            
            log_content = log_files[0].read_text(encoding='utf-8')
            
            # Check content
            assert session_id in log_content
            assert "[keyboard]" in log_content
            assert "INFO - This is an info message" in log_content
            assert "DEBUG - This is a debug message" in log_content
            assert "WARNING - This is a warning message" in log_content
            
            # Check timestamp format (should be UTC ISO format)
            lines = log_content.strip().split('\n')
            for line in lines:
                if " - " in line:  # Skip initialization messages
                    # Should have format: YYYY-MM-DDTHH:MM:SS.mmm [session] [monitor] [thread] LEVEL - message
                    assert "T" in line  # ISO timestamp
                    assert "[" in line and "]" in line  # Context brackets
    
    def test_logging_summarizes_batches(self):
        """Test that logging summarizes batches instead of individual events."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_dir = Path(temp_dir)
            
            setup_logging(log_dir=log_dir, console=False)
            logger = get_logger("spooler")
            
            # Log batch summary (this is how spoolers should log)
            logger.info("Batch written: 15 events, 2.3KB, monitor=active_window")
            logger.debug("Event details: window_focus, window_blur, ...")  # Details at DEBUG
            
            # Force flush
            for handler in logging.getLogger("lb3").handlers:
                handler.flush()
            
            log_content = log_files[0].read_text(encoding='utf-8')
            assert "Batch written: 15 events" in log_content


class TestThreadSafety:
    """Tests for thread safety of logging setup."""
    
    def setup_method(self):
        """Reset logging state."""
        global _logging_initialized
        with _logging_lock:
            _logging_initialized = False
            # Close and clear any existing handlers
            root_logger = logging.getLogger("lb3")
            for handler in root_logger.handlers[:]:
                handler.close()
                root_logger.removeHandler(handler)
            root_logger.handlers.clear()
            set_session_id(None)
    
    def test_concurrent_setup(self):
        """Test that concurrent setup calls don't cause issues."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_dir = Path(temp_dir)
            results = []
            
            def setup_in_thread():
                logger = setup_logging(log_dir=log_dir, console=False)
                results.append(logger)
            
            # Start multiple threads trying to setup logging
            threads = []
            for _ in range(5):
                thread = threading.Thread(target=setup_in_thread)
                threads.append(thread)
                thread.start()
            
            # Wait for all threads
            for thread in threads:
                thread.join()
            
            # All should return the same logger instance
            assert len(results) == 5
            for logger in results[1:]:
                assert logger is results[0]
            
            # Should only create one log file
            log_files = list(log_dir.glob("*.log"))
            assert len(log_files) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])