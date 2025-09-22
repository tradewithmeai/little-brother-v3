"""Unit tests for the log_once functionality."""

import logging
from unittest.mock import Mock

from lb3.logging_setup import log_once


def test_log_once_basic_functionality():
    """Test that log_once logs a message only once."""
    # Create a mock logger
    mock_logger = Mock()
    mock_logger.name = "test_logger"

    # Call log_once multiple times with the same message
    log_once(mock_logger, logging.INFO, "Test message %s", "arg1")
    log_once(mock_logger, logging.INFO, "Test message %s", "arg1")
    log_once(mock_logger, logging.INFO, "Test message %s", "arg1")

    # Should only be called once
    assert mock_logger.log.call_count == 1
    mock_logger.log.assert_called_with(logging.INFO, "Test message %s", "arg1")


def test_log_once_different_messages():
    """Test that log_once allows different messages."""
    mock_logger = Mock()
    mock_logger.name = "test_logger"

    # Call log_once with different messages
    log_once(mock_logger, logging.INFO, "Message 1")
    log_once(mock_logger, logging.INFO, "Message 2")
    log_once(mock_logger, logging.INFO, "Message 1")  # Duplicate

    # Should be called twice (once for each unique message)
    assert mock_logger.log.call_count == 2
    calls = mock_logger.log.call_args_list
    assert calls[0][0] == (logging.INFO, "Message 1")
    assert calls[1][0] == (logging.INFO, "Message 2")


def test_log_once_different_loggers():
    """Test that log_once tracks messages per logger."""
    mock_logger1 = Mock()
    mock_logger1.name = "logger1"

    mock_logger2 = Mock()
    mock_logger2.name = "logger2"

    # Same message to different loggers
    log_once(mock_logger1, logging.INFO, "Same message")
    log_once(mock_logger2, logging.INFO, "Same message")
    log_once(mock_logger1, logging.INFO, "Same message")  # Duplicate for logger1

    # Both loggers should be called once
    assert mock_logger1.log.call_count == 1
    assert mock_logger2.log.call_count == 1


def test_log_once_different_levels():
    """Test that log_once treats different log levels as different messages."""
    mock_logger = Mock()
    mock_logger.name = "test_logger"

    # Same message at different levels
    log_once(mock_logger, logging.INFO, "Test message")
    log_once(mock_logger, logging.WARNING, "Test message")
    log_once(mock_logger, logging.INFO, "Test message")  # Duplicate INFO

    # Should be called twice (once for each level)
    assert mock_logger.log.call_count == 2
    calls = mock_logger.log.call_args_list
    assert calls[0][0] == (logging.INFO, "Test message")
    assert calls[1][0] == (logging.WARNING, "Test message")


def test_log_once_with_format_args():
    """Test that log_once handles format arguments correctly."""
    mock_logger = Mock()
    mock_logger.name = "test_logger"

    # Same message template with same args
    log_once(mock_logger, logging.INFO, "Message with %s and %d", "text", 42)
    log_once(mock_logger, logging.INFO, "Message with %s and %d", "text", 42)

    # Same message template with different args
    log_once(mock_logger, logging.INFO, "Message with %s and %d", "other", 24)

    # Should be called twice (different formatted messages)
    assert mock_logger.log.call_count == 2


def test_log_once_with_kwargs():
    """Test that log_once passes through additional kwargs."""
    mock_logger = Mock()
    mock_logger.name = "test_logger"

    # Call with extra kwargs
    log_once(
        mock_logger,
        logging.ERROR,
        "Error message",
        exc_info=True,
        extra={"test": "value"},
    )

    # Should pass through kwargs
    mock_logger.log.assert_called_with(
        logging.ERROR, "Error message", exc_info=True, extra={"test": "value"}
    )


def test_log_once_thread_safety():
    """Test that log_once is thread-safe (basic check)."""
    import threading

    mock_logger = Mock()
    mock_logger.name = "test_logger"

    call_count = [0]

    def log_in_thread():
        log_once(mock_logger, logging.INFO, "Thread message")
        call_count[0] += 1

    # Create multiple threads that all try to log the same message
    threads = []
    for i in range(10):
        thread = threading.Thread(target=log_in_thread)
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # All threads should have run
    assert call_count[0] == 10

    # But logger should only be called once
    assert mock_logger.log.call_count == 1


def test_log_once_with_no_args():
    """Test log_once with no format arguments."""
    mock_logger = Mock()
    mock_logger.name = "test_logger"

    log_once(mock_logger, logging.INFO, "Simple message")
    log_once(mock_logger, logging.INFO, "Simple message")

    assert mock_logger.log.call_count == 1
    mock_logger.log.assert_called_with(logging.INFO, "Simple message")
