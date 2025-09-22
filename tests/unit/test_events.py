"""Unit tests for event system."""

import time
from unittest.mock import MagicMock, patch

from lb3.events import Event, EventBus, SpoolerSink


class TestEvent:
    """Test Event model."""

    def test_event_creation(self):
        """Test creating an Event."""
        event = Event(
            id="test_id",
            ts_utc=1234567890,
            monitor="test_monitor",
            action="test_action",
            subject_type="test_subject",
            session_id="test_session",
        )

        assert event.id == "test_id"
        assert event.ts_utc == 1234567890
        assert event.monitor == "test_monitor"
        assert event.action == "test_action"
        assert event.subject_type == "test_subject"
        assert event.session_id == "test_session"

    def test_event_to_dict(self):
        """Test converting Event to dictionary."""
        event = Event(
            id="test_id",
            ts_utc=1234567890,
            monitor="test_monitor",
            action="test_action",
            subject_type="test_subject",
            session_id="test_session",
            attrs_json='{"key": "value"}',
        )

        event_dict = event.to_dict()

        assert event_dict["id"] == "test_id"
        assert event_dict["ts_utc"] == 1234567890
        assert event_dict["monitor"] == "test_monitor"
        assert event_dict["action"] == "test_action"
        assert event_dict["subject_type"] == "test_subject"
        assert event_dict["session_id"] == "test_session"
        assert event_dict["attrs_json"] == '{"key": "value"}'

        # Check all fields are present
        expected_fields = {
            "id",
            "ts_utc",
            "monitor",
            "action",
            "subject_type",
            "session_id",
            "subject_id",
            "batch_id",
            "pid",
            "exe_name",
            "exe_path_hash",
            "window_title_hash",
            "url_hash",
            "file_path_hash",
            "attrs_json",
        }
        assert set(event_dict.keys()) == expected_fields

    def test_event_from_dict(self):
        """Test creating Event from dictionary."""
        event_dict = {
            "id": "test_id",
            "ts_utc": 1234567890,
            "monitor": "test_monitor",
            "action": "test_action",
            "subject_type": "test_subject",
            "session_id": "test_session",
            "subject_id": None,
            "batch_id": None,
            "pid": None,
            "exe_name": None,
            "exe_path_hash": None,
            "window_title_hash": None,
            "url_hash": None,
            "file_path_hash": None,
            "attrs_json": '{"key": "value"}',
        }

        event = Event.from_dict(event_dict)

        assert event.id == "test_id"
        assert event.ts_utc == 1234567890
        assert event.monitor == "test_monitor"
        assert event.action == "test_action"
        assert event.subject_type == "test_subject"
        assert event.session_id == "test_session"
        assert event.attrs_json == '{"key": "value"}'


class TestEventBus:
    """Test EventBus functionality."""

    def test_event_bus_creation(self):
        """Test creating an EventBus."""
        bus = EventBus()
        assert not bus._running
        assert bus._queue.empty()

    def test_start_stop(self):
        """Test starting and stopping EventBus."""
        bus = EventBus()

        bus.start()
        assert bus._running

        bus.stop()
        assert not bus._running

    def test_publish_event(self):
        """Test publishing events to bus."""
        bus = EventBus()
        bus.start()

        event = Event(
            id="test_id",
            ts_utc=1234567890,
            monitor="test_monitor",
            action="test_action",
            subject_type="test_subject",
            session_id="test_session",
        )

        success = bus.publish(event)
        assert success

        # Give worker thread time to process
        time.sleep(0.1)

        bus.stop()

    def test_publish_when_stopped(self):
        """Test publishing events when bus is stopped."""
        bus = EventBus()

        event = Event(
            id="test_id",
            ts_utc=1234567890,
            monitor="test_monitor",
            action="test_action",
            subject_type="test_subject",
            session_id="test_session",
        )

        success = bus.publish(event)
        # Bus still accepts events when stopped, but they won't be processed
        assert success

    def test_subscribe_unsubscribe(self):
        """Test subscribing and unsubscribing to events."""
        bus = EventBus()

        received_events = []

        def handler(event):
            received_events.append(event)

        # Subscribe
        bus.subscribe(handler)

        bus.start()

        event = Event(
            id="test_id",
            ts_utc=1234567890,
            monitor="test_monitor",
            action="test_action",
            subject_type="test_subject",
            session_id="test_session",
        )

        bus.publish(event)

        # Give worker thread time to process
        time.sleep(0.1)

        assert len(received_events) == 1
        assert received_events[0] == event

        # Unsubscribe
        bus.unsubscribe(handler)
        received_events.clear()

        bus.publish(event)
        time.sleep(0.1)

        assert len(received_events) == 0

        bus.stop()

    def test_fifo_ordering(self):
        """Test that events are delivered in FIFO order."""
        bus = EventBus()
        received_events = []

        def handler(event):
            received_events.append(event)

        bus.subscribe(handler)
        bus.start()

        # Publish multiple events quickly
        events = []
        for i in range(5):
            event = Event(
                id=f"test_id_{i}",
                ts_utc=1234567890 + i,
                monitor="test_monitor",
                action=f"test_action_{i}",
                subject_type="test_subject",
                session_id="test_session",
            )
            events.append(event)
            bus.publish(event)

        # Give worker thread time to process all events
        time.sleep(0.2)

        assert len(received_events) == 5
        for i, received_event in enumerate(received_events):
            assert received_event.id == f"test_id_{i}"
            assert received_event.action == f"test_action_{i}"

        bus.stop()

    def test_exception_handling(self):
        """Test that exceptions in handlers don't crash the bus."""
        bus = EventBus()
        received_events = []

        def good_handler(event):
            received_events.append(event)

        def bad_handler(event):
            raise Exception("Handler error")

        bus.subscribe(good_handler)
        bus.subscribe(bad_handler)
        bus.start()

        event = Event(
            id="test_id",
            ts_utc=1234567890,
            monitor="test_monitor",
            action="test_action",
            subject_type="test_subject",
            session_id="test_session",
        )

        bus.publish(event)

        # Give worker thread time to process
        time.sleep(0.1)

        # Good handler should still receive event despite bad handler exception
        assert len(received_events) == 1
        assert received_events[0] == event

        # Bus should still be running
        assert bus._running

        bus.stop()


class TestSpoolerSink:
    """Test SpoolerSink adapter."""

    @patch("lb3.spooler.get_spooler_manager")
    def test_spooler_sink_creation(self, mock_get_manager):
        """Test creating SpoolerSink."""
        mock_manager = MagicMock()
        mock_get_manager.return_value = mock_manager

        sink = SpoolerSink()
        assert sink.spooler_manager == mock_manager
        mock_get_manager.assert_called_once()

    @patch("lb3.spooler.get_spooler_manager")
    def test_handle_event(self, mock_get_manager):
        """Test handling events through SpoolerSink."""
        mock_manager = MagicMock()
        mock_get_manager.return_value = mock_manager

        sink = SpoolerSink()

        event = Event(
            id="test_id",
            ts_utc=1234567890,
            monitor="test_monitor",
            action="test_action",
            subject_type="test_subject",
            session_id="test_session",
        )

        sink(event)

        # Verify spooler manager was called with correct arguments
        mock_manager.write_event.assert_called_once()
        args = mock_manager.write_event.call_args
        assert args[0][0] == "test_monitor"  # monitor name
        assert args[0][1] == event.to_dict()  # event dict

    @patch("lb3.spooler.get_spooler_manager")
    def test_handle_event_exception(self, mock_get_manager):
        """Test that SpoolerSink handles exceptions gracefully."""
        mock_manager = MagicMock()
        mock_manager.write_event.side_effect = Exception("Spooler error")
        mock_get_manager.return_value = mock_manager

        sink = SpoolerSink()

        event = Event(
            id="test_id",
            ts_utc=1234567890,
            monitor="test_monitor",
            action="test_action",
            subject_type="test_subject",
            session_id="test_session",
        )

        # Should not raise exception
        sink(event)

        mock_manager.write_event.assert_called_once()

    @patch("lb3.spooler.get_spooler_manager")
    def test_integration_with_event_bus(self, mock_get_manager):
        """Test SpoolerSink integration with EventBus."""
        mock_manager = MagicMock()
        mock_get_manager.return_value = mock_manager

        bus = EventBus()
        sink = SpoolerSink()
        bus.subscribe(sink)

        bus.start()

        event = Event(
            id="test_id",
            ts_utc=1234567890,
            monitor="test_monitor",
            action="test_action",
            subject_type="test_subject",
            session_id="test_session",
        )

        bus.publish(event)

        # Give worker thread time to process
        time.sleep(0.1)

        # Verify event was written to spooler
        mock_manager.write_event.assert_called_once()
        args = mock_manager.write_event.call_args
        assert args[0][0] == "test_monitor"
        assert args[0][1] == event.to_dict()

        bus.stop()


class TestEventBusGlobalInstance:
    """Test global event bus instance."""

    def test_get_event_bus_singleton(self):
        """Test that get_event_bus returns singleton instance."""
        from lb3.events import get_event_bus

        bus1 = get_event_bus()
        bus2 = get_event_bus()

        assert bus1 is bus2

    def test_publish_event_function(self):
        """Test global publish_event function."""
        from lb3.events import get_event_bus, publish_event

        bus = get_event_bus()
        received_events = []

        def handler(event):
            received_events.append(event)

        bus.subscribe(handler)
        bus.start()

        event = Event(
            id="test_id",
            ts_utc=1234567890,
            monitor="test_monitor",
            action="test_action",
            subject_type="test_subject",
            session_id="test_session",
        )

        success = publish_event(event)
        assert success

        # Give worker thread time to process
        time.sleep(0.1)

        assert len(received_events) == 1
        assert received_events[0] == event

        bus.stop()
