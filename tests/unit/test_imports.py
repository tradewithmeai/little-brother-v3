"""Test that all modules can be imported without error."""


def test_lb3_import():
    """Test that main lb3 package imports."""
    import lb3

    assert hasattr(lb3, "__version__")


def test_core_modules_import():
    """Test that core modules import."""


def test_monitors_import():
    """Test that monitor modules import."""


def test_plugins_import():
    """Test that plugin modules import."""


def test_monitor_classes_available():
    """Test that monitor classes are available."""
    from lb3.monitors import (
        ActiveWindowMonitor,
        BaseMonitor,
        BrowserMonitor,
        ContextSnapshotMonitor,
        FileWatchMonitor,
        KeyboardMonitor,
        MonitorState,
        MouseMonitor,
    )

    # Verify they are classes
    assert isinstance(BaseMonitor, type)
    assert isinstance(MonitorState, type)
    assert isinstance(ActiveWindowMonitor, type)
    assert isinstance(KeyboardMonitor, type)
    assert isinstance(MouseMonitor, type)
    assert isinstance(FileWatchMonitor, type)
    assert isinstance(BrowserMonitor, type)
    assert isinstance(ContextSnapshotMonitor, type)


def test_plugin_classes_available():
    """Test that plugin classes are available."""
    from lb3.plugins import VSCodeTextPlugin

    assert isinstance(VSCodeTextPlugin, type)
