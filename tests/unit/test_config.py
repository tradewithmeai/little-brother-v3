"""Test configuration system."""

import tempfile
from pathlib import Path

import pytest
import yaml

from lb3.config import Config, load_config


class TestConfig:
    """Test configuration functionality."""

    def test_default_config_values(self):
        """Test that default configuration has all required keys."""
        config = Config()
        
        assert config.time_zone_handling == "UTC_store_only"
        assert config.storage.sqlite_path == "./lb_data/local.db"
        assert config.storage.spool_dir == "./lb_data/spool"
        assert config.identifiers.type == "ULID"
        assert config.guardrails.no_global_text_keylogging is True
        assert config.plugins.enabled == []
        assert config.browser.integration.disabled_by_default is True
        assert config.browser.integration.chrome_remote_debug_port == 0
        assert config.heartbeat.poll_intervals.active_window == "1.2s"
        assert config.heartbeat.poll_intervals.browser == "2.0s"
        assert config.heartbeat.poll_intervals.context_idle_gap == "7.0s"
        assert config.batch.flush_thresholds.keyboard_events == "128 or 1.5s"
        assert config.batch.flush_thresholds.mouse_events == "64 or 1.5s"

    def test_salt_generation(self):
        """Test that salt is generated and is hex string."""
        config = Config()
        
        assert config.hashing.salt is not None
        assert isinstance(config.hashing.salt, str)
        assert len(config.hashing.salt) == 64  # 32 bytes as hex
        # Should be valid hex
        int(config.hashing.salt, 16)

    def test_salt_persistence(self):
        """Test that salt persists across config loads."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.yaml"
            
            # First load - creates config
            config1 = load_config(config_path)
            original_salt = config1.hashing.salt
            
            # Second load - should reuse same salt
            config2 = load_config(config_path)
            assert config2.hashing.salt == original_salt

    def test_guardrails_enforcement(self):
        """Test that guardrails.no_global_text_keylogging cannot be disabled."""
        with pytest.raises(ValueError, match="guardrails.no_global_text_keylogging must be True"):
            Config(guardrails={"no_global_text_keylogging": False})

    def test_yaml_serialization(self):
        """Test config can be serialized to/from YAML."""
        config = Config()
        yaml_str = config.to_yaml()
        
        # Parse YAML to verify structure
        data = yaml.safe_load(yaml_str)
        assert data["time_zone_handling"] == "UTC_store_only"
        assert data["storage"]["sqlite_path"] == "./lb_data/local.db"
        assert data["guardrails"]["no_global_text_keylogging"] is True

    def test_config_file_creation(self):
        """Test that config file is created on first load."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.yaml"
            
            # File should not exist initially
            assert not config_path.exists()
            
            # Loading should create the file
            config = load_config(config_path)
            assert config_path.exists()
            
            # File should contain valid YAML
            with open(config_path) as f:
                data = yaml.safe_load(f)
            assert data["time_zone_handling"] == "UTC_store_only"

    def test_config_file_loading(self):
        """Test loading config from existing file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.yaml"
            
            # Create config file with custom values
            custom_config = {
                "time_zone_handling": "UTC_store_only",
                "storage": {
                    "sqlite_path": "./custom/path.db",
                    "spool_dir": "./custom/spool"
                },
                "hashing": {
                    "salt": "abc123" * 10  # Custom salt
                },
                "identifiers": {"type": "ULID"},
                "guardrails": {"no_global_text_keylogging": True},
                "plugins": {"enabled": ["test_plugin"]},
                "browser": {
                    "integration": {
                        "disabled_by_default": False,
                        "chrome_remote_debug_port": 9222
                    }
                },
                "heartbeat": {
                    "poll_intervals": {
                        "active_window": "2.0s",
                        "browser": "5.0s",
                        "context_idle_gap": "10.0s"
                    }
                },
                "batch": {
                    "flush_thresholds": {
                        "keyboard_events": "256 or 2.0s",
                        "mouse_events": "128 or 2.0s"
                    }
                }
            }
            
            with open(config_path, 'w') as f:
                yaml.dump(custom_config, f)
            
            # Load config
            config = load_config(config_path)
            
            # Verify custom values were loaded
            assert config.storage.sqlite_path == "./custom/path.db"
            assert config.storage.spool_dir == "./custom/spool"
            assert config.hashing.salt == "abc123" * 10
            assert config.plugins.enabled == ["test_plugin"]
            assert config.browser.integration.disabled_by_default is False
            assert config.browser.integration.chrome_remote_debug_port == 9222
            assert config.heartbeat.poll_intervals.active_window == "2.0s"

    def test_config_path_resolution(self):
        """Test that config path resolves correctly."""
        config_path = Config.get_config_path()
        
        assert config_path.is_absolute()
        assert config_path.name == "config.yaml"
        assert "lb_data" in str(config_path)

    def test_directory_creation(self):
        """Test that ensure_data_dirs creates necessary directories."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Change to temp directory context
            original_cwd = Path.cwd()
            temp_path = Path(temp_dir)
            
            try:
                # Create config in temp context
                config = Config()
                config.storage.spool_dir = str(temp_path / "test_spool")
                
                # Directories should not exist yet
                lb_data = temp_path / "lb_data"
                spool_dir = temp_path / "test_spool"
                
                config.ensure_data_dirs()
                
                # Spool directory should be created
                assert spool_dir.exists()
                assert spool_dir.is_dir()
                
            finally:
                # Restore working directory
                pass