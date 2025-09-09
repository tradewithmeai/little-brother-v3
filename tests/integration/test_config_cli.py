"""Integration tests for config CLI commands."""

import subprocess
import tempfile
from pathlib import Path

import yaml


class TestConfigCLI:
    """Integration tests for config CLI functionality."""

    def test_config_path_command(self):
        """Test lb3 config path returns correct path."""
        result = subprocess.run(
            ["lb3", "config", "path"],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        assert result.returncode == 0
        path_output = result.stdout.strip()
        
        # Should be absolute path ending with config.yaml
        assert Path(path_output).is_absolute()
        assert path_output.endswith("config.yaml")
        assert "lb_data" in path_output

    def test_config_show_command_first_run(self):
        """Test lb3 config show creates config and shows YAML."""
        # Use a temporary directory to simulate fresh environment
        with tempfile.TemporaryDirectory() as temp_dir:
            # Change to temp directory for isolated test
            original_cwd = Path.cwd()
            temp_path = Path(temp_dir)
            
            try:
                # Change working directory to temp
                import os
                os.chdir(temp_path)
                
                result = subprocess.run(
                    ["lb3", "config", "show"],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    cwd=str(temp_path)
                )
                
                assert result.returncode == 0
                yaml_output = result.stdout
                
                # Parse YAML output
                config_data = yaml.safe_load(yaml_output)
                
                # Verify all required keys are present
                assert config_data["time_zone_handling"] == "UTC_store_only"
                assert config_data["storage"]["sqlite_path"] == "./lb_data/local.db"
                assert config_data["storage"]["spool_dir"] == "./lb_data/spool"
                assert config_data["identifiers"]["type"] == "ULID"
                assert config_data["guardrails"]["no_global_text_keylogging"] is True
                assert config_data["plugins"]["enabled"] == []
                assert config_data["browser"]["integration"]["disabled_by_default"] is True
                assert config_data["browser"]["integration"]["chrome_remote_debug_port"] == 0
                assert config_data["heartbeat"]["poll_intervals"]["active_window"] == "1.2s"
                assert config_data["heartbeat"]["poll_intervals"]["browser"] == "2.0s"
                assert config_data["heartbeat"]["poll_intervals"]["context_idle_gap"] == "7.0s"
                assert config_data["batch"]["flush_thresholds"]["keyboard_events"] == "128 or 1.5s"
                assert config_data["batch"]["flush_thresholds"]["mouse_events"] == "64 or 1.5s"
                
                # Verify hashing salt is present and valid
                assert "hashing" in config_data
                assert "salt" in config_data["hashing"]
                salt = config_data["hashing"]["salt"]
                assert isinstance(salt, str)
                assert len(salt) == 64  # 32 bytes as hex
                int(salt, 16)  # Should be valid hex
                
                # Verify config file was created
                config_file = temp_path / "lb_data" / "config.yaml"
                assert config_file.exists()
                
            finally:
                os.chdir(original_cwd)

    def test_config_show_salt_persistence(self):
        """Test that salt persists between config show commands."""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = Path.cwd()
            temp_path = Path(temp_dir)
            
            try:
                import os
                os.chdir(temp_path)
                
                # First run
                result1 = subprocess.run(
                    ["lb3", "config", "show"],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    cwd=str(temp_path)
                )
                
                assert result1.returncode == 0
                config_data1 = yaml.safe_load(result1.stdout)
                salt1 = config_data1["hashing"]["salt"]
                
                # Second run
                result2 = subprocess.run(
                    ["lb3", "config", "show"],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    cwd=str(temp_path)
                )
                
                assert result2.returncode == 0
                config_data2 = yaml.safe_load(result2.stdout)
                salt2 = config_data2["hashing"]["salt"]
                
                # Salt should be identical
                assert salt1 == salt2
                
            finally:
                os.chdir(original_cwd)

    def test_config_show_respects_user_edits(self):
        """Test that config show respects manual edits to config file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = Path.cwd()
            temp_path = Path(temp_dir)
            
            try:
                import os
                os.chdir(temp_path)
                
                # First run to create config
                result1 = subprocess.run(
                    ["lb3", "config", "show"],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    cwd=str(temp_path)
                )
                
                assert result1.returncode == 0
                original_config = yaml.safe_load(result1.stdout)
                
                # Manually edit config file
                config_file = temp_path / "lb_data" / "config.yaml"
                with open(config_file) as f:
                    config_data = yaml.safe_load(f)
                
                # Change some values
                config_data["storage"]["sqlite_path"] = "./custom/database.db"
                config_data["plugins"]["enabled"] = ["custom_plugin"]
                config_data["browser"]["integration"]["chrome_remote_debug_port"] = 9222
                
                with open(config_file, 'w') as f:
                    yaml.dump(config_data, f, default_flow_style=False)
                
                # Run config show again
                result2 = subprocess.run(
                    ["lb3", "config", "show"],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    cwd=str(temp_path)
                )
                
                assert result2.returncode == 0
                modified_config = yaml.safe_load(result2.stdout)
                
                # Verify our changes were preserved
                assert modified_config["storage"]["sqlite_path"] == "./custom/database.db"
                assert modified_config["plugins"]["enabled"] == ["custom_plugin"]
                assert modified_config["browser"]["integration"]["chrome_remote_debug_port"] == 9222
                
                # Salt should remain the same
                assert modified_config["hashing"]["salt"] == original_config["hashing"]["salt"]
                
            finally:
                os.chdir(original_cwd)

    def test_config_commands_in_help(self):
        """Test that config commands appear in help."""
        # Test main help
        result = subprocess.run(
            ["lb3", "--help"],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        assert result.returncode == 0
        assert "config" in result.stdout
        
        # Test config subcommand help
        result = subprocess.run(
            ["lb3", "config", "--help"],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        assert result.returncode == 0
        assert "show" in result.stdout
        assert "path" in result.stdout