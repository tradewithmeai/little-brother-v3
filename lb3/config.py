"""Configuration management for Little Brother v3."""

import secrets
from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import BaseModel, Field, validator


class StorageConfig(BaseModel):
    """Storage configuration."""

    sqlite_path: str = "./lb_data/local.db"
    spool_dir: str = "./lb_data/spool"
    spool_quota_mb: int = 512
    spool_soft_pct: int = 90
    spool_hard_pct: int = 100


class HashingConfig(BaseModel):
    """Hashing configuration."""

    salt: str = Field(default_factory=lambda: secrets.token_hex(32))


class IdentifiersConfig(BaseModel):
    """Identifiers configuration."""

    type: str = "ULID"


class GuardrailsConfig(BaseModel):
    """Guardrails configuration."""

    no_global_text_keylogging: bool = True


class MonitorConfig(BaseModel):
    """Monitor configuration."""

    enabled: bool = True
    interval: float = 1.0


class PluginsConfig(BaseModel):
    """Plugins configuration."""

    enabled: list[str] = Field(default_factory=list)


class BrowserIntegrationConfig(BaseModel):
    """Browser integration configuration."""

    disabled_by_default: bool = True
    chrome_remote_debug_port: int = 0


class BrowserConfig(BaseModel):
    """Browser configuration."""

    integration: BrowserIntegrationConfig = Field(
        default_factory=BrowserIntegrationConfig
    )


class PollIntervalsConfig(BaseModel):
    """Poll intervals configuration."""

    active_window: str = "1.2s"
    browser: str = "2.0s"
    context_idle_gap: str = "7.0s"


class HeartbeatConfig(BaseModel):
    """Heartbeat configuration."""

    poll_intervals: PollIntervalsConfig = Field(default_factory=PollIntervalsConfig)


class FlushThresholdsConfig(BaseModel):
    """Flush thresholds configuration."""

    keyboard_events: str = "128 or 1.5s"
    mouse_events: str = "64 or 1.5s"


class BatchConfig(BaseModel):
    """Batch processing configuration."""

    flush_thresholds: FlushThresholdsConfig = Field(
        default_factory=FlushThresholdsConfig
    )


class LoggingConfig(BaseModel):
    """Logging configuration."""

    quota_log_interval_s: int = 60


class Config(BaseModel):
    """Main configuration class."""

    time_zone_handling: str = "UTC_store_only"
    storage: StorageConfig = Field(default_factory=StorageConfig)
    hashing: HashingConfig = Field(default_factory=HashingConfig)
    identifiers: IdentifiersConfig = Field(default_factory=IdentifiersConfig)
    guardrails: GuardrailsConfig = Field(default_factory=GuardrailsConfig)
    plugins: PluginsConfig = Field(default_factory=PluginsConfig)
    browser: BrowserConfig = Field(default_factory=BrowserConfig)
    heartbeat: HeartbeatConfig = Field(default_factory=HeartbeatConfig)
    batch: BatchConfig = Field(default_factory=BatchConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    @validator("guardrails")
    def validate_guardrails(cls, v):
        """Ensure guardrails.no_global_text_keylogging is always True."""
        if not v.no_global_text_keylogging:
            raise ValueError("guardrails.no_global_text_keylogging must be True")
        return v

    @classmethod
    def get_config_path(cls) -> Path:
        """Get the configuration file path."""
        return Path("./lb_data/config.yaml").resolve()

    def ensure_data_dirs(self) -> None:
        """Ensure all required data directories exist."""
        # Create lb_data directory
        lb_data_dir = Path("./lb_data")
        lb_data_dir.mkdir(exist_ok=True)

        # Create spool directory
        spool_path = Path(self.storage.spool_dir)
        spool_path.mkdir(parents=True, exist_ok=True)

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dictionary for YAML serialization."""
        return self.dict()

    def to_yaml(self) -> str:
        """Convert config to YAML string."""
        return yaml.dump(self.to_dict(), default_flow_style=False, sort_keys=False)

    @classmethod
    def from_yaml_file(cls, config_path: Path) -> "Config":
        """Load configuration from YAML file."""
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        return cls(**data)

    def save_to_yaml_file(self, config_path: Path) -> None:
        """Save configuration to YAML file."""
        config_path.parent.mkdir(parents=True, exist_ok=True)

        with open(config_path, "w", encoding="utf-8") as f:
            f.write(self.to_yaml())


def load_config(config_path: Optional[Path] = None) -> Config:
    """Load configuration from file or create with defaults."""
    if config_path is None:
        config_path = Config.get_config_path()

    # If config file exists, load it
    if config_path.exists():
        try:
            return Config.from_yaml_file(config_path)
        except Exception as e:
            # If config file is corrupted, fall back to defaults
            print(f"Warning: Failed to load config from {config_path}: {e}")
            print("Using default configuration.")

    # Create new config with defaults
    config = Config()

    # Ensure directories exist and save config file
    config.ensure_data_dirs()
    config.save_to_yaml_file(config_path)

    return config


def get_effective_config() -> Config:
    """Get the effective configuration (load or create)."""
    return load_config()
