"""Config: database type, connection string, snapshot directory, retention policy."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

CONFIG_FILENAME = ".db-time-machine.json"
DEFAULT_SNAPSHOT_DIR = ".db-snapshots"


@dataclass
class RetentionPolicy:
    """Snapshot retention policy."""

    max_snapshots: int = 50  # Maximum number of snapshots to keep
    max_age_days: int = 90  # Delete snapshots older than this
    keep_named: bool = True  # Always keep manually named snapshots


@dataclass
class DatabaseConfig:
    """Database connection configuration."""

    db_type: str = "sqlite"  # sqlite, postgres, mysql
    connection_string: str = ""  # Connection string or file path
    host: str = "localhost"
    port: int = 5432
    database: str = ""
    username: str = ""
    password: str = ""
    snapshot_dir: str = DEFAULT_SNAPSHOT_DIR
    retention: RetentionPolicy = field(default_factory=RetentionPolicy)

    @property
    def snapshot_path(self) -> Path:
        """Get the absolute path to the snapshot directory."""
        return Path(self.snapshot_dir).resolve()

    def get_connection_string(self) -> str:
        """Build a connection string if not explicitly set."""
        if self.connection_string:
            return self.connection_string

        if self.db_type == "sqlite":
            return self.database or "database.db"

        if self.db_type == "postgres":
            port = self.port or 5432
            auth = f"{self.username}:{self.password}@" if self.username else ""
            return f"postgresql://{auth}{self.host}:{port}/{self.database}"

        if self.db_type == "mysql":
            port = self.port or 3306
            auth = f"{self.username}:{self.password}@" if self.username else ""
            return f"mysql://{auth}{self.host}:{port}/{self.database}"

        return self.database


def find_config(start_dir: Optional[str] = None) -> Optional[Path]:
    """Search for config file starting from given directory, walking up to root."""
    current = Path(start_dir or os.getcwd()).resolve()

    while True:
        config_path = current / CONFIG_FILENAME
        if config_path.exists():
            return config_path
        parent = current.parent
        if parent == current:
            break
        current = parent
    return None


def load_config(config_path: Optional[str] = None) -> DatabaseConfig:
    """
    Load configuration from file.

    Args:
        config_path: Explicit path to config file. If None, searches upward.

    Returns:
        DatabaseConfig instance.
    """
    if config_path:
        path = Path(config_path)
    else:
        path = find_config()

    if path and path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            retention_data = data.pop("retention", {})
            retention = RetentionPolicy(**retention_data) if retention_data else RetentionPolicy()

            config = DatabaseConfig(**data, retention=retention)
            return config
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid config file {path}: {e}")

    return DatabaseConfig()


def save_config(config: DatabaseConfig, config_path: Optional[str] = None) -> Path:
    """
    Save configuration to file.

    Args:
        config: DatabaseConfig instance.
        config_path: Path to save to. Defaults to current directory.

    Returns:
        Path where config was saved.
    """
    if config_path:
        path = Path(config_path)
    else:
        path = Path(os.getcwd()) / CONFIG_FILENAME

    data = asdict(config)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    return path


def init_config(
    db_type: str = "sqlite",
    connection_string: str = "",
    database: str = "",
    host: str = "localhost",
    port: int = 0,
    username: str = "",
    password: str = "",
    snapshot_dir: str = DEFAULT_SNAPSHOT_DIR,
) -> DatabaseConfig:
    """Create and save a new configuration."""
    if not port:
        port = {"sqlite": 0, "postgres": 5432, "mysql": 3306}.get(db_type, 0)

    config = DatabaseConfig(
        db_type=db_type,
        connection_string=connection_string,
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        snapshot_dir=snapshot_dir,
    )

    save_config(config)
    return config
