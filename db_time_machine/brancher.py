"""Branch: create a copy of current DB for experimentation. Track branches."""

from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .config import DatabaseConfig
from .snapshot import SnapshotEngine
from .storage import SnapshotStorage


@dataclass
class Branch:
    """A database branch (experimental copy)."""

    name: str
    source_snapshot: str
    db_path: str  # For SQLite: file path; for PG/MySQL: database name
    db_type: str
    created_from: str  # Original database name/path


class Brancher:
    """Create and manage database branches."""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.storage = SnapshotStorage(config)
        self.engine = SnapshotEngine(config)

    def create_branch(self, branch_name: str, from_snapshot: Optional[str] = None) -> Branch:
        """
        Create a database branch (a separate copy for experimentation).

        For SQLite: Creates a new .db file.
        For PostgreSQL: Creates a new database with the snapshot data.
        For MySQL: Creates a new database with the snapshot data.

        Args:
            branch_name: Name for the branch.
            from_snapshot: Snapshot to branch from. If None, snapshots current state first.

        Returns:
            Branch info.
        """
        db_type = self.config.db_type.lower()

        # If no snapshot specified, create one first
        if from_snapshot is None:
            snap_name = f"branch-point-{branch_name}"
            self.engine.save(snap_name, notes=f"Auto-snapshot before branching to {branch_name}")
            from_snapshot = snap_name

        meta = self.storage.get_snapshot(from_snapshot)
        if not meta:
            raise ValueError(f"Snapshot '{from_snapshot}' not found.")

        if db_type == "sqlite":
            return self._branch_sqlite(branch_name, from_snapshot)
        elif db_type in ("postgres", "postgresql"):
            return self._branch_postgres(branch_name, from_snapshot)
        elif db_type == "mysql":
            return self._branch_mysql(branch_name, from_snapshot)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    def _branch_sqlite(self, branch_name: str, from_snapshot: str) -> Branch:
        """Create a SQLite branch by extracting snapshot to a new file."""
        original_path = self.config.database or self.config.get_connection_string()
        if original_path.startswith("sqlite:///"):
            original_path = original_path[len("sqlite:///"):]

        # Create branch file alongside original
        base = Path(original_path)
        branch_path = str(base.parent / f"{base.stem}_branch_{branch_name}{base.suffix}")

        self.storage.extract_snapshot(from_snapshot, branch_path)

        # Record as a branch snapshot
        self.storage.store_snapshot(
            name=f"branch/{branch_name}",
            data_path=branch_path,
            tables=[],
            row_counts={},
            notes=f"Branch from {from_snapshot}",
            is_branch=True,
            parent_snapshot=from_snapshot,
        )

        return Branch(
            name=branch_name,
            source_snapshot=from_snapshot,
            db_path=branch_path,
            db_type="sqlite",
            created_from=original_path,
        )

    def _branch_postgres(self, branch_name: str, from_snapshot: str) -> Branch:
        """Create a PostgreSQL branch as a new database."""
        branch_db = f"{self.config.database}_branch_{branch_name}"

        env = os.environ.copy()
        if self.config.password:
            env["PGPASSWORD"] = self.config.password

        base_cmd = []
        if self.config.host:
            base_cmd.extend(["-h", self.config.host])
        if self.config.port:
            base_cmd.extend(["-p", str(self.config.port)])
        if self.config.username:
            base_cmd.extend(["-U", self.config.username])

        # Create database
        create_cmd = ["createdb"] + base_cmd + [branch_db]
        result = subprocess.run(create_cmd, env=env, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to create branch database: {result.stderr}")

        # Extract snapshot and restore to new database
        tmp_path = tempfile.mktemp(suffix=".sql")
        try:
            self.storage.extract_snapshot(from_snapshot, tmp_path)

            restore_cmd = ["psql"] + base_cmd + [branch_db, "-f", tmp_path]
            result = subprocess.run(restore_cmd, env=env, capture_output=True, text=True, timeout=300)
            if result.returncode != 0:
                raise RuntimeError(f"Failed to restore to branch: {result.stderr}")
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

        return Branch(
            name=branch_name,
            source_snapshot=from_snapshot,
            db_path=branch_db,
            db_type="postgres",
            created_from=self.config.database,
        )

    def _branch_mysql(self, branch_name: str, from_snapshot: str) -> Branch:
        """Create a MySQL branch as a new database."""
        branch_db = f"{self.config.database}_branch_{branch_name}"

        base_cmd = ["mysql"]
        if self.config.host:
            base_cmd.extend(["-h", self.config.host])
        if self.config.port:
            base_cmd.extend(["-P", str(self.config.port)])
        if self.config.username:
            base_cmd.extend(["-u", self.config.username])
        if self.config.password:
            base_cmd.append(f"-p{self.config.password}")

        # Create database
        create_cmd = base_cmd + ["-e", f"CREATE DATABASE `{branch_db}`"]
        result = subprocess.run(create_cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to create branch database: {result.stderr}")

        # Extract and restore
        tmp_path = tempfile.mktemp(suffix=".sql")
        try:
            self.storage.extract_snapshot(from_snapshot, tmp_path)

            restore_cmd = base_cmd + [branch_db]
            with open(tmp_path, "r") as f:
                result = subprocess.run(restore_cmd, stdin=f, capture_output=True, text=True, timeout=300)
            if result.returncode != 0:
                raise RuntimeError(f"Failed to restore to branch: {result.stderr}")
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

        return Branch(
            name=branch_name,
            source_snapshot=from_snapshot,
            db_path=branch_db,
            db_type="mysql",
            created_from=self.config.database,
        )

    def list_branches(self) -> list[dict]:
        """List all branches."""
        snapshots = self.storage.list_snapshots(include_branches=True)
        branches = []
        for snap in snapshots:
            if snap.is_branch:
                branches.append({
                    "name": snap.name.replace("branch/", ""),
                    "parent_snapshot": snap.parent_snapshot,
                    "created": snap.timestamp,
                    "notes": snap.notes,
                })
        return branches

    def delete_branch(self, branch_name: str) -> bool:
        """Delete a branch and its associated database."""
        db_type = self.config.db_type.lower()

        # Delete the branch snapshot
        self.storage.delete_snapshot(f"branch/{branch_name}")

        if db_type == "sqlite":
            original_path = self.config.database or self.config.get_connection_string()
            if original_path.startswith("sqlite:///"):
                original_path = original_path[len("sqlite:///"):]
            base = Path(original_path)
            branch_path = base.parent / f"{base.stem}_branch_{branch_name}{base.suffix}"
            if branch_path.exists():
                branch_path.unlink()
                return True

        elif db_type in ("postgres", "postgresql"):
            branch_db = f"{self.config.database}_branch_{branch_name}"
            env = os.environ.copy()
            if self.config.password:
                env["PGPASSWORD"] = self.config.password
            cmd = ["dropdb"]
            if self.config.host:
                cmd.extend(["-h", self.config.host])
            if self.config.port:
                cmd.extend(["-p", str(self.config.port)])
            if self.config.username:
                cmd.extend(["-U", self.config.username])
            cmd.append(branch_db)
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=30)
            return result.returncode == 0

        elif db_type == "mysql":
            branch_db = f"{self.config.database}_branch_{branch_name}"
            cmd = ["mysql"]
            if self.config.host:
                cmd.extend(["-h", self.config.host])
            if self.config.port:
                cmd.extend(["-P", str(self.config.port)])
            if self.config.username:
                cmd.extend(["-u", self.config.username])
            if self.config.password:
                cmd.append(f"-p{self.config.password}")
            cmd.extend(["-e", f"DROP DATABASE IF EXISTS `{branch_db}`"])
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            return result.returncode == 0

        return False
