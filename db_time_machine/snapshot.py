"""Snapshot engine: create snapshots for SQLite, PostgreSQL, and MySQL."""

from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

from .config import DatabaseConfig
from .storage import SnapshotMeta, SnapshotStorage


class SnapshotEngine:
    """Create database snapshots for different database types."""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.storage = SnapshotStorage(config)

    def save(self, name: str, notes: str = "") -> SnapshotMeta:
        """
        Create a snapshot of the current database state.

        Args:
            name: Human-readable name for the snapshot.
            notes: Optional description.

        Returns:
            SnapshotMeta for the created snapshot.
        """
        db_type = self.config.db_type.lower()

        if db_type == "sqlite":
            return self._save_sqlite(name, notes)
        elif db_type in ("postgres", "postgresql"):
            return self._save_postgres(name, notes)
        elif db_type == "mysql":
            return self._save_mysql(name, notes)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    def _get_sqlite_path(self) -> str:
        """Get the SQLite database file path."""
        path = self.config.database or self.config.get_connection_string()
        if path.startswith("sqlite:///"):
            path = path[len("sqlite:///"):]
        return path

    def _save_sqlite(self, name: str, notes: str) -> SnapshotMeta:
        """Snapshot SQLite by copying the database file."""
        db_path = self._get_sqlite_path()
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"SQLite database not found: {db_path}")

        # Get table info
        tables, row_counts = self._get_sqlite_info(db_path)

        # Use SQLite's backup API for a consistent copy
        tmp_path = tempfile.mktemp(suffix=".db")
        try:
            src = sqlite3.connect(db_path)
            dst = sqlite3.connect(tmp_path)
            src.backup(dst)
            src.close()
            dst.close()

            meta = self.storage.store_snapshot(
                name=name,
                data_path=tmp_path,
                tables=tables,
                row_counts=row_counts,
                notes=notes,
            )
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

        return meta

    def _get_sqlite_info(self, db_path: str) -> tuple[list[str], dict[str, int]]:
        """Get table names and row counts from SQLite database."""
        tables = []
        row_counts = {}
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
            for (table_name,) in cursor.fetchall():
                tables.append(table_name)
                try:
                    count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
                    row_counts[table_name] = count
                except sqlite3.Error:
                    row_counts[table_name] = -1
            conn.close()
        except sqlite3.Error:
            pass
        return tables, row_counts

    def _save_postgres(self, name: str, notes: str) -> SnapshotMeta:
        """Snapshot PostgreSQL using pg_dump."""
        tmp_path = tempfile.mktemp(suffix=".sql")

        env = os.environ.copy()
        if self.config.password:
            env["PGPASSWORD"] = self.config.password

        cmd = ["pg_dump"]
        if self.config.host:
            cmd.extend(["-h", self.config.host])
        if self.config.port:
            cmd.extend(["-p", str(self.config.port)])
        if self.config.username:
            cmd.extend(["-U", self.config.username])
        if self.config.database:
            cmd.append(self.config.database)

        cmd.extend(["-f", tmp_path, "--no-owner", "--no-acl"])

        try:
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=300)
            if result.returncode != 0:
                raise RuntimeError(f"pg_dump failed: {result.stderr}")

            # Get table info from pg_dump output
            tables, row_counts = self._get_postgres_info(env)

            meta = self.storage.store_snapshot(
                name=name,
                data_path=tmp_path,
                tables=tables,
                row_counts=row_counts,
                notes=notes,
            )
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

        return meta

    def _get_postgres_info(self, env: dict) -> tuple[list[str], dict[str, int]]:
        """Get table info from PostgreSQL using psql."""
        tables = []
        row_counts = {}

        cmd = ["psql"]
        if self.config.host:
            cmd.extend(["-h", self.config.host])
        if self.config.port:
            cmd.extend(["-p", str(self.config.port)])
        if self.config.username:
            cmd.extend(["-U", self.config.username])
        if self.config.database:
            cmd.append(self.config.database)

        cmd.extend(["-t", "-A", "-c",
                     "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"])

        try:
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                for line in result.stdout.strip().splitlines():
                    table = line.strip()
                    if table:
                        tables.append(table)

                # Get row counts
                for table in tables:
                    count_cmd = cmd[:-2] + ["-c", f'SELECT COUNT(*) FROM "{table}"']
                    count_result = subprocess.run(
                        count_cmd, env=env, capture_output=True, text=True, timeout=30
                    )
                    if count_result.returncode == 0:
                        try:
                            row_counts[table] = int(count_result.stdout.strip())
                        except ValueError:
                            row_counts[table] = -1
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass

        return tables, row_counts

    def _save_mysql(self, name: str, notes: str) -> SnapshotMeta:
        """Snapshot MySQL using mysqldump."""
        tmp_path = tempfile.mktemp(suffix=".sql")

        cmd = ["mysqldump"]
        if self.config.host:
            cmd.extend(["-h", self.config.host])
        if self.config.port:
            cmd.extend(["-P", str(self.config.port)])
        if self.config.username:
            cmd.extend(["-u", self.config.username])
        if self.config.password:
            cmd.append(f"-p{self.config.password}")
        if self.config.database:
            cmd.append(self.config.database)

        cmd.extend(["--result-file", tmp_path, "--single-transaction"])

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode != 0:
                raise RuntimeError(f"mysqldump failed: {result.stderr}")

            tables, row_counts = self._get_mysql_info()

            meta = self.storage.store_snapshot(
                name=name,
                data_path=tmp_path,
                tables=tables,
                row_counts=row_counts,
                notes=notes,
            )
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

        return meta

    def _get_mysql_info(self) -> tuple[list[str], dict[str, int]]:
        """Get table info from MySQL."""
        tables = []
        row_counts = {}

        cmd = ["mysql"]
        if self.config.host:
            cmd.extend(["-h", self.config.host])
        if self.config.port:
            cmd.extend(["-P", str(self.config.port)])
        if self.config.username:
            cmd.extend(["-u", self.config.username])
        if self.config.password:
            cmd.append(f"-p{self.config.password}")
        if self.config.database:
            cmd.append(self.config.database)

        cmd.extend(["-N", "-e", "SHOW TABLES"])

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                for line in result.stdout.strip().splitlines():
                    table = line.strip()
                    if table:
                        tables.append(table)

                for table in tables:
                    count_cmd = cmd[:-2] + ["-e", f"SELECT COUNT(*) FROM `{table}`"]
                    count_result = subprocess.run(
                        count_cmd, capture_output=True, text=True, timeout=30
                    )
                    if count_result.returncode == 0:
                        try:
                            row_counts[table] = int(count_result.stdout.strip())
                        except ValueError:
                            row_counts[table] = -1
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass

        return tables, row_counts
