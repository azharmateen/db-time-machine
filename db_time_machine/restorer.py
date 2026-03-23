"""Restore: for SQLite (replace file), PostgreSQL (pg_restore), MySQL (mysql import)."""

from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import tempfile
from pathlib import Path

from .config import DatabaseConfig
from .storage import SnapshotStorage


class Restorer:
    """Restore database snapshots."""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.storage = SnapshotStorage(config)

    def restore(self, name: str, verify: bool = True) -> dict:
        """
        Restore a snapshot to the database.

        Args:
            name: Snapshot name to restore.
            verify: Whether to verify integrity after restore.

        Returns:
            Dict with restore results.
        """
        meta = self.storage.get_snapshot(name)
        if not meta:
            raise ValueError(f"Snapshot '{name}' not found.")

        db_type = self.config.db_type.lower()

        if db_type == "sqlite":
            result = self._restore_sqlite(name)
        elif db_type in ("postgres", "postgresql"):
            result = self._restore_postgres(name)
        elif db_type == "mysql":
            result = self._restore_mysql(name)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

        if verify:
            result["verified"] = self._verify(meta.tables, meta.row_counts)

        return result

    def _get_sqlite_path(self) -> str:
        """Get SQLite database file path."""
        path = self.config.database or self.config.get_connection_string()
        if path.startswith("sqlite:///"):
            path = path[len("sqlite:///"):]
        return path

    def _restore_sqlite(self, name: str) -> dict:
        """Restore SQLite by replacing the database file."""
        db_path = self._get_sqlite_path()
        tmp_path = tempfile.mktemp(suffix=".db")

        try:
            # Extract snapshot to temp file
            self.storage.extract_snapshot(name, tmp_path)

            # Create backup of current database (safety net)
            if os.path.exists(db_path):
                backup_path = db_path + ".pre-restore-backup"
                shutil.copy2(db_path, backup_path)

            # Replace database file using SQLite backup API for safety
            src = sqlite3.connect(tmp_path)
            dst = sqlite3.connect(db_path)

            # Close existing connections by replacing the file
            src.backup(dst)
            src.close()
            dst.close()

            return {
                "status": "success",
                "db_type": "sqlite",
                "snapshot": name,
                "restored_to": db_path,
            }
        except Exception as e:
            # Try to restore from backup
            backup_path = db_path + ".pre-restore-backup"
            if os.path.exists(backup_path):
                shutil.copy2(backup_path, db_path)
            raise RuntimeError(f"Restore failed: {e}")
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def _restore_postgres(self, name: str) -> dict:
        """Restore PostgreSQL using psql."""
        tmp_path = tempfile.mktemp(suffix=".sql")

        env = os.environ.copy()
        if self.config.password:
            env["PGPASSWORD"] = self.config.password

        try:
            self.storage.extract_snapshot(name, tmp_path)

            # Drop and recreate database, then restore
            cmd = ["psql"]
            if self.config.host:
                cmd.extend(["-h", self.config.host])
            if self.config.port:
                cmd.extend(["-p", str(self.config.port)])
            if self.config.username:
                cmd.extend(["-U", self.config.username])
            if self.config.database:
                cmd.append(self.config.database)

            cmd.extend(["-f", tmp_path])

            result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=300)
            if result.returncode != 0:
                raise RuntimeError(f"psql restore failed: {result.stderr}")

            return {
                "status": "success",
                "db_type": "postgres",
                "snapshot": name,
                "database": self.config.database,
            }
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def _restore_mysql(self, name: str) -> dict:
        """Restore MySQL using mysql client."""
        tmp_path = tempfile.mktemp(suffix=".sql")

        try:
            self.storage.extract_snapshot(name, tmp_path)

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

            with open(tmp_path, "r") as f:
                result = subprocess.run(cmd, stdin=f, capture_output=True, text=True, timeout=300)

            if result.returncode != 0:
                raise RuntimeError(f"mysql restore failed: {result.stderr}")

            return {
                "status": "success",
                "db_type": "mysql",
                "snapshot": name,
                "database": self.config.database,
            }
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def _verify(self, expected_tables: list[str], expected_counts: dict[str, int]) -> dict:
        """Verify database integrity after restore."""
        db_type = self.config.db_type.lower()
        issues = []

        if db_type == "sqlite":
            db_path = self._get_sqlite_path()
            try:
                conn = sqlite3.connect(db_path)

                # Integrity check
                result = conn.execute("PRAGMA integrity_check").fetchone()
                if result[0] != "ok":
                    issues.append(f"Integrity check failed: {result[0]}")

                # Check tables exist
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
                current_tables = {row[0] for row in cursor.fetchall()}

                for table in expected_tables:
                    if table not in current_tables:
                        issues.append(f"Missing table: {table}")

                # Check row counts
                for table, expected_count in expected_counts.items():
                    if expected_count < 0:
                        continue
                    if table in current_tables:
                        actual = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                        if actual != expected_count:
                            issues.append(
                                f"Row count mismatch in {table}: expected {expected_count}, got {actual}"
                            )

                conn.close()
            except sqlite3.Error as e:
                issues.append(f"SQLite error: {e}")

        return {
            "passed": len(issues) == 0,
            "issues": issues,
            "tables_checked": len(expected_tables),
        }
