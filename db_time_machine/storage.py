"""Snapshot storage: .db-snapshots/ directory, index in SQLite, compression, cleanup."""

from __future__ import annotations

import gzip
import json
import os
import shutil
import sqlite3
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from .config import DatabaseConfig, RetentionPolicy


@dataclass
class SnapshotMeta:
    """Metadata for a snapshot."""

    name: str
    timestamp: str  # ISO format
    db_type: str
    database: str
    size_bytes: int
    compressed_size_bytes: int
    tables: list[str] = field(default_factory=list)
    row_counts: dict[str, int] = field(default_factory=dict)
    filename: str = ""
    notes: str = ""
    is_branch: bool = False
    parent_snapshot: str = ""

    @property
    def size_mb(self) -> float:
        return round(self.size_bytes / (1024 * 1024), 2)

    @property
    def compressed_size_mb(self) -> float:
        return round(self.compressed_size_bytes / (1024 * 1024), 2)


class SnapshotStorage:
    """Manages snapshot storage with SQLite index and gzip compression."""

    INDEX_DB = "snapshots.db"

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.snapshot_dir = config.snapshot_path
        self._ensure_dirs()
        self._init_index()

    def _ensure_dirs(self):
        """Create snapshot directory if it doesn't exist."""
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)

    def _index_path(self) -> Path:
        return self.snapshot_dir / self.INDEX_DB

    def _init_index(self):
        """Initialize the SQLite index database."""
        with sqlite3.connect(str(self._index_path())) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS snapshots (
                    name TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    db_type TEXT NOT NULL,
                    database_name TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    compressed_size_bytes INTEGER NOT NULL,
                    tables_json TEXT DEFAULT '[]',
                    row_counts_json TEXT DEFAULT '{}',
                    filename TEXT NOT NULL,
                    notes TEXT DEFAULT '',
                    is_branch INTEGER DEFAULT 0,
                    parent_snapshot TEXT DEFAULT ''
                )
            """)
            conn.commit()

    def _generate_filename(self, name: str) -> str:
        """Generate a unique filename for a snapshot."""
        safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in name)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{safe_name}_{ts}.gz"

    def store_snapshot(
        self,
        name: str,
        data_path: str,
        tables: list[str],
        row_counts: dict[str, int],
        notes: str = "",
        is_branch: bool = False,
        parent_snapshot: str = "",
    ) -> SnapshotMeta:
        """
        Store a snapshot: compress data and save metadata.

        Args:
            name: Snapshot name.
            data_path: Path to the dump file or database file.
            tables: List of table names.
            row_counts: Dict of table -> row count.
            notes: Optional notes.
            is_branch: Whether this is a branch snapshot.
            parent_snapshot: Parent snapshot name if branched.

        Returns:
            SnapshotMeta for the stored snapshot.
        """
        # Check if name already exists
        existing = self.get_snapshot(name)
        if existing:
            raise ValueError(f"Snapshot '{name}' already exists. Delete it first or use a different name.")

        source = Path(data_path)
        if not source.exists():
            raise FileNotFoundError(f"Source file not found: {data_path}")

        original_size = source.stat().st_size
        filename = self._generate_filename(name)
        dest = self.snapshot_dir / filename

        # Compress with gzip
        with open(source, "rb") as f_in:
            with gzip.open(str(dest), "wb", compresslevel=6) as f_out:
                shutil.copyfileobj(f_in, f_out)

        compressed_size = dest.stat().st_size

        meta = SnapshotMeta(
            name=name,
            timestamp=datetime.now().isoformat(),
            db_type=self.config.db_type,
            database=self.config.database or self.config.get_connection_string(),
            size_bytes=original_size,
            compressed_size_bytes=compressed_size,
            tables=tables,
            row_counts=row_counts,
            filename=filename,
            notes=notes,
            is_branch=is_branch,
            parent_snapshot=parent_snapshot,
        )

        # Save to index
        with sqlite3.connect(str(self._index_path())) as conn:
            conn.execute(
                """INSERT INTO snapshots
                   (name, timestamp, db_type, database_name, size_bytes,
                    compressed_size_bytes, tables_json, row_counts_json,
                    filename, notes, is_branch, parent_snapshot)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    meta.name, meta.timestamp, meta.db_type, meta.database,
                    meta.size_bytes, meta.compressed_size_bytes,
                    json.dumps(meta.tables), json.dumps(meta.row_counts),
                    meta.filename, meta.notes, 1 if meta.is_branch else 0,
                    meta.parent_snapshot,
                ),
            )
            conn.commit()

        return meta

    def extract_snapshot(self, name: str, dest_path: str) -> str:
        """
        Extract a compressed snapshot to a file.

        Args:
            name: Snapshot name.
            dest_path: Where to extract the file.

        Returns:
            Path to the extracted file.
        """
        meta = self.get_snapshot(name)
        if not meta:
            raise ValueError(f"Snapshot '{name}' not found.")

        source = self.snapshot_dir / meta.filename
        if not source.exists():
            raise FileNotFoundError(f"Snapshot file missing: {source}")

        with gzip.open(str(source), "rb") as f_in:
            with open(dest_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        return dest_path

    def get_snapshot(self, name: str) -> Optional[SnapshotMeta]:
        """Get snapshot metadata by name."""
        with sqlite3.connect(str(self._index_path())) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM snapshots WHERE name = ?", (name,)).fetchone()

        if not row:
            return None

        return SnapshotMeta(
            name=row["name"],
            timestamp=row["timestamp"],
            db_type=row["db_type"],
            database=row["database_name"],
            size_bytes=row["size_bytes"],
            compressed_size_bytes=row["compressed_size_bytes"],
            tables=json.loads(row["tables_json"]),
            row_counts=json.loads(row["row_counts_json"]),
            filename=row["filename"],
            notes=row["notes"],
            is_branch=bool(row["is_branch"]),
            parent_snapshot=row["parent_snapshot"],
        )

    def list_snapshots(self, include_branches: bool = True) -> list[SnapshotMeta]:
        """List all snapshots, ordered by timestamp descending."""
        with sqlite3.connect(str(self._index_path())) as conn:
            conn.row_factory = sqlite3.Row
            query = "SELECT * FROM snapshots"
            if not include_branches:
                query += " WHERE is_branch = 0"
            query += " ORDER BY timestamp DESC"
            rows = conn.execute(query).fetchall()

        return [
            SnapshotMeta(
                name=r["name"],
                timestamp=r["timestamp"],
                db_type=r["db_type"],
                database=r["database_name"],
                size_bytes=r["size_bytes"],
                compressed_size_bytes=r["compressed_size_bytes"],
                tables=json.loads(r["tables_json"]),
                row_counts=json.loads(r["row_counts_json"]),
                filename=r["filename"],
                notes=r["notes"],
                is_branch=bool(r["is_branch"]),
                parent_snapshot=r["parent_snapshot"],
            )
            for r in rows
        ]

    def delete_snapshot(self, name: str) -> bool:
        """Delete a snapshot and its data file."""
        meta = self.get_snapshot(name)
        if not meta:
            return False

        # Delete compressed file
        data_file = self.snapshot_dir / meta.filename
        if data_file.exists():
            data_file.unlink()

        # Remove from index
        with sqlite3.connect(str(self._index_path())) as conn:
            conn.execute("DELETE FROM snapshots WHERE name = ?", (name,))
            conn.commit()

        return True

    def cleanup(self, policy: Optional[RetentionPolicy] = None) -> list[str]:
        """
        Apply retention policy and clean up old snapshots.

        Returns:
            List of deleted snapshot names.
        """
        policy = policy or self.config.retention
        snapshots = self.list_snapshots()
        deleted = []

        # Delete by age
        if policy.max_age_days > 0:
            cutoff = datetime.now() - timedelta(days=policy.max_age_days)
            for snap in snapshots:
                snap_time = datetime.fromisoformat(snap.timestamp)
                if snap_time < cutoff:
                    if policy.keep_named and not snap.name.startswith("auto_"):
                        continue
                    self.delete_snapshot(snap.name)
                    deleted.append(snap.name)

        # Delete by count (keep newest)
        remaining = self.list_snapshots()
        if policy.max_snapshots > 0 and len(remaining) > policy.max_snapshots:
            to_delete = remaining[policy.max_snapshots:]
            for snap in to_delete:
                if policy.keep_named and not snap.name.startswith("auto_"):
                    continue
                self.delete_snapshot(snap.name)
                deleted.append(snap.name)

        return deleted

    def get_disk_usage(self) -> dict:
        """Get total disk usage of snapshots."""
        snapshots = self.list_snapshots()
        total_original = sum(s.size_bytes for s in snapshots)
        total_compressed = sum(s.compressed_size_bytes for s in snapshots)

        return {
            "total_snapshots": len(snapshots),
            "total_original_mb": round(total_original / (1024 * 1024), 2),
            "total_compressed_mb": round(total_compressed / (1024 * 1024), 2),
            "compression_ratio": round(total_compressed / total_original, 2) if total_original > 0 else 0,
        }
