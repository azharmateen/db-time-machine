"""Diff two snapshots: compare table schemas, row counts, sample data differences."""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .config import DatabaseConfig
from .storage import SnapshotMeta, SnapshotStorage


@dataclass
class TableDiff:
    """Differences for a single table."""

    table_name: str
    status: str  # "added", "removed", "modified", "unchanged"
    row_count_before: int = 0
    row_count_after: int = 0
    row_count_delta: int = 0
    schema_changes: list[str] = field(default_factory=list)
    sample_differences: list[dict] = field(default_factory=list)


@dataclass
class SnapshotDiff:
    """Complete diff between two snapshots."""

    snap_before: str
    snap_after: str
    tables_added: list[str] = field(default_factory=list)
    tables_removed: list[str] = field(default_factory=list)
    tables_modified: list[str] = field(default_factory=list)
    tables_unchanged: list[str] = field(default_factory=list)
    table_diffs: list[TableDiff] = field(default_factory=list)
    size_before: int = 0
    size_after: int = 0
    size_delta: int = 0

    @property
    def has_changes(self) -> bool:
        return bool(self.tables_added or self.tables_removed or self.tables_modified)


class SnapshotDiffer:
    """Compare two database snapshots."""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.storage = SnapshotStorage(config)

    def diff(self, name_before: str, name_after: str, sample_rows: int = 5) -> SnapshotDiff:
        """
        Diff two snapshots.

        Args:
            name_before: Name of the first (older) snapshot.
            name_after: Name of the second (newer) snapshot.
            sample_rows: Number of sample rows to compare for differences.

        Returns:
            SnapshotDiff with all differences.
        """
        meta_before = self.storage.get_snapshot(name_before)
        meta_after = self.storage.get_snapshot(name_after)

        if not meta_before:
            raise ValueError(f"Snapshot '{name_before}' not found.")
        if not meta_after:
            raise ValueError(f"Snapshot '{name_after}' not found.")

        db_type = self.config.db_type.lower()
        if db_type == "sqlite":
            return self._diff_sqlite(meta_before, meta_after, sample_rows)
        else:
            return self._diff_metadata_only(meta_before, meta_after)

    def _diff_metadata_only(self, before: SnapshotMeta, after: SnapshotMeta) -> SnapshotDiff:
        """Diff using only metadata (for PostgreSQL/MySQL without extracting)."""
        tables_before = set(before.tables)
        tables_after = set(after.tables)

        added = sorted(tables_after - tables_before)
        removed = sorted(tables_before - tables_after)
        common = sorted(tables_before & tables_after)

        modified = []
        unchanged = []
        table_diffs = []

        for table in common:
            count_before = before.row_counts.get(table, -1)
            count_after = after.row_counts.get(table, -1)

            if count_before != count_after:
                modified.append(table)
                table_diffs.append(TableDiff(
                    table_name=table,
                    status="modified",
                    row_count_before=count_before,
                    row_count_after=count_after,
                    row_count_delta=count_after - count_before,
                ))
            else:
                unchanged.append(table)
                table_diffs.append(TableDiff(
                    table_name=table,
                    status="unchanged",
                    row_count_before=count_before,
                    row_count_after=count_after,
                ))

        for table in added:
            count = after.row_counts.get(table, -1)
            table_diffs.append(TableDiff(
                table_name=table,
                status="added",
                row_count_after=count,
                row_count_delta=count,
            ))

        for table in removed:
            count = before.row_counts.get(table, -1)
            table_diffs.append(TableDiff(
                table_name=table,
                status="removed",
                row_count_before=count,
                row_count_delta=-count,
            ))

        return SnapshotDiff(
            snap_before=before.name,
            snap_after=after.name,
            tables_added=added,
            tables_removed=removed,
            tables_modified=modified,
            tables_unchanged=unchanged,
            table_diffs=table_diffs,
            size_before=before.size_bytes,
            size_after=after.size_bytes,
            size_delta=after.size_bytes - before.size_bytes,
        )

    def _diff_sqlite(
        self, before: SnapshotMeta, after: SnapshotMeta, sample_rows: int
    ) -> SnapshotDiff:
        """Full diff for SQLite snapshots by extracting and comparing."""
        tmp_before = tempfile.mktemp(suffix="_before.db")
        tmp_after = tempfile.mktemp(suffix="_after.db")

        try:
            self.storage.extract_snapshot(before.name, tmp_before)
            self.storage.extract_snapshot(after.name, tmp_after)

            conn_before = sqlite3.connect(tmp_before)
            conn_after = sqlite3.connect(tmp_after)

            # Get tables
            tables_before = set(self._get_tables(conn_before))
            tables_after = set(self._get_tables(conn_after))

            added = sorted(tables_after - tables_before)
            removed = sorted(tables_before - tables_after)
            common = sorted(tables_before & tables_after)

            modified = []
            unchanged = []
            table_diffs = []

            for table in common:
                td = self._diff_table(conn_before, conn_after, table, sample_rows)
                table_diffs.append(td)
                if td.status == "modified":
                    modified.append(table)
                else:
                    unchanged.append(table)

            for table in added:
                count = conn_after.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                table_diffs.append(TableDiff(
                    table_name=table,
                    status="added",
                    row_count_after=count,
                    row_count_delta=count,
                ))

            for table in removed:
                count = conn_before.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                table_diffs.append(TableDiff(
                    table_name=table,
                    status="removed",
                    row_count_before=count,
                    row_count_delta=-count,
                ))

            conn_before.close()
            conn_after.close()

            return SnapshotDiff(
                snap_before=before.name,
                snap_after=after.name,
                tables_added=added,
                tables_removed=removed,
                tables_modified=modified,
                tables_unchanged=unchanged,
                table_diffs=table_diffs,
                size_before=before.size_bytes,
                size_after=after.size_bytes,
                size_delta=after.size_bytes - before.size_bytes,
            )
        finally:
            for f in (tmp_before, tmp_after):
                if os.path.exists(f):
                    os.unlink(f)

    def _get_tables(self, conn: sqlite3.Connection) -> list[str]:
        """Get table names from a SQLite connection."""
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        return [row[0] for row in cursor.fetchall()]

    def _get_schema(self, conn: sqlite3.Connection, table: str) -> str:
        """Get CREATE TABLE statement."""
        cursor = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
        )
        row = cursor.fetchone()
        return row[0] if row else ""

    def _diff_table(
        self, conn_before: sqlite3.Connection, conn_after: sqlite3.Connection,
        table: str, sample_rows: int
    ) -> TableDiff:
        """Diff a single table between two SQLite databases."""
        count_before = conn_before.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        count_after = conn_after.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]

        schema_before = self._get_schema(conn_before, table)
        schema_after = self._get_schema(conn_after, table)

        schema_changes = []
        if schema_before != schema_after:
            schema_changes.append(f"Schema changed from:\n  {schema_before}\nto:\n  {schema_after}")

        # Sample data comparison
        sample_diffs = []
        if sample_rows > 0 and count_before > 0 and count_after > 0:
            try:
                cols_before = [
                    desc[1] for desc in
                    conn_before.execute(f'PRAGMA table_info("{table}")').fetchall()
                ]
                cols_after = [
                    desc[1] for desc in
                    conn_after.execute(f'PRAGMA table_info("{table}")').fetchall()
                ]

                common_cols = [c for c in cols_before if c in cols_after]
                if common_cols:
                    cols_str = ", ".join(f'"{c}"' for c in common_cols)
                    rows_b = conn_before.execute(
                        f'SELECT {cols_str} FROM "{table}" LIMIT {sample_rows}'
                    ).fetchall()
                    rows_a = conn_after.execute(
                        f'SELECT {cols_str} FROM "{table}" LIMIT {sample_rows}'
                    ).fetchall()

                    for i, (rb, ra) in enumerate(zip(rows_b, rows_a)):
                        if rb != ra:
                            sample_diffs.append({
                                "row": i,
                                "before": dict(zip(common_cols, rb)),
                                "after": dict(zip(common_cols, ra)),
                            })
            except sqlite3.Error:
                pass

        is_modified = (
            count_before != count_after
            or bool(schema_changes)
            or bool(sample_diffs)
        )

        return TableDiff(
            table_name=table,
            status="modified" if is_modified else "unchanged",
            row_count_before=count_before,
            row_count_after=count_after,
            row_count_delta=count_after - count_before,
            schema_changes=schema_changes,
            sample_differences=sample_diffs,
        )


def format_diff(diff: SnapshotDiff) -> str:
    """Format a SnapshotDiff for terminal display."""
    lines = [
        f"\nDiff: {diff.snap_before} -> {diff.snap_after}",
        "=" * 60,
    ]

    if not diff.has_changes:
        lines.append("No changes detected.")
        return "\n".join(lines)

    # Size change
    if diff.size_delta != 0:
        sign = "+" if diff.size_delta > 0 else ""
        lines.append(f"\nSize: {sign}{diff.size_delta / 1024:.1f} KB")

    # Tables added/removed
    if diff.tables_added:
        lines.append(f"\n+ Tables added ({len(diff.tables_added)}):")
        for t in diff.tables_added:
            lines.append(f"    + {t}")

    if diff.tables_removed:
        lines.append(f"\n- Tables removed ({len(diff.tables_removed)}):")
        for t in diff.tables_removed:
            lines.append(f"    - {t}")

    # Modified tables
    if diff.tables_modified:
        lines.append(f"\n~ Tables modified ({len(diff.tables_modified)}):")
        for td in diff.table_diffs:
            if td.status == "modified":
                sign = "+" if td.row_count_delta > 0 else ""
                lines.append(
                    f"    ~ {td.table_name}: "
                    f"{td.row_count_before} -> {td.row_count_after} rows "
                    f"({sign}{td.row_count_delta})"
                )
                for sc in td.schema_changes:
                    lines.append(f"      Schema: {sc}")
                for sd in td.sample_differences[:3]:
                    lines.append(f"      Row {sd['row']}: {sd['before']} -> {sd['after']}")

    lines.append(f"\nUnchanged: {len(diff.tables_unchanged)} tables")
    return "\n".join(lines)
