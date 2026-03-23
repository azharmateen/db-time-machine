"""Click CLI: db save, list, restore, delete, diff, branch."""

from __future__ import annotations

import json
import os
import sys

import click
from rich.console import Console
from rich.table import Table

from . import __version__
from .brancher import Brancher
from .config import DatabaseConfig, init_config, load_config
from .differ import SnapshotDiffer, format_diff
from .restorer import Restorer
from .snapshot import SnapshotEngine
from .storage import SnapshotStorage

console = Console()


def _load_config_safe() -> DatabaseConfig:
    """Load config or return defaults with helpful message."""
    try:
        return load_config()
    except ValueError as e:
        console.print(f"[red]Config error: {e}[/red]")
        sys.exit(1)


def _format_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.1f} MB"


@click.group()
@click.version_option(version=__version__, prog_name="db-time-machine")
def cli():
    """Instant local database snapshots and restore -- like git branches for your DB."""
    pass


@cli.command()
@click.option("--type", "-t", "db_type", type=click.Choice(["sqlite", "postgres", "mysql"]), default="sqlite")
@click.option("--database", "-d", default="", help="Database name or file path")
@click.option("--host", "-h", "host", default="localhost", help="Database host")
@click.option("--port", "-p", default=0, type=int, help="Database port")
@click.option("--username", "-u", default="", help="Database username")
@click.option("--password", default="", help="Database password")
def init(db_type: str, database: str, host: str, port: int, username: str, password: str):
    """Initialize db-time-machine for this project."""
    config = init_config(
        db_type=db_type,
        database=database,
        host=host,
        port=port,
        username=username,
        password=password,
    )
    console.print(f"[green]Initialized db-time-machine[/green]")
    console.print(f"  Type: {config.db_type}")
    console.print(f"  Database: {config.database or config.get_connection_string()}")
    console.print(f"  Snapshots: {config.snapshot_dir}")
    console.print(f"\nConfig saved to .db-time-machine.json")


@cli.command()
@click.argument("name")
@click.option("--notes", "-n", default="", help="Optional notes for this snapshot")
def save(name: str, notes: str):
    """Save a snapshot of the current database state."""
    config = _load_config_safe()
    engine = SnapshotEngine(config)

    try:
        with console.status(f"Saving snapshot '{name}'..."):
            meta = engine.save(name, notes=notes)

        console.print(f"[green]Snapshot saved:[/green] {meta.name}")
        console.print(f"  Database: {meta.database}")
        console.print(f"  Tables: {len(meta.tables)}")
        console.print(f"  Size: {_format_size(meta.size_bytes)} -> {_format_size(meta.compressed_size_bytes)} (compressed)")

        if meta.row_counts:
            total_rows = sum(v for v in meta.row_counts.values() if v >= 0)
            console.print(f"  Total rows: {total_rows:,}")

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


@cli.command(name="list")
@click.option("--branches/--no-branches", default=True, help="Include branches")
@click.option("--json-output", is_flag=True, help="Output as JSON")
def list_snapshots(branches: bool, json_output: bool):
    """List all snapshots."""
    config = _load_config_safe()
    storage = SnapshotStorage(config)
    snapshots = storage.list_snapshots(include_branches=branches)

    if not snapshots:
        console.print("No snapshots found. Run [cyan]db save <name>[/cyan] to create one.")
        return

    if json_output:
        data = [
            {
                "name": s.name,
                "timestamp": s.timestamp,
                "size": s.size_bytes,
                "compressed": s.compressed_size_bytes,
                "tables": len(s.tables),
                "is_branch": s.is_branch,
            }
            for s in snapshots
        ]
        click.echo(json.dumps(data, indent=2))
        return

    table = Table(title="Database Snapshots")
    table.add_column("#", style="dim", width=4)
    table.add_column("Name", style="cyan")
    table.add_column("Timestamp", style="dim")
    table.add_column("Size", justify="right")
    table.add_column("Compressed", justify="right")
    table.add_column("Tables", justify="right")
    table.add_column("Type", style="dim")

    for i, snap in enumerate(snapshots, 1):
        snap_type = "branch" if snap.is_branch else "snapshot"
        name_style = "magenta" if snap.is_branch else "cyan"

        table.add_row(
            str(i),
            f"[{name_style}]{snap.name}[/{name_style}]",
            snap.timestamp[:19],
            _format_size(snap.size_bytes),
            _format_size(snap.compressed_size_bytes),
            str(len(snap.tables)),
            snap_type,
        )

    console.print(table)

    # Show disk usage
    usage = storage.get_disk_usage()
    console.print(
        f"\nTotal: {usage['total_snapshots']} snapshots, "
        f"{usage['total_compressed_mb']} MB on disk "
        f"({usage['compression_ratio']:.0%} compression)"
    )


@cli.command()
@click.argument("name")
@click.option("--no-verify", is_flag=True, help="Skip integrity verification")
@click.confirmation_option(prompt="Are you sure you want to restore? This will overwrite the current database.")
def restore(name: str, no_verify: bool):
    """Restore database from a snapshot."""
    config = _load_config_safe()
    restorer = Restorer(config)

    try:
        with console.status(f"Restoring snapshot '{name}'..."):
            result = restorer.restore(name, verify=not no_verify)

        console.print(f"[green]Restored:[/green] {result['snapshot']}")

        if "verified" in result:
            v = result["verified"]
            if v["passed"]:
                console.print(f"[green]Verification passed[/green] ({v['tables_checked']} tables checked)")
            else:
                console.print("[yellow]Verification issues:[/yellow]")
                for issue in v["issues"]:
                    console.print(f"  - {issue}")

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


@cli.command()
@click.argument("name")
@click.confirmation_option(prompt="Delete this snapshot?")
def delete(name: str):
    """Delete a snapshot."""
    config = _load_config_safe()
    storage = SnapshotStorage(config)

    if storage.delete_snapshot(name):
        console.print(f"[green]Deleted:[/green] {name}")
    else:
        console.print(f"[red]Snapshot '{name}' not found.[/red]")
        sys.exit(1)


@cli.command()
@click.argument("snap1")
@click.argument("snap2")
@click.option("--samples", "-s", default=5, help="Number of sample rows to compare")
def diff(snap1: str, snap2: str, samples: int):
    """Compare two snapshots."""
    config = _load_config_safe()
    differ = SnapshotDiffer(config)

    try:
        result = differ.diff(snap1, snap2, sample_rows=samples)
        console.print(format_diff(result))
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


@cli.command()
@click.argument("name")
@click.option("--from-snapshot", "-f", default=None, help="Branch from a specific snapshot")
def branch(name: str, from_snapshot: str | None):
    """Create a database branch for experimentation."""
    config = _load_config_safe()
    brancher = Brancher(config)

    try:
        with console.status(f"Creating branch '{name}'..."):
            result = brancher.create_branch(name, from_snapshot=from_snapshot)

        console.print(f"[green]Branch created:[/green] {result.name}")
        console.print(f"  Source: {result.source_snapshot}")
        console.print(f"  Database: {result.db_path}")
        console.print(f"  Type: {result.db_type}")

        if result.db_type == "sqlite":
            console.print(f"\nConnect to branch:")
            console.print(f"  sqlite3 {result.db_path}")
        elif result.db_type == "postgres":
            console.print(f"\nConnect to branch:")
            console.print(f"  psql {result.db_path}")

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


@cli.command()
def status():
    """Show current database and snapshot status."""
    config = _load_config_safe()
    storage = SnapshotStorage(config)

    console.print(f"[bold]Database:[/bold] {config.db_type}")
    console.print(f"[bold]Connection:[/bold] {config.get_connection_string()}")
    console.print(f"[bold]Snapshot dir:[/bold] {config.snapshot_dir}")

    usage = storage.get_disk_usage()
    console.print(f"\n[bold]Snapshots:[/bold] {usage['total_snapshots']}")
    console.print(f"[bold]Disk usage:[/bold] {usage['total_compressed_mb']} MB")
    console.print(f"[bold]Compression:[/bold] {usage['compression_ratio']:.0%}")

    # Show retention policy
    console.print(f"\n[bold]Retention:[/bold]")
    console.print(f"  Max snapshots: {config.retention.max_snapshots}")
    console.print(f"  Max age: {config.retention.max_age_days} days")
    console.print(f"  Keep named: {config.retention.keep_named}")


def main():
    cli()


if __name__ == "__main__":
    main()
