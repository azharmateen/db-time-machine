# db-time-machine

**Git branches for your database. Save. Branch. Restore. Diff.**

Instant local database snapshots with compression, integrity verification, and branch support. Works with SQLite, PostgreSQL, and MySQL.

```
$ db save "before-migration"
Snapshot saved: before-migration
  Database: app.db
  Tables: 12
  Size: 48.2 MB -> 11.3 MB (compressed)
  Total rows: 284,391

$ db list
  #  Name               Timestamp            Size      Compressed  Tables
  1  before-migration   2026-03-24 14:30:01  48.2 MB   11.3 MB    12
  2  after-seeding      2026-03-24 12:15:44  32.1 MB   7.8 MB     12
  3  clean-slate        2026-03-23 09:00:00  1.2 MB    0.3 MB     12

$ db restore "before-migration"
Restored: before-migration
Verification passed (12 tables checked)
```

## Why?

- Migrations go wrong. You need a **one-command rollback**
- You want to **experiment** with data without fear
- `pg_dump` and `mysqldump` are verbose and easy to get wrong
- You need to **diff** what changed between two points in time
- You want **branches** -- try something, then throw it away

## Install

```bash
pip install db-time-machine
```

## Quick Start

```bash
# Initialize for your project
db init --type sqlite --database ./app.db

# Save a snapshot
db save "before-migration"

# Run your migration...
python manage.py migrate

# Something broke? Restore instantly
db restore "before-migration"

# Compare two snapshots
db diff "before-migration" "after-migration"

# Create an experimental branch
db branch "experiment"
```

## Commands

| Command | Description |
|---------|-------------|
| `db init` | Initialize for a project |
| `db save <name>` | Save a snapshot |
| `db list` | List all snapshots |
| `db restore <name>` | Restore from snapshot |
| `db delete <name>` | Delete a snapshot |
| `db diff <snap1> <snap2>` | Compare two snapshots |
| `db branch <name>` | Create experimental branch |
| `db status` | Show current status |

## Database Support

| Feature | SQLite | PostgreSQL | MySQL |
|---------|--------|------------|-------|
| Snapshot | Copy + backup API | pg_dump | mysqldump |
| Restore | Replace file | psql | mysql |
| Diff | Full (schema + data) | Metadata | Metadata |
| Branch | New file | New database | New database |
| Compression | gzip | gzip | gzip |
| Verification | integrity_check | - | - |

## Configuration

```json
{
  "db_type": "sqlite",
  "database": "./app.db",
  "snapshot_dir": ".db-snapshots",
  "retention": {
    "max_snapshots": 50,
    "max_age_days": 90,
    "keep_named": true
  }
}
```

## How It Works

1. **Save**: Dumps database using native tools (SQLite backup API, pg_dump, mysqldump)
2. **Compress**: gzip compression (typically 60-80% reduction)
3. **Index**: SQLite index tracks metadata, table info, row counts
4. **Restore**: Extracts and restores using native tools with integrity verification
5. **Diff**: Compares schemas, row counts, and sample data between snapshots

## License

MIT
