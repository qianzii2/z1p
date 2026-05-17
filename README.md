# z1p

An interactive CLI for exploring and editing Parquet files with SQL. Powered by [Apache DataFusion](https://arrow.apache.org/datafusion/).

## Features

- **SQL-first** — Query Parquet files with standard SQL (SELECT, INSERT, UPDATE, DELETE, JOIN, GROUP BY, etc.)
- **Two access modes**
  - `OPEN` — Read-only, zero-copy access via DataFusion ListingTable (open multiple at once)
  - `USE` — Read-write, full data loaded into memory as MemTable (exclusive), changes saved on `CLOSE USE` or `EXIT`
- **Tab completion** — SQL keywords, table names, and `table.column` references
- **Command history** — Persisted across sessions via rustyline
- **Export** — Write query results to Parquet with configurable compression (snappy, gzip, zstd, lz4, uncompressed)
- **File association** — Register `.parquet` files to open directly in `z1p` on double-click (Windows)

## Install

```bash
cargo install --path .
```

Or build from source:

```bash
cargo build --release
./target/release/z1p
```

## Quick Start

```bash
# Interactive mode
z1p

# Open a file directly (auto-enters USE mode)
z1p data.parquet

# Register .parquet file association (run once as admin, then restart Explorer)
z1p --register
```

## Commands

| Command | Description |
|---|---|
| `OPEN 'file.parquet' AS t` | Open as read-only table (can open many) |
| `USE 'file.parquet' [AS t]` | Open as read-write table (exclusive; auto-names from file stem if `AS` omitted) |
| `CLOSE USE` | Save changes and release the USE table |
| `CLOSE t` | Deregister an OPEN'd table |
| `LIST` | Show all open tables |
| `SCHEMA t` | Print column names and types |
| `EXPORT 'out.parquet' [WITH (compression=...)]` | Export last result set |
| `EXIT` / `QUIT` | Exit; auto-saves any USE table first |

## SQL Examples

```sql
-- Read-only: open multiple files and query
OPEN 'users.parquet' AS users;
OPEN 'orders.parquet' AS orders;

SELECT u.name, COUNT(o.id) AS order_count
FROM users u
JOIN orders o ON u.id = o.user_id
GROUP BY u.name
ORDER BY order_count DESC
LIMIT 10;

-- Read-write: load into memory, modify, save
USE 'data.parquet' AS d;

INSERT INTO d VALUES (1, 'Alice'), (2, 'Bob');

UPDATE d SET name = 'Charlie' WHERE id = 1;

DELETE FROM d WHERE id < 0;

CLOSE USE;
```

## Compression

```sql
EXPORT 'out.parquet' WITH (compression=gz);   -- gzip
EXPORT 'out.parquet' WITH (compression=zst);  -- zstd
EXPORT 'out.parquet' WITH (compression=lz4);  -- lz4
EXPORT 'out.parquet' WITH (compression=none);  -- uncompressed
-- default: snappy
```

## Architecture

```
Session
  ctx: SessionContext       -- DataFusion query engine
  tables: HashMap            -- OPEN'd ListingTables (read-only)
  use_table: Option<UseSession>  -- USE'd MemTable (read-write)
  last_result: Vec<RecordBatch>  -- last query output (for EXPORT)
```

- `OPEN` uses DataFusion's `ListingTable` — zero-copy, streaming reads
- `USE` loads data into DataFusion's `MemTable` — supports INSERT/UPDATE/DELETE, written back on close
- All SQL execution goes through DataFusion's query planner

## Tech Stack

- [DataFusion](https://arrow.apache.org/datafusion/) — SQL engine
- [arrow-rs](https://github.com/apache/arrow-rs) — Arrow data structures
- [parquet-rs](https://github.com/apache/arrow-datafusion) — Parquet read/write
- [rustyline](https://github.com/editorfilerustyline) — Readline-style editing + tab completion
