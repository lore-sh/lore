---
title: CLI Reference
description: Complete reference for all Lore CLI commands, flags, and options.
order: 3
---

# CLI Reference

## Global Options

```bash
lore --version    # Print version
lore --help       # Show help
```

## Commands

### lore update

Update Lore CLI to the latest release (or install a specific version).

```bash
lore update [--version <semver>]
```

### lore init

Initialize a new Lore database and install AI skill files.

```bash
lore init [options]
```

| Flag | Description |
|------|-------------|
| `--platforms <list>` | Comma-separated platforms: `claude`, `cursor`, `codex`, `opencode`, `openclaw` |
| `--no-skills` | Skip skill file generation |
| `--no-heartbeat` | Skip OpenClaw heartbeat setup |
| `--force-new` | Delete existing database and start fresh |
| `--yes` | Accept all defaults (all platforms) |
| `--json` | Output JSON result |

### lore clean

Remove all Lore-generated skill files and managed blocks.

```bash
lore clean [--yes] [--json]
```

### lore schema

Print the current database schema.

```bash
lore schema [<table>]
```

Without arguments, prints all tables with their columns, types, and constraints. With a table name, prints only that table's schema. The output includes `schemaHash`; copy it into plan `baseSchemaHash`.

### lore plan

Dry-run a plan without applying changes.

```bash
lore plan -f <file|->
```

Pass `-` to read from stdin. Returns risk level, predicted effects, and any validation errors.
If `baseSchemaHash` does not match the current schema, this returns `STALE_PLAN` without running a dry-run simulation.

### lore apply

Apply a plan to the database.

```bash
lore apply -f <file|->
```

Pass `-` to read from stdin. Executes all operations atomically and creates a versioned commit.

### lore read

Query the database with a SELECT statement.

```bash
lore read --sql "<SELECT ...>" [--json]
```

Only `SELECT` statements are allowed. Use `--json` for machine-readable output.

### lore status

Show the current database status.

```bash
lore status [--json]
```

Reports the current commit, table count, row counts, remote sync state, and last verification time.

### lore history

List commit history.

```bash
lore history [--verbose] [--json]
```

Shows all commits in reverse chronological order. With `--verbose`, includes parent count, revert target, state hash, and revertibility.

### lore revert

Create an inverse commit that undoes a previous commit.

```bash
lore revert <commit_id>
```

Revert checks for conflicts with later commits. If later changes touch the same rows or schema, the revert is rejected with conflict details.

### lore verify

Check database integrity and hash chain validity.

```bash
lore verify [--full]
```

Quick mode (default) recomputes all commit IDs and runs `PRAGMA quick_check`. Full mode adds a complete B-tree integrity scan with `PRAGMA integrity_check`.

### lore recover

Restore the database from a snapshot and replay commits.

```bash
lore recover <commit_id>
```

Finds the snapshot for the given commit, copies it, then replays all subsequent commits with full hash verification.

### lore remote connect

Configure a remote database for syncing.

```bash
# Interactive
lore remote connect

# Non-interactive
lore remote connect --platform <turso|libsql> --url <url> [--token <token>|--clear-token]
```

### lore remote status

Show remote connection and sync state.

```bash
lore remote status
```

### lore push

Push local commits to the remote.

```bash
lore push
```

Fails with `SYNC_NON_FAST_FORWARD` if the remote has commits you don't have locally. Run `lore pull` first.

### lore pull

Pull remote commits to the local database.

```bash
lore pull
```

### lore sync

Pull then push — synchronize in both directions.

```bash
lore sync
```

### lore clone

Clone a remote database to a new local instance.

```bash
lore clone <url> --platform <turso|libsql> [--force-new]
```

Creates a fresh local database, connects to the remote, and pulls all commits.

### lore studio

Open a web-based database viewer.

```bash
lore studio [--port <n>] [--no-open]
```

## Configuration Files

| File | Purpose |
|------|---------|
| `~/.lore/lore.db` | Your database |
| `~/.lore/config.json` | Remote connection settings |
| `~/.lore/credentials.json` | Auth tokens (chmod 600) |
| `~/.lore/snapshots/` | Automatic database snapshots |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `TURSO_AUTH_TOKEN` | Auth token fallback for Turso (useful in CI) |
| `LORE_INSTALL_DIR` | Override binary install location |
| `LORE_VERSION` | Pin installer to a specific release |
