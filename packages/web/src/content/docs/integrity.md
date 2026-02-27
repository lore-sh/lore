---
title: Integrity
description: Verify the hash chain, recover from snapshots, and ensure data integrity.
order: 6
---

# Integrity

Lore maintains a tamper-evident hash chain across all commits and provides tools to verify and recover your database.

## Verify

Check that your database is intact:

```bash
lore verify
```

Quick mode (default) performs two checks:

1. **Hash chain validation** — iterates every commit in sequence order, recomputes each `commitId` from its stored fields, and compares. Any tampering or corruption is detected.
2. **SQLite quick check** — runs `PRAGMA quick_check` to verify page-level integrity.

For a thorough scan:

```bash
lore verify --full
```

Full mode adds `PRAGMA integrity_check`, which performs a complete B-tree scan of the entire database file.

The output includes:

| Field | Description |
|-------|-------------|
| `ok` | Overall pass/fail |
| `mode` | `"quick"` or `"full"` |
| `chainValid` | Whether the hash chain is intact |
| `quickCheck` | SQLite quick check result |
| `integrityCheck` | Full integrity result (full mode only) |
| `issues` | Array of any problems found |

Lore records the last verification time so agents can periodically check integrity.

## Snapshots

Lore automatically takes snapshots of your database every **100 commits**. Up to **20 snapshots** are retained; older ones are pruned.

A snapshot is a clean copy of the database created with SQLite's `VACUUM INTO` command. Each snapshot is stored in `~/.lore/snapshots/` with a filename like `42-abc123...def.db` (sequence number and commit ID).

The SHA-256 hash of each snapshot file is recorded internally, so recovery can verify the snapshot hasn't been modified.

## Recover

If your database is corrupted, recover from a snapshot:

```bash
lore recover <commit_id>
```

The recovery process:

1. Finds the snapshot associated with the given commit ID
2. Copies the snapshot to a staging location
3. Replays all commits that occurred after the snapshot, verifying every hash
4. Atomically replaces the live database with the recovered copy

Each replayed commit is fully verified — schema hashes, plan hashes, and the complete commit ID must all match. If any commit fails verification during replay, recovery stops with an error.

## Hash Functions

All hashes in Lore use SHA-256. Values are converted to a canonical JSON format (with sorted object keys) before hashing. This ensures deterministic results regardless of insertion order.

The hash chain means that each commit's ID depends on its own content, making it impossible to modify historical data without breaking the chain — which `lore verify` would detect.
