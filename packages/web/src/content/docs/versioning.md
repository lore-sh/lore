---
title: Versioning
description: How Lore versions every change with commits, effect tracking, and revert.
order: 5
---

# Versioning

Every change in Lore produces a versioned commit with full before/after tracking. Nothing is lost.

## Commits

Each `lore apply` or `lore revert` creates a commit stored in the internal `_lore_commit` table. A commit records:

| Field | Description |
|-------|-------------|
| `commitId` | SHA-256 hash of all commit contents — deterministic and tamper-evident |
| `seq` | Monotonically increasing sequence number |
| `kind` | `"apply"` or `"revert"` |
| `message` | The commit message from the plan |
| `createdAt` | Unix milliseconds timestamp |
| `schemaHashBefore` | Schema state before this commit |
| `schemaHashAfter` | Schema state after this commit |
| `stateHashAfter` | Full database state hash after this commit |
| `planHash` | SHA-256 of the operations array |
| `revertible` | Whether this commit can be reverted |

The `commitId` is computed as a SHA-256 hash over all fields, operations, row effects, and schema effects. This makes every commit tamper-evident — changing any detail would change the ID.

## Effect Tracking

Every commit records what changed at two levels:

### Row Effects

For each row modified by a commit, Lore stores:

- The table name and primary key
- The operation kind: `insert`, `update`, or `delete`
- The full row as JSON before and after the change
- SHA-256 hashes of both states

This means you can always see exactly what a commit changed — not just that it ran an `UPDATE`, but the precise before and after values of every affected row.

### Schema Effects

When a commit modifies the schema (creating tables, adding columns, etc.), Lore stores complete table snapshots before and after — including the DDL, all rows, and secondary objects like indexes and triggers.

## History

View your commit history:

```bash
lore history
```

This lists all commits in reverse chronological order, showing the commit ID, message, kind, and timestamp.

For more detail:

```bash
lore history --verbose
```

Adds parent count, revert target (for revert commits), state hash, and revertibility status.

## Revert

Undo a previous commit by creating an inverse commit:

```bash
lore revert <commit_id>
```

Revert is non-destructive — it doesn't delete the original commit. Instead, it creates a new commit of kind `"revert"` that applies the inverse of the original operations.

### Conflict Detection

Before reverting, Lore checks for conflicts:

- **Row conflicts** — if later commits modified the same rows, the current state may not match what the revert expects
- **Schema conflicts** — if later commits changed the same tables' schema
- **Foreign key violations** — if the inverse operations would break referential integrity

If conflicts are found, the revert is rejected with detailed conflict information so the AI can handle the situation.

### Refs and Reflog

Lore maintains a `main` ref that points to the current HEAD commit. Every commit or revert updates this ref and appends an entry to the reflog with the old and new commit IDs and the reason (`"apply"` or `"revert"`).
