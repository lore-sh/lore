---
title: Remote Sync
description: Sync your database with Turso or LibSQL remotes using push, pull, and clone.
order: 7
---

# Remote Sync

Lore can sync your local database with a remote Turso or LibSQL instance, giving you backups and multi-device access.

## Supported Platforms

| Platform | Description |
|----------|-------------|
| **Turso** | Managed database service. Supports `https://` and `libsql://` URLs. |
| **LibSQL** | Self-hosted or non-Turso LibSQL endpoints. |

## Connect

### Interactive

```bash
lore remote connect
```

Prompts for platform, URL, and auth token.

### Non-interactive

```bash
lore remote connect --platform turso --url https://mydb-myorg.turso.io --token <token>
```

| Flag | Description |
|------|-------------|
| `--platform <turso\|libsql>` | Remote platform |
| `--url <url>` | Database URL |
| `--token <token>` | Auth token |
| `--clear-token` | Remove stored token |

Connection settings are saved to `~/.lore/config.json`. Tokens are stored separately in `~/.lore/credentials.json` with restricted permissions.

## Push

Send local commits to the remote:

```bash
lore push
```

Push reads all local commits after the remote's HEAD and sends them sequentially. If the remote has commits you don't have locally, push fails with `SYNC_NON_FAST_FORWARD` — run `lore pull` first.

## Pull

Fetch remote commits to your local database:

```bash
lore pull
```

Pull finds the common ancestor between local and remote histories, then fetches and replays all remote commits after that point. Each replayed commit is fully hash-verified.

If the histories have diverged with no common ancestor, pull fails with `SYNC_DIVERGED`.

## Sync

Pull then push in one command:

```bash
lore sync
```

## Clone

Set up a new local database from a remote:

```bash
lore clone <url> --platform <turso|libsql> [--force-new]
```

This creates a fresh local database, saves the remote configuration, and pulls all commits. Use `--force-new` to overwrite an existing local database.

## Auto-Sync

After every `lore apply`, Lore automatically attempts to push if a remote is configured. Errors are handled silently — auto-sync won't interrupt your workflow.

## Sync State

Check the sync status:

```bash
lore remote status
```

The sync state is one of:

| State | Meaning |
|-------|---------|
| `synced` | Local HEAD matches last pushed commit |
| `pending` | Local commits not yet pushed |
| `conflict` | Histories diverged or non-fast-forward |
| `offline` | No remote configured |

## CI Usage

In CI environments where interactive prompts aren't available, use the `TURSO_AUTH_TOKEN` environment variable as a fallback for authentication:

```bash
export TURSO_AUTH_TOKEN="your-token"
lore push
```
