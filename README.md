<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://getlore.sh/banner-dark.svg">
    <img src="https://getlore.sh/banner-light.svg" alt="Lore" height="48" />
  </picture>
</p>

<p align="center">
  <strong>The database of your life.</strong><br>
  One SQLite file on your machine. AI structures everything. You just talk.
</p>

<p align="center">
  <a href="https://getlore.sh">Website</a> &nbsp;&middot;&nbsp; <a href="https://getlore.sh/docs/getting-started">Docs</a> &nbsp;&middot;&nbsp; <a href="https://getlore.sh/docs/cli-reference">CLI Reference</a>
</p>

---

Lore is a local, versioned database managed by AI agents. You describe data in natural language — AI handles schema design, migrations, and data operations. Your data stays on your machine, versioned like Git, never locked into a SaaS.

## Install

```bash
curl -fsSL https://getlore.sh/install | bash
```

Supports macOS and Linux (x64 / arm64). Binary installs to `~/.local/bin/lore`.

## Quick Start

```bash
# Initialize your database and install AI skill files
lore init

# Check what's in your database
lore status
```

Once initialized, your AI agent (Claude Code, Cursor, Codex, etc.) can start managing data immediately. The agent reads the schema, creates tables as needed, and applies changes through structured JSON plans.

**You say:** "I had coffee with Sarah today, spent $4.50"

**The agent:**

1. Runs `lore schema` to check the current state
2. Creates a plan to add a table (if needed) and insert a row
3. Runs `lore apply -f -` with the plan via stdin

Read your data back anytime:

```bash
lore read --sql "SELECT * FROM expenses ORDER BY date DESC"
```

## How It Works

Lore separates **AI planning** from **database execution**. The AI never touches SQLite directly.

```
You: "dentist on Feb 3"
        |
        v
 +-----------------+
 | AI Planner       |  ← Reads schema, generates JSON plan
 | (Claude, Cursor) |
 +--------+--------+
          | stdin
          v
    +-----------+
    | lore apply |     ← Validates, executes atomically, commits
    +-----+-----+
          |
          v
      lore.db
```

Every change is wrapped in a transaction, tracked with before/after effects, and recorded as a content-addressable commit (SHA-256). Schema changes and data changes live in the same commit — no migration files.

## Commands

| Command | Description |
|---------|-------------|
| `lore init` | Initialize database and install AI skill files |
| `lore update [--version <semver>]` | Update Lore CLI to latest (or specific) release |
| `lore schema [table]` | Print current database schema |
| `lore plan -f <file\|->` | Dry-run a plan (validate without applying) |
| `lore apply -f <file\|->` | Apply a plan and create a versioned commit |
| `lore read --sql "..."` | Query data (SELECT only) |
| `lore status` | Show database state |
| `lore history` | List commit history |
| `lore revert <commit>` | Revert a commit (creates a new reverse commit) |
| `lore verify` | Verify commit chain integrity |
| `lore studio` | Launch web UI for browsing data |
| `lore push` / `pull` / `sync` | Remote sync via Turso/libsql |
| `lore clone <url>` | Clone a remote database locally |

## AI Integration

`lore init` generates skill files that teach AI agents how to use Lore:

| Platform | Skill location |
|----------|---------------|
| Claude Code | `~/.claude/skills/lore/` |
| Cursor | `~/.cursor/rules/lore.mdc` |
| Codex CLI | `~/.agents/skills/lore/` |
| OpenCode | `~/.agents/skills/lore/` |
| OpenClaw | `~/.openclaw/workspace/skills/lore/` |

```bash
# Install for specific platforms
lore init --platforms claude,cursor

# Skip skill files entirely
lore init --no-skills
```

## Architecture

**Two-layer model** inspired by Git:

- **Layer 1 — Commit Log** (append-only, immutable): Every operation is recorded with full effects, forming a content-addressable chain. This is the source of truth.
- **Layer 2 — HEAD State** (derived, queryable): The current database state, reconstructable from the commit log at any time.

**Safety guarantees:**

- **Schema hash guard** — plans include a hash of the schema they were built against. Stale plans are rejected.
- **Atomic transactions** — all operations in a plan succeed or none do.
- **Effect tracking** — row-level and schema-level before/after snapshots on every commit.
- **Snapshots & recovery** — periodic snapshots with full commit replay verification.

## Development

```bash
# Run CLI from source
bun run lore

# Type check all packages
bun run typecheck

# Run tests
bun test
```

### Project Structure

```
packages/
  cli/       Command-line interface
  core/      Database engine (commit log, operations, sync)
  studio/    Web UI (Hono + React + TanStack)
  web/       Documentation site (Astro)
  types/     Shared type definitions
```

## License

[MIT](LICENSE)
