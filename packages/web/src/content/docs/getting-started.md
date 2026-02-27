---
title: Getting Started
description: Install Lore and create your first AI-managed database in under a minute.
order: 1
---

# Getting Started

Lore is a local SQLite database managed by AI agents. You talk naturally — your data is structured, versioned, and stored on your machine.

## Install

```bash
curl -fsSL https://getlore.sh/install | bash
```

This downloads the latest binary for your platform (macOS or Linux, x64 or arm64) to `~/.local/bin/lore`.

You can customize the install with environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `LORE_INSTALL_DIR` | `~/.local/bin` | Where to place the binary |
| `LORE_VERSION` | latest | Pin a specific release tag |

Verify the installation:

```bash
lore --version
```

## Initialize

```bash
lore init
```

This creates your database at `~/.lore/lore.db` and installs AI skill files for your coding platforms.

You'll be prompted to select which platforms to configure:

- **Claude Code** — writes skill files to `~/.claude/skills/lore/`
- **Cursor** — writes a rule to `~/.cursor/rules/lore.mdc`
- **Codex CLI** — writes to `~/.agents/skills/lore/`
- **OpenCode** — writes to `~/.agents/skills/lore/`
- **OpenClaw** — writes to `~/.openclaw/workspace/skills/lore/`

Or skip the prompt:

```bash
# Install for specific platforms
lore init --platforms claude,cursor

# Install for all platforms, accept all defaults
lore init --yes

# Skip skill file generation entirely
lore init --no-skills
```

## First Use

Once initialized, your AI agent can start storing data immediately. The agent reads the current schema, creates tables as needed, and applies changes — all through structured JSON plans.

Here's what a typical interaction looks like:

**You say:** "I had coffee with Sarah today, spent $4.50"

**The agent:**
1. Runs `lore schema` to check the current database state
2. Creates a plan to add a table (if needed) and insert a row
3. Runs `lore apply -f -` with the plan

You can read your data back anytime:

```bash
lore read --sql "SELECT * FROM expenses ORDER BY date DESC"
```

Or check what's been stored:

```bash
lore status
```

## What's Next

- [How It Works](/docs/how-it-works) — understand the two-layer architecture
- [CLI Reference](/docs/cli-reference) — full list of commands and flags
- [AI Integration](/docs/ai-integration) — how skill files teach AI agents to use Lore
