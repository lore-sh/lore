---
title: AI Integration
description: How Lore skill files teach AI agents to manage your database.
order: 8
---

# AI Integration

Lore generates skill files that teach AI coding agents how to use your database. Each supported platform gets instructions in its native format.

## Supported Platforms

| Platform | Skill Location |
|----------|---------------|
| **Claude Code** | `~/.claude/skills/lore/SKILL.md` + managed block in `~/.claude/CLAUDE.md` |
| **Cursor** | `~/.cursor/rules/lore.mdc` |
| **Codex CLI** | `~/.agents/skills/lore/SKILL.md` + managed block in `~/.codex/AGENTS.md` |
| **OpenCode** | `~/.agents/skills/lore/SKILL.md` + managed block in `~/.config/opencode/AGENTS.md` |
| **OpenClaw** | `~/.openclaw/workspace/skills/lore/SKILL.md` + managed block in `~/.openclaw/workspace/AGENTS.md` |

Platform aliases accepted with `--platforms`:

```bash
# These are equivalent
lore init --platforms claude
lore init --platforms claudecode
```

## What Skill Files Teach

The generated skill files instruct AI agents on:

### Core Behavior
- **Proactive storage** — the agent stores information without being asked
- **Schema ownership** — the agent creates and evolves tables as needed
- **State-first writes** — read schema, plan, apply (never guess)
- **Language fidelity** — store data in the language the user used

### The Remember Flow
1. Run `lore schema` to read the current database state
2. Compose a JSON plan with the right `baseSchemaHash`
3. Run `lore apply -f -` to execute

### The Recall Flow
- Run `lore read --sql "SELECT ..."` to query data
- Answer user questions based on stored data

### Hard Rules
- Must read schema before any write
- Must include a descriptive `message` in every plan
- Must use `where` clauses on updates and deletes
- Never store secrets, passwords, or API keys
- One semantic unit per apply (don't mix unrelated changes)

## Managed Blocks

For platforms that use a central config file (like `CLAUDE.md` or `AGENTS.md`), Lore uses idempotent managed blocks:

```
<!-- lore:init:claude:start -->
... lore configuration ...
<!-- lore:init:claude:end -->
```

Running `lore init` again updates the block in place. Running `lore clean` removes it.

## OpenClaw Heartbeat

When OpenClaw is selected during init, you can optionally enable heartbeat patrol:

```bash
lore init --platforms openclaw
# Prompts: Enable heartbeat? (or use --no-heartbeat to skip)
```

The heartbeat instructs the agent to:
- Run `lore status` periodically
- Alert on due or overdue time-sensitive entries
- Run `lore verify` once per day
- Auto-clean stale completed tasks and past events

## Cleaning Up

Remove all Lore-generated skill files:

```bash
lore clean
```

This removes skill directories, managed blocks, and rule files for all platforms. It does **not** delete your database.
