---
title: How It Works
description: The two-layer architecture that separates AI planning from database execution.
order: 2
---

# How It Works

Lore uses a two-layer architecture where AI is the **planner** and Lore is the **executor**. This separation keeps your data safe — the AI never touches SQLite directly.

## The Two Layers

### Layer 1: AI Planner

The AI agent reads the current schema with `lore schema`, understands the database state, and composes a JSON plan describing what to change. The AI decides *what* should happen but never executes SQL directly.

### Layer 2: Lore Executor

The `lore apply` command takes the plan, validates it, executes all operations atomically inside a transaction, records full before/after effects, and creates a versioned commit. If anything fails, nothing changes.

```
AI reads schema → AI writes plan → Lore validates → Lore executes → Commit recorded
```

## The Schema Hash Guard

Every plan includes a `baseSchemaHash` — a SHA-256 hash of the schema at the time the AI read it. When Lore receives a plan, it recomputes the current schema hash and rejects the plan if they don't match.

This prevents a critical class of bugs: the AI applying a plan against a schema that has changed since it last checked. The error code is `STALE_PLAN`, and the AI must re-read the schema and create a new plan.

## Apply Flow

When you run `lore apply -f plan.json`, here's what happens:

1. **Parse** — the JSON plan is validated against the schema
2. **Hash check** — `baseSchemaHash` is compared to the current schema
3. **Transaction** — a `BEGIN IMMEDIATE` transaction wraps all operations
4. **Capture before state** — row and schema snapshots are taken
5. **Execute** — each operation runs in order
6. **Capture after state** — row and schema changes are recorded
7. **Commit** — a versioned commit is created with full effect tracking
8. **Snapshot** — every 100 commits, an automatic snapshot is taken
9. **Auto-sync** — if a remote is configured, changes push automatically

## Dry Run with Plan

Before applying, the AI can check a plan without modifying data:

```bash
lore plan -f plan.json
```

This runs the plan inside a savepoint that is always rolled back. It returns a risk assessment (`low`, `medium`, or `high`), counts of predicted row and schema effects, and any validation errors — without changing a single byte.
If `baseSchemaHash` is stale, `lore plan` returns `STALE_PLAN` immediately and skips simulation.

## Schema Evolution

Tables and columns are created, modified, and dropped through operations in the plan. The AI evolves the schema over time based on what you need to store. If you mention a new kind of data, the AI adds the right columns. If a column type needs to change, the AI plans an `alter_column_type` operation.

Every schema change is versioned alongside data changes in the same commit, so you always have a complete history of how your database evolved.
