---
title: Operation Plans
description: The JSON plan format, all operation types, and how schema and data operations work.
order: 4
---

# Operation Plans

Every write to Lore goes through a JSON plan. The plan describes what to change, and Lore executes it atomically.

## Plan Format

```json
{
  "baseSchemaHash": "a1b2c3...64 hex chars",
  "message": "Add expenses table and first entry",
  "operations": [...]
}
```

| Field | Description |
|-------|-------------|
| `baseSchemaHash` | SHA-256 of the current schema. Copy `schemaHash` from `lore schema` output |
| `message` | Commit message describing the change |
| `operations` | Array of operations to execute in order |

Generate plans immediately after `lore schema` and copy `schemaHash` into `baseSchemaHash`. If the schema changes before `lore plan` or `lore apply`, Lore returns `STALE_PLAN` and the plan must be regenerated.

## Schema Operations

### create_table

Create a new table. Exactly one column must have `primaryKey: true`.

```json
{
  "type": "create_table",
  "table": "expenses",
  "columns": [
    { "name": "id", "type": "INTEGER", "primaryKey": true },
    { "name": "title", "type": "TEXT", "notNull": true },
    { "name": "amount", "type": "REAL" },
    { "name": "date", "type": "TEXT" },
    { "name": "created_at", "type": "TEXT",
      "default": { "kind": "sql", "expr": "CURRENT_TIMESTAMP" } }
  ]
}
```

Column defaults support two kinds:

- `{ "kind": "literal", "value": 0 }` — a static value
- `{ "kind": "sql", "expr": "CURRENT_TIMESTAMP" }` — a SQL expression (`CURRENT_TIMESTAMP`, `CURRENT_DATE`, or `CURRENT_TIME`)

### add_column

Add a column to an existing table.

```json
{
  "type": "add_column",
  "table": "expenses",
  "column": { "name": "location", "type": "TEXT" }
}
```

> If `notNull` is true, a `default` is required. SQL expression defaults are only allowed on empty tables. `primaryKey` and `unique` columns cannot be added this way.

### alter_column_type

Change a column's type.

```json
{
  "type": "alter_column_type",
  "table": "expenses",
  "column": "amount",
  "newType": "REAL"
}
```

### drop_table

```json
{ "type": "drop_table", "table": "expenses" }
```

### drop_column

```json
{ "type": "drop_column", "table": "expenses", "column": "location" }
```

### add_check

Add a CHECK constraint to a table.

```json
{
  "type": "add_check",
  "table": "tasks",
  "expression": "status IN ('todo','doing','done')"
}
```

### drop_check

```json
{
  "type": "drop_check",
  "table": "tasks",
  "expression": "status IN ('todo','doing','done')"
}
```

### drop_index

```json
{ "type": "drop_index", "table": "expenses", "name": "idx_expenses_date" }
```

### drop_trigger

```json
{ "type": "drop_trigger", "table": "expenses", "name": "trig_after_insert" }
```

### drop_view

```json
{ "type": "drop_view", "name": "monthly_summary" }
```

## Data Operations

### insert

```json
{
  "type": "insert",
  "table": "expenses",
  "values": {
    "title": "Coffee with Sarah",
    "amount": 4.50,
    "date": "2026-02-27"
  }
}
```

### update

```json
{
  "type": "update",
  "table": "expenses",
  "values": { "amount": 5.00 },
  "where": { "id": 1 }
}
```

> `where` is required and must be non-empty.

### delete

```json
{
  "type": "delete",
  "table": "expenses",
  "where": { "id": 1 }
}
```

> `where` is required and must be non-empty.

## Value Types

Values in `insert`, `update`, and `where` clauses support: **string**, **number**, **boolean**, and **null**.

## Naming Rules

Table and column names must contain only letters, digits, and underscores. Names starting with `_lore_` or `sqlite_` are reserved.

## Dependency Conflicts

When dropping a table or column, Lore checks for dependent objects — indexes, triggers, views, foreign keys, and generated columns. If conflicts exist, the operation fails with `DEPENDENCY_CONFLICT` and a `suggestedOps` array listing the drop operations needed first.

For example, dropping a column that has an index will suggest a `drop_index` operation that should be added before the `drop_column`.
