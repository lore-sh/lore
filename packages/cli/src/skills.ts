import { mkdir, rm, stat } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { deleteIfExists, isEnoent, resolveHomeDir } from "@toss/core";
import type { SkillPlatform } from "@toss/core";

const ALL_PLATFORMS: SkillPlatform[] = ["claude", "cursor", "codex", "opencode", "openclaw"];
const PLATFORM_SET = new Set<SkillPlatform>(ALL_PLATFORMS);

const AGENTS_BLOCK_START = "<!-- toss:init:agents:start -->";
const AGENTS_BLOCK_END = "<!-- toss:init:agents:end -->";
const CLAUDE_BLOCK_START = "<!-- toss:init:claude:start -->";
const CLAUDE_BLOCK_END = "<!-- toss:init:claude:end -->";
const HEARTBEAT_BLOCK_START = "<!-- toss:init:heartbeat:start -->";
const HEARTBEAT_BLOCK_END = "<!-- toss:init:heartbeat:end -->";
const AGENTS_BLOCKS: ManagedBlock[] = [{ start: AGENTS_BLOCK_START, end: AGENTS_BLOCK_END }];
const CLAUDE_BLOCKS: ManagedBlock[] = [{ start: CLAUDE_BLOCK_START, end: CLAUDE_BLOCK_END }];
const HEARTBEAT_BLOCKS: ManagedBlock[] = [{ start: HEARTBEAT_BLOCK_START, end: HEARTBEAT_BLOCK_END }];

type GeneratedPlatform = SkillPlatform | "shared";

interface ManagedBlock {
  start: string;
  end: string;
}

interface SkillPaths {
  sharedSkillDir: string;
  sharedSkillPath: string;
  sharedContractsPath: string;
  codexAgentsPath: string;
  opencodeAgentsPath: string;
  cursorRulePath: string;
  claudeSkillDir: string;
  claudeSkillPath: string;
  claudeContractsPath: string;
  claudeDocPath: string;
  openclawSkillDir: string;
  openclawSkillPath: string;
  openclawContractsPath: string;
  openclawAgentsPath: string;
  openclawHeartbeatPath: string;
}

export interface GeneratedSkillFile {
  platform: GeneratedPlatform;
  path: string;
}

export interface GeneratedSkills {
  canonicalSkillPath: string;
  files: GeneratedSkillFile[];
}

export interface CleanSkillFile {
  platform: GeneratedPlatform;
  path: string;
  removed: boolean;
}

export interface CleanSkillsResult {
  files: CleanSkillFile[];
}

export interface GenerateSkillsOptions {
  platforms?: SkillPlatform[] | undefined;
  openclawHeartbeat?: boolean | undefined;
}

function envPath(name: string): string | undefined {
  const trimmed = process.env[name]?.trim();
  if (!trimmed) {
    return undefined;
  }
  return resolve(trimmed);
}

function resolveSkillPaths(): SkillPaths {
  const home = resolveHomeDir();
  const codexHome = envPath("CODEX_HOME") ?? resolve(home, ".codex");
  const configHome = envPath("XDG_CONFIG_HOME") ?? resolve(home, ".config");
  const opencodeHome = resolve(configHome, "opencode");
  const openclawWorkspace = resolve(home, ".openclaw", "workspace");

  const sharedSkillDir = resolve(home, ".agents", "skills", "toss");
  const sharedReferencesDir = resolve(sharedSkillDir, "references");
  const claudeSkillDir = resolve(home, ".claude", "skills", "toss");
  const claudeReferencesDir = resolve(claudeSkillDir, "references");
  const openclawSkillDir = resolve(openclawWorkspace, "skills", "toss");
  const openclawReferencesDir = resolve(openclawSkillDir, "references");

  return {
    sharedSkillDir,
    sharedSkillPath: resolve(sharedSkillDir, "SKILL.md"),
    sharedContractsPath: resolve(sharedReferencesDir, "contracts.md"),
    codexAgentsPath: resolve(codexHome, "AGENTS.md"),
    opencodeAgentsPath: resolve(opencodeHome, "AGENTS.md"),
    cursorRulePath: resolve(home, ".cursor", "rules", "toss.mdc"),
    claudeSkillDir,
    claudeSkillPath: resolve(claudeSkillDir, "SKILL.md"),
    claudeContractsPath: resolve(claudeReferencesDir, "contracts.md"),
    claudeDocPath: resolve(home, ".claude", "CLAUDE.md"),
    openclawSkillDir,
    openclawSkillPath: resolve(openclawSkillDir, "SKILL.md"),
    openclawContractsPath: resolve(openclawReferencesDir, "contracts.md"),
    openclawAgentsPath: resolve(openclawWorkspace, "AGENTS.md"),
    openclawHeartbeatPath: resolve(openclawWorkspace, "HEARTBEAT.md"),
  };
}

function normalizePlatforms(platforms?: SkillPlatform[] | undefined): SkillPlatform[] {
  if (!platforms) {
    return [...ALL_PLATFORMS];
  }
  const seen = new Set<SkillPlatform>();
  return platforms.filter((platform) => {
    if (!PLATFORM_SET.has(platform) || seen.has(platform)) {
      return false;
    }
    seen.add(platform);
    return true;
  });
}

function tossSkillContent(): string {
  return `---
name: toss
description: Detects information worth remembering from conversation — schedules, tasks, expenses, decisions, learnings, thoughts — and stores it in a personal database proactively. Also recalls and analyzes stored data. Activate whenever user mentions dates, plans, purchases, reflections, or asks about past data.
---

# toss

A personal database that AI manages on behalf of humans. You design the schema, evolve it, and store data proactively — users never think about tables or migrations.

## Core Behavior

1. **Proactive storage**: When conversation contains information worth remembering, store it immediately. Do not ask for permission. Briefly mention what you stored after the fact.
2. **Schema ownership**: You own the schema. Read current schema, decide if it fits the data, create or alter tables as needed. Continuously optimize naming and structure.
3. **Autonomous evolution**: When new attributes appear, add columns. When names are unclear, rename. When tables grow unwieldy, split them. Schema changes and data mutations go in the same apply.
4. **State-first writes**: Prefer current-state tables over append-only logs. Use \`update\` when an existing entity changes, \`insert\` for genuinely new entities, and \`update + insert\` only when you intentionally need both current state and event history.
5. **Recall on demand**: When users ask about their data, query it with SQL and present results clearly.
6. **Language fidelity for stored content**: Keep user-facing values (for example \`title\`, \`detail\`, \`note\`, \`item\`, \`insight\`) in the language explicitly requested by the user. If no explicit instruction exists, follow the language used in the current user message. Do not translate unless asked.

## What to Store

Store anything that would be useful to remember later. If you think "this person might want to recall this," store it.

**Store immediately (high confidence):**
- Schedules and appointments: "dentist next Tuesday", "meeting at 3pm"
- Tasks and todos: "need to buy groceries", "renew passport before June"
- Expenses and purchases: "spent 850 yen on ramen", "bought a keyboard"
- Deadlines: "report due March 15", "visa expires June"
- Goals and plans: "want to read 20 books this year", "planning to move in April"
- Life events: "started new job", "signed lease for new apartment"
- Decisions and reasons: "chose Next.js over Remix because of file-based routing"
- Learnings and insights: "TIL: SQLite WAL mode improves read concurrency"
- Health and habits: "ran 5km", "started intermittent fasting"
- People and context: "met Tanaka from the Sales team at Acme Corp"

**Store when relevant to ongoing work:**
- Reasoning and thought process during coding or debugging
- Architecture decisions and trade-offs considered
- Problem-solving context that would help future sessions
- Research findings and comparisons

**Never store:**
- Secrets, passwords, API keys, tokens, credentials
- Transient conversational noise with no informational value

## Schema Design

You design all tables. Follow these conventions:

**Naming:**
- Tables: English, plural, snake_case (\`expenses\`, \`schedules\`, \`reading_list\`)
- Columns: English, descriptive, snake_case (\`due_date\`, \`amount\`, \`completed_at\`)
- Every table MUST have \`id INTEGER PRIMARY KEY\`

**Types:**
- Text: \`TEXT\` (strings, notes, descriptions)
- Numbers: \`INTEGER\` (counts, IDs) or \`REAL\` (decimals, amounts)
- Dates/times: \`TEXT\` in ISO 8601 (\`2026-02-20\`, \`2026-02-20T15:00:00\`)
- Booleans: \`INTEGER\` (0 or 1)
- Categories: \`TEXT\` + named \`CHECK\` constraint when values are finite
- System timestamps: prefer SQLite defaults (\`{"kind":"sql","expr":"CURRENT_TIMESTAMP"}\`) over planner-generated "now" values

**Structure decisions:**
- Start simple. One table per domain (\`expenses\`, \`schedules\`, \`tasks\`).
- Add columns when new attributes appear — do not create columns speculatively.
- Split into separate tables only when data has clearly different lifecycles or needs many-to-many relationships.
- When schema grows unclear, refactor: rename for clarity, split bloated tables, merge redundant ones.

**Before creating a new table, check for semantic overlap:**
- Read the current schema and examine existing tables. If a table covering a similar domain already exists, query its rows with \`toss read\` to understand how it is actually used.
- If the existing table serves the same purpose under a different name (e.g., \`events\` already stores what would go in \`schedules\`), reuse it — or rename it via migration if the new name is clearly better.
- If the existing table serves a genuinely different purpose despite a similar name, keep both and create the new table.
- If the overlap is ambiguous, ask the user which direction to take before writing.

**Write strategy (state-first):**
- If a fact changes an existing entity, prefer \`update\` over adding a new row.
- If a fact introduces a new entity, use \`insert\`.
- If both current state and event trail are needed, do \`update\` for state and \`insert\` for event in one apply.
- Use append-only tables only when temporal analysis/audit is an explicit requirement.

## Remember Flow

\`\`\`
schema -> plan -> apply
\`\`\`

1. Read current schema:
\`\`\`bash
toss schema
\`\`\`

2. Build OperationPlan. Include schema changes and data mutations together:
\`\`\`json
{
  "message": "track lunch expense 850 yen",
  "operations": [
    {
      "type": "create_table",
      "table": "expenses",
      "columns": [
        {"name": "id", "type": "INTEGER", "primaryKey": true},
        {"name": "date", "type": "TEXT", "notNull": true},
        {"name": "item", "type": "TEXT", "notNull": true},
        {"name": "amount", "type": "REAL", "notNull": true},
        {"name": "category", "type": "TEXT"},
        {"name": "note", "type": "TEXT"}
      ]
    },
    {
      "type": "insert",
      "table": "expenses",
      "values": {"date": "2026-02-20", "item": "ramen lunch", "amount": 850, "category": "food"}
    }
  ]
}
\`\`\`

3. Dry-run (recommended for schema changes, optional for simple inserts):
\`\`\`bash
cat <<'JSON' | toss plan -
<plan JSON>
JSON
\`\`\`

4. Apply:
\`\`\`bash
cat <<'JSON' | toss apply -
<plan JSON>
JSON
\`\`\`

5. If apply fails with schema mismatch, re-read schema and retry once.

## Recall Flow

Convert request to SQL and query:
\`\`\`bash
toss read --sql "SELECT date, item, amount FROM expenses WHERE category = 'food' AND date >= '2026-02-01' ORDER BY date" --json
\`\`\`

Present results with a short interpretation.

## Hard Rules

- MUST read schema before every write.
- MUST include a descriptive, non-empty \`message\` in every apply.
- MUST use \`where\` for \`update\` and \`delete\` — never omit it.
- MUST choose write mode deliberately: update current state unless append-only intent is explicit.
- MUST NOT store secrets, credentials, or tokens.
- MUST NOT ask permission before storing — store and report afterward.
- MUST keep stored content fields in the user-requested language (or current user message language when unspecified).
- MUST keep one semantic unit per apply.
- MUST prefer SQLite-generated timestamps for system fields (via SQL defaults) instead of injecting current time in values.
- For finite category/status fields, define and maintain explicit \`CHECK\` constraints.
- For destructive operations (\`drop_table\`, \`drop_column\`): prefer staged migration — add new -> migrate data -> verify -> drop old.
- MUST NOT run \`toss clean\`, \`toss init --force-new\`, or \`toss clone --force-new\` unless the user explicitly requests destructive reset/replacement.

## Commands

| Command | Purpose |
|---------|---------|
| \`toss schema [<table>]\` | Read current schema |
| \`toss plan <file\\|->\` | Dry-run validation |
| \`toss apply <file\\|->\` | Execute and commit |
| \`toss read --sql "<SELECT>" --json\` | Read-only query |
| \`toss status [--json]\` | Database overview |
| \`toss history [--verbose] [--json]\` | Commit log |
| \`toss verify [--full]\` | Integrity check |
| \`toss revert <commit_id>\` | Reverse a commit |
| \`toss recover <commit_id>\` | Restore from snapshot |
| \`toss remote connect --platform <turso\\|libsql> --url <url> [--token <token>\\|--clear-token]\` | Configure remote |
| \`toss remote status\` | Show local/remote sync state |
| \`toss push\` | Push local commits |
| \`toss pull\` | Pull remote commits |
| \`toss sync\` | Pull then push |
| \`toss clone <url> --platform <turso\\|libsql> [--force-new]\` | Initialize local DB from remote |
| \`toss init [--platforms <list>] [--no-skills] [--no-heartbeat] [--force-new] [--yes] [--json]\` | Initialize local DB |
| \`toss clean [--yes] [--json]\` | Remove global integration files |

IMPORTANT: You can run toss commands from any directory. toss always uses \`~/.toss/toss.db\`.

## Examples

### Proactive storage from conversation

User says: "I booked a dentist appointment tomorrow at 2pm"

\`\`\`bash
# 1. Read schema -> schedules table does not exist yet

# 2. Apply with table creation + insert
cat <<'JSON' | toss apply -
{
  "message": "dentist appointment 2026-02-21 14:00",
  "operations": [
    {
      "type": "create_table",
      "table": "schedules",
      "columns": [
        {"name": "id", "type": "INTEGER", "primaryKey": true},
        {"name": "title", "type": "TEXT", "notNull": true},
        {"name": "date", "type": "TEXT", "notNull": true},
        {"name": "time", "type": "TEXT"},
        {"name": "location", "type": "TEXT"},
        {"name": "note", "type": "TEXT"}
      ]
    },
    {
      "type": "insert",
      "table": "schedules",
      "values": {"title": "dentist appointment", "date": "2026-02-21", "time": "14:00"}
    }
  ]
}
JSON
\`\`\`

Response: "Saved your dentist appointment for tomorrow at 2pm."

### Schema evolution

User says: "Had lunch at Ichiran in Shibuya, 1200 yen"

Schema already has \`expenses(id, date, item, amount, category, note)\` — location is new.

\`\`\`bash
cat <<'JSON' | toss apply -
{
  "message": "lunch expense with location tracking",
  "operations": [
    {
      "type": "add_column",
      "table": "expenses",
      "column": {"name": "location", "type": "TEXT"}
    },
    {
      "type": "insert",
      "table": "expenses",
      "values": {
        "date": "2026-02-20", "item": "ichiran ramen", "amount": 1200,
        "category": "food", "location": "shibuya"
      }
    }
  ]
}
JSON
\`\`\`

### State-first decision update

User says: "We changed the auth decision from session to JWT."

\`\`\`bash
cat <<'JSON' | toss apply -
{
  "message": "update auth decision to JWT",
  "operations": [
    {
      "type": "update",
      "table": "decisions",
      "values": {
        "decision": "use JWT",
        "reason": "mobile clients and stateless API gateway requirements"
      },
      "where": {"topic": "auth strategy"}
    }
  ]
}
JSON
\`\`\`

### Storing reasoning context

During debugging, user resolves a tricky issue:

\`\`\`bash
cat <<'JSON' | toss apply -
{
  "message": "debug insight: SQLite WAL mode lock contention",
  "operations": [
    {
      "type": "insert",
      "table": "learnings",
      "values": {
        "date": "2026-02-20",
        "topic": "sqlite",
        "insight": "WAL mode with PRAGMA busy_timeout=5000 resolves intermittent SQLITE_BUSY in concurrent reads",
        "context": "toss CLI failing under parallel test runs",
        "tags": "sqlite,debugging,concurrency"
      }
    }
  ]
}
JSON
\`\`\`

### Recall and analysis

User asks: "How much did I spend on food this month?"

\`\`\`bash
toss read --sql "SELECT SUM(amount) as total, COUNT(*) as count FROM expenses WHERE category = 'food' AND date >= '2026-02-01'" --json
\`\`\`

Response: "You spent a total of 12,450 yen on food this month across 14 meals."

## References

- Operation type contracts and specifications: [references/contracts.md](references/contracts.md)
`;
}

function contractsReferenceContent(): string {
  return `# toss Operation Contracts

## OperationPlan Envelope

Every write goes through this JSON envelope piped to \`toss plan -\` or \`toss apply -\`:

\`\`\`json
{
  "message": "descriptive commit message",
  "operations": [...]
}
\`\`\`

- \`message\`: Required, non-empty. Describes what this commit does.
- \`operations\`: Required, at least one operation.

## Schema Operations

### create_table
\`\`\`json
{
  "type": "create_table",
  "table": "table_name",
  "columns": [
    {"name": "id", "type": "INTEGER", "primaryKey": true},
    {"name": "title", "type": "TEXT", "notNull": true},
    {"name": "count", "type": "INTEGER", "default": {"kind": "literal", "value": 0}},
    {"name": "created_at", "type": "TEXT", "default": {"kind": "sql", "expr": "CURRENT_TIMESTAMP"}}
  ]
}
\`\`\`
- Exactly one column MUST have \`"primaryKey": true\`.
- Optional column fields: \`notNull\`, \`unique\`, \`default\`.
- \`default\` supports:
  - \`{"kind":"literal","value":<string|number|boolean|null>}\`
  - \`{"kind":"sql","expr":"CURRENT_TIMESTAMP"|"CURRENT_DATE"|"CURRENT_TIME"}\`
- Table/column names: letters, digits, underscore. No \`_toss_\` or \`sqlite_\` prefix.

### add_column
\`\`\`json
{
  "type": "add_column",
  "table": "existing_table",
  "column": {"name": "new_col", "type": "TEXT", "default": {"kind": "literal", "value": "value"}}
}
\`\`\`
- Cannot add PRIMARY KEY or UNIQUE columns.
- If \`notNull: true\`, must provide \`default\`.
- SQL defaults in \`add_column\` are only allowed when the target table is empty.

### drop_table
\`\`\`json
{"type": "drop_table", "table": "table_name"}
\`\`\`

### drop_column
\`\`\`json
{"type": "drop_column", "table": "table_name", "column": "col_name"}
\`\`\`

### alter_column_type
\`\`\`json
{"type": "alter_column_type", "table": "table_name", "column": "col_name", "newType": "INTEGER"}
\`\`\`

### add_check
\`\`\`json
{
  "type": "add_check",
  "table": "table_name",
  "expression": "status IN ('todo','doing','done')"
}
\`\`\`
- Adds a table-level \`CHECK\` constraint by rebuilding the table internally.
- Use for finite category/status values that must be enforced at DB level.

### drop_check
\`\`\`json
{
  "type": "drop_check",
  "table": "table_name",
  "expression": "status IN ('todo','doing','done')"
}
\`\`\`
- Drops matching table-level \`CHECK\` constraint(s) by rebuilding the table internally.
- For enum updates, use staged migration: \`drop_check\` old expression -> \`add_check\` new expression.

## Data Operations

### insert
\`\`\`json
{
  "type": "insert",
  "table": "table_name",
  "values": {"col1": "text", "col2": 42, "col3": true, "col4": null}
}
\`\`\`
- Values: string, number, boolean, or null only.

### update
\`\`\`json
{
  "type": "update",
  "table": "table_name",
  "values": {"col1": "new_value"},
  "where": {"id": 1}
}
\`\`\`
- \`where\` is required and MUST be non-empty.

### delete
\`\`\`json
{
  "type": "delete",
  "table": "table_name",
  "where": {"id": 1}
}
\`\`\`
- \`where\` is required and MUST be non-empty.

## Read Operations

### schema
\`\`\`bash
toss schema [<table>]
\`\`\`
Returns JSON with tables, columns, indexes, foreign keys, triggers, checks, and row counts.

### read
\`\`\`bash
toss read --sql "<SELECT | WITH...SELECT>" [--json]
\`\`\`
Single statement, read-only. Use \`--json\` for structured output.

### plan (dry-run)
\`\`\`bash
toss plan <file|->
\`\`\`
Returns: \`ok\`, \`errors\`, \`warnings\`, \`risk\` (low/medium/high), predicted effects.

## Other Commands

- \`toss history [--verbose] [--json]\`: Commit log with optional detail.
- \`toss verify [--full]\`: Chain hash validation + optional SQLite integrity check.
- \`toss revert <commit_id>\`: Inverse commit. Returns conflict details if revert is blocked.
- \`toss status [--json]\`: Database path, table count, HEAD commit info.
- \`toss recover <commit_id>\`: Disaster recovery from snapshot.
- \`toss remote connect --platform <turso|libsql> --url <url> [--token <token>|--clear-token]\`: Configure remote connection.
- \`toss remote status\`: Show local/remote sync state.
- \`toss push\` / \`toss pull\` / \`toss sync\`: Explicit sync controls.
- \`toss clone <url> --platform <turso|libsql> [--force-new]\`: Initialize from remote.
- \`toss init [--platforms <list>] [--no-skills] [--no-heartbeat] [--force-new] [--yes] [--json]\`: Initialize local DB.
- \`toss clean [--yes] [--json]\`: Remove global integration files (destructive).
`;
}

function cursorRuleContent(): string {
  return `---
description: toss personal database — detects storable info (schedules, tasks, expenses, decisions, learnings) from conversation and manages schema evolution proactively
alwaysApply: false
---

Activate when conversation contains information worth remembering or user asks about past data.

## Behavior
- Store proactively. Do not ask permission — briefly report what you stored.
- Design and evolve schema autonomously (English, plural snake_case table names).
- Read schema before every write. Use schema -> plan -> apply flow.
- Keep written content values in the user-requested language; default to the current user message language when unspecified.

## Commands
- \`toss schema\` — read current schema
- \`toss plan -\` — dry-run validation
- \`toss apply -\` — execute and commit
- \`toss read --sql "<SELECT ...>" --json\` — query data
- \`toss remote connect --platform <turso|libsql> --url <url> [--token <token>|--clear-token]\` — configure remote
- \`toss remote status\` / \`toss push\` / \`toss pull\` / \`toss sync\` — remote sync controls
- \`toss clone <url> --platform <turso|libsql> [--force-new]\` — clone from remote

## Rules
- One semantic unit per apply.
- Never update/delete without explicit where.
- Never store secrets or credentials.
- Include a descriptive message in every apply.
- Keep stored content fields in the user-requested language.
- Never run \`toss clean\`, \`toss init --force-new\`, or \`toss clone --force-new\` unless the user explicitly asks for destructive reset/replacement.
`;
}

function agentsBlock(skillPath: string): string {
  return `${AGENTS_BLOCK_START}
## Skills
### Available skills
- toss: Personal database managed by AI. Detects storable information (schedules, tasks, expenses, decisions, learnings, thoughts) from conversation and stores proactively. Handles schema design, evolution, and data recall. (file: ${skillPath})
### How to use skills
- Activate \`toss\` whenever conversation contains information worth remembering or user asks about past data.
- Store proactively — do not ask permission. Briefly report what was stored afterward.
- Store user-facing content fields in the language requested by the user (or the current user message language when unspecified).
- For writes: read schema first, then schema -> plan -> apply.
- For reads: generate read-only SQL with \`toss read\`.
${AGENTS_BLOCK_END}
`;
}

function heartbeatBlock(): string {
  return `${HEARTBEAT_BLOCK_START}
## toss: Personal Data Patrol

Check personal database and act on what you find.

### Routine (every heartbeat)
1. \`toss status\` — discover tables and row counts
2. For time-sensitive tables (schedules, tasks, deadlines, etc.):
   - Due within 2h → **alert immediately**
   - Due within 24h → mention once per item
   - Overdue → remind with list
3. \`toss verify\` — once per day (track in heartbeat-state.json)

### Cleanup (auto-execute, notify afterward)
All toss deletions are committed and revertable via \`toss revert\`.
- Completed tasks/goals older than 30 days → delete, report
- Past events older than 7 days with no notes → delete, report
- Ambiguous cases → suggest to human, wait for approval

### Quiet rules
- No time-sensitive data + verify OK + no cleanup targets → HEARTBEAT_OK
- Already notified same items within 4h → HEARTBEAT_OK
${HEARTBEAT_BLOCK_END}
`;
}

function claudeBlock(skillPath: string): string {
  return `${CLAUDE_BLOCK_START}
## Skills
- toss: Personal database managed by AI. Detects storable information (schedules, tasks, expenses, decisions, learnings, thoughts) from conversation and stores proactively. Handles schema design, evolution, and data recall. (file: ${skillPath})
## How to use skills
- Use \`toss\` whenever conversation contains dates, plans, expenses, tasks, decisions, learnings, or user asks about past data.
- Store proactively — do not ask permission, briefly report after storing.
- Store user-facing content fields in the user-requested language (or the current user message language when unspecified).
- Read schema before every write. Schema -> plan -> apply.
${CLAUDE_BLOCK_END}
`;
}

async function writeText(path: string, content: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await Bun.write(path, content);
}

function stripManagedBlock(text: string, block: ManagedBlock, path: string): string {
  let next = text;
  while (true) {
    const start = next.indexOf(block.start);
    const end = next.indexOf(block.end);
    if (start < 0 && end < 0) {
      return next;
    }
    if (start < 0 || end < 0 || end < start) {
      throw new Error(
        `Malformed managed block in ${path}. Expected paired markers: ${block.start} ... ${block.end}`,
      );
    }
    next = `${next.slice(0, start)}${next.slice(end + block.end.length)}`;
  }
}

function stripManagedBlocks(text: string, blocks: ManagedBlock[], path: string): string {
  let next = text;
  for (const block of blocks) {
    next = stripManagedBlock(next, block, path);
  }
  return next;
}

async function removeManagedBlocks(path: string, blocks: ManagedBlock[]): Promise<boolean> {
  const file = Bun.file(path);
  if (!(await file.exists())) {
    return false;
  }
  const current = await file.text();
  const stripped = stripManagedBlocks(current, blocks, path);
  if (stripped === current) {
    return false;
  }

  const trimmed = stripped.trim();
  if (trimmed.length === 0) {
    await deleteIfExists(path);
    return true;
  }
  await writeText(path, `${trimmed}\n`);
  return true;
}

async function upsertManagedBlock(path: string, block: string, blocks: ManagedBlock[], initial: string): Promise<void> {
  const file = Bun.file(path);
  const current = (await file.exists()) ? await file.text() : initial;
  const stripped = stripManagedBlocks(current, blocks, path).trimEnd();
  const next = stripped.length === 0 ? `${block}\n` : `${stripped}\n\n${block}\n`;
  await writeText(path, next);
}

async function removeDirIfExists(path: string): Promise<boolean> {
  let info: Awaited<ReturnType<typeof stat>>;
  try {
    info = await stat(path);
  } catch (error) {
    if (isEnoent(error)) {
      return false;
    }
    throw error;
  }
  if (!info.isDirectory()) {
    return false;
  }
  await rm(path, { recursive: true, force: true });
  return true;
}

async function removeFileIfExists(path: string): Promise<boolean> {
  const file = Bun.file(path);
  if (!(await file.exists())) {
    return false;
  }
  await file.delete();
  return true;
}

async function writeSkillBundle(skillPath: string, contractsPath: string): Promise<void> {
  await Promise.all([
    writeText(skillPath, tossSkillContent()),
    writeText(contractsPath, contractsReferenceContent()),
  ]);
}

function addGeneratedFiles(files: GeneratedSkillFile[], platform: GeneratedPlatform, ...paths: string[]): void {
  for (const path of paths) {
    files.push({ platform, path });
  }
}

export async function generateSkills(options: GenerateSkillsOptions = {}): Promise<GeneratedSkills> {
  const paths = resolveSkillPaths();
  const selected = normalizePlatforms(options.platforms);
  const selectedSet = new Set(selected);
  const files: GeneratedSkillFile[] = [];

  if (selected.length > 0) {
    await writeSkillBundle(paths.sharedSkillPath, paths.sharedContractsPath);
    addGeneratedFiles(files, "shared", paths.sharedSkillPath, paths.sharedContractsPath);
  } else {
    await removeDirIfExists(paths.sharedSkillDir);
  }

  if (selectedSet.has("codex")) {
    await upsertManagedBlock(
      paths.codexAgentsPath,
      agentsBlock(paths.sharedSkillPath),
      AGENTS_BLOCKS,
      "# AGENTS.md\n",
    );
    addGeneratedFiles(files, "codex", paths.codexAgentsPath);
  } else {
    await removeManagedBlocks(paths.codexAgentsPath, AGENTS_BLOCKS);
  }

  if (selectedSet.has("opencode")) {
    await upsertManagedBlock(
      paths.opencodeAgentsPath,
      agentsBlock(paths.sharedSkillPath),
      AGENTS_BLOCKS,
      "# AGENTS.md\n",
    );
    addGeneratedFiles(files, "opencode", paths.opencodeAgentsPath);
  } else {
    await removeManagedBlocks(paths.opencodeAgentsPath, AGENTS_BLOCKS);
  }

  if (selectedSet.has("cursor")) {
    await writeText(paths.cursorRulePath, cursorRuleContent());
    addGeneratedFiles(files, "cursor", paths.cursorRulePath);
  } else {
    await removeFileIfExists(paths.cursorRulePath);
  }

  if (selectedSet.has("claude")) {
    await writeSkillBundle(paths.claudeSkillPath, paths.claudeContractsPath);
    await upsertManagedBlock(
      paths.claudeDocPath,
      claudeBlock(paths.claudeSkillPath),
      CLAUDE_BLOCKS,
      "# CLAUDE.md\n",
    );
    addGeneratedFiles(
      files,
      "claude",
      paths.claudeSkillPath,
      paths.claudeContractsPath,
      paths.claudeDocPath,
    );
  } else {
    await removeDirIfExists(paths.claudeSkillDir);
    await removeManagedBlocks(paths.claudeDocPath, CLAUDE_BLOCKS);
  }

  if (selectedSet.has("openclaw")) {
    await writeSkillBundle(paths.openclawSkillPath, paths.openclawContractsPath);
    await upsertManagedBlock(
      paths.openclawAgentsPath,
      agentsBlock(paths.openclawSkillPath),
      AGENTS_BLOCKS,
      "# AGENTS.md\n",
    );
    addGeneratedFiles(
      files,
      "openclaw",
      paths.openclawSkillPath,
      paths.openclawContractsPath,
      paths.openclawAgentsPath,
    );
    if (options.openclawHeartbeat) {
      await upsertManagedBlock(
        paths.openclawHeartbeatPath,
        heartbeatBlock(),
        HEARTBEAT_BLOCKS,
        "# HEARTBEAT.md\n",
      );
      addGeneratedFiles(files, "openclaw", paths.openclawHeartbeatPath);
    } else {
      await removeManagedBlocks(paths.openclawHeartbeatPath, HEARTBEAT_BLOCKS);
    }
  } else {
    await removeDirIfExists(paths.openclawSkillDir);
    await removeManagedBlocks(paths.openclawAgentsPath, AGENTS_BLOCKS);
    await removeManagedBlocks(paths.openclawHeartbeatPath, HEARTBEAT_BLOCKS);
  }

  return {
    canonicalSkillPath: paths.sharedSkillPath,
    files,
  };
}

export async function cleanSkills(): Promise<CleanSkillsResult> {
  const paths = resolveSkillPaths();

  const targets: Array<{ platform: GeneratedPlatform; path: string; remove: () => Promise<boolean> }> = [
    { platform: "shared", path: paths.sharedSkillDir, remove: () => removeDirIfExists(paths.sharedSkillDir) },
    { platform: "codex", path: paths.codexAgentsPath, remove: () => removeManagedBlocks(paths.codexAgentsPath, AGENTS_BLOCKS) },
    { platform: "opencode", path: paths.opencodeAgentsPath, remove: () => removeManagedBlocks(paths.opencodeAgentsPath, AGENTS_BLOCKS) },
    { platform: "cursor", path: paths.cursorRulePath, remove: () => removeFileIfExists(paths.cursorRulePath) },
    { platform: "claude", path: paths.claudeSkillDir, remove: () => removeDirIfExists(paths.claudeSkillDir) },
    { platform: "claude", path: paths.claudeDocPath, remove: () => removeManagedBlocks(paths.claudeDocPath, CLAUDE_BLOCKS) },
    { platform: "openclaw", path: paths.openclawSkillDir, remove: () => removeDirIfExists(paths.openclawSkillDir) },
    { platform: "openclaw", path: paths.openclawAgentsPath, remove: () => removeManagedBlocks(paths.openclawAgentsPath, AGENTS_BLOCKS) },
    { platform: "openclaw", path: paths.openclawHeartbeatPath, remove: () => removeManagedBlocks(paths.openclawHeartbeatPath, HEARTBEAT_BLOCKS) },
  ];

  const results = await Promise.all(targets.map((t) => t.remove()));
  const files = targets.map((t, i) => ({ platform: t.platform, path: t.path, removed: results[i]! }));
  return { files };
}
