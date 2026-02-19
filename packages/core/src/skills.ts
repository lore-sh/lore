import { dirname, resolve } from "node:path";

const AGENTS_BLOCK_START = "<!-- toss:init:skills:start -->";
const AGENTS_BLOCK_END = "<!-- toss:init:skills:end -->";

export interface GeneratedSkills {
  skillsRoot: string;
  skillDir: string;
  skillPath: string;
  referencesDir: string;
  agentsPath: string;
}

function tossSkillContent(workspacePath: string): string {
  return `---
name: toss
description: Use toss CLI to remember/store data, evolve schemas with migrations, and recall/query safely via read-before-apply workflows.
---

# toss

Use this skill for all toss operations in Claude Code style workflows.

## When to use
- The user asks to remember/save/store life logs or structured personal data.
- The user asks to search/analyze/summarize data already stored in toss.

## Command surface
- Write path: \`bun run --cwd "${workspacePath}" toss apply --plan -\`
- Read path: \`bun run --cwd "${workspacePath}" toss read --sql "<SELECT ...>" --json\`
- History path: \`bun run --cwd "${workspacePath}" toss history --verbose\`
- Verify path: \`bun run --cwd "${workspacePath}" toss verify --full\`

## Workflow Router
1. If request is store/remember:
   - Follow **Remember Flow** below.
2. If request is query/recall/analyze:
   - Follow **Recall Flow** below.

## Remember Flow (read-before-apply)
1. Read current schema snapshot:
\`\`\`bash
bun run --cwd "${workspacePath}" toss read --sql "SELECT m.name AS table_name, p.name AS column_name, p.type, p.notnull FROM sqlite_master m JOIN pragma_table_info(m.name) p WHERE m.type='table' AND m.name NOT LIKE '_toss_%' AND m.name NOT LIKE 'sqlite_%' ORDER BY m.name, p.cid" --json
\`\`\`
2. Build OperationPlan from schema + user intent:
   - Missing table -> include \`create_table\`
   - Missing column -> include \`add_column\`
   - New record -> include \`insert\`
   - Data correction/backfill -> include \`update\` with explicit \`where\`
   - Data cleanup -> include \`delete\` with explicit \`where\`
   - Schema cleanup -> include \`drop_column\` / \`drop_table\` if explicitly requested
   - Type migration -> include \`alter_column_type\` when the user asks to change storage type
3. Apply plan:
\`\`\`bash
cat <<'JSON' | bun run --cwd "${workspacePath}" toss apply --plan -
{"message":"<what this apply does>","operations":[...],"source":{"planner":"claude-code-skill","skill":"toss"}}
JSON
\`\`\`
4. If apply fails due to schema mismatch, re-read schema and retry once with corrected plan.
5. For schema migration tasks, run a verification read query and summarize before/after impact.
6. Before destructive schema changes, run \`toss history --verbose\` and \`toss verify --quick\`.

## Recall Flow
1. Convert intent to read-only SQL (\`SELECT\` or \`WITH ... SELECT\` only).
2. Run:
\`\`\`bash
bun run --cwd "${workspacePath}" toss read --sql "<SELECT ...>" --json
\`\`\`
3. Return structured results and short interpretation.

## Hard Rules
- Keep one semantic unit per apply.
- Always include non-empty \`message\`.
- Prefer stdin for apply (\`--plan -\`).
- Never run \`update\` or \`delete\` without explicit \`where\`.
- Prefer staged migrations:
  1) additive change (\`add_column\` / new table),
  2) data backfill (\`update\`),
  3) verification read,
  4) cleanup (\`drop_column\` / \`drop_table\`).

## References
- Product background and use cases: [references/context.md](references/context.md)
- Operation contracts and examples: [references/contracts.md](references/contracts.md)
`;
}

function contextReferenceContent(): string {
  return `# toss Context

## Background
toss is a personal database for the AI era. It separates:
- Planner: natural language -> structured plan/query
- Executor (toss CLI): validate + apply + log + revert

This separation keeps the execution layer deterministic and safe.

## Philosophy
1. Humans should not need schema/migration design.
2. Data is owned by individuals (local-first SQLite).
3. Be bold with safety: append-only history + revert.
4. Schema should evolve continuously with data migration, not stay fixed forever.

## 2-layer model
- Commit Log: immutable source of truth (\`_toss_commit\`, \`_toss_op\`, \`_toss_effect_*\`)
- HEAD State: materialized current tables, always rebuildable

## Use cases
1. Life log input:
   - User: "Lunch ramen 850 yen"
   - Skill: build OperationPlan and apply
2. Recall/analysis:
   - User: "This month's food expense"
   - Skill: build read-only SQL and summarize
3. Dashboard:
   - Apps read \`toss.db\` directly for visualization
`;
}

function contractsReferenceContent(workspacePath: string): string {
  return `# toss Contracts

## apply contract
\`toss apply --plan <file|->\` accepts OperationPlan envelope JSON:

\`\`\`json
{
  "message": "2026-02-18 dinner expense added",
  "operations": [
    {
      "type": "insert",
      "table": "expenses",
      "values": { "date": "2026-02-18", "item": "dinner", "amount": 1200 }
    }
  ],
  "source": { "planner": "claude-code-skill", "skill": "toss" }
}
\`\`\`

Allowed operation types:
- \`create_table\`
- \`add_column\`
- \`insert\`
- \`update\` (requires non-empty \`where\`)
- \`delete\` (requires non-empty \`where\`)
- \`drop_table\`
- \`drop_column\`
- \`alter_column_type\`

Schema evolution best practice:
1. Read schema snapshot first.
2. Additive migration first (\`create_table\`, \`add_column\`).
3. Backfill/correct data (\`update\`).
4. Verify with read query.
5. Cleanup old structures (\`drop_column\`, \`drop_table\`) only after verification.

## read contract
\`toss read --sql "<query>" [--json]\`
- Only \`SELECT\` / \`WITH ... SELECT\`
- Single statement only

## history / verify / revert
- \`toss history --verbose\`: includes parent ids, state hash, inverse readiness.
- \`toss verify\` / \`toss verify --full\`: checks commit chain hash + SQLite integrity checks.
- \`toss revert <commit_id>\`: returns conflict details for row/schema collisions.

## schema introspection query (for remember flow)
\`\`\`sql
SELECT m.name AS table_name, p.name AS column_name, p.type, p.notnull
FROM sqlite_master m
JOIN pragma_table_info(m.name) p
WHERE m.type='table'
  AND m.name NOT LIKE '_toss_%'
  AND m.name NOT LIKE 'sqlite_%'
ORDER BY m.name, p.cid;
\`\`\`

## Recommended execution snippets
\`\`\`bash
# Read schema snapshot
bun run --cwd "${workspacePath}" toss read --sql "SELECT m.name AS table_name, p.name AS column_name, p.type, p.notnull FROM sqlite_master m JOIN pragma_table_info(m.name) p WHERE m.type='table' AND m.name NOT LIKE '_toss_%' AND m.name NOT LIKE 'sqlite_%' ORDER BY m.name, p.cid" --json

# Apply via stdin
cat <<'JSON' | bun run --cwd "${workspacePath}" toss apply --plan -
{"message":"<summary>","operations":[...],"source":{"planner":"claude-code-skill","skill":"toss"}}
JSON
\`\`\`
`;
}

function agentsBlock(skills: GeneratedSkills): string {
  return `${AGENTS_BLOCK_START}
## Skills
### Available skills
- toss: Unified toss workflow for remember/store, schema evolution + data migration, and recall/query with read-before-apply safety. (file: ${skills.skillPath})
### How to use skills
- Trigger rules: Use \`toss\` whenever user intent touches toss memory, storage, query, or analysis.
- For writes: always run schema introspection first, then build OperationPlan with explicit migration intent.
- For reads: generate read-only SQL only.
- For destructive changes: run \`toss history --verbose\` and \`toss verify --quick\` before apply.
- If \`toss revert\` returns conflicts, present conflict details and propose a staged migration plan.
- Execution: Skills should call toss through \`bun run --cwd "${dirname(skills.agentsPath)}" toss ...\`.
${AGENTS_BLOCK_END}
`;
}

async function upsertAgentsFile(path: string, block: string): Promise<void> {
  let current = "# AGENTS.md\n\n";
  const file = Bun.file(path);
  if (await file.exists()) {
    current = await file.text();
  }

  const hasStart = current.includes(AGENTS_BLOCK_START);
  const hasEnd = current.includes(AGENTS_BLOCK_END);
  let next: string;
  if (hasStart && hasEnd) {
    const start = current.indexOf(AGENTS_BLOCK_START);
    const end = current.indexOf(AGENTS_BLOCK_END) + AGENTS_BLOCK_END.length;
    next = `${current.slice(0, start)}${block}${current.slice(end)}`;
  } else {
    const suffix = current.endsWith("\n") ? "" : "\n";
    next = `${current}${suffix}\n${block}`;
  }

  await Bun.write(path, next);
}

export async function generateSkills(workspacePathInput?: string): Promise<GeneratedSkills> {
  const workspacePath = resolve(process.cwd(), workspacePathInput ?? ".");
  const skillsRoot = resolve(workspacePath, ".toss", "skills");
  const skillDir = resolve(skillsRoot, "toss");
  const referencesDir = resolve(skillDir, "references");
  const skillPath = resolve(skillDir, "SKILL.md");
  const contextPath = resolve(referencesDir, "context.md");
  const contractsPath = resolve(referencesDir, "contracts.md");
  const agentsPath = resolve(workspacePath, "AGENTS.md");

  await Promise.all([
    Bun.write(skillPath, tossSkillContent(workspacePath)),
    Bun.write(contextPath, contextReferenceContent()),
    Bun.write(contractsPath, contractsReferenceContent(workspacePath)),
  ]);

  const generated = { skillsRoot, skillDir, skillPath, referencesDir, agentsPath };
  await upsertAgentsFile(agentsPath, agentsBlock(generated));
  return generated;
}
