import { mkdir, rm, stat } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { deleteIfExists } from "./fsx";
import type { SkillPlatform } from "./types";

const ALL_PLATFORMS: SkillPlatform[] = ["claude", "cursor", "codex", "opencode", "openclaw"];
const PLATFORM_SET = new Set<SkillPlatform>(ALL_PLATFORMS);

const AGENTS_BLOCK_START = "<!-- toss:init:agents:start -->";
const AGENTS_BLOCK_END = "<!-- toss:init:agents:end -->";
const CLAUDE_BLOCK_START = "<!-- toss:init:claude:start -->";
const CLAUDE_BLOCK_END = "<!-- toss:init:claude:end -->";
const AGENTS_BLOCKS: ManagedBlock[] = [{ start: AGENTS_BLOCK_START, end: AGENTS_BLOCK_END }];
const CLAUDE_BLOCKS: ManagedBlock[] = [{ start: CLAUDE_BLOCK_START, end: CLAUDE_BLOCK_END }];

type GeneratedPlatform = SkillPlatform | "shared";

interface ManagedBlock {
  start: string;
  end: string;
}

interface SkillPaths {
  sharedSkillDir: string;
  sharedSkillPath: string;
  sharedContextPath: string;
  sharedContractsPath: string;
  codexAgentsPath: string;
  opencodeAgentsPath: string;
  cursorRulePath: string;
  claudeSkillDir: string;
  claudeSkillPath: string;
  claudeContextPath: string;
  claudeContractsPath: string;
  claudeDocPath: string;
  openclawSkillDir: string;
  openclawSkillPath: string;
  openclawContextPath: string;
  openclawContractsPath: string;
  openclawAgentsPath: string;
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
}

function resolveHomeDir(): string {
  const home = process.env.HOME ?? process.env.USERPROFILE;
  if (!home) {
    throw new Error("HOME (or USERPROFILE) is required for global skill installation");
  }
  return resolve(home);
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
    sharedContextPath: resolve(sharedReferencesDir, "context.md"),
    sharedContractsPath: resolve(sharedReferencesDir, "contracts.md"),
    codexAgentsPath: resolve(codexHome, "AGENTS.md"),
    opencodeAgentsPath: resolve(opencodeHome, "AGENTS.md"),
    cursorRulePath: resolve(home, ".cursor", "rules", "toss.mdc"),
    claudeSkillDir,
    claudeSkillPath: resolve(claudeSkillDir, "SKILL.md"),
    claudeContextPath: resolve(claudeReferencesDir, "context.md"),
    claudeContractsPath: resolve(claudeReferencesDir, "contracts.md"),
    claudeDocPath: resolve(home, ".claude", "CLAUDE.md"),
    openclawSkillDir,
    openclawSkillPath: resolve(openclawSkillDir, "SKILL.md"),
    openclawContextPath: resolve(openclawReferencesDir, "context.md"),
    openclawContractsPath: resolve(openclawReferencesDir, "contracts.md"),
    openclawAgentsPath: resolve(openclawWorkspace, "AGENTS.md"),
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
description: Use toss CLI to remember/store data, evolve schemas with migrations, and recall/query safely via read-before-apply workflows.
---

# toss

Use this skill for toss workflows that need durable writes, schema changes, and read-side analysis.

## Command surface
- Write path: \`bun run --cwd "$PWD" toss apply --plan -\`
- Read path: \`bun run --cwd "$PWD" toss read --sql "<SELECT ...>" --json\`
- History path: \`bun run --cwd "$PWD" toss history --verbose\`
- Verify path: \`bun run --cwd "$PWD" toss verify --full\`

## Remember Flow (read-before-apply)
1. Read schema snapshot first:
\`\`\`bash
bun run --cwd "$PWD" toss read --sql "SELECT m.name AS table_name, p.name AS column_name, p.type, p.notnull FROM sqlite_master m JOIN pragma_table_info(m.name) p WHERE m.type='table' AND m.name NOT LIKE '_toss_%' AND m.name NOT LIKE 'sqlite_%' ORDER BY m.name, p.cid" --json
\`\`\`
2. Build an OperationPlan from current schema plus user intent.
3. Apply via stdin:
\`\`\`bash
cat <<'JSON' | bun run --cwd "$PWD" toss apply --plan -
{"message":"<what this apply does>","operations":[...],"source":{"planner":"agent-skill","skill":"toss"}}
JSON
\`\`\`
4. If apply fails due to schema mismatch, re-read schema and retry once.

## Recall Flow
1. Convert request to read-only SQL (\`SELECT\` or \`WITH ... SELECT\`).
2. Run:
\`\`\`bash
bun run --cwd "$PWD" toss read --sql "<SELECT ...>" --json
\`\`\`
3. Return structured results with a short interpretation.

## Hard Rules
- Keep one semantic unit per apply.
- Always include a non-empty \`message\`.
- Never run \`update\` or \`delete\` without explicit \`where\`.
- Prefer staged migrations: additive -> backfill -> verify -> cleanup.

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

This keeps execution deterministic and auditable.

## Philosophy
1. Humans should not need to design schema/migrations manually.
2. Data is owned by individuals (local-first SQLite).
3. Be bold with safety: append-only history + revert.
4. Schema should evolve continuously with data migration.

## 2-layer model
- Commit Log: immutable source of truth (\`_toss_commit\`, \`_toss_op\`, \`_toss_effect_*\`)
- HEAD State: materialized current tables, always rebuildable

## Typical use cases
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

function contractsReferenceContent(): string {
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
  "source": { "planner": "agent-skill", "skill": "toss" }
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

## read contract
\`toss read --sql "<query>" [--json]\`
- Only \`SELECT\` / \`WITH ... SELECT\`
- Single statement only

## history / verify / revert
- \`toss history --verbose\`: includes parent ids, state hash, inverse readiness
- \`toss verify\` / \`toss verify --full\`: chain hash + SQLite integrity checks
- \`toss revert <commit_id>\`: returns row/schema conflict details
`;
}

function cursorRuleContent(): string {
  return `---
description: toss workflow for durable storage and query
alwaysApply: false
---

Use toss for memory/storage requests and analytical reads.

Commands:
- \`bun run --cwd "$PWD" toss read --sql "<SELECT ...>" --json\`
- \`bun run --cwd "$PWD" toss apply --plan -\`

Rules:
- Read schema before write plans.
- Keep one semantic unit per apply.
- Never run update/delete without explicit where.
- For destructive migration, verify before and after.
`;
}

function agentsBlock(skillPath: string): string {
  return `${AGENTS_BLOCK_START}
## Skills
### Available skills
- toss: Unified toss workflow for remember/store, schema evolution + data migration, and recall/query with read-before-apply safety. (file: ${skillPath})
### How to use skills
- Trigger: use \`toss\` whenever user intent touches toss memory, storage, query, or analysis.
- For writes: read schema first, then build OperationPlan with explicit migration intent.
- For reads: generate read-only SQL only.
- For destructive changes: run \`toss history --verbose\` and \`toss verify --quick\` before apply.
${AGENTS_BLOCK_END}
`;
}

function claudeBlock(skillPath: string): string {
  return `${CLAUDE_BLOCK_START}
## Skills
- toss: Unified toss workflow for remember/store, schema evolution + data migration, and recall/query with read-before-apply safety. (file: ${skillPath})
## How to use skills
- Use \`toss\` whenever a request needs durable write/query on toss data.
- Read schema before apply, and keep destructive changes staged.
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

function isEnoent(error: unknown): boolean {
  return error instanceof Error && "code" in error && error.code === "ENOENT";
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

async function writeSkillBundle(skillPath: string, contextPath: string, contractsPath: string): Promise<void> {
  await Promise.all([
    writeText(skillPath, tossSkillContent()),
    writeText(contextPath, contextReferenceContent()),
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
    await writeSkillBundle(paths.sharedSkillPath, paths.sharedContextPath, paths.sharedContractsPath);
    addGeneratedFiles(files, "shared", paths.sharedSkillPath, paths.sharedContextPath, paths.sharedContractsPath);
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
    await writeSkillBundle(paths.claudeSkillPath, paths.claudeContextPath, paths.claudeContractsPath);
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
      paths.claudeContextPath,
      paths.claudeContractsPath,
      paths.claudeDocPath,
    );
  } else {
    await removeDirIfExists(paths.claudeSkillDir);
    await removeManagedBlocks(paths.claudeDocPath, CLAUDE_BLOCKS);
  }

  if (selectedSet.has("openclaw")) {
    await writeSkillBundle(paths.openclawSkillPath, paths.openclawContextPath, paths.openclawContractsPath);
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
      paths.openclawContextPath,
      paths.openclawContractsPath,
      paths.openclawAgentsPath,
    );
  } else {
    await removeDirIfExists(paths.openclawSkillDir);
    await removeManagedBlocks(paths.openclawAgentsPath, AGENTS_BLOCKS);
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
  ];

  const results = await Promise.all(targets.map((t) => t.remove()));
  const files = targets.map((t, i) => ({ platform: t.platform, path: t.path, removed: results[i]! }));
  return { files };
}
