import { afterEach, describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  applyPlan,
  getHistory,
  getStatus,
  initDatabase,
  isTossError,
  readQuery,
  recoverFromSnapshot,
  revertCommit,
  verifyDatabase,
} from "../src";

const tmpDirs: string[] = [];

function createTestContext(): { dir: string; dbPath: string } {
  const dir = mkdtempSync(join(tmpdir(), "toss-core-test-"));
  tmpDirs.push(dir);
  const dbPath = join(dir, "toss.db");
  return { dir, dbPath };
}

function writePlanFile(dir: string, name: string, payload: unknown): string {
  const path = join(dir, name);
  writeFileSync(path, JSON.stringify(payload), "utf8");
  return path;
}

afterEach(() => {
  while (tmpDirs.length > 0) {
    const dir = tmpDirs.pop();
    if (dir) {
      rmSync(dir, { recursive: true, force: true });
    }
  }
});

describe("toss strong history engine", () => {
  test("init -> apply -> status -> history works with new metadata", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const createPlanPath = writePlanFile(dir, "create.json", {
      message: "create expenses table",
      operations: [
        {
          type: "create_table",
          table: "expenses",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "item", type: "TEXT", notNull: true },
            { name: "amount", type: "INTEGER", notNull: true },
          ],
        },
      ],
    });
    const insertPlanPath = writePlanFile(dir, "insert.json", {
      message: "insert dinner",
      operations: [{ type: "insert", table: "expenses", values: { id: 1, item: "dinner", amount: 1200 } }],
    });

    await applyPlan(createPlanPath, { dbPath });
    const insertCommit = await applyPlan(insertPlanPath, { dbPath });

    const status = getStatus({ dbPath });
    expect(status.historyEngine).toBe("gitlike");
    expect(status.formatGeneration).toBe(1);
    expect(status.tableCount).toBe(1);
    expect(status.headCommit?.commitId).toBe(insertCommit.commitId);
    expect(status.snapshotCount).toBe(0);

    const history = getHistory({ dbPath, verbose: true });
    expect(history).toHaveLength(2);
    expect(history[0]?.commitId).toBe(insertCommit.commitId);
    expect(history[0]?.parentIds).toHaveLength(1);
    expect(history[0]?.stateHashAfter.length).toBeGreaterThan(10);
  });

  test("hard reset format rejects legacy schema unless force-new", async () => {
    const { dbPath } = createTestContext();
    const legacy = new Database(dbPath);
    legacy.run("CREATE TABLE _toss_log(id TEXT PRIMARY KEY)");
    legacy.close(false);

    try {
      await initDatabase({ dbPath });
      throw new Error("initDatabase should have failed for legacy schema");
    } catch (error) {
      expect(isTossError(error)).toBe(true);
      if (isTossError(error)) {
        expect(error.code).toBe("FORMAT_MISMATCH");
      }
    }

    const reinit = await initDatabase({ dbPath, forceNew: true });
    expect(reinit.dbPath).toBe(dbPath);
  });

  test("drop_table revert restores table definition and rows", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup.json", {
      message: "setup",
      operations: [
        {
          type: "create_table",
          table: "expenses",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "item", type: "TEXT", notNull: true },
          ],
        },
        { type: "insert", table: "expenses", values: { id: 1, item: "dinner" } },
      ],
    });
    const drop = writePlanFile(dir, "drop.json", {
      message: "drop table",
      operations: [{ type: "drop_table", table: "expenses" }],
    });

    await applyPlan(setup, { dbPath });
    const dropCommit = await applyPlan(drop, { dbPath });

    const revert = revertCommit(dropCommit.commitId, { dbPath });
    expect(revert.ok).toBe(true);

    const rows = readQuery("SELECT id, item FROM expenses", { dbPath });
    expect(rows).toEqual([{ id: 1, item: "dinner" }]);
  });

  test("drop_column and alter_column_type revert restore schema and values", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup.json", {
      message: "setup ledger",
      operations: [
        {
          type: "create_table",
          table: "ledger",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "amount", type: "TEXT", notNull: true },
            { name: "note", type: "TEXT" },
          ],
        },
        { type: "insert", table: "ledger", values: { id: 1, amount: "1200", note: "meal" } },
      ],
    });
    const mutate = writePlanFile(dir, "mutate.json", {
      message: "schema mutate",
      operations: [
        { type: "alter_column_type", table: "ledger", column: "amount", newType: "INTEGER" },
        { type: "drop_column", table: "ledger", column: "note" },
      ],
    });

    await applyPlan(setup, { dbPath });
    const mutateCommit = await applyPlan(mutate, { dbPath });

    const revert = revertCommit(mutateCommit.commitId, { dbPath });
    expect(revert.ok).toBe(true);

    const typeRow = readQuery("SELECT typeof(amount) AS t, note FROM ledger WHERE id=1", { dbPath });
    expect(typeRow).toEqual([{ t: "text", note: "meal" }]);
  });

  test("update/delete revert detects row conflicts", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup.json", {
      message: "setup",
      operations: [
        {
          type: "create_table",
          table: "items",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "name", type: "TEXT", notNull: true },
            { name: "qty", type: "INTEGER", notNull: true },
          ],
        },
        { type: "insert", table: "items", values: { id: 1, name: "apple", qty: 3 } },
      ],
    });
    const updateA = writePlanFile(dir, "update-a.json", {
      message: "set qty 5",
      operations: [{ type: "update", table: "items", values: { qty: 5 }, where: { id: 1 } }],
    });
    const updateB = writePlanFile(dir, "update-b.json", {
      message: "set qty 7",
      operations: [{ type: "update", table: "items", values: { qty: 7 }, where: { id: 1 } }],
    });

    await applyPlan(setup, { dbPath });
    const target = await applyPlan(updateA, { dbPath });
    await applyPlan(updateB, { dbPath });

    const result = revertCommit(target.commitId, { dbPath });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.conflicts.length).toBeGreaterThan(0);
      expect(result.conflicts[0]?.kind).toBe("row");
    }
  });

  test("revert of revert is supported", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup.json", {
      message: "setup",
      operations: [
        {
          type: "create_table",
          table: "events",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "title", type: "TEXT", notNull: true },
          ],
        },
        { type: "insert", table: "events", values: { id: 1, title: "dentist" } },
      ],
    });
    const drop = writePlanFile(dir, "drop.json", {
      message: "drop events",
      operations: [{ type: "drop_table", table: "events" }],
    });

    await applyPlan(setup, { dbPath });
    const dropped = await applyPlan(drop, { dbPath });
    const first = revertCommit(dropped.commitId, { dbPath });
    expect(first.ok).toBe(true);
    if (!first.ok) {
      throw new Error("expected first revert success");
    }

    const second = revertCommit(first.revertCommit.commitId, { dbPath });
    expect(second.ok).toBe(true);
    if (!second.ok) {
      throw new Error("expected second revert success");
    }

    const tableCount = readQuery("SELECT COUNT(*) AS c FROM sqlite_master WHERE type='table' AND name='events'", { dbPath });
    expect(tableCount).toEqual([{ c: 0 }]);
  });

  test("verify quick/full checks pass and update last_verified_at", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup.json", {
      message: "setup",
      operations: [
        {
          type: "create_table",
          table: "notes",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "body", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    await applyPlan(setup, { dbPath });

    const quick = verifyDatabase({ dbPath });
    expect(quick.ok).toBe(true);
    expect(quick.mode).toBe("quick");

    const full = verifyDatabase({ dbPath, full: true });
    expect(full.ok).toBe(true);
    expect(full.mode).toBe("full");

    const status = getStatus({ dbPath });
    expect(status.lastVerifiedAt).not.toBeNull();
  });

  test("snapshot recover restores and replays commits", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const tweak = new Database(dbPath);
    tweak
      .query("UPDATE _toss_repo_meta SET value='1' WHERE key='snapshot_interval'")
      .run();
    tweak
      .query("UPDATE _toss_repo_meta SET value='10' WHERE key='snapshot_retain'")
      .run();
    tweak.close(false);

    const create = writePlanFile(dir, "create.json", {
      message: "create logs",
      operations: [
        {
          type: "create_table",
          table: "logs",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "msg", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    const insert = writePlanFile(dir, "insert.json", {
      message: "insert log",
      operations: [{ type: "insert", table: "logs", values: { id: 1, msg: "hello" } }],
    });

    const firstCommit = await applyPlan(create, { dbPath });
    await applyPlan(insert, { dbPath });

    const result = await recoverFromSnapshot(firstCommit.commitId, { dbPath });
    expect(result.replayedCommits).toBeGreaterThanOrEqual(1);

    const rows = readQuery("SELECT id, msg FROM logs", { dbPath });
    expect(rows).toEqual([{ id: 1, msg: "hello" }]);
  });

  test("init generates toss skill with migration guidance", async () => {
    const { dir, dbPath } = createTestContext();
    const result = await initDatabase({ dbPath, generateSkills: true, workspacePath: dir });
    expect(result.generatedSkills).not.toBeNull();
    if (!result.generatedSkills) {
      throw new Error("generatedSkills should exist");
    }

    expect(existsSync(result.generatedSkills.skillPath)).toBe(true);
    const skill = readFileSync(result.generatedSkills.skillPath, "utf8");
    expect(skill.includes("toss history --verbose")).toBe(true);
    expect(skill.includes("toss verify --quick")).toBe(true);
    expect(skill.includes("staged migrations")).toBe(true);
  });
});
