import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import {
  history,
  commitSize,
  historySize,
  findCommit,
  commitOperations,
  commitRowEffects,
  decodeRowEffects,
  commitSchemaEffects,
  initDb,
  status,
  verify,
} from "../src";
import { COMMIT_TABLE } from "../src/db";
import { applyPlan, createTestContext, writePlanFile, withTmpDirCleanup, currentDb } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

async function seedTaskHistory(dir: string): Promise<void> {
  const createPlanPath = await writePlanFile(dir, "history-create.json", {
    message: "create tasks",
    operations: [
      {
        type: "create_table",
        table: "tasks",
        columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
      },
    ],
  });
  const insertPlanPath = await writePlanFile(dir, "history-insert.json", {
    message: "insert task",
    operations: [{ type: "insert", table: "tasks", values: { id: 1 } }],
  });

  await applyPlan(currentDb(), createPlanPath);
  await applyPlan(currentDb(), insertPlanPath);
}

describe("history domain", () => {
  testWithTmp("history and commit detail APIs expose commit domain objects", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });
    await seedTaskHistory(dir);

    const summaries = history(currentDb());
    expect(summaries).toHaveLength(2);
    expect(summaries[0]?.message).toBe("insert task");

    const latestId = summaries[0]?.commitId;
    expect(typeof latestId).toBe("string");
    if (!latestId) {
      throw new Error("expected latest commit id");
    }

    const commit = findCommit(currentDb(), latestId);
    expect(commit).not.toBeNull();
    expect(commitOperations(currentDb(), latestId)).toHaveLength(1);

    const rowEffects = decodeRowEffects(commitRowEffects(currentDb(), latestId));
    expect(rowEffects).toHaveLength(1);
    expect(rowEffects[0]?.tableName).toBe("tasks");
    const schemaEffects = commitSchemaEffects(currentDb(), latestId);
    expect(schemaEffects).toHaveLength(0);
  });

  testWithTmp("history filters by historical table names even after drop", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const createPlanPath = await writePlanFile(dir, "history-filter-create.json", {
      message: "create invoices",
      operations: [
        {
          type: "create_table",
          table: "invoices",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });
    const dropPlanPath = await writePlanFile(dir, "history-filter-drop.json", {
      message: "drop invoices",
      operations: [{ type: "drop_table", table: "invoices" }],
    });

    await applyPlan(currentDb(), createPlanPath);
    await applyPlan(currentDb(), dropPlanPath);

    const filtered = history(currentDb(), { table: "INVOICES" });
    expect(filtered.length).toBeGreaterThan(0);
    expect(history(currentDb(), { table: "missing_table" })).toEqual([]);
  });

  testWithTmp("commitSize and historySize return non-zero values for populated history", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });
    await seedTaskHistory(dir);

    const latestCommit = history(currentDb(), { limit: 1 })[0];
    if (!latestCommit) {
      throw new Error("expected latest commit");
    }

    const latestSize = commitSize(currentDb(), latestCommit.commitId);
    const totalHistorySize = historySize(currentDb());
    expect(latestSize).toBeGreaterThan(0);
    expect(totalHistorySize).toBeGreaterThanOrEqual(latestSize);
  });

  testWithTmp("verify stores last_verified_ok and reports tampering", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const setup = await writePlanFile(dir, "verify-setup.json", {
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
    await applyPlan(currentDb(), setup);

    const quick = verify(currentDb());
    expect(quick.ok).toBe(true);
    expect(quick.mode).toBe("quick");

    const firstStatus = status(currentDb());
    expect(firstStatus.lastVerifiedAt).not.toBeNull();
    expect(firstStatus.lastVerifiedOk).toBe(true);

    const tamper = new Database(dbPath);
    tamper.query(`UPDATE ${COMMIT_TABLE} SET message='tampered' WHERE seq=1`).run();
    tamper.close(false);

    const broken = verify(currentDb(), { full: true });
    expect(broken.ok).toBe(false);
    expect(broken.mode).toBe("full");

    const secondStatus = status(currentDb());
    expect(secondStatus.lastVerifiedOk).toBe(false);
  });
});
