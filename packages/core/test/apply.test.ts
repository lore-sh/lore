import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import {
  applyPlan,
  getHistory,
  getStatus,
  initDatabase,
  isTossError,
} from "../src";
import { createTestContext, writePlanFile, withTmpDirCleanup } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));


describe("applyPlan", () => {
  testWithTmp("init -> apply -> status -> history works", async () => {
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
    expect(status.tableCount).toBe(1);
    expect(status.headCommit?.commitId).toBe(insertCommit.commitId);
    expect(status.snapshotCount).toBe(0);
    expect(status.lastVerifiedAt).toBeNull();
    expect(status.lastVerifiedOk).toBeNull();
    expect(status.lastVerifiedOkAt).toBeNull();

    const history = getHistory({ dbPath, verbose: true });
    expect(history).toHaveLength(2);
    expect(history[0]?.commitId).toBe(insertCommit.commitId);
    expect(history[0]?.parentIds).toHaveLength(1);
    expect(history[0]?.stateHashAfter.length).toBeGreaterThan(10);
  });

  testWithTmp("operations fail for table without primary key", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE no_pk (name TEXT)");
    direct.close(false);

    const insertPlan = writePlanFile(dir, "insert-no-pk.json", {
      message: "insert no pk",
      operations: [{ type: "insert", table: "no_pk", values: { name: "a" } }],
    });

    try {
      await applyPlan(insertPlan, { dbPath });
      throw new Error("applyPlan should fail for table without PK");
    } catch (error) {
      expect(isTossError(error)).toBe(true);
      if (isTossError(error)) {
        expect(error.code).toBe("TABLE_WITHOUT_PRIMARY_KEY");
      }
    }
  });
});
