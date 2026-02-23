import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import {
  getStatus,
  initDatabase,
  verifyDatabase,
} from "../src";
import { COMMIT_TABLE } from "../src/engine/db";
import { applyPlan, createTestContext, writePlanFile, withTmpDirCleanup, currentDb } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

describe("verifyDatabase", () => {
  testWithTmp("verify stores last_verified_ok", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = await writePlanFile(dir, "setup.json", {
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

    const quick = verifyDatabase(currentDb());
    expect(quick.ok).toBe(true);
    expect(quick.mode).toBe("quick");

    const firstStatus = getStatus(currentDb());
    expect(firstStatus.lastVerifiedAt).not.toBeNull();
    expect(firstStatus.lastVerifiedOk).toBe(true);

    const tamper = new Database(dbPath);
    tamper.query(`UPDATE ${COMMIT_TABLE} SET message='tampered' WHERE seq=1`).run();
    tamper.close(false);

    const broken = verifyDatabase(currentDb(), { full: true });
    expect(broken.ok).toBe(false);
    expect(broken.mode).toBe("full");

    const secondStatus = getStatus(currentDb());
    expect(secondStatus.lastVerifiedOk).toBe(false);
  });
});
