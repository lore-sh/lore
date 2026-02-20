import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import {
  applyPlan,
  getStatus,
  initDatabase,
  verifyDatabase,
} from "../src";
import { COMMIT_TABLE } from "../src/db";
import { createTestContext, writePlanFile, withTmpDirCleanup } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));


describe("verifyDatabase", () => {
  testWithTmp("verify stores last_verified_ok and preserves last_verified_ok_at on failure", async () => {
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
    await applyPlan(setup);

    const quick = verifyDatabase();
    expect(quick.ok).toBe(true);
    expect(quick.mode).toBe("quick");

    const firstStatus = getStatus();
    expect(firstStatus.lastVerifiedAt).not.toBeNull();
    expect(firstStatus.lastVerifiedOk).toBe(true);
    expect(firstStatus.lastVerifiedOkAt).not.toBeNull();

    const tamper = new Database(dbPath);
    tamper.query(`UPDATE ${COMMIT_TABLE} SET message='tampered' WHERE seq=1`).run();
    tamper.close(false);

    const broken = verifyDatabase({ full: true });
    expect(broken.ok).toBe(false);
    expect(broken.mode).toBe("full");

    const secondStatus = getStatus();
    expect(secondStatus.lastVerifiedOk).toBe(false);
    expect(secondStatus.lastVerifiedOkAt).toBe(firstStatus.lastVerifiedOkAt);
  });
});
