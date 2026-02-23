import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { createEngineDb } from "../../src/engine/client";
import { initDatabase } from "../../src";
import { createTestContext, withTmpDirCleanup } from "../helpers";
import { MetaTable } from "../../src/engine/schema.sql";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

describe("engine client", () => {
  testWithTmp("createEngineDb binds drizzle to sqlite handle", async () => {
    const { dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const db = new Database(dbPath, { strict: true });
    try {
      const engineDb = createEngineDb(db);
      const rows = engineDb.select().from(MetaTable).limit(1).all();
      expect(rows.length).toBeGreaterThan(0);
    } finally {
      db.close(false);
    }
  });
});
