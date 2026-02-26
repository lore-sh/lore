import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { CodedError, connect, initDb, push } from "../src";
import { installHttpRemoteFixture, registerHttpRemoteFixture } from "./fixtures/http-remote";
import { createTestContext, currentDb, withDbPath, withTmpDirCleanup } from "./helpers";

installHttpRemoteFixture();

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

function firstAppliedMigrationId(db: Database): string {
  const row = db.query<{ id: string }, []>("SELECT id FROM __drizzle_migrations ORDER BY id LIMIT 1").get();
  if (!row?.id) {
    throw new Error("Expected at least one applied migration");
  }
  return row.id;
}

describe("migration immutability", () => {
  testWithTmp("local init fails when an applied migration hash is edited", async () => {
    const { dbPath } = createTestContext();
    await initDb({ dbPath });

    const raw = new Database(dbPath);
    try {
      const id = firstAppliedMigrationId(raw);
      raw.query("UPDATE __drizzle_migrations SET hash = ? WHERE id = ?").run("tampered", id);
    } finally {
      raw.close(false);
    }

    try {
      await initDb({ dbPath });
      throw new Error("initDb should fail when migration hash is tampered");
    } catch (error) {
      expect(CodedError.hasCode(error, "IMMUTABLE_MIGRATION_EDITED")).toBe(true);
    }
  });

  testWithTmp("remote push fails when an applied migration hash is edited", async () => {
    const local = createTestContext();
    const remote = createTestContext();
    const remoteUrl = registerHttpRemoteFixture(remote.dbPath);

    await initDb({ dbPath: local.dbPath });
    await withDbPath(local.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await push(currentDb());
    });

    const remoteRaw = new Database(remote.dbPath);
    try {
      const id = firstAppliedMigrationId(remoteRaw);
      remoteRaw.query("UPDATE __drizzle_migrations SET hash = ? WHERE id = ?").run("tampered", id);
    } finally {
      remoteRaw.close(false);
    }

    await withDbPath(local.dbPath, async () => {
      try {
        await push(currentDb());
        throw new Error("push should fail when remote migration hash is tampered");
      } catch (error) {
        expect(CodedError.hasCode(error, "IMMUTABLE_MIGRATION_EDITED")).toBe(true);
      }
    });
  });
});
