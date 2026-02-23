import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { chmod } from "node:fs/promises";
import {
  autoSync,
  clone,
  connect,
  getSyncConfig,
  remoteStatus,
  status,
  initDb,
  CodedError,
  pull,
  push,
  readAuthToken,
  query,
  revert,
  verify,
  writeAuthToken,
  writeRemoteConfig,
} from "../src";
import { LAST_SYNC_STATE_META_KEY } from "../src/engine/db";
import { applyPlan, createTestContext, currentDb, withDbPath, withTmpDirCleanup, writePlanFile } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

function remoteUrlFor(path: string): string {
  return `file:${path}`;
}

describe("sync with Turso protocol", () => {
  testWithTmp("push without remote config returns SYNC_NOT_CONFIGURED", async () => {
    const local = createTestContext();
    await initDb({ dbPath: local.dbPath });

    await withDbPath(local.dbPath, async () => {
      try {
        await push(currentDb());
        throw new Error("push should fail when remote is not configured");
      } catch (error) {
        expect(CodedError.hasCode(error, "SYNC_NOT_CONFIGURED")).toBe(true);
      }

      const currentStatus = status(currentDb());
      expect(currentStatus.sync.state).toBe("offline");
      expect(currentStatus.sync.lastError).toBe("Remote is not configured");
    });
  });

  testWithTmp("A apply -> push and B pull reaches same state", async () => {
    const a = createTestContext();
    const b = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: a.dbPath });
    await initDb({ dbPath: b.dbPath });

    const createPlan = await writePlanFile(a.dir, "create.json", {
      message: "create tasks",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "title", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    const insertPlan = await writePlanFile(a.dir, "insert.json", {
      message: "insert task",
      operations: [{ type: "insert", table: "tasks", values: { id: 1, title: "buy milk" } }],
    });

    const expectedHead = await withDbPath(a.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await applyPlan(currentDb(), createPlan);
      const commit = await applyPlan(currentDb(), insertPlan);
      const pushed = await push(currentDb());
      expect(pushed.pushed).toBe(2);
      return commit.commitId;
    });

    await withDbPath(b.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      const pulled = await pull(currentDb());
      expect(pulled.pulled).toBe(2);
      const rows = query(currentDb(), "SELECT id, title FROM tasks ORDER BY id");
      expect(rows).toEqual([{ id: 1, title: "buy milk" }]);
      expect(status(currentDb()).headCommit?.commitId).toBe(expectedHead);
    });
  });

  testWithTmp("push materializes remote projection and reports projection health", async () => {
    const local = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: local.dbPath });

    const createPlan = await writePlanFile(local.dir, "create.json", {
      message: "create tasks",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "title", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    const insertPlan = await writePlanFile(local.dir, "insert.json", {
      message: "insert task",
      operations: [{ type: "insert", table: "tasks", values: { id: 1, title: "milk" } }],
    });

    await withDbPath(local.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await applyPlan(currentDb(), createPlan);
      const inserted = await applyPlan(currentDb(), insertPlan);
      const pushed = await push(currentDb());
      expect(pushed.remoteHead).toBe(inserted.commitId);

      const remoteInfo = await remoteStatus(currentDb());
      expect(remoteInfo.projectionHead).toBe(inserted.commitId);
      expect(remoteInfo.projectionLagCommits).toBe(0);
      expect(remoteInfo.projectionError).toBeNull();
    });

    const remoteDb = new Database(remote.dbPath);
    try {
      const rows = remoteDb.query<{ id: number; title: string }, []>("SELECT id, title FROM tasks ORDER BY id").all();
      expect(rows).toEqual([{ id: 1, title: "milk" }]);
    } finally {
      remoteDb.close(false);
    }
  });

  testWithTmp("first push rebuilds projection when remote head is null and user tables already exist", async () => {
    const local = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: local.dbPath });

    const createPlan = await writePlanFile(local.dir, "create.json", {
      message: "create tasks",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "title", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    const insertPlan = await writePlanFile(local.dir, "insert.json", {
      message: "insert task",
      operations: [{ type: "insert", table: "tasks", values: { id: 1, title: "fresh" } }],
    });

    await withDbPath(local.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await applyPlan(currentDb(), createPlan);
      const inserted = await applyPlan(currentDb(), insertPlan);

      const remoteDb = new Database(remote.dbPath);
      try {
        remoteDb.query("CREATE TABLE legacy (id INTEGER PRIMARY KEY, note TEXT NOT NULL)").run();
        remoteDb.query("INSERT INTO legacy(id, note) VALUES (1, 'stale')").run();
      } finally {
        remoteDb.close(false);
      }

      const pushed = await push(currentDb());
      expect(pushed.pushed).toBe(2);
      expect(pushed.remoteHead).toBe(inserted.commitId);

      const remoteInfo = await remoteStatus(currentDb());
      expect(remoteInfo.projectionHead).toBe(inserted.commitId);
      expect(remoteInfo.projectionError).toBeNull();
    });

    const remoteDbAfter = new Database(remote.dbPath);
    try {
      const rows = remoteDbAfter.query<{ id: number; title: string }, []>("SELECT id, title FROM tasks ORDER BY id").all();
      expect(rows).toEqual([{ id: 1, title: "fresh" }]);

      const legacyTable = remoteDbAfter
        .query<{ name: string }, []>("SELECT name FROM sqlite_master WHERE type='table' AND name='legacy' LIMIT 1")
        .all();
      expect(legacyTable).toEqual([]);
    } finally {
      remoteDbAfter.close(false);
    }
  });

  testWithTmp("push materialization stays stable when schema replay changes physical rowid order", async () => {
    const local = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: local.dbPath });

    const createPlan = await writePlanFile(local.dir, "create-accounts.json", {
      message: "create accounts",
      operations: [
        {
          type: "create_table",
          table: "accounts",
          columns: [
            { name: "id", type: "TEXT", primaryKey: true },
            { name: "note", type: "TEXT" },
          ],
        },
      ],
    });
    const insertBPlan = await writePlanFile(local.dir, "insert-b.json", {
      message: "insert b",
      operations: [{ type: "insert", table: "accounts", values: { id: "b", note: "first" } }],
    });
    const insertAPlan = await writePlanFile(local.dir, "insert-a.json", {
      message: "insert a",
      operations: [{ type: "insert", table: "accounts", values: { id: "a", note: "second" } }],
    });
    const rebuildPlan = await writePlanFile(local.dir, "drop-note.json", {
      message: "drop note",
      operations: [{ type: "drop_column", table: "accounts", column: "note" }],
    });

    await withDbPath(local.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await applyPlan(currentDb(), createPlan);
      await applyPlan(currentDb(), insertBPlan);
      await applyPlan(currentDb(), insertAPlan);
      const rebuilt = await applyPlan(currentDb(), rebuildPlan);

      const pushed = await push(currentDb());
      expect(pushed.remoteHead).toBe(rebuilt.commitId);

      const remoteInfo = await remoteStatus(currentDb());
      expect(remoteInfo.projectionHead).toBe(rebuilt.commitId);
      expect(remoteInfo.projectionLagCommits).toBe(0);
      expect(remoteInfo.projectionError).toBeNull();
    });

    const remoteDb = new Database(remote.dbPath);
    try {
      const rows = remoteDb.query<{ id: string }, []>("SELECT id FROM accounts ORDER BY id").all();
      expect(rows).toEqual([{ id: "a" }, { id: "b" }]);
    } finally {
      remoteDb.close(false);
    }
  });

  testWithTmp("repeated push is idempotent", async () => {
    const a = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: a.dbPath });

    const createPlan = await writePlanFile(a.dir, "create.json", {
      message: "create notes",
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
    const insertPlan = await writePlanFile(a.dir, "insert.json", {
      message: "insert note",
      operations: [{ type: "insert", table: "notes", values: { id: 1, body: "hello" } }],
    });

    await withDbPath(a.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await applyPlan(currentDb(), createPlan);
      await applyPlan(currentDb(), insertPlan);
      const first = await push(currentDb());
      const second = await push(currentDb());
      expect(first.pushed).toBe(2);
      expect(second.pushed).toBe(0);
      expect(second.state).toBe("synced");
    });

    const remoteDb = new Database(remote.dbPath);
    try {
      const rows = remoteDb.query<{ id: number; body: string }, []>("SELECT id, body FROM notes ORDER BY id").all();
      expect(rows).toEqual([{ id: 1, body: "hello" }]);
    } finally {
      remoteDb.close(false);
    }
  });

  testWithTmp("revert commit materializes correctly on remote", async () => {
    const local = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: local.dbPath });

    const createPlan = await writePlanFile(local.dir, "create.json", {
      message: "create tasks",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "title", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    const insertPlan = await writePlanFile(local.dir, "insert.json", {
      message: "insert task",
      operations: [{ type: "insert", table: "tasks", values: { id: 1, title: "walk" } }],
    });

    await withDbPath(local.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await applyPlan(currentDb(), createPlan);
      const inserted = await applyPlan(currentDb(), insertPlan);
      await push(currentDb());
      const reverted = revert(currentDb(), inserted.commitId);
      expect(reverted.ok).toBe(true);
      await push(currentDb());
    });

    const remoteDb = new Database(remote.dbPath);
    try {
      const rows = remoteDb.query<{ c: number }, []>("SELECT COUNT(*) AS c FROM tasks").get();
      expect(rows?.c ?? 0).toBe(0);
    } finally {
      remoteDb.close(false);
    }
  });

  testWithTmp("projection failure keeps canonical history unchanged", async () => {
    const local = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: local.dbPath });

    const createPlan = await writePlanFile(local.dir, "create.json", {
      message: "create notes",
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
    const insertPlan = await writePlanFile(local.dir, "insert.json", {
      message: "insert note",
      operations: [{ type: "insert", table: "notes", values: { id: 1, body: "from-local" } }],
    });

    let initialHead: string | null = null;
    await withDbPath(local.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      const created = await applyPlan(currentDb(), createPlan);
      initialHead = created.commitId;
      await push(currentDb());
    });

    const remoteDb = new Database(remote.dbPath);
    try {
      remoteDb.query("INSERT INTO notes(id, body) VALUES (1, 'tampered')").run();
    } finally {
      remoteDb.close(false);
    }

    await withDbPath(local.dbPath, async () => {
      await applyPlan(currentDb(), insertPlan);
      try {
        await push(currentDb());
        throw new Error("push should fail when projection precondition is broken");
      } catch (error) {
        expect(CodedError.is(error)).toBe(true);
        if (CodedError.is(error)) {
          expect(error.code).toBe("SYNC_DIVERGED");
        }
      }
    });

    const remoteDbAfter = new Database(remote.dbPath);
    try {
      const head = remoteDbAfter
        .query<{ commit_id: string | null }, []>("SELECT commit_id FROM _toss_ref WHERE name='main' LIMIT 1")
        .get();
      expect(head?.commit_id ?? null).toBe(initialHead);
      const count = remoteDbAfter.query<{ c: number }, []>("SELECT COUNT(*) AS c FROM _toss_commit").get();
      expect(count?.c ?? 0).toBe(1);
      const projectionError = remoteDbAfter
        .query<{ value: string }, []>(
          "SELECT value FROM _toss_meta WHERE key='last_materialized_error' LIMIT 1",
        )
        .get();
      expect((projectionError?.value ?? "").length).toBeGreaterThan(0);
    } finally {
      remoteDbAfter.close(false);
    }
  });

  testWithTmp("drift on unrelated remote row is detected before push", async () => {
    const local = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: local.dbPath });

    const createPlan = await writePlanFile(local.dir, "create.json", {
      message: "create notes",
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
    const insert1 = await writePlanFile(local.dir, "insert-1.json", {
      message: "insert 1",
      operations: [{ type: "insert", table: "notes", values: { id: 1, body: "a" } }],
    });
    const insert2 = await writePlanFile(local.dir, "insert-2.json", {
      message: "insert 2",
      operations: [{ type: "insert", table: "notes", values: { id: 2, body: "b" } }],
    });

    await withDbPath(local.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await applyPlan(currentDb(), createPlan);
      await applyPlan(currentDb(), insert1);
      await push(currentDb());
    });

    const remoteDb = new Database(remote.dbPath);
    try {
      remoteDb.query("UPDATE notes SET body='tampered' WHERE id=1").run();
    } finally {
      remoteDb.close(false);
    }

    await withDbPath(local.dbPath, async () => {
      await applyPlan(currentDb(), insert2);
      try {
        await push(currentDb());
        throw new Error("push should fail when projection drift exists");
      } catch (error) {
        expect(CodedError.is(error)).toBe(true);
        if (CodedError.is(error)) {
          expect(error.code).toBe("SYNC_DIVERGED");
          expect(error.message.includes("state_hash_after mismatch")).toBe(true);
        }
      }
    });

    const remoteDbAfter = new Database(remote.dbPath);
    try {
      const count = remoteDbAfter.query<{ c: number }, []>("SELECT COUNT(*) AS c FROM _toss_commit").get();
      expect(count?.c ?? 0).toBe(2);
    } finally {
      remoteDbAfter.close(false);
    }
  });

  testWithTmp("materialization SQL runtime error is persisted to last_materialized_error", async () => {
    const local = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: local.dbPath });

    const createPlan = await writePlanFile(local.dir, "create.json", {
      message: "create notes",
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
    const insertPlan = await writePlanFile(local.dir, "insert.json", {
      message: "insert note",
      operations: [{ type: "insert", table: "notes", values: { id: 1, body: "ok" } }],
    });

    let createCommitId = "";
    let insertCommitId = "";
    await withDbPath(local.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      const c1 = await applyPlan(currentDb(), createPlan);
      const c2 = await applyPlan(currentDb(), insertPlan);
      createCommitId = c1.commitId;
      insertCommitId = c2.commitId;
      await push(currentDb());
    });

    const tamperedAfterRow = JSON.stringify({
      id: { storageClass: "integer", sqlLiteral: "1" },
      body: { storageClass: "text", sqlLiteral: "BROKEN(" },
    });
    const remoteDb = new Database(remote.dbPath);
    try {
      remoteDb
        .query("UPDATE _toss_row_effect SET after_json = ? WHERE commit_id = ? AND effect_index = 0")
        .run(tamperedAfterRow, insertCommitId);
      remoteDb
        .query(
          "INSERT INTO _toss_meta(key, value) VALUES ('last_materialized_commit', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        )
        .run(createCommitId);
    } finally {
      remoteDb.close(false);
    }

    await withDbPath(local.dbPath, async () => {
      try {
        await push(currentDb());
        throw new Error("push should fail when replay SQL runtime error occurs");
      } catch (error) {
        expect(CodedError.is(error)).toBe(true);
        if (CodedError.is(error)) {
          expect(error.code).toBe("SYNC_DIVERGED");
        }
      }
    });

    const remoteDbAfter = new Database(remote.dbPath);
    try {
      const projectionError = remoteDbAfter
        .query<{ value: string }, []>(
          "SELECT value FROM _toss_meta WHERE key='last_materialized_error' LIMIT 1",
        )
        .get();
      expect((projectionError?.value ?? "").startsWith("Remote projection failed:")).toBe(true);
    } finally {
      remoteDbAfter.close(false);
    }
  });

  testWithTmp("remote status detects projection drift even without push", async () => {
    const local = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: local.dbPath });

    const createPlan = await writePlanFile(local.dir, "create.json", {
      message: "create tasks",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "title", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    const insertPlan = await writePlanFile(local.dir, "insert.json", {
      message: "insert task",
      operations: [{ type: "insert", table: "tasks", values: { id: 1, title: "clean" } }],
    });

    await withDbPath(local.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await applyPlan(currentDb(), createPlan);
      await applyPlan(currentDb(), insertPlan);
      await push(currentDb());
    });

    const remoteDb = new Database(remote.dbPath);
    try {
      remoteDb.query("UPDATE tasks SET title='dirty' WHERE id=1").run();
    } finally {
      remoteDb.close(false);
    }

    await withDbPath(local.dbPath, async () => {
      const remoteInfo = await remoteStatus(currentDb());
      expect(remoteInfo.projectionLagCommits).toBe(0);
      expect(remoteInfo.projectionError).not.toBeNull();
      expect(remoteInfo.projectionError?.includes("state_hash_after mismatch")).toBe(true);
    });
  });

  testWithTmp("remote status flags projection checkpoint ahead of remote head", async () => {
    const local = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: local.dbPath });

    const createPlan = await writePlanFile(local.dir, "create.json", {
      message: "create tasks",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "title", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    const insert1 = await writePlanFile(local.dir, "insert-1.json", {
      message: "insert 1",
      operations: [{ type: "insert", table: "tasks", values: { id: 1, title: "one" } }],
    });
    const insert2 = await writePlanFile(local.dir, "insert-2.json", {
      message: "insert 2",
      operations: [{ type: "insert", table: "tasks", values: { id: 2, title: "two" } }],
    });

    let rollbackHead = "";
    let projectionHead = "";
    await withDbPath(local.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await applyPlan(currentDb(), createPlan);
      const c2 = await applyPlan(currentDb(), insert1);
      const c3 = await applyPlan(currentDb(), insert2);
      rollbackHead = c2.commitId;
      projectionHead = c3.commitId;
      await push(currentDb());
    });

    const remoteDb = new Database(remote.dbPath);
    try {
      remoteDb.query("UPDATE _toss_ref SET commit_id=?, updated_at=? WHERE name='main'").run(rollbackHead, Date.now());
    } finally {
      remoteDb.close(false);
    }

    await withDbPath(local.dbPath, async () => {
      const remoteInfo = await remoteStatus(currentDb());
      expect(remoteInfo.projectionHead).toBe(projectionHead);
      expect(remoteInfo.projectionLagCommits).toBe(0);
      expect(remoteInfo.projectionError).not.toBeNull();
      expect(remoteInfo.projectionError?.includes("ahead of remote HEAD")).toBe(true);
    });
  });

  testWithTmp("push rebuilds projection when checkpoint is ahead of remote head", async () => {
    const local = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: local.dbPath });

    const createPlan = await writePlanFile(local.dir, "create.json", {
      message: "create tasks",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "title", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    const insert1 = await writePlanFile(local.dir, "insert-1.json", {
      message: "insert 1",
      operations: [{ type: "insert", table: "tasks", values: { id: 1, title: "one" } }],
    });
    const insert2 = await writePlanFile(local.dir, "insert-2.json", {
      message: "insert 2",
      operations: [{ type: "insert", table: "tasks", values: { id: 2, title: "two" } }],
    });

    let rollbackHead = "";
    let canonicalHead = "";
    await withDbPath(local.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await applyPlan(currentDb(), createPlan);
      const c2 = await applyPlan(currentDb(), insert1);
      const c3 = await applyPlan(currentDb(), insert2);
      rollbackHead = c2.commitId;
      canonicalHead = c3.commitId;
      await push(currentDb());
    });

    const remoteDb = new Database(remote.dbPath);
    try {
      remoteDb.query("UPDATE _toss_ref SET commit_id=?, updated_at=? WHERE name='main'").run(rollbackHead, Date.now());
    } finally {
      remoteDb.close(false);
    }

    await withDbPath(local.dbPath, async () => {
      const pushed = await push(currentDb());
      expect(pushed.pushed).toBe(1);
      expect(pushed.remoteHead).toBe(canonicalHead);

      const remoteInfo = await remoteStatus(currentDb());
      expect(remoteInfo.projectionHead).toBe(canonicalHead);
      expect(remoteInfo.projectionLagCommits).toBe(0);
      expect(remoteInfo.projectionError).toBeNull();
    });

    const remoteDbAfter = new Database(remote.dbPath);
    try {
      const rows = remoteDbAfter
        .query<{ id: number; title: string }, []>("SELECT id, title FROM tasks ORDER BY id")
        .all();
      expect(rows).toEqual([
        { id: 1, title: "one" },
        { id: 2, title: "two" },
      ]);
    } finally {
      remoteDbAfter.close(false);
    }
  });

  testWithTmp("materialization resumes from checkpoint before new push", async () => {
    const local = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: local.dbPath });

    const createPlan = await writePlanFile(local.dir, "create.json", {
      message: "create tasks",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "title", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    const insert1 = await writePlanFile(local.dir, "insert-1.json", {
      message: "insert 1",
      operations: [{ type: "insert", table: "tasks", values: { id: 1, title: "one" } }],
    });
    const insert2 = await writePlanFile(local.dir, "insert-2.json", {
      message: "insert 2",
      operations: [{ type: "insert", table: "tasks", values: { id: 2, title: "two" } }],
    });
    const insert3 = await writePlanFile(local.dir, "insert-3.json", {
      message: "insert 3",
      operations: [{ type: "insert", table: "tasks", values: { id: 3, title: "three" } }],
    });

    let checkpointCommitId = "";
    let finalHead = "";
    await withDbPath(local.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await applyPlan(currentDb(), createPlan);
      const c2 = await applyPlan(currentDb(), insert1);
      checkpointCommitId = c2.commitId;
      await applyPlan(currentDb(), insert2);
      await push(currentDb());
    });

    const remoteDb = new Database(remote.dbPath);
    try {
      remoteDb.query("DELETE FROM tasks WHERE id = 2").run();
      remoteDb
        .query(
          "INSERT INTO _toss_meta(key, value) VALUES ('last_materialized_commit', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        )
        .run(checkpointCommitId);
    } finally {
      remoteDb.close(false);
    }

    await withDbPath(local.dbPath, async () => {
      const c4 = await applyPlan(currentDb(), insert3);
      finalHead = c4.commitId;
      await push(currentDb());
      const remoteInfo = await remoteStatus(currentDb());
      expect(remoteInfo.projectionHead).toBe(finalHead);
      expect(remoteInfo.projectionLagCommits).toBe(0);
      expect(remoteInfo.projectionError).toBeNull();
    });

    const remoteDbAfter = new Database(remote.dbPath);
    try {
      const rows = remoteDbAfter
        .query<{ id: number; title: string }, []>("SELECT id, title FROM tasks ORDER BY id")
        .all();
      expect(rows).toEqual([
        { id: 1, title: "one" },
        { id: 2, title: "two" },
        { id: 3, title: "three" },
      ]);
    } finally {
      remoteDbAfter.close(false);
    }
  });

  testWithTmp("missing checkpoint commit triggers projection rebuild", async () => {
    const local = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: local.dbPath });

    const createPlan = await writePlanFile(local.dir, "create.json", {
      message: "create tasks",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "title", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    const insertPlan = await writePlanFile(local.dir, "insert.json", {
      message: "insert task",
      operations: [{ type: "insert", table: "tasks", values: { id: 1, title: "rebuilt" } }],
    });

    await withDbPath(local.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await applyPlan(currentDb(), createPlan);
      await applyPlan(currentDb(), insertPlan);
      await push(currentDb());
    });

    const remoteDb = new Database(remote.dbPath);
    try {
      remoteDb.query("DROP TABLE tasks").run();
      remoteDb
        .query(
          "INSERT INTO _toss_meta(key, value) VALUES ('last_materialized_commit', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        )
        .run("f".repeat(64));
      remoteDb
        .query(
          "INSERT INTO _toss_meta(key, value) VALUES ('last_materialized_error', 'stale') ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        )
        .run();
    } finally {
      remoteDb.close(false);
    }

    await withDbPath(local.dbPath, async () => {
      const pushed = await push(currentDb());
      expect(pushed.pushed).toBe(0);
      const remoteInfo = await remoteStatus(currentDb());
      expect(remoteInfo.projectionLagCommits).toBe(0);
      expect(remoteInfo.projectionError).toBeNull();
    });

    const remoteDbAfter = new Database(remote.dbPath);
    try {
      const rows = remoteDbAfter.query<{ id: number; title: string }, []>("SELECT id, title FROM tasks ORDER BY id").all();
      expect(rows).toEqual([{ id: 1, title: "rebuilt" }]);
    } finally {
      remoteDbAfter.close(false);
    }
  });

  testWithTmp("connect keeps pending state until first sync", async () => {
    const local = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: local.dbPath });

    await withDbPath(local.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      const currentStatus = status(currentDb());
      expect(currentStatus.sync.state).toBe("pending");
      expect(currentStatus.sync.pendingCommits).toBe(0);
    });
  });

  testWithTmp("connect rejects unsupported platform from untyped input", async () => {
    const local = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: local.dbPath });

    await withDbPath(local.dbPath, async () => {
      try {
        await connect(currentDb(), {
          platform: "unsupported" as unknown as "libsql",
          url: remoteUrl,
        });
        throw new Error("connect should fail for unsupported platform");
      } catch (error) {
        expect(CodedError.is(error)).toBe(true);
        if (CodedError.is(error)) {
          expect(error.code).toBe("CONFIG");
        }
      }
      expect(getSyncConfig()).toBeNull();
    });
  });

  testWithTmp("fresh replica pull becomes synced when local and remote heads match", async () => {
    const source = createTestContext();
    const replica = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: source.dbPath });
    await initDb({ dbPath: replica.dbPath });

    const createPlan = await writePlanFile(source.dir, "create.json", {
      message: "create tasks",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "title", type: "TEXT", notNull: true },
          ],
        },
      ],
    });

    await withDbPath(source.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await applyPlan(currentDb(), createPlan);
      await push(currentDb());
    });

    await withDbPath(replica.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      const pulled = await pull(currentDb());
      expect(pulled.state).toBe("synced");
      const currentStatus = status(currentDb());
      expect(currentStatus.sync.state).toBe("synced");
      expect(currentStatus.sync.pendingCommits).toBe(0);
    });
  });

  testWithTmp("pull and remote status work against read-only remote", async () => {
    const source = createTestContext();
    const replica = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: source.dbPath });
    await initDb({ dbPath: replica.dbPath });

    const createPlan = await writePlanFile(source.dir, "create.json", {
      message: "create docs",
      operations: [
        {
          type: "create_table",
          table: "docs",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });

    await withDbPath(source.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await applyPlan(currentDb(), createPlan);
      await push(currentDb());
    });

    await chmod(remote.dbPath, 0o444);
    try {
      await withDbPath(replica.dbPath, async () => {
        await connect(currentDb(), { platform: "libsql", url: remoteUrl });
        const pulledSync = await pull(currentDb());
        expect(pulledSync.state).toBe("synced");
        const remoteInfo = await remoteStatus(currentDb());
        expect(remoteInfo.remoteHead?.commitId).toBe(status(currentDb()).headCommit?.commitId ?? null);
      });
    } finally {
      await chmod(remote.dbPath, 0o644);
    }
  });

  testWithTmp("clone refuses to overwrite existing db unless force-new is set", async () => {
    const existing = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: existing.dbPath });

    const createPlan = await writePlanFile(existing.dir, "create-local.json", {
      message: "create local_only",
      operations: [
        {
          type: "create_table",
          table: "local_only",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });
    const insertPlan = await writePlanFile(existing.dir, "insert-local.json", {
      message: "insert local row",
      operations: [{ type: "insert", table: "local_only", values: { id: 1 } }],
    });

    await withDbPath(existing.dbPath, async () => {
      await applyPlan(currentDb(), createPlan);
      await applyPlan(currentDb(), insertPlan);
    });

    await withDbPath(existing.dbPath, async () => {
      try {
        await clone({
          platform: "libsql",
          url: remoteUrl,
          dbPath: existing.dbPath,
        });
        throw new Error("clone should fail when destination db already exists");
      } catch (error) {
        expect(CodedError.is(error)).toBe(true);
        if (CodedError.is(error)) {
          expect(error.code).toBe("CONFIG");
        }
      }
    });

    await withDbPath(existing.dbPath, async () => {
      const rows = query(currentDb(), "SELECT id FROM local_only ORDER BY id");
      expect(rows).toEqual([{ id: 1 }]);
    });
  });

  testWithTmp("switching remote resets sync markers and keeps pending on empty new remote", async () => {
    const local = createTestContext();
    const remoteA = createTestContext();
    const remoteB = createTestContext();
    const remoteAUrl = remoteUrlFor(remoteA.dbPath);
    const remoteBUrl = remoteUrlFor(remoteB.dbPath);
    await initDb({ dbPath: local.dbPath });

    const createPlan = await writePlanFile(local.dir, "create.json", {
      message: "create tasks",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });

    await withDbPath(local.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteAUrl });
      await applyPlan(currentDb(), createPlan);
      const pushed = await push(currentDb());
      expect(pushed.state).toBe("synced");

      await connect(currentDb(), { platform: "libsql", url: remoteBUrl });
      const pulledSync = await pull(currentDb());
      expect(pulledSync.pulled).toBe(0);
      expect(pulledSync.state).toBe("pending");
      const currentStatus = status(currentDb());
      expect(currentStatus.sync.state).toBe("pending");
      expect(currentStatus.sync.pendingCommits).toBeGreaterThan(0);
    });
  });

  testWithTmp("tampered remote commit payload is classified as SYNC_DIVERGED", async () => {
    const source = createTestContext();
    const replica = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: source.dbPath });
    await initDb({ dbPath: replica.dbPath });

    const createPlan = await writePlanFile(source.dir, "create.json", {
      message: "create tasks",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });

    await withDbPath(source.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await applyPlan(currentDb(), createPlan);
      await push(currentDb());
    });

    const remoteDb = new Database(remote.dbPath);
    try {
      remoteDb.query("UPDATE _toss_commit SET message = 'tampered message' WHERE seq = 1").run();
    } finally {
      remoteDb.close(false);
    }

    await withDbPath(replica.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      try {
        await pull(currentDb());
        throw new Error("pull should fail on tampered remote payload");
      } catch (error) {
        expect(CodedError.is(error)).toBe(true);
        if (CodedError.is(error)) {
          expect(error.code).toBe("SYNC_DIVERGED");
        }
      }
      const currentStatus = status(currentDb());
      expect(currentStatus.headCommit).toBeNull();
      expect(currentStatus.sync.state).toBe("conflict");
    });
  });

  testWithTmp("non-fast-forward push fails with SYNC_NON_FAST_FORWARD", async () => {
    const a = createTestContext();
    const b = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: a.dbPath });
    await initDb({ dbPath: b.dbPath });

    const createPlan = await writePlanFile(a.dir, "create.json", {
      message: "create items",
      operations: [
        {
          type: "create_table",
          table: "items",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "name", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    const localPlan = await writePlanFile(a.dir, "local.json", {
      message: "local only",
      operations: [{ type: "insert", table: "items", values: { id: 1, name: "local" } }],
    });
    const remotePlan = await writePlanFile(b.dir, "remote.json", {
      message: "remote only",
      operations: [{ type: "insert", table: "items", values: { id: 2, name: "remote" } }],
    });

    await withDbPath(a.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await applyPlan(currentDb(), createPlan);
      await push(currentDb());
      await applyPlan(currentDb(), localPlan);
    });

    await withDbPath(b.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await pull(currentDb());
      await applyPlan(currentDb(), remotePlan);
      await push(currentDb());
    });

    await withDbPath(a.dbPath, async () => {
      try {
        await push(currentDb());
        throw new Error("push should fail with non-fast-forward");
      } catch (error) {
        expect(CodedError.is(error)).toBe(true);
        if (CodedError.is(error)) {
          expect(error.code).toBe("SYNC_NON_FAST_FORWARD");
        }
      }
    });
  });

  testWithTmp("diverged pull fails with SYNC_DIVERGED", async () => {
    const a = createTestContext();
    const b = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: a.dbPath });
    await initDb({ dbPath: b.dbPath });

    const createPlan = await writePlanFile(a.dir, "create.json", {
      message: "create tx",
      operations: [
        {
          type: "create_table",
          table: "tx",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "amount", type: "INTEGER", notNull: true },
          ],
        },
      ],
    });
    const localPlan = await writePlanFile(a.dir, "local.json", {
      message: "local tx",
      operations: [{ type: "insert", table: "tx", values: { id: 1, amount: 100 } }],
    });
    const remotePlan = await writePlanFile(b.dir, "remote.json", {
      message: "remote tx",
      operations: [{ type: "insert", table: "tx", values: { id: 2, amount: 200 } }],
    });

    await withDbPath(a.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await applyPlan(currentDb(), createPlan);
      await push(currentDb());
      await applyPlan(currentDb(), localPlan);
    });

    await withDbPath(b.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await pull(currentDb());
      await applyPlan(currentDb(), remotePlan);
      await push(currentDb());
    });

    await withDbPath(a.dbPath, async () => {
      try {
        await pull(currentDb());
        throw new Error("pull should fail on diverged history");
      } catch (error) {
        expect(CodedError.is(error)).toBe(true);
        if (CodedError.is(error)) {
          expect(error.code).toBe("SYNC_DIVERGED");
        }
      }
    });
  });

  testWithTmp("clone fetches remote history and verify full passes", async () => {
    const source = createTestContext();
    const cloneCtx = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: source.dbPath });

    const createPlan = await writePlanFile(source.dir, "create.json", {
      message: "create books",
      operations: [
        {
          type: "create_table",
          table: "books",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "title", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    const insertPlan = await writePlanFile(source.dir, "insert.json", {
      message: "insert one",
      operations: [{ type: "insert", table: "books", values: { id: 1, title: "Deep Work" } }],
    });

    await withDbPath(source.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await applyPlan(currentDb(), createPlan);
      await applyPlan(currentDb(), insertPlan);
      await push(currentDb());
    });

    const cloned = await withDbPath(cloneCtx.dbPath, async () => {
      return await clone({
        platform: "libsql",
        url: remoteUrl,
        dbPath: cloneCtx.dbPath,
        forceNew: true,
      });
    });
    expect(cloned.dbPath).toBe(cloneCtx.dbPath);
    expect(cloned.sync.pulled).toBeGreaterThan(0);

    await withDbPath(cloneCtx.dbPath, async () => {
      const verification = verify(currentDb(), { full: true });
      expect(verification.ok).toBe(true);
      const rows = query(currentDb(), "SELECT id, title FROM books");
      expect(rows).toEqual([{ id: 1, title: "Deep Work" }]);
      expect(getSyncConfig()?.platform).toBe("libsql");
    });
  });

  testWithTmp("clone allows explicit platform override", async () => {
    const source = createTestContext();
    const cloneCtx = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: source.dbPath });

    const createPlan = await writePlanFile(source.dir, "create.json", {
      message: "create notes",
      operations: [
        {
          type: "create_table",
          table: "notes",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });

    await withDbPath(source.dbPath, async () => {
      await connect(currentDb(), { platform: "libsql", url: remoteUrl });
      await applyPlan(currentDb(), createPlan);
      await push(currentDb());
    });

    await withDbPath(cloneCtx.dbPath, async () => {
      await clone({
        platform: "turso",
        url: remoteUrl,
        dbPath: cloneCtx.dbPath,
        forceNew: true,
      });
      expect(getSyncConfig()?.platform).toBe("turso");
    });
  });

  testWithTmp("connect with null authToken clears stored platform token", async () => {
    const local = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDb({ dbPath: local.dbPath });

    await withDbPath(local.dbPath, async () => {
      writeAuthToken("libsql", "stale-token");
      expect(readAuthToken("libsql")).toBe("stale-token");

      await connect(currentDb(), {
        platform: "libsql",
        url: remoteUrl,
        authToken: null,
      });

      expect(readAuthToken("libsql")).toBeUndefined();
      const status = await remoteStatus(currentDb());
      expect(status.hasAuthToken).toBe(false);
    });
  });

  testWithTmp("auto sync failure keeps local commit and returns pending state", async () => {
    const ctx = createTestContext();
    await initDb({ dbPath: ctx.dbPath });
    const createPlan = await writePlanFile(ctx.dir, "create.json", {
      message: "create reminders",
      operations: [
        {
          type: "create_table",
          table: "reminders",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });

    await withDbPath(ctx.dbPath, async () => {
      await applyPlan(currentDb(), createPlan);
      writeRemoteConfig({
        platform: "turso",
        url: "libsql://not-existing-host.invalid",
      });
      const sync = await autoSync(currentDb());
      expect(sync).not.toBeNull();
      expect(sync?.state).toBe("pending");
      const currentStatus = status(currentDb());
      expect(currentStatus.headCommit).not.toBeNull();
      expect(currentStatus.sync.state).toBe("pending");
      expect(getMetaValueOrThrow(currentDb(), LAST_SYNC_STATE_META_KEY)).toBe("pending");
    });
  });
});

function getMetaValueOrThrow(db: Database, key: string): string {
  const row = db.query<{ value: string }, [string]>("SELECT value FROM _toss_meta WHERE key = ? LIMIT 1").get(key);
  if (!row) {
    throw new Error(`missing meta key: ${key}`);
  }
  return row.value;
}
