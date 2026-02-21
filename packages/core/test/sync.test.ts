import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { chmod } from "node:fs/promises";
import {
  applyPlan,
  autoSyncAfterApply,
  cloneFromRemote,
  connectRemote,
  getRemoteStatus,
  getStatus,
  initDatabase,
  isTossError,
  pullFromRemote,
  pushToRemote,
  readQuery,
  verifyDatabase,
} from "../src";
import { LAST_SYNC_STATE_META_KEY, REMOTE_AUTO_SYNC_META_KEY, REMOTE_URL_META_KEY, setMetaValue, withInitializedDatabase } from "../src/db";
import { closeClient, getClientPath } from "../src/engine/client";
import { createTestContext, withTmpDirCleanup, writePlanFile } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

const ENV_KEYS = ["TOSS_DB_PATH", "TOSS_TURSO_URL", "TOSS_TURSO_AUTH_TOKEN", "TURSO_AUTH_TOKEN"] as const;
type EnvSnapshot = Record<string, string | undefined>;

function captureEnv(): EnvSnapshot {
  const snapshot: EnvSnapshot = {};
  for (const key of ENV_KEYS) {
    snapshot[key] = process.env[key];
  }
  return snapshot;
}

function restoreEnv(snapshot: EnvSnapshot): void {
  for (const key of ENV_KEYS) {
    if (snapshot[key] === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = snapshot[key];
    }
  }
}

async function withDbPath<T>(dbPath: string, run: () => Promise<T>): Promise<T> {
  const snapshot = captureEnv();
  closeClient();
  process.env.TOSS_DB_PATH = dbPath;
  delete process.env.TOSS_TURSO_URL;
  delete process.env.TOSS_TURSO_AUTH_TOKEN;
  delete process.env.TURSO_AUTH_TOKEN;
  try {
    return await run();
  } finally {
    closeClient();
    restoreEnv(snapshot);
  }
}

function remoteUrlFor(path: string): string {
  return `file:${path}`;
}

describe("sync with Turso protocol", () => {
  testWithTmp("A apply -> push and B pull reaches same state", async () => {
    const a = createTestContext();
    const b = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDatabase({ dbPath: a.dbPath });
    await initDatabase({ dbPath: b.dbPath });

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
      await connectRemote({ url: remoteUrl, autoSync: false });
      await applyPlan(createPlan);
      const commit = await applyPlan(insertPlan);
      const pushed = await pushToRemote();
      expect(pushed.pushed).toBe(2);
      return commit.commitId;
    });

    await withDbPath(b.dbPath, async () => {
      await connectRemote({ url: remoteUrl, autoSync: false });
      const pulled = await pullFromRemote();
      expect(pulled.pulled).toBe(2);
      const rows = readQuery("SELECT id, title FROM tasks ORDER BY id");
      expect(rows).toEqual([{ id: 1, title: "buy milk" }]);
      expect(getStatus().headCommit?.commitId).toBe(expectedHead);
    });
  });

  testWithTmp("repeated push is idempotent", async () => {
    const a = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDatabase({ dbPath: a.dbPath });

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

    await withDbPath(a.dbPath, async () => {
      await connectRemote({ url: remoteUrl, autoSync: false });
      await applyPlan(createPlan);
      const first = await pushToRemote();
      const second = await pushToRemote();
      expect(first.pushed).toBe(1);
      expect(second.pushed).toBe(0);
      expect(second.state).toBe("synced");
    });
  });

  testWithTmp("connect keeps pending state until first sync", async () => {
    const local = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDatabase({ dbPath: local.dbPath });

    await withDbPath(local.dbPath, async () => {
      await connectRemote({ url: remoteUrl, autoSync: false });
      const status = getStatus();
      expect(status.sync.state).toBe("pending");
      expect(status.sync.pendingCommits).toBe(0);
    });
  });

  testWithTmp("fresh replica pull becomes synced when local and remote heads match", async () => {
    const source = createTestContext();
    const replica = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDatabase({ dbPath: source.dbPath });
    await initDatabase({ dbPath: replica.dbPath });

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
      await connectRemote({ url: remoteUrl, autoSync: false });
      await applyPlan(createPlan);
      await pushToRemote();
    });

    await withDbPath(replica.dbPath, async () => {
      await connectRemote({ url: remoteUrl, autoSync: false });
      const pulled = await pullFromRemote();
      expect(pulled.state).toBe("synced");
      const status = getStatus();
      expect(status.sync.state).toBe("synced");
      expect(status.sync.pendingCommits).toBe(0);
    });
  });

  testWithTmp("pull and remote status work against read-only remote", async () => {
    const source = createTestContext();
    const replica = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDatabase({ dbPath: source.dbPath });
    await initDatabase({ dbPath: replica.dbPath });

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
      await connectRemote({ url: remoteUrl, autoSync: false });
      await applyPlan(createPlan);
      await pushToRemote();
    });

    await chmod(remote.dbPath, 0o444);
    try {
      await withDbPath(replica.dbPath, async () => {
        await connectRemote({ url: remoteUrl, autoSync: false });
        const pull = await pullFromRemote();
        expect(pull.state).toBe("synced");
        const remoteStatus = await getRemoteStatus();
        expect(remoteStatus.remoteHead?.commitId).toBe(getStatus().headCommit?.commitId ?? null);
      });
    } finally {
      await chmod(remote.dbPath, 0o644);
    }
  });

  testWithTmp("clone refuses to overwrite existing db unless force-new is set", async () => {
    const existing = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDatabase({ dbPath: existing.dbPath });

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
      await applyPlan(createPlan);
      await applyPlan(insertPlan);
    });

    try {
      await cloneFromRemote({
        url: remoteUrl,
        dbPath: existing.dbPath,
        autoSync: false,
      });
      throw new Error("clone should fail when destination db already exists");
    } catch (error) {
      expect(isTossError(error)).toBe(true);
      if (isTossError(error)) {
        expect(error.code).toBe("CONFIG_ERROR");
      }
    }

    await withDbPath(existing.dbPath, async () => {
      const rows = readQuery("SELECT id FROM local_only ORDER BY id");
      expect(rows).toEqual([{ id: 1 }]);
    });
  });

  testWithTmp("switching remote resets sync markers and keeps pending on empty new remote", async () => {
    const local = createTestContext();
    const remoteA = createTestContext();
    const remoteB = createTestContext();
    const remoteAUrl = remoteUrlFor(remoteA.dbPath);
    const remoteBUrl = remoteUrlFor(remoteB.dbPath);
    await initDatabase({ dbPath: local.dbPath });

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
      await connectRemote({ url: remoteAUrl, autoSync: false });
      await applyPlan(createPlan);
      const pushed = await pushToRemote();
      expect(pushed.state).toBe("synced");

      await connectRemote({ url: remoteBUrl, autoSync: false });
      const pull = await pullFromRemote();
      expect(pull.pulled).toBe(0);
      expect(pull.state).toBe("pending");
      const status = getStatus();
      expect(status.sync.state).toBe("pending");
      expect(status.sync.pendingCommits).toBeGreaterThan(0);
    });
  });

  testWithTmp("tampered remote commit payload is classified as SYNC_DIVERGED", async () => {
    const source = createTestContext();
    const replica = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDatabase({ dbPath: source.dbPath });
    await initDatabase({ dbPath: replica.dbPath });

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
      await connectRemote({ url: remoteUrl, autoSync: false });
      await applyPlan(createPlan);
      await pushToRemote();
    });

    const remoteDb = new Database(remote.dbPath);
    try {
      remoteDb.query("UPDATE _toss_commit SET message = 'tampered message' WHERE seq = 1").run();
    } finally {
      remoteDb.close(false);
    }

    await withDbPath(replica.dbPath, async () => {
      await connectRemote({ url: remoteUrl, autoSync: false });
      try {
        await pullFromRemote();
        throw new Error("pull should fail on tampered remote payload");
      } catch (error) {
        expect(isTossError(error)).toBe(true);
        if (isTossError(error)) {
          expect(error.code).toBe("SYNC_DIVERGED");
        }
      }
      const status = getStatus();
      expect(status.headCommit).toBeNull();
      expect(status.sync.state).toBe("conflict");
    });
  });

  testWithTmp("non-fast-forward push fails with SYNC_NON_FAST_FORWARD", async () => {
    const a = createTestContext();
    const b = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDatabase({ dbPath: a.dbPath });
    await initDatabase({ dbPath: b.dbPath });

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
      await connectRemote({ url: remoteUrl, autoSync: false });
      await applyPlan(createPlan);
      await pushToRemote();
      await applyPlan(localPlan);
    });

    await withDbPath(b.dbPath, async () => {
      await connectRemote({ url: remoteUrl, autoSync: false });
      await pullFromRemote();
      await applyPlan(remotePlan);
      await pushToRemote();
    });

    await withDbPath(a.dbPath, async () => {
      try {
        await pushToRemote();
        throw new Error("push should fail with non-fast-forward");
      } catch (error) {
        expect(isTossError(error)).toBe(true);
        if (isTossError(error)) {
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
    await initDatabase({ dbPath: a.dbPath });
    await initDatabase({ dbPath: b.dbPath });

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
      await connectRemote({ url: remoteUrl, autoSync: false });
      await applyPlan(createPlan);
      await pushToRemote();
      await applyPlan(localPlan);
    });

    await withDbPath(b.dbPath, async () => {
      await connectRemote({ url: remoteUrl, autoSync: false });
      await pullFromRemote();
      await applyPlan(remotePlan);
      await pushToRemote();
    });

    await withDbPath(a.dbPath, async () => {
      try {
        await pullFromRemote();
        throw new Error("pull should fail on diverged history");
      } catch (error) {
        expect(isTossError(error)).toBe(true);
        if (isTossError(error)) {
          expect(error.code).toBe("SYNC_DIVERGED");
        }
      }
    });
  });

  testWithTmp("clone fetches remote history and verify full passes", async () => {
    const source = createTestContext();
    const clone = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDatabase({ dbPath: source.dbPath });

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
      await connectRemote({ url: remoteUrl, autoSync: false });
      await applyPlan(createPlan);
      await applyPlan(insertPlan);
      await pushToRemote();
    });

    const cloned = await cloneFromRemote({
      url: remoteUrl,
      dbPath: clone.dbPath,
      autoSync: false,
      forceNew: true,
    });
    expect(cloned.dbPath).toBe(clone.dbPath);
    expect(cloned.sync.pulled).toBeGreaterThan(0);

    await withDbPath(clone.dbPath, async () => {
      const verify = verifyDatabase({ full: true });
      expect(verify.ok).toBe(true);
      const rows = readQuery("SELECT id, title FROM books");
      expect(rows).toEqual([{ id: 1, title: "Deep Work" }]);
    });
  });

  testWithTmp("clone restores previous db client context in long-lived process", async () => {
    const source = createTestContext();
    const clone = createTestContext();
    const remote = createTestContext();
    const stable = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDatabase({ dbPath: source.dbPath });
    await initDatabase({ dbPath: stable.dbPath });

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
      await connectRemote({ url: remoteUrl, autoSync: false });
      await applyPlan(createPlan);
      await pushToRemote();
    });

    await withDbPath(stable.dbPath, async () => {
      const before = getStatus();
      expect(before.dbPath).toBe(stable.dbPath);
      expect(getClientPath()).toBe(stable.dbPath);
      await cloneFromRemote({
        url: remoteUrl,
        dbPath: clone.dbPath,
        autoSync: false,
        forceNew: true,
      });
      expect(getClientPath()).toBe(stable.dbPath);
      const status = getStatus();
      expect(status.dbPath).toBe(stable.dbPath);
    });
  });

  testWithTmp("clone restores previous db client context when initialization fails", async () => {
    const stable = createTestContext();
    const remote = createTestContext();
    const remoteUrl = remoteUrlFor(remote.dbPath);
    await initDatabase({ dbPath: stable.dbPath });

    const blockerFile = `${stable.dir}/blocker`;
    await Bun.write(blockerFile, "not-a-directory");
    const invalidClonePath = `${blockerFile}/clone.db`;

    await withDbPath(stable.dbPath, async () => {
      const before = getStatus();
      expect(before.dbPath).toBe(stable.dbPath);
      expect(getClientPath()).toBe(stable.dbPath);
      let failed = false;
      try {
        await cloneFromRemote({
          url: remoteUrl,
          dbPath: invalidClonePath,
          autoSync: false,
          forceNew: true,
        });
      } catch {
        failed = true;
      }
      expect(failed).toBe(true);
      expect(getClientPath()).toBe(stable.dbPath);
      const status = getStatus();
      expect(status.dbPath).toBe(stable.dbPath);
    });
  });

  testWithTmp("auto sync failure keeps local commit and returns pending state", async () => {
    const ctx = createTestContext();
    await initDatabase({ dbPath: ctx.dbPath });
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
      await applyPlan(createPlan);
      withInitializedDatabase(({ db }) => {
        setMetaValue(db, REMOTE_URL_META_KEY, "libsql://not-existing-host.invalid");
        setMetaValue(db, REMOTE_AUTO_SYNC_META_KEY, "1");
      });
      const sync = await autoSyncAfterApply();
      expect(sync).not.toBeNull();
      expect(sync?.state).toBe("pending");
      const status = getStatus();
      expect(status.headCommit).not.toBeNull();
      expect(status.sync.state).toBe("pending");
      withInitializedDatabase(({ db }) => {
        expect(getMetaValueOrThrow(db, LAST_SYNC_STATE_META_KEY)).toBe("pending");
      });
    });
  });
});

function getMetaValueOrThrow(db: Database, key: string): string {
  const row = db.query<{ value: string }, [string]>("SELECT value FROM _toss_engine_meta WHERE key = ? LIMIT 1").get(key);
  if (!row) {
    throw new Error(`missing meta key: ${key}`);
  }
  return row.value;
}
