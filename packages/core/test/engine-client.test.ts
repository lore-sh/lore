import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { closeClient, getClient, initClient, openIsolatedClient, withTransaction } from "../src/engine/client";
import { createTestContext, withTmpDirCleanup } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

describe("engine client", () => {
  testWithTmp("requires explicit init before get", () => {
    expect(() => getClient()).toThrow("Database client is not initialized");
  });

  testWithTmp("init is idempotent for the same db path", () => {
    const { dbPath } = createTestContext();
    try {
      const a = initClient(dbPath);
      const b = initClient(dbPath);
      expect(a).toBe(b);
    } finally {
      closeClient();
    }
  });

  testWithTmp("init rejects switching db path", () => {
    const first = createTestContext().dbPath;
    const second = createTestContext().dbPath;
    try {
      const a = initClient(first);
      expect(() => initClient(second)).toThrow("Refusing to switch");
      const b = getClient();
      expect(a).toBe(b);
    } finally {
      closeClient();
    }
  });

  testWithTmp("init can switch db path when recreate is requested", () => {
    const first = createTestContext().dbPath;
    const second = createTestContext().dbPath;
    try {
      initClient(first);
      const switched = initClient(second, { recreate: true });
      expect(switched.path).toBe(second);
      const current = getClient();
      expect(current.path).toBe(second);
    } finally {
      closeClient();
    }
  });

  testWithTmp("isolated client does not share handle with cached client", () => {
    const { dbPath } = createTestContext();
    const isolated = openIsolatedClient(dbPath);
    try {
      initClient(dbPath);
      const shared = getClient();
      expect(isolated).not.toBe(shared);

      withTransaction((tx) => {
        tx.run("CREATE TABLE IF NOT EXISTS _toss_engine_client_test(id INTEGER PRIMARY KEY, v TEXT NOT NULL)");
        tx.run("INSERT INTO _toss_engine_client_test(v) VALUES('ok')");
      });

      const read = new Database(dbPath, { readonly: true });
      try {
        const row = read.query<{ c: number }, []>("SELECT COUNT(*) AS c FROM _toss_engine_client_test").get();
        expect(row).toEqual({ c: 1 });
      } finally {
        read.close(false);
      }
    } finally {
      isolated.close();
      closeClient();
    }
  });
});
