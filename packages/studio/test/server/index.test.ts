import { expect, test } from "bun:test";
import { initDb, openDb } from "@lore/core";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { attachServerCleanup } from "../../src/server/index";

test("attachServerCleanup closes db and detaches signal listeners when server stops", async () => {
  const dir = mkdtempSync(join(tmpdir(), "studio-index-test-"));
  const dbPath = join(dir, "lore.db");
  await initDb({ dbPath });
  const db = openDb(dbPath);
  let stopCalls = 0;
  const server = {
    stop: () => {
      stopCalls += 1;
    },
  } as unknown as Bun.Server<unknown>;

  const beforeSigint = process.listenerCount("SIGINT");
  const beforeSigterm = process.listenerCount("SIGTERM");
  attachServerCleanup(server, db);

  expect(process.listenerCount("SIGINT")).toBe(beforeSigint + 1);
  expect(process.listenerCount("SIGTERM")).toBe(beforeSigterm + 1);

  server.stop();
  expect(stopCalls).toBe(1);
  expect(process.listenerCount("SIGINT")).toBe(beforeSigint);
  expect(process.listenerCount("SIGTERM")).toBe(beforeSigterm);
  expect(() => db.$client.query("SELECT 1").get()).toThrow();

  server.stop();
  expect(stopCalls).toBe(2);
  expect(process.listenerCount("SIGINT")).toBe(beforeSigint);
  expect(process.listenerCount("SIGTERM")).toBe(beforeSigterm);
  rmSync(dir, { recursive: true, force: true });
});

test("attachServerCleanup stops server and closes db on SIGINT", async () => {
  const dir = mkdtempSync(join(tmpdir(), "studio-index-test-"));
  const dbPath = join(dir, "lore.db");
  await initDb({ dbPath });
  const db = openDb(dbPath);
  let stopCalls = 0;
  const server = {
    stop: () => {
      stopCalls += 1;
    },
  } as unknown as Bun.Server<unknown>;

  const originalKill = process.kill;
  let killedWith: NodeJS.Signals | null = null;
  (process as unknown as { kill: typeof process.kill }).kill = ((_: number, signal?: number | NodeJS.Signals) => {
    if (typeof signal === "string") {
      killedWith = signal;
    }
    return true;
  }) as typeof process.kill;
  try {
    attachServerCleanup(server, db);
    process.emit("SIGINT");
    expect(stopCalls).toBe(1);
    expect(killedWith === "SIGINT").toBe(true);
    expect(() => db.$client.query("SELECT 1").get()).toThrow();
  } finally {
    (process as unknown as { kill: typeof process.kill }).kill = originalKill;
    rmSync(dir, { recursive: true, force: true });
  }
});
