import { describe, expect, test } from "bun:test";
import {
  commitOperations,
  commitRowEffects,
  initDb,
  listCommits,
  openDb,
  readCommit,
  replayCommit,
  rowHash,
  runInDeferredTransaction,
} from "../src";
import { ROW_EFFECT_TABLE } from "../src/db";
import { applyPlan, createTestContext, withTmpDirCleanup, writePlanFile } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

describe("commit persistence", () => {
  testWithTmp("replay writes row-effect hashes from row images, not input metadata", async () => {
    const sourceCtx = createTestContext();
    const targetCtx = createTestContext();
    await initDb({ dbPath: sourceCtx.dbPath });
    await initDb({ dbPath: targetCtx.dbPath });
    const sourceDb = openDb(sourceCtx.dbPath);
    const targetDb = openDb(targetCtx.dbPath);
    try {
      const setupPlan = await writePlanFile(sourceCtx.dir, "commit-seed.json", {
        message: "seed tasks",
        operations: [
          {
            type: "create_table",
            table: "tasks",
            columns: [
              { name: "id", type: "INTEGER", primaryKey: true },
              { name: "title", type: "TEXT", notNull: true },
            ],
          },
          { type: "insert", table: "tasks", values: { id: 1, title: "before" } },
        ],
      });
      const updatePlan = await writePlanFile(sourceCtx.dir, "commit-update.json", {
        message: "update task",
        operations: [{ type: "update", table: "tasks", values: { title: "after" }, where: { id: 1 } }],
      });

      await applyPlan(sourceDb, setupPlan);
      await applyPlan(sourceDb, updatePlan);

      const commits = listCommits(sourceDb, false);
      expect(commits).toHaveLength(2);
      const first = readCommit(sourceDb, commits[0]!.commitId);
      const second = readCommit(sourceDb, commits[1]!.commitId);
      const tamperedSecond = {
        ...second,
        rowEffects: second.rowEffects.map((effect) => ({
          ...effect,
          beforeHash: effect.beforeHash ? "0".repeat(64) : null,
          afterHash: effect.afterHash ? "f".repeat(64) : null,
        })),
      };

      runInDeferredTransaction(targetDb, () => {
        replayCommit(targetDb, first, { errorCode: "RECOVER_FAILED" });
      });
      runInDeferredTransaction(targetDb, () => {
        replayCommit(targetDb, tamperedSecond, { errorCode: "RECOVER_FAILED" });
      });

      const persisted = commitRowEffects(targetDb, second.commit.commitId);
      expect(persisted).toHaveLength(1);
      const persistedRow = persisted[0]!;
      expect(persistedRow.beforeHash).toBe(rowHash(persistedRow.beforeRow));
      expect(persistedRow.afterHash).toBe(rowHash(persistedRow.afterRow));
      expect(persistedRow.beforeHash).not.toBe(tamperedSecond.rowEffects[0]?.beforeHash);
      expect(persistedRow.afterHash).not.toBe(tamperedSecond.rowEffects[0]?.afterHash);

      targetDb.$client
        .query(`UPDATE ${ROW_EFFECT_TABLE} SET before_hash = ?, after_hash = ? WHERE commit_id = ?`)
        .run("a".repeat(64), "b".repeat(64), second.commit.commitId);
      const redecoded = commitRowEffects(targetDb, second.commit.commitId)[0]!;
      expect(redecoded.beforeHash).toBe(rowHash(redecoded.beforeRow));
      expect(redecoded.afterHash).toBe(rowHash(redecoded.afterRow));
      expect(redecoded.beforeHash).not.toBe("a".repeat(64));
      expect(redecoded.afterHash).not.toBe("b".repeat(64));
    } finally {
      sourceDb.$client.close(false);
      targetDb.$client.close(false);
    }
  });

  testWithTmp("apply handles large commits without exceeding sqlite variable limits", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });
    const db = openDb(dbPath);
    try {
      const insertCount = 320;
      const setupPath = await writePlanFile(dir, "bulk-setup.json", {
        message: "create table for bulk insert",
        operations: [
          {
            type: "create_table",
            table: "bulk_items",
            columns: [
              { name: "id", type: "INTEGER", primaryKey: true },
              { name: "name", type: "TEXT", notNull: true },
            ],
          },
        ],
      });
      const bulkPath = await writePlanFile(dir, "bulk-commit.json", {
        message: "bulk insert for chunked commit writes",
        operations: Array.from({ length: insertCount }, (_, i) => ({
          type: "insert",
          table: "bulk_items",
          values: { id: i + 1, name: `item-${i + 1}` },
        })),
      });

      await applyPlan(db, setupPath);
      await applyPlan(db, bulkPath);

      const commits = listCommits(db, true);
      expect(commits).toHaveLength(2);
      const commitId = commits[0]!.commitId;
      expect(commitOperations(db, commitId)).toHaveLength(insertCount);
      expect(commitRowEffects(db, commitId)).toHaveLength(insertCount);
    } finally {
      db.$client.close(false);
    }
  });
});
