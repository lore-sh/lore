import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import {
  commitOperations,
  commitRowEffects,
  commitSchemaEffects,
  initDb,
  listCommits,
  openDb,
} from "../src";
import { computeCommitId, readCommit, replayCommit } from "../src/commit";
import { ROW_EFFECT_TABLE, runInSavepoint, runSchemaAwareTransaction } from "../src/db";
import { applyEffects, applyRowEffects, rowHash } from "../src/effect";
import { CodedError } from "../src/error";
import { stateHash } from "../src/inspect";
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

      runSchemaAwareTransaction(targetDb, () => {
        replayCommit(targetDb, first, { errorCode: "RECOVER_FAILED" });
      }, {
        hasSchemaChanges: first.schemaEffects.length > 0,
        context: `replay ${first.commit.commitId}`,
      });
      runSchemaAwareTransaction(targetDb, () => {
        replayCommit(targetDb, tamperedSecond, { errorCode: "RECOVER_FAILED" });
      }, {
        hasSchemaChanges: tamperedSecond.schemaEffects.length > 0,
        context: `replay ${tamperedSecond.commit.commitId}`,
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
      expect(() => commitRowEffects(targetDb, second.commit.commitId)).toThrow(
        "row effect before_hash does not match before_json",
      );
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

  testWithTmp("replay maps deferred FK failures to replay-scoped error code", async () => {
    const sourceCtx = createTestContext();
    const targetCtx = createTestContext();
    await initDb({ dbPath: sourceCtx.dbPath });
    await initDb({ dbPath: targetCtx.dbPath });
    const sourceDb = openDb(sourceCtx.dbPath);
    const targetDb = openDb(targetCtx.dbPath);
    try {
      const setup = (dbPath: string) => {
        const direct = new Database(dbPath);
        direct.run("PRAGMA foreign_keys=ON");
        direct.run("CREATE TABLE fk_parents (id INTEGER PRIMARY KEY)");
        direct.run(
          "CREATE TABLE fk_children (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES fk_parents(id))",
        );
        direct.run("INSERT INTO fk_parents(id) VALUES (1)");
        direct.close(false);
      };
      setup(sourceCtx.dbPath);
      setup(targetCtx.dbPath);

      const childPlan = await writePlanFile(sourceCtx.dir, "fk-replay-child.json", {
        message: "insert fk child",
        operations: [{ type: "insert", table: "fk_children", values: { id: 1, parent_id: 1 } }],
      });

      const inserted = await applyPlan(sourceDb, childPlan);
      const childReplay = readCommit(sourceDb, inserted.commitId);

      const tamperedRowEffects = childReplay.rowEffects.map((effect) => {
        if (effect.tableName !== "fk_children" || !effect.afterRow?.parent_id) {
          return effect;
        }
        return {
          ...effect,
          afterRow: {
            ...effect.afterRow,
            parent_id: {
              ...effect.afterRow.parent_id,
              sqlLiteral: "999",
            },
          },
        };
      });

      let tamperedStateHash = "";
      runInSavepoint(targetDb, "lore_replay_fk_probe", () => {
        targetDb.$client.run("PRAGMA defer_foreign_keys=ON");
        applyEffects(targetDb, tamperedRowEffects, childReplay.schemaEffects, "forward", {
          disableTableTriggers: true,
        });
        applyRowEffects(targetDb, tamperedRowEffects, "forward", {
          disableTableTriggers: true,
          includeUserEffects: false,
          includeSystemEffects: true,
          systemPolicy: "reconcile",
        });
        tamperedStateHash = stateHash(targetDb);
      }, { rollbackOnSuccess: true });

      const tamperedCommit = {
        ...childReplay.commit,
        stateHashAfter: tamperedStateHash,
      };
      const tamperedCommitId = computeCommitId({
        seq: tamperedCommit.seq,
        kind: tamperedCommit.kind,
        message: tamperedCommit.message,
        createdAt: tamperedCommit.createdAt,
        schemaHashBefore: tamperedCommit.schemaHashBefore,
        schemaHashAfter: tamperedCommit.schemaHashAfter,
        stateHashAfter: tamperedCommit.stateHashAfter,
        planHash: tamperedCommit.planHash,
        revertible: tamperedCommit.revertible,
        revertTargetId: tamperedCommit.revertTargetId,
        parentIds: childReplay.parentIds,
        operations: childReplay.operations,
        rowEffects: tamperedRowEffects,
        schemaEffects: childReplay.schemaEffects,
      });
      const tamperedReplay = {
        ...childReplay,
        commit: {
          ...tamperedCommit,
          commitId: tamperedCommitId,
        },
        rowEffects: tamperedRowEffects,
      };

      let thrown: unknown;
      try {
        runSchemaAwareTransaction(targetDb, () => {
          replayCommit(targetDb, tamperedReplay, { errorCode: "SYNC_DIVERGED" });
        }, {
          hasSchemaChanges: false,
          context: `replay ${tamperedReplay.commit.commitId}`,
        });
      } catch (error) {
        thrown = error;
      }
      expect(CodedError.hasCode(thrown, "SYNC_DIVERGED")).toBe(true);
      if (CodedError.hasCode(thrown, "SYNC_DIVERGED")) {
        expect(thrown.message.includes("foreign_key_check failed")).toBe(true);
      }
    } finally {
      sourceDb.$client.close(false);
      targetDb.$client.close(false);
    }
  });

  testWithTmp("replay ignores unchanged view aliases when unrelated tables are dropped", async () => {
    const sourceCtx = createTestContext();
    const targetCtx = createTestContext();
    await initDb({ dbPath: sourceCtx.dbPath });
    await initDb({ dbPath: targetCtx.dbPath });
    const sourceDb = openDb(sourceCtx.dbPath);
    const targetDb = openDb(targetCtx.dbPath);
    try {
      const prepare = (dbPath: string) => {
        const direct = new Database(dbPath);
        direct.run("CREATE TABLE users (id INTEGER PRIMARY KEY)");
        direct.run("CREATE VIEW replay_alias_view AS SELECT 1 AS users");
        direct.close(false);
      };
      prepare(sourceCtx.dbPath);
      prepare(targetCtx.dbPath);

      const dropTablePlan = await writePlanFile(sourceCtx.dir, "drop-users-with-view-alias-replay.json", {
        message: "drop users table",
        operations: [{ type: "drop_table", table: "users" }],
      });
      const dropped = await applyPlan(sourceDb, dropTablePlan);
      const sourceReplay = readCommit(sourceDb, dropped.commitId);
      expect(sourceReplay.schemaEffects.some((effect) => effect.tableName === "replay_alias_view")).toBe(false);

      runSchemaAwareTransaction(targetDb, () => {
        replayCommit(targetDb, sourceReplay, { errorCode: "RECOVER_FAILED" });
      }, {
        hasSchemaChanges: sourceReplay.schemaEffects.length > 0,
        context: `replay ${sourceReplay.commit.commitId}`,
      });

      const users = targetDb.$client
        .query<{ name: string }, []>(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='users' LIMIT 1",
        )
        .all();
      expect(users).toEqual([]);

      const views = targetDb.$client
        .query<{ name: string }, []>(
          "SELECT name FROM sqlite_master WHERE type='view' AND name='replay_alias_view' LIMIT 1",
        )
        .all();
      expect(views).toEqual([{ name: "replay_alias_view" }]);
    } finally {
      sourceDb.$client.close(false);
      targetDb.$client.close(false);
    }
  });

  testWithTmp("drop_view is persisted as schema effect and replay removes the view", async () => {
    const sourceCtx = createTestContext();
    const targetCtx = createTestContext();
    await initDb({ dbPath: sourceCtx.dbPath });
    await initDb({ dbPath: targetCtx.dbPath });
    const sourceDb = openDb(sourceCtx.dbPath);
    const targetDb = openDb(targetCtx.dbPath);
    try {
      const prepare = (dbPath: string) => {
        const direct = new Database(dbPath);
        direct.run("CREATE TABLE replay_view_tasks (id INTEGER PRIMARY KEY, title TEXT NOT NULL)");
        direct.run("CREATE VIEW replay_view_open AS SELECT id, title FROM replay_view_tasks WHERE title <> ''");
        direct.close(false);
      };
      prepare(sourceCtx.dbPath);
      prepare(targetCtx.dbPath);

      const dropViewPlan = await writePlanFile(sourceCtx.dir, "drop-view-for-replay.json", {
        message: "drop replay view",
        operations: [{ type: "drop_view", name: "replay_view_open" }],
      });
      const dropped = await applyPlan(sourceDb, dropViewPlan);

      const sourceHead = listCommits(sourceDb, true)[0]!;
      expect(sourceHead.schemaHashBefore).not.toBe(sourceHead.schemaHashAfter);

      const sourceReplay = readCommit(sourceDb, dropped.commitId);
      expect(sourceReplay.schemaEffects.some((effect) => effect.tableName === "replay_view_open")).toBe(true);
      expect(sourceReplay.schemaEffects[0]?.beforeTable?.kind).toBe("view");
      expect(sourceReplay.schemaEffects[0]?.afterTable).toBeNull();

      runSchemaAwareTransaction(targetDb, () => {
        replayCommit(targetDb, sourceReplay, { errorCode: "RECOVER_FAILED" });
      }, {
        hasSchemaChanges: sourceReplay.schemaEffects.length > 0,
        context: `replay ${sourceReplay.commit.commitId}`,
      });

      const remainingViews = targetDb.$client
        .query<{ name: string }, []>(
          "SELECT name FROM sqlite_master WHERE type='view' AND name='replay_view_open' LIMIT 1",
        )
        .all();
      expect(remainingViews).toEqual([]);
      expect(commitSchemaEffects(targetDb, dropped.commitId).some((effect) => effect.tableName === "replay_view_open")).toBe(true);
    } finally {
      sourceDb.$client.close(false);
      targetDb.$client.close(false);
    }
  });
});
