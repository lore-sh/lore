import { readFile } from "node:fs/promises";
import { activeApplyLogs, appendLog, listLogsAscending, listLogsDescending } from "./log";
import { executeOperations } from "./executors/apply";
import { executeReadSql } from "./executors/read";
import { generateSkills, type GeneratedSkills } from "./skills";
import {
  assertInitialized,
  closeDatabase,
  getSchemaVersion,
  initializeStorage,
  listUserTables,
  openDatabase,
  runInTransaction,
} from "./db";
import { TossError } from "./errors";
import { rebuildHeadState, validateRevertSafety } from "./materializer";
import type { LogEntry, TossStatus } from "./types";
import { parseAndValidateOperationPlan } from "./validators/operation";
import { validateReadSql } from "./validators/sql";

export interface ServiceOptions {
  dbPath?: string;
}

export interface InitDatabaseOptions extends ServiceOptions {
  generateSkills?: boolean;
  workspacePath?: string;
}

export async function initDatabase(
  options: InitDatabaseOptions = {},
): Promise<{ dbPath: string; generatedSkills: GeneratedSkills | null }> {
  const { db, dbPath } = openDatabase(options.dbPath);
  try {
    initializeStorage(db);
    const generatedSkills = options.generateSkills ? await generateSkills(options.workspacePath) : null;
    return { dbPath, generatedSkills };
  } finally {
    closeDatabase(db);
  }
}

async function readPlanInput(planRef: string): Promise<string> {
  if (planRef === "-") {
    return await Bun.stdin.text();
  }
  return await readFile(planRef, "utf8");
}

export async function applyPlan(planRef: string, options: ServiceOptions = {}): Promise<LogEntry> {
  const payload = await readPlanInput(planRef);
  const plan = parseAndValidateOperationPlan(payload);

  const { db, dbPath } = openDatabase(options.dbPath);
  try {
    assertInitialized(db, dbPath);
    return runInTransaction(db, () => {
      executeOperations(db, plan.operations);
      const schemaVersion = getSchemaVersion(db);
      return appendLog(db, {
        kind: "apply",
        message: plan.message,
        operations: plan.operations,
        schemaVersion,
      });
    });
  } finally {
    closeDatabase(db);
  }
}

export function readQuery(sqlInput: string, options: ServiceOptions = {}): Record<string, unknown>[] {
  const sql = validateReadSql(sqlInput);

  const { db, dbPath } = openDatabase(options.dbPath);
  try {
    assertInitialized(db, dbPath);
    return executeReadSql(db, sql);
  } finally {
    closeDatabase(db);
  }
}

export function getStatus(options: ServiceOptions = {}): TossStatus {
  const { db, dbPath } = openDatabase(options.dbPath);
  try {
    assertInitialized(db, dbPath);

    const tables = listUserTables(db).map((table) => {
      const countRow = db.query(`SELECT COUNT(*) AS count FROM "${table.replaceAll('"', '""')}"`).get() as {
        count: number;
      };
      return { name: table, count: countRow.count };
    });

    const latestRow = db
      .query("SELECT id, timestamp, kind, message FROM _toss_log ORDER BY rowid DESC LIMIT 1")
      .get() as {
      id: string;
      timestamp: string;
      kind: "apply" | "revert" | "system";
      message: string;
    } | null;

    return {
      dbPath,
      schemaVersion: getSchemaVersion(db),
      tableCount: tables.length,
      tables,
      latestCommit: latestRow
        ? {
            id: latestRow.id,
            timestamp: latestRow.timestamp,
            kind: latestRow.kind,
            message: latestRow.message,
          }
        : null,
    };
  } finally {
    closeDatabase(db);
  }
}

export function getHistory(options: ServiceOptions = {}): LogEntry[] {
  const { db, dbPath } = openDatabase(options.dbPath);
  try {
    assertInitialized(db, dbPath);
    return listLogsDescending(db);
  } finally {
    closeDatabase(db);
  }
}

function resolveRevertTarget(logs: LogEntry[], commitId: string): LogEntry {
  const target = logs.find((entry) => entry.id === commitId);
  if (!target) {
    throw new TossError("NOT_FOUND", `Commit not found: ${commitId}`);
  }
  if (target.kind !== "apply") {
    throw new TossError("INVALID_REVERT_TARGET", `Only apply commits can be reverted: ${commitId}`);
  }

  const alreadyReverted = logs.some((entry) => entry.kind === "revert" && entry.revertedTargetId === commitId);
  if (alreadyReverted) {
    throw new TossError("ALREADY_REVERTED", `Commit is already reverted: ${commitId}`);
  }

  return target;
}

export function revertCommit(commitId: string, options: ServiceOptions = {}): {
  revertCommit: LogEntry;
  replayedApplyCount: number;
} {
  const { db, dbPath } = openDatabase(options.dbPath);
  try {
    assertInitialized(db, dbPath);

    const logs = listLogsAscending(db);
    const target = resolveRevertTarget(logs, commitId);

    const activeLogs = activeApplyLogs(logs);
    const laterActiveLogs = activeLogs.filter((entry) => entry.rowid > target.rowid && entry.id !== target.id);
    validateRevertSafety(target, laterActiveLogs);

    return runInTransaction(db, () => {
      const schemaVersion = getSchemaVersion(db);
      const revertEntry = appendLog(db, {
        kind: "revert",
        message: `Revert ${target.id}: ${target.message}`,
        operations: [],
        schemaVersion,
        revertedTargetId: target.id,
      });

      const replayedApplyCount = rebuildHeadState(db, logs, [target.id]);
      return { revertCommit: revertEntry, replayedApplyCount };
    });
  } finally {
    closeDatabase(db);
  }
}
