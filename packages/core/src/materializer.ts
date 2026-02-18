import type { Database } from "bun:sqlite";
import { executeOperations } from "./executors/apply";
import { TossError } from "./errors";
import { activeApplyLogs } from "./log";
import type { LogEntry } from "./types";

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

export function dropUserTables(db: Database): void {
  const rows = db
    .query(
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '_toss_%' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .all() as Array<{ name: string }>;

  for (const row of rows) {
    db.exec(`DROP TABLE ${quoteIdentifier(row.name)}`);
  }
}

export function rebuildHeadState(db: Database, logs: LogEntry[], extraRevertedTargets: string[] = []): number {
  dropUserTables(db);

  const applyLogs = activeApplyLogs(logs, extraRevertedTargets).sort((a, b) => a.rowid - b.rowid);
  for (const entry of applyLogs) {
    executeOperations(db, entry.operations);
  }
  return applyLogs.length;
}

function hasLaterOperationsOnTable(laterEntries: LogEntry[], table: string): boolean {
  return laterEntries.some((entry) => entry.operations.some((operation) => operation.table === table));
}

function hasLaterReferencesToColumn(laterEntries: LogEntry[], table: string, columnName: string): boolean {
  return laterEntries.some((entry) =>
    entry.operations.some((operation) => {
      if (operation.table !== table) {
        return false;
      }
      if (operation.type === "insert") {
        return Object.hasOwn(operation.values, columnName);
      }
      if (operation.type === "add_column") {
        return operation.column.name === columnName;
      }
      return false;
    }),
  );
}

export function validateRevertSafety(target: LogEntry, activeApplyLogsAfterTarget: LogEntry[]): void {
  for (const operation of target.operations) {
    if (operation.type === "create_table") {
      if (hasLaterOperationsOnTable(activeApplyLogsAfterTarget, operation.table)) {
        throw new TossError(
          "UNSAFE_REVERT",
          `Cannot revert ${target.id}: later commits depend on table ${operation.table}`,
        );
      }
    }

    if (operation.type === "add_column") {
      if (hasLaterReferencesToColumn(activeApplyLogsAfterTarget, operation.table, operation.column.name)) {
        throw new TossError(
          "UNSAFE_REVERT",
          `Cannot revert ${target.id}: later commits depend on ${operation.table}.${operation.column.name}`,
        );
      }
    }
  }
}
