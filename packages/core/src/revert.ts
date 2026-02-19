import type { Database } from "bun:sqlite";
import { canonicalJson } from "./checksum";
import { buildCommitOperationsResult } from "./commit";
import {
  assertInitialized,
  closeDatabase,
  COMMIT_TABLE,
  openDatabase,
  runInTransaction,
} from "./db";
import { TossError } from "./errors";
import {
  getCommitById,
  getRowEffectsByCommitId,
  getSchemaEffectsByCommitId,
  type StoredRowEffect,
  type StoredSchemaEffect,
} from "./log";
import { fetchRowByPk, normalizeRowObject, rowHash } from "./rows";
import type {
  JsonPrimitive,
  Operation,
  RevertConflict,
  RevertResult,
  ServiceOptions,
} from "./types";

export function detectSchemaConflicts(
  schemaEffects: StoredSchemaEffect[],
  laterSchemaEffects: StoredSchemaEffect[],
): RevertConflict[] {
  const conflicts: RevertConflict[] = [];
  for (const effect of schemaEffects) {
    const matched = laterSchemaEffects.filter((later) => {
      if (later.tableName !== effect.tableName) {
        return false;
      }
      if (!effect.columnName) {
        return true;
      }
      return later.columnName === effect.columnName || later.columnName === null;
    });
    for (const later of matched) {
      conflicts.push({
        kind: "schema",
        table: effect.tableName,
        column: effect.columnName ?? undefined,
        reason: `Later schema change found on ${later.tableName}${later.columnName ? `.${later.columnName}` : ""}`,
      });
    }
  }
  return conflicts;
}

export function detectRowConflict(
  db: Database,
  targetRowEffects: StoredRowEffect[],
  laterRowEffects: StoredRowEffect[],
): RevertConflict[] {
  const conflicts: RevertConflict[] = [];
  for (const effect of targetRowEffects) {
    const pkJson = canonicalJson(effect.pk);
    const touchedLater = laterRowEffects.some(
      (later) => later.tableName === effect.tableName && canonicalJson(later.pk) === pkJson,
    );
    const currentRowRaw = fetchRowByPk(db, effect.tableName, effect.pk);
    const currentRow = currentRowRaw ? normalizeRowObject(currentRowRaw) : null;
    const currentHash = rowHash(currentRow);
    if (effect.opKind === "update" && currentHash !== effect.afterHash) {
      conflicts.push({
        kind: "row",
        table: effect.tableName,
        pk: effect.pk,
        reason: "Current row hash differs from target after-image.",
      });
      continue;
    }
    if (effect.opKind === "insert" && currentHash !== effect.afterHash) {
      conflicts.push({
        kind: "row",
        table: effect.tableName,
        pk: effect.pk,
        reason: "Inserted row was changed or removed after the target commit.",
      });
      continue;
    }
    if (effect.opKind === "delete" && currentRow) {
      conflicts.push({
        kind: "row",
        table: effect.tableName,
        pk: effect.pk,
        reason: touchedLater
          ? "Later commits touched this deleted row and it exists now; revert insert would violate PRIMARY KEY."
          : "Deleted row already exists now; revert insert would violate PRIMARY KEY.",
      });
    }
  }
  return conflicts;
}

export function fetchLaterEffects(db: Database, seq: number): { rows: StoredRowEffect[]; schemas: StoredSchemaEffect[] } {
  const laterCommits = db
    .query(`SELECT commit_id FROM ${COMMIT_TABLE} WHERE seq > ? ORDER BY seq ASC`)
    .all(seq) as Array<{ commit_id: string }>;
  const rows: StoredRowEffect[] = [];
  const schemas: StoredSchemaEffect[] = [];
  for (const commit of laterCommits) {
    rows.push(...getRowEffectsByCommitId(db, commit.commit_id));
    schemas.push(...getSchemaEffectsByCommitId(db, commit.commit_id));
  }
  return { rows, schemas };
}

function primitiveValues(row: Record<string, unknown>): Record<string, JsonPrimitive> {
  const values: Record<string, JsonPrimitive> = {};
  for (const [key, value] of Object.entries(row)) {
    if (key === "__toss_rowid") {
      continue;
    }
    if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      values[key] = value;
      continue;
    }
    throw new TossError(
      "REVERT_FAILED",
      `Non-primitive value cannot be represented in inverse operation: ${key} (${typeof value})`,
    );
  }
  return values;
}

export function applyInverseEffects(db: Database, commitId: string): { operations: Operation[] } {
  const rowEffects = getRowEffectsByCommitId(db, commitId);
  const schemaEffects = getSchemaEffectsByCommitId(db, commitId);
  const inverseOperations: Operation[] = [];

  for (const effect of schemaEffects.toReversed()) {
    if (effect.opKind === "create_table") {
      inverseOperations.push({ type: "drop_table", table: effect.tableName });
      continue;
    }

    if (
      effect.opKind === "drop_table" ||
      effect.opKind === "drop_column" ||
      effect.opKind === "add_column" ||
      effect.opKind === "alter_column_type"
    ) {
      if (!effect.ddlBeforeSql) {
        throw new TossError("REVERT_FAILED", `Missing ddl_before_sql for ${commitId}`);
      }
      inverseOperations.push({
        type: "restore_table",
        table: effect.tableName,
        ddlSql: effect.ddlBeforeSql,
        rows: effect.tableRowsBefore,
      });
      continue;
    }

    if (effect.opKind === "restore_table") {
      if (!effect.ddlBeforeSql) {
        inverseOperations.push({ type: "drop_table", table: effect.tableName });
        continue;
      }
      inverseOperations.push({
        type: "restore_table",
        table: effect.tableName,
        ddlSql: effect.ddlBeforeSql,
        rows: effect.tableRowsBefore,
      });
      continue;
    }

    throw new TossError("REVERT_FAILED", `Unsupported schema effect kind: ${effect.opKind}`);
  }

  for (const effect of rowEffects.toReversed()) {
    if (effect.opKind === "insert") {
      inverseOperations.push({ type: "delete", table: effect.tableName, where: effect.pk });
      continue;
    }

    if (effect.opKind === "delete") {
      if (!effect.beforeRow) {
        continue;
      }
      inverseOperations.push({
        type: "insert",
        table: effect.tableName,
        values: primitiveValues(effect.beforeRow),
      });
      continue;
    }

    if (effect.opKind === "update") {
      if (!effect.beforeRow) {
        throw new TossError(
          "REVERT_FAILED",
          `Missing beforeRow for update effect on ${effect.tableName} at commit ${commitId}`,
        );
      }
      inverseOperations.push({
        type: "update",
        table: effect.tableName,
        values: primitiveValues(effect.beforeRow),
        where: effect.pk,
      });
      continue;
    }
  }

  return { operations: inverseOperations };
}

export function revertCommit(commitId: string, options: ServiceOptions = {}): RevertResult {
  const { db, dbPath } = openDatabase(options.dbPath);
  try {
    assertInitialized(db, dbPath);
    return runInTransaction(db, () => {
      const targetCommit = getCommitById(db, commitId);
      if (!targetCommit) {
        throw new TossError("NOT_FOUND", `Commit not found: ${commitId}`);
      }
      if (!targetCommit.inverseReady) {
        throw new TossError("REVERT_UNSUPPORTED", `Commit ${commitId} has no inverse metadata`);
      }

      const already = db
        .query(`SELECT 1 AS ok FROM ${COMMIT_TABLE} WHERE kind='revert' AND reverted_target_id=? LIMIT 1`)
        .get(commitId) as { ok?: number } | null;
      if (already?.ok === 1) {
        throw new TossError("ALREADY_REVERTED", `Commit is already reverted: ${commitId}`);
      }

      const targetRows = getRowEffectsByCommitId(db, commitId);
      const targetSchemas = getSchemaEffectsByCommitId(db, commitId);
      const later = fetchLaterEffects(db, targetCommit.seq);
      const conflicts = [
        ...detectRowConflict(db, targetRows, later.rows),
        ...detectSchemaConflicts(targetSchemas, later.schemas),
      ];
      if (conflicts.length > 0) {
        return { ok: false, conflicts };
      }

      const inverse = applyInverseEffects(db, commitId);
      const revertCommitEntry = buildCommitOperationsResult(
        db,
        inverse.operations,
        "revert",
        `Revert ${targetCommit.commitId}: ${targetCommit.message}`,
        targetCommit.commitId,
      );
      return { ok: true, revertCommit: revertCommitEntry };
    });
  } finally {
    closeDatabase(db);
  }
}
