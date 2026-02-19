import type { Database } from "bun:sqlite";
import { canonicalJson } from "./checksum";
import { appendCommitFromObservedChange } from "./commit";
import {
  assertInitialized,
  closeDatabase,
  COMMIT_TABLE,
  openDatabase,
  runInTransactionWithDeferredForeignKeys,
  tableExists,
} from "./db";
import { TossError, isTossError } from "./errors";
import {
  getCommitById,
  getRowEffectsByCommitId,
  getSchemaEffectsByCommitId,
  type StoredRowEffect,
  type StoredSchemaEffect,
} from "./log";
import {
  applyRowEffectsWithOptions,
  applyUserRowAndSchemaEffects,
  assertNoForeignKeyViolations,
  captureObservedState,
  fetchObservedRowByPk,
  isSystemSideEffectTable,
  rowHash,
} from "./observed";
import { schemaHash } from "./rows";
import type { DatabaseOptions, RevertConflict, RevertResult } from "./types";

export function detectSchemaConflicts(
  schemaEffects: StoredSchemaEffect[],
  laterSchemaEffects: StoredSchemaEffect[],
): RevertConflict[] {
  const conflicts: RevertConflict[] = [];
  for (const effect of schemaEffects) {
    const matched = laterSchemaEffects.filter((later) => later.tableName === effect.tableName);
    for (const later of matched) {
      conflicts.push({
        kind: "schema",
        table: effect.tableName,
        reason: `Later schema change found on ${later.tableName}`,
      });
    }
  }
  return conflicts;
}

export function detectSchemaRowConflicts(
  schemaEffects: StoredSchemaEffect[],
  laterRowEffects: StoredRowEffect[],
): RevertConflict[] {
  const conflicts: RevertConflict[] = [];
  for (const effect of schemaEffects) {
    const touched = laterRowEffects.filter((later) => later.tableName === effect.tableName);
    for (const later of touched) {
      conflicts.push({
        kind: "schema",
        table: effect.tableName,
        pk: later.pk,
        reason: `Later row change found on ${effect.tableName}; reverting schema would discard post-commit row mutations.`,
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
  const missingTables = new Set<string>();
  for (const effect of targetRowEffects) {
    if (!tableExists(db, effect.tableName)) {
      if (!missingTables.has(effect.tableName)) {
        conflicts.push({
          kind: isSystemSideEffectTable(effect.tableName) ? "row" : "schema",
          table: effect.tableName,
          reason: `Current table is missing (${effect.tableName}); later schema changes prevent row-level revert.`,
        });
        missingTables.add(effect.tableName);
      }
      continue;
    }
    const currentRow = fetchObservedRowByPk(db, effect.tableName, effect.pk);
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
      const pkJson = canonicalJson(effect.pk);
      const touchedLater = laterRowEffects.some(
        (later) => later.tableName === effect.tableName && canonicalJson(later.pk) === pkJson,
      );
      conflicts.push({
        kind: "row",
        table: effect.tableName,
        pk: effect.pk,
        reason: touchedLater
          ? "Later commits touched this deleted row and it exists now; inverse insert would violate PRIMARY KEY."
          : "Deleted row already exists now; inverse insert would violate PRIMARY KEY.",
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

function sqliteConstraintConflict(error: unknown): RevertConflict | null {
  if (!(error instanceof Error)) {
    return null;
  }
  const message = error.message;
  if (!message.toUpperCase().includes("CONSTRAINT")) {
    return null;
  }
  const unique = /UNIQUE constraint failed: ([^.]+)\./i.exec(message);
  if (unique?.[1]) {
    return {
      kind: "row",
      table: unique[1],
      reason: message,
    };
  }
  const notNull = /NOT NULL constraint failed: ([^.]+)\./i.exec(message);
  if (notNull?.[1]) {
    return {
      kind: "row",
      table: notNull[1],
      reason: message,
    };
  }
  return {
    kind: "schema",
    table: "(unknown)",
    reason: message,
  };
}

function preflightInverseApply(
  db: Database,
  targetRows: StoredRowEffect[],
  targetSchemas: StoredSchemaEffect[],
): RevertConflict[] {
  db.run("SAVEPOINT toss_revert_preflight");
  try {
    applyUserRowAndSchemaEffects(db, targetRows, targetSchemas, "inverse", {
      disableTableTriggers: true,
    });
    applyRowEffectsWithOptions(db, targetRows, "inverse", {
      disableTableTriggers: true,
      includeUserEffects: false,
      includeSystemEffects: true,
      systemPolicy: "reconcile",
    });
    assertNoForeignKeyViolations(db, "REVERT_FAILED", "revert preflight");
    db.run("ROLLBACK TO toss_revert_preflight");
    db.run("RELEASE toss_revert_preflight");
    return [];
  } catch (error) {
    try {
      db.run("ROLLBACK TO toss_revert_preflight");
      db.run("RELEASE toss_revert_preflight");
    } catch {
      // no-op
    }
    if (isTossError(error) && error.code === "REVERT_FAILED") {
      return [{ kind: "schema", table: "(unknown)", reason: error.message }];
    }
    const conflict = sqliteConstraintConflict(error);
    if (conflict) {
      return [conflict];
    }
    throw error;
  }
}

export function revertCommit(commitId: string, options: DatabaseOptions = {}): RevertResult {
  const { db, dbPath } = openDatabase(options.dbPath);
  try {
    assertInitialized(db, dbPath);
    return runInTransactionWithDeferredForeignKeys(db, () => {
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
        ...detectSchemaRowConflicts(targetSchemas, later.rows),
        ...preflightInverseApply(db, targetRows, targetSchemas),
      ];
      if (conflicts.length > 0) {
        return { ok: false, conflicts };
      }

      const beforeSchemaHash = schemaHash(db);
      const beforeObservedState = captureObservedState(db);
      applyUserRowAndSchemaEffects(db, targetRows, targetSchemas, "inverse", {
        disableTableTriggers: true,
      });
      applyRowEffectsWithOptions(db, targetRows, "inverse", {
        disableTableTriggers: true,
        includeUserEffects: false,
        includeSystemEffects: true,
        systemPolicy: "reconcile",
      });
      assertNoForeignKeyViolations(db, "REVERT_FAILED", "revert apply");

      const revertCommitEntry = appendCommitFromObservedChange(db, {
        operations: [],
        kind: "revert",
        message: `Revert ${targetCommit.commitId}: ${targetCommit.message}`,
        revertedTargetId: targetCommit.commitId,
        beforeSchemaHash,
        beforeObservedState,
      });
      return { ok: true, revertCommit: revertCommitEntry };
    });
  } finally {
    closeDatabase(db);
  }
}
