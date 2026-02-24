import { and, asc, eq, gt } from "drizzle-orm";
import {
  createCommit,
  decodeRowEffects,
  findCommit,
  commitRowEffects,
  commitSchemaEffects,
  commitSeq,
} from "./commit";
import {
  runInDeferredTransaction,
  runInSavepoint,
  tableExists,
  type Database,
} from "./db";
import {
  applyRowEffects,
  applyEffects,
  assertForeignKeys,
  captureState,
  readRow,
  isSystemTable,
  rowHash,
  type RowEffect,
  type SchemaEffect,
} from "./effect";
import { CodedError } from "./error";
import { canonicalJson } from "./hash";
import { schemaHash } from "./inspect";
import { CommitTable } from "./schema";

export function detectSchemaConflicts(
  schemaEffects: SchemaEffect[],
  laterSchemaEffects: SchemaEffect[],
) {
  const conflicts: Array<{
    kind: "row" | "schema";
    table: string;
    pk?: Record<string, string> | undefined;
    column?: string | undefined;
    reason: string;
  }> = [];
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
  schemaEffects: SchemaEffect[],
  laterRowEffects: RowEffect[],
) {
  const conflicts: Array<{
    kind: "row" | "schema";
    table: string;
    pk?: Record<string, string> | undefined;
    column?: string | undefined;
    reason: string;
  }> = [];
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
  targetRowEffects: RowEffect[],
  laterRowEffects: RowEffect[],
) {
  const conflicts: Array<{
    kind: "row" | "schema";
    table: string;
    pk?: Record<string, string> | undefined;
    column?: string | undefined;
    reason: string;
  }> = [];
  const missingTables = new Set<string>();
  for (const effect of targetRowEffects) {
    const pk = effect.pk;
    if (!tableExists(db, effect.tableName)) {
      if (!missingTables.has(effect.tableName)) {
        conflicts.push({
          kind: isSystemTable(effect.tableName) ? "row" : "schema",
          table: effect.tableName,
          reason: `Current table is missing (${effect.tableName}); later schema changes prevent row-level revert.`,
        });
        missingTables.add(effect.tableName);
      }
      continue;
    }
    const currentRow = readRow(db, effect.tableName, pk);
    const currentHash = rowHash(currentRow);

    if (effect.opKind === "update" && currentHash !== effect.afterHash) {
      conflicts.push({
        kind: "row",
        table: effect.tableName,
        pk,
        reason: "Current row hash differs from target after-image.",
      });
      continue;
    }

    if (effect.opKind === "insert" && currentHash !== effect.afterHash) {
      conflicts.push({
        kind: "row",
        table: effect.tableName,
        pk,
        reason: "Inserted row was changed or removed after the target commit.",
      });
      continue;
    }

    if (effect.opKind === "delete" && currentRow) {
      const pkJson = canonicalJson(pk);
      const touchedLater = laterRowEffects.some(
        (later) => later.tableName === effect.tableName && canonicalJson(later.pk) === pkJson,
      );
      conflicts.push({
        kind: "row",
        table: effect.tableName,
        pk,
        reason: touchedLater
          ? "Later commits touched this deleted row and it exists now; inverse insert would violate PRIMARY KEY."
          : "Deleted row already exists now; inverse insert would violate PRIMARY KEY.",
      });
    }
  }
  return conflicts;
}

export function getLaterEffects(db: Database, seq: number): { rows: RowEffect[]; schemas: SchemaEffect[] } {
  const laterCommits = db
    .select({ commitId: CommitTable.commitId })
    .from(CommitTable)
    .where(gt(CommitTable.seq, seq))
    .orderBy(asc(CommitTable.seq))
    .all();
  const rows: RowEffect[] = [];
  const schemas: SchemaEffect[] = [];
  for (const commit of laterCommits) {
    rows.push(...decodeRowEffects(commitRowEffects(db, commit.commitId)));
    schemas.push(...commitSchemaEffects(db, commit.commitId));
  }
  return { rows, schemas };
}

function sqliteConstraintConflict(error: unknown): ReturnType<typeof detectRowConflict>[number] | null {
  if (!(error instanceof Error)) {
    return null;
  }
  const message = error.message;
  if (!message.toUpperCase().includes("CONSTRAINT")) {
    return null;
  }
  const tableMatch =
    /UNIQUE constraint failed: ([^.]+)\./i.exec(message) ??
    /NOT NULL constraint failed: ([^.]+)\./i.exec(message);
  if (tableMatch?.[1]) {
    return { kind: "row", table: tableMatch[1], reason: message };
  }
  return { kind: "schema", table: "(unknown)", reason: message };
}

function preflightInverseApply(
  db: Database,
  targetRows: RowEffect[],
  targetSchemas: SchemaEffect[],
): Array<ReturnType<typeof detectRowConflict>[number]> {
  try {
    runInSavepoint(
      db,
      "toss_revert_preflight",
      () => {
        applyEffects(db, targetRows, targetSchemas, "inverse", {
          disableTableTriggers: true,
        });
        applyRowEffects(db, targetRows, "inverse", {
          disableTableTriggers: true,
          includeUserEffects: false,
          includeSystemEffects: true,
          systemPolicy: "reconcile",
        });
        assertForeignKeys(db, "REVERT_FAILED", "revert preflight");
      },
      { rollbackOnSuccess: true },
    );
    return [];
  } catch (error) {
    if (CodedError.hasCode(error, "REVERT_FAILED")) {
      return [{ kind: "schema", table: "(unknown)", reason: error.message }];
    }
    const conflict = sqliteConstraintConflict(error);
    if (conflict) {
      return [conflict];
    }
    throw error;
  }
}

export function revert(
  db: Database,
  commitId: string,
):
  | { ok: true; revertCommit: ReturnType<typeof createCommit> }
  | { ok: false; conflicts: Array<ReturnType<typeof detectRowConflict>[number]> } {
  return runInDeferredTransaction(db, () => {
    const targetCommit = findCommit(db, commitId);
    if (!targetCommit) {
      throw new CodedError("NOT_FOUND", `Commit not found: ${commitId}`);
    }
    if (targetCommit.revertible !== 1) {
      throw new CodedError("NOT_REVERTIBLE", `Commit ${commitId} has no inverse metadata`);
    }

    const already = db
      .select({ commitId: CommitTable.commitId })
      .from(CommitTable)
      .where(and(eq(CommitTable.kind, "revert"), eq(CommitTable.revertTargetId, commitId)))
      .limit(1)
      .get();
    if (already) {
      throw new CodedError("ALREADY_REVERTED", `Commit is already reverted: ${commitId}`);
    }

    const targetRows = decodeRowEffects(commitRowEffects(db, commitId));
    const targetSchemas = commitSchemaEffects(db, commitId);
    const seq = commitSeq(db, commitId);
    if (seq === null) {
      throw new CodedError("NOT_FOUND", `Commit not found: ${commitId}`);
    }
    const later = getLaterEffects(db, seq);
    const conflicts: Array<ReturnType<typeof detectRowConflict>[number]> = [
      ...detectRowConflict(db, targetRows, later.rows),
      ...detectSchemaConflicts(targetSchemas, later.schemas),
      ...detectSchemaRowConflicts(targetSchemas, later.rows),
      ...preflightInverseApply(db, targetRows, targetSchemas),
    ];
    if (conflicts.length > 0) {
      return { ok: false, conflicts };
    }

    const beforeSchemaHash = schemaHash(db);
    const beforeState = captureState(db);
    applyEffects(db, targetRows, targetSchemas, "inverse", {
      disableTableTriggers: true,
    });
    applyRowEffects(db, targetRows, "inverse", {
      disableTableTriggers: true,
      includeUserEffects: false,
      includeSystemEffects: true,
      systemPolicy: "reconcile",
    });
    assertForeignKeys(db, "REVERT_FAILED", "revert apply");

    const revertCommitEntry = createCommit(db, {
      operations: [],
      kind: "revert",
      message: `Revert ${targetCommit.commitId}: ${targetCommit.message}`,
      revertTargetId: targetCommit.commitId,
      beforeSchemaHash,
      beforeState,
    });
    return { ok: true, revertCommit: revertCommitEntry };
  });
}
