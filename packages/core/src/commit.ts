import { asc, desc, eq, gt, sql } from "drizzle-orm";
import { MAIN_REF_NAME, type Database } from "./db";
import {
  applyRowEffectsWithOptions,
  applyUserRowAndSchemaEffects,
  assertNoForeignKeyViolations,
  captureObservedState,
  diffObservedState,
  type CapturedObservedState,
  type RowEffect,
  type SchemaEffect,
} from "./effect";
import { CodedError, type ErrorCode } from "./error";
import { canonicalJson, sha256Hex } from "./hash";
import { schemaHash, stateHash } from "./inspect";
import type { Operation } from "./operation";
import {
  type Commit,
  type CommitKind,
  type RowEffectRow,
  CommitParentTable,
  CommitTable,
  OpTable,
  RefTable,
  ReflogTable,
  RowEffectTable,
  SchemaEffectTable,
} from "./schema";

export type CommitPayload = Omit<Commit, "commitId" | "parentCount">;

export interface CommitBundle {
  commitId: string;
  commit: CommitPayload;
  parentIds: string[];
  operations: Operation[];
  rowEffects: RowEffect[];
  schemaEffects: SchemaEffect[];
}

export function getHeadCommitId(db: Database): string | null {
  const row = db
    .select({ commitId: RefTable.commitId })
    .from(RefTable)
    .where(eq(RefTable.name, MAIN_REF_NAME))
    .limit(1)
    .get();
  return row?.commitId ?? null;
}

export function getHeadCommit(db: Database): Commit | null {
  const head = getHeadCommitId(db);
  if (!head) {
    return null;
  }
  return getCommitById(db, head);
}

export function getNextCommitSeq(db: Database): number {
  const row = db
    .select({ maxSeq: sql<number>`coalesce(max(${CommitTable.seq}), 0)` })
    .from(CommitTable)
    .get();
  return (row?.maxSeq ?? 0) + 1;
}

function commitHashPayload(
  commit: CommitPayload,
  parentIds: string[],
  operations: Operation[],
  rowEffects: RowEffect[],
  schemaEffects: SchemaEffect[],
): Record<string, unknown> {
  return {
    seq: commit.seq,
    kind: commit.kind,
    message: commit.message,
    createdAt: commit.createdAt,
    schemaHashBefore: commit.schemaHashBefore,
    schemaHashAfter: commit.schemaHashAfter,
    stateHashAfter: commit.stateHashAfter,
    planHash: commit.planHash,
    revertible: commit.revertible,
    revertTargetId: commit.revertTargetId,
    parentIds,
    operations,
    rowEffects: rowEffects.map((effect) => ({
      tableName: effect.tableName,
      pk: effect.pk,
      opKind: effect.opKind,
      beforeRow: effect.beforeRow,
      afterRow: effect.afterRow,
    })),
    schemaEffects: schemaEffects.map((effect) => ({
      tableName: effect.tableName,
      beforeTable: effect.beforeTable,
      afterTable: effect.afterTable,
    })),
  };
}

export function computeCommitId(
  commit: CommitPayload,
  parentIds: string[],
  operations: Operation[],
  rowEffects: RowEffect[],
  schemaEffects: SchemaEffect[],
): string {
  return sha256Hex(commitHashPayload(commit, parentIds, operations, rowEffects, schemaEffects));
}

export function appendCommit(
  db: Database,
  commit: CommitPayload,
  parentIds: string[],
  operations: Operation[],
  rowEffects: RowEffect[],
  schemaEffects: SchemaEffect[],
): Commit {
  const commitId = computeCommitId(commit, parentIds, operations, rowEffects, schemaEffects);
  return appendCommitExact(db, commitId, commit, parentIds, operations, rowEffects, schemaEffects);
}

export function appendCommitObserved(
  db: Database,
  input: {
    operations: Operation[];
    kind: "apply" | "revert";
    message: string;
    revertTargetId: string | null;
    beforeSchemaHash: string;
    beforeObservedState: CapturedObservedState;
  },
): Commit {
  const parent = getHeadCommit(db);
  const parentIds = parent ? [parent.commitId] : [];
  const seq = getNextCommitSeq(db);
  const createdAt = Date.now();
  const afterObservedState = captureObservedState(db);
  const captured = diffObservedState(input.beforeObservedState, afterObservedState);

  return appendCommit(
    db,
    {
      seq,
      kind: input.kind,
      message: input.message,
      createdAt,
      schemaHashBefore: input.beforeSchemaHash,
      schemaHashAfter: schemaHash(db),
      stateHashAfter: stateHash(db),
      planHash: sha256Hex(input.operations),
      revertible: 1,
      revertTargetId: input.revertTargetId,
    },
    parentIds,
    input.operations,
    captured.rowEffects,
    captured.schemaEffects,
  );
}

export function appendCommitExact(
  db: Database,
  commitId: string,
  commit: CommitPayload,
  parentIds: string[],
  operations: Operation[],
  rowEffects: RowEffect[],
  schemaEffects: SchemaEffect[],
  options: { errorCode?: ErrorCode } = {},
): Commit {
  const errorCode = options.errorCode ?? "RECOVER_FAILED";
  const expected = computeCommitId(commit, parentIds, operations, rowEffects, schemaEffects);
  if (expected !== commitId) {
    throw new CodedError(errorCode, `Commit payload mismatch for replayed commit ${commitId}`);
  }

  const oldHead = getHeadCommitId(db);

  db.insert(CommitTable)
    .values({
      commitId,
      ...commit,
      parentCount: parentIds.length,
    })
    .run();

  for (let i = 0; i < parentIds.length; i += 1) {
    db.insert(CommitParentTable).values({ commitId, parentCommitId: parentIds[i]!, ord: i }).run();
  }

  for (let i = 0; i < operations.length; i += 1) {
    const operation = operations[i]!;
    db.insert(OpTable)
      .values({ commitId, opIndex: i, opType: operation.type, opJson: canonicalJson(operation) })
      .run();
  }

  for (let i = 0; i < rowEffects.length; i += 1) {
    const effect = rowEffects[i]!;
    const beforeJson = effect.beforeRow ? canonicalJson(effect.beforeRow) : null;
    const afterJson = effect.afterRow ? canonicalJson(effect.afterRow) : null;
    db.insert(RowEffectTable)
      .values({
        commitId,
        effectIndex: i,
        tableName: effect.tableName,
        pkJson: canonicalJson(effect.pk),
        opKind: effect.opKind,
        beforeJson,
        afterJson,
        beforeHash: beforeJson ? sha256Hex(beforeJson) : null,
        afterHash: afterJson ? sha256Hex(afterJson) : null,
      })
      .run();
  }

  for (let i = 0; i < schemaEffects.length; i += 1) {
    const effect = schemaEffects[i]!;
    db.insert(SchemaEffectTable)
      .values({
        commitId,
        effectIndex: i,
        tableName: effect.tableName,
        beforeJson: effect.beforeTable ? canonicalJson(effect.beforeTable) : null,
        afterJson: effect.afterTable ? canonicalJson(effect.afterTable) : null,
      })
      .run();
  }

  db.update(RefTable)
    .set({ commitId, updatedAt: commit.createdAt })
    .where(eq(RefTable.name, MAIN_REF_NAME))
    .run();

  db.insert(ReflogTable)
    .values({
      refName: MAIN_REF_NAME,
      oldCommitId: oldHead,
      newCommitId: commitId,
      reason: commit.kind === "revert" ? "revert" : "apply",
      createdAt: commit.createdAt,
    })
    .run();

  const row = db.select().from(CommitTable).where(eq(CommitTable.commitId, commitId)).limit(1).get();
  if (!row) {
    throw new CodedError("INTERNAL", `Inserted commit not found: ${commitId}`);
  }
  return row;
}

export function getCommitById(db: Database, commitId: string): Commit | null {
  return db.select().from(CommitTable).where(eq(CommitTable.commitId, commitId)).limit(1).get() ?? null;
}

export function listCommits(db: Database, descending: boolean): Commit[] {
  return db
    .select()
    .from(CommitTable)
    .orderBy(descending ? desc(CommitTable.seq) : asc(CommitTable.seq))
    .all();
}

export function getCommitParentIds(db: Database, commitId: string): string[] {
  return db
    .select({ parentCommitId: CommitParentTable.parentCommitId })
    .from(CommitParentTable)
    .where(eq(CommitParentTable.commitId, commitId))
    .orderBy(asc(CommitParentTable.ord))
    .all()
    .map((row) => row.parentCommitId);
}

export function getCommitOperations(db: Database, commitId: string): Operation[] {
  return db
    .select({ opJson: OpTable.opJson })
    .from(OpTable)
    .where(eq(OpTable.commitId, commitId))
    .orderBy(asc(OpTable.opIndex))
    .all()
    .map((row) => JSON.parse(row.opJson) as Operation);
}

export function getCommitCount(db: Database): number {
  const row = db
    .select({ c: sql<number>`count(*)` })
    .from(CommitTable)
    .get();
  return row?.c ?? 0;
}

export function getRowEffectsByCommitId(db: Database, commitId: string): RowEffectRow[] {
  return db
    .select()
    .from(RowEffectTable)
    .where(eq(RowEffectTable.commitId, commitId))
    .orderBy(asc(RowEffectTable.effectIndex))
    .all();
}

export function decodeRowEffects(rows: RowEffectRow[]): RowEffect[] {
  return rows.map((row) => ({
    tableName: row.tableName,
    pk: JSON.parse(row.pkJson) as RowEffect["pk"],
    opKind: row.opKind,
    beforeRow: row.beforeJson ? (JSON.parse(row.beforeJson) as RowEffect["beforeRow"]) : null,
    afterRow: row.afterJson ? (JSON.parse(row.afterJson) as RowEffect["afterRow"]) : null,
  }));
}

export function getSchemaEffectsByCommitId(db: Database, commitId: string): SchemaEffect[] {
  const rows = db
    .select()
    .from(SchemaEffectTable)
    .where(eq(SchemaEffectTable.commitId, commitId))
    .orderBy(asc(SchemaEffectTable.effectIndex))
    .all();

  return rows.map((row) => ({
    tableName: row.tableName,
    beforeTable: row.beforeJson ? (JSON.parse(row.beforeJson) as SchemaEffect["beforeTable"]) : null,
    afterTable: row.afterJson ? (JSON.parse(row.afterJson) as SchemaEffect["afterTable"]) : null,
  }));
}

export function getCommitReplayInput(db: Database, commitId: string): CommitBundle {
  const commitRow = getCommitById(db, commitId);
  if (!commitRow) {
    throw new CodedError("NOT_FOUND", `Commit not found: ${commitId}`);
  }
  return {
    commitId: commitRow.commitId,
    commit: {
      seq: commitRow.seq,
      kind: commitRow.kind,
      message: commitRow.message,
      createdAt: commitRow.createdAt,
      schemaHashBefore: commitRow.schemaHashBefore,
      schemaHashAfter: commitRow.schemaHashAfter,
      stateHashAfter: commitRow.stateHashAfter,
      planHash: commitRow.planHash,
      revertible: commitRow.revertible,
      revertTargetId: commitRow.revertTargetId,
    },
    parentIds: getCommitParentIds(db, commitRow.commitId),
    operations: getCommitOperations(db, commitRow.commitId),
    rowEffects: decodeRowEffects(getRowEffectsByCommitId(db, commitRow.commitId)),
    schemaEffects: getSchemaEffectsByCommitId(db, commitRow.commitId),
  };
}

export function loadCommitReplayInputs(db: Database, fromSeqExclusive: number): CommitBundle[] {
  const commitRows = db
    .select({ commitId: CommitTable.commitId })
    .from(CommitTable)
    .where(gt(CommitTable.seq, fromSeqExclusive))
    .orderBy(asc(CommitTable.seq))
    .all();
  return commitRows.map((row) => getCommitReplayInput(db, row.commitId));
}

export function findCommitSeq(db: Database, commitId: string): number | null {
  const row = db
    .select({ seq: CommitTable.seq })
    .from(CommitTable)
    .where(eq(CommitTable.commitId, commitId))
    .limit(1)
    .get();
  return row?.seq ?? null;
}

export function replayCommitExactly(
  db: Database,
  replay: CommitBundle,
  options: { errorCode?: ErrorCode } = {},
): void {
  const errorCode = options.errorCode ?? "RECOVER_FAILED";
  const beforeSchemaHash = schemaHash(db);
  if (beforeSchemaHash !== replay.commit.schemaHashBefore) {
    throw new CodedError(
      errorCode,
      `schema_hash_before mismatch for replay ${replay.commitId}: expected ${replay.commit.schemaHashBefore}, got ${beforeSchemaHash}`,
    );
  }

  const computedPlanHash = sha256Hex(replay.operations);
  if (computedPlanHash !== replay.commit.planHash) {
    throw new CodedError(
      errorCode,
      `plan_hash mismatch for replay ${replay.commitId}: expected ${replay.commit.planHash}, got ${computedPlanHash}`,
    );
  }

  applyUserRowAndSchemaEffects(db, replay.rowEffects, replay.schemaEffects, "forward", {
    disableTableTriggers: true,
  });
  applyRowEffectsWithOptions(db, replay.rowEffects, "forward", {
    disableTableTriggers: true,
    includeUserEffects: false,
    includeSystemEffects: true,
    systemPolicy: "reconcile",
  });
  assertNoForeignKeyViolations(db, errorCode, `replay ${replay.commitId}`);

  const afterSchemaHash = schemaHash(db);
  if (afterSchemaHash !== replay.commit.schemaHashAfter) {
    throw new CodedError(
      errorCode,
      `schema_hash_after mismatch for replay ${replay.commitId}: expected ${replay.commit.schemaHashAfter}, got ${afterSchemaHash}`,
    );
  }

  const afterStateHash = stateHash(db);
  if (afterStateHash !== replay.commit.stateHashAfter) {
    throw new CodedError(
      errorCode,
      `state_hash_after mismatch for replay ${replay.commitId}: expected ${replay.commit.stateHashAfter}, got ${afterStateHash}`,
    );
  }

  appendCommitExact(
    db,
    replay.commitId,
    replay.commit,
    replay.parentIds,
    replay.operations,
    replay.rowEffects,
    replay.schemaEffects,
    { errorCode },
  );
}
