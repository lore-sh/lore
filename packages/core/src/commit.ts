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
  CommitParentTable,
  CommitTable,
  OpTable,
  RefTable,
  ReflogTable,
  RowEffectTable,
  SchemaEffectTable,
} from "./schema";

export interface CommitEffects {
  rows: CommitRowEffect[];
  schemas: SchemaEffect[];
}

export interface CommitWriteInput {
  seq: number;
  kind: CommitKind;
  message: string;
  createdAt: number;
  parentIds: string[];
  schemaHashBefore: string;
  schemaHashAfter: string;
  stateHashAfter: string;
  planHash: string;
  revertible: Commit["revertible"];
  revertTargetId: string | null;
  operations: Operation[];
  rowEffects: RowEffect[];
  schemaEffects: SchemaEffect[];
}

export interface CommitReplayInput extends CommitWriteInput {
  commitId: string;
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

function commitHashPayload(input: CommitWriteInput): Record<string, unknown> {
  const rowEffects = input.rowEffects.map((effect) => ({
    tableName: effect.tableName,
    pk: effect.pk,
    opKind: effect.opKind,
    beforeRow: effect.beforeRow,
    afterRow: effect.afterRow,
  }));
  const schemaEffects = input.schemaEffects.map((effect) => ({
    tableName: effect.tableName,
    beforeTable: effect.beforeTable,
    afterTable: effect.afterTable,
  }));
  return {
    seq: input.seq,
    kind: input.kind,
    message: input.message,
    createdAt: input.createdAt,
    parentIds: input.parentIds,
    schemaHashBefore: input.schemaHashBefore,
    schemaHashAfter: input.schemaHashAfter,
    stateHashAfter: input.stateHashAfter,
    planHash: input.planHash,
    revertible: input.revertible,
    revertTargetId: input.revertTargetId,
    operations: input.operations,
    rowEffects,
    schemaEffects,
  };
}

export function computeCommitId(input: CommitWriteInput): string {
  return sha256Hex(commitHashPayload(input));
}

export function appendCommit(db: Database, input: CommitWriteInput): Commit {
  return appendCommitExact(db, {
    ...input,
    commitId: computeCommitId(input),
  });
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
  const afterSchemaHash = schemaHash(db);
  const afterStateHash = stateHash(db);
  const planHash = sha256Hex(input.operations);

  return appendCommit(db, {
    seq,
    kind: input.kind,
    message: input.message,
    createdAt,
    parentIds,
    schemaHashBefore: input.beforeSchemaHash,
    schemaHashAfter: afterSchemaHash,
    stateHashAfter: afterStateHash,
    planHash,
    revertible: 1,
    revertTargetId: input.revertTargetId,
    operations: input.operations,
    rowEffects: captured.rowEffects,
    schemaEffects: captured.schemaEffects,
  });
}

export function appendCommitExact(
  db: Database,
  input: CommitReplayInput,
  options: { errorCode?: ErrorCode } = {},
): Commit {
  const errorCode = options.errorCode ?? "RECOVER_FAILED";
  const commitId = input.commitId;
  const expected = computeCommitId(input);
  if (expected !== commitId) {
    throw new CodedError(errorCode, `Commit payload mismatch for replayed commit ${commitId}`);
  }

  const oldHead = getHeadCommitId(db);

  db.insert(CommitTable)
    .values({
      commitId,
      seq: input.seq,
      kind: input.kind,
      message: input.message,
      createdAt: input.createdAt,
      parentCount: input.parentIds.length,
      schemaHashBefore: input.schemaHashBefore,
      schemaHashAfter: input.schemaHashAfter,
      stateHashAfter: input.stateHashAfter,
      planHash: input.planHash,
      revertible: input.revertible,
      revertTargetId: input.revertTargetId,
    })
    .run();

  for (let i = 0; i < input.parentIds.length; i += 1) {
    db.insert(CommitParentTable).values({ commitId, parentCommitId: input.parentIds[i]!, ord: i }).run();
  }

  for (let i = 0; i < input.operations.length; i += 1) {
    const operation = input.operations[i]!;
    db.insert(OpTable)
      .values({ commitId, opIndex: i, opType: operation.type, opJson: canonicalJson(operation) })
      .run();
  }

  for (let i = 0; i < input.rowEffects.length; i += 1) {
    const effect = input.rowEffects[i]!;
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

  for (let i = 0; i < input.schemaEffects.length; i += 1) {
    const effect = input.schemaEffects[i]!;
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
    .set({ commitId, updatedAt: input.createdAt })
    .where(eq(RefTable.name, MAIN_REF_NAME))
    .run();

  db.insert(ReflogTable)
    .values({
      refName: MAIN_REF_NAME,
      oldCommitId: oldHead,
      newCommitId: commitId,
      reason: input.kind === "revert" ? "revert" : "apply",
      createdAt: input.createdAt,
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

export interface CommitRowEffect extends RowEffect {
  beforeHash: string | null;
  afterHash: string | null;
}

export function getRowEffectsByCommitId(db: Database, commitId: string): CommitRowEffect[] {
  const rows = db
    .select()
    .from(RowEffectTable)
    .where(eq(RowEffectTable.commitId, commitId))
    .orderBy(asc(RowEffectTable.effectIndex))
    .all();

  return rows.map((row) => ({
    tableName: row.tableName,
    pk: JSON.parse(row.pkJson) as Record<string, string>,
    opKind: row.opKind,
    beforeRow: row.beforeJson ? (JSON.parse(row.beforeJson) as RowEffect["beforeRow"]) : null,
    afterRow: row.afterJson ? (JSON.parse(row.afterJson) as RowEffect["afterRow"]) : null,
    beforeHash: row.beforeHash,
    afterHash: row.afterHash,
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

export function getCommitReplayInput(db: Database, commitId: string): CommitReplayInput {
  const commit = getCommitById(db, commitId);
  if (!commit) {
    throw new CodedError("NOT_FOUND", `Commit not found: ${commitId}`);
  }
  return {
    commitId: commit.commitId,
    seq: commit.seq,
    kind: commit.kind,
    message: commit.message,
    createdAt: commit.createdAt,
    parentIds: getCommitParentIds(db, commit.commitId),
    schemaHashBefore: commit.schemaHashBefore,
    schemaHashAfter: commit.schemaHashAfter,
    stateHashAfter: commit.stateHashAfter,
    planHash: commit.planHash,
    revertible: commit.revertible,
    revertTargetId: commit.revertTargetId,
    operations: getCommitOperations(db, commit.commitId),
    rowEffects: getRowEffectsByCommitId(db, commit.commitId),
    schemaEffects: getSchemaEffectsByCommitId(db, commit.commitId),
  };
}

export function loadCommitReplayInputs(db: Database, fromSeqExclusive: number): CommitReplayInput[] {
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
  replay: CommitReplayInput,
  options: { errorCode?: ErrorCode } = {},
): void {
  const errorCode = options.errorCode ?? "RECOVER_FAILED";
  const beforeSchemaHash = schemaHash(db);
  if (beforeSchemaHash !== replay.schemaHashBefore) {
    throw new CodedError(
      errorCode,
      `schema_hash_before mismatch for replay ${replay.commitId}: expected ${replay.schemaHashBefore}, got ${beforeSchemaHash}`,
    );
  }

  const computedPlanHash = sha256Hex(replay.operations);
  if (computedPlanHash !== replay.planHash) {
    throw new CodedError(
      errorCode,
      `plan_hash mismatch for replay ${replay.commitId}: expected ${replay.planHash}, got ${computedPlanHash}`,
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
  if (afterSchemaHash !== replay.schemaHashAfter) {
    throw new CodedError(
      errorCode,
      `schema_hash_after mismatch for replay ${replay.commitId}: expected ${replay.schemaHashAfter}, got ${afterSchemaHash}`,
    );
  }

  const afterStateHash = stateHash(db);
  if (afterStateHash !== replay.stateHashAfter) {
    throw new CodedError(
      errorCode,
      `state_hash_after mismatch for replay ${replay.commitId}: expected ${replay.stateHashAfter}, got ${afterStateHash}`,
    );
  }

  appendCommitExact(db, replay, { errorCode });
}

export function getCommitEffects(db: Database, commitId: string): CommitEffects {
  return {
    rows: getRowEffectsByCommitId(db, commitId),
    schemas: getSchemaEffectsByCommitId(db, commitId),
  };
}
