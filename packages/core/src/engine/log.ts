import { asc, desc, eq, sql } from "drizzle-orm";
import { canonicalJson, sha256Hex } from "./checksum";
import { MAIN_REF_NAME, type Database } from "./db";
import { captureObservedState, diffObservedState, type CapturedObservedState, type RowEffect, type SchemaEffect } from "./diff";
import { schemaHash, stateHash } from "./inspect";
import {
  CommitParentTable,
  CommitTable,
  RowEffectTable,
  SchemaEffectTable,
  OpTable,
  ReflogTable,
  RefTable,
} from "./schema.sql";
import { CodedError, type ErrorCode } from "../error";
import type { Operation } from "../apply";
import type { Commit, CommitKind } from "../history";

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
  revertible: boolean;
  revertTargetId: string | null;
  operations: Operation[];
  rowEffects: RowEffect[];
  schemaEffects: SchemaEffect[];
}

export interface CommitReplayInput extends CommitWriteInput {
  commitId: string;
}

type CommitRow = typeof CommitTable.$inferSelect;

function decodeCommit(db: Database, row: CommitRow): Commit {
  const engineDb = db;
  const parents = engineDb
    .select({ parentCommitId: CommitParentTable.parentCommitId })
    .from(CommitParentTable)
    .where(eq(CommitParentTable.commitId, row.commitId))
    .orderBy(asc(CommitParentTable.ord))
    .all();
  const operations = engineDb
    .select({ opJson: OpTable.opJson })
    .from(OpTable)
    .where(eq(OpTable.commitId, row.commitId))
    .orderBy(asc(OpTable.opIndex))
    .all();

  return {
    commitId: row.commitId,
    seq: row.seq,
    kind: row.kind,
    message: row.message,
    createdAt: row.createdAt,
    parentIds: parents.map((parent) => parent.parentCommitId),
    parentCount: row.parentCount,
    schemaHashBefore: row.schemaHashBefore,
    schemaHashAfter: row.schemaHashAfter,
    stateHashAfter: row.stateHashAfter,
    planHash: row.planHash,
    revertible: row.revertible === 1,
    revertTargetId: row.revertTargetId,
    operations: operations.map((operation) => JSON.parse(operation.opJson) as Operation),
  };
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
    revertible: true,
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

  const engineDb = db;
  const oldHead = getHeadCommitId(db);

  engineDb
    .insert(CommitTable)
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
      revertible: input.revertible ? 1 : 0,
      revertTargetId: input.revertTargetId,
    })
    .run();

  for (let i = 0; i < input.parentIds.length; i += 1) {
    engineDb.insert(CommitParentTable).values({ commitId, parentCommitId: input.parentIds[i]!, ord: i }).run();
  }

  for (let i = 0; i < input.operations.length; i += 1) {
    const operation = input.operations[i]!;
    engineDb
      .insert(OpTable)
      .values({ commitId, opIndex: i, opType: operation.type, opJson: canonicalJson(operation) })
      .run();
  }

  for (let i = 0; i < input.rowEffects.length; i += 1) {
    const effect = input.rowEffects[i]!;
    const beforeJson = effect.beforeRow ? canonicalJson(effect.beforeRow) : null;
    const afterJson = effect.afterRow ? canonicalJson(effect.afterRow) : null;
    engineDb
      .insert(RowEffectTable)
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
    engineDb
      .insert(SchemaEffectTable)
      .values({
        commitId,
        effectIndex: i,
        tableName: effect.tableName,
        beforeJson: effect.beforeTable ? canonicalJson(effect.beforeTable) : null,
        afterJson: effect.afterTable ? canonicalJson(effect.afterTable) : null,
      })
      .run();
  }

  engineDb
    .update(RefTable)
    .set({ commitId, updatedAt: input.createdAt })
    .where(eq(RefTable.name, MAIN_REF_NAME))
    .run();

  engineDb
    .insert(ReflogTable)
    .values({
      refName: MAIN_REF_NAME,
      oldCommitId: oldHead,
      newCommitId: commitId,
      reason: input.kind === "revert" ? "revert" : "apply",
      createdAt: input.createdAt,
    })
    .run();

  const row = engineDb.select().from(CommitTable).where(eq(CommitTable.commitId, commitId)).limit(1).get();
  if (!row) {
    throw new CodedError("INTERNAL", `Inserted commit not found: ${commitId}`);
  }
  return decodeCommit(db, row);
}

export function getCommitById(db: Database, commitId: string): Commit | null {
  const row = db.select().from(CommitTable).where(eq(CommitTable.commitId, commitId)).limit(1).get();
  if (!row) {
    return null;
  }
  return decodeCommit(db, row);
}

export function listCommits(db: Database, descending: boolean): Commit[] {
  const rows = db
    .select()
    .from(CommitTable)
    .orderBy(descending ? desc(CommitTable.seq) : asc(CommitTable.seq))
    .all();
  return rows.map((row) => decodeCommit(db, row));
}

export function getCommitCount(db: Database): number {
  const row = db
    .select({ c: sql<number>`count(*)` })
    .from(CommitTable)
    .get();
  return row?.c ?? 0;
}

export interface StoredRowEffect extends RowEffect {
  beforeHash: string | null;
  afterHash: string | null;
}

export type StoredSchemaEffect = SchemaEffect;

export function getRowEffectsByCommitId(db: Database, commitId: string): StoredRowEffect[] {
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

export function getSchemaEffectsByCommitId(db: Database, commitId: string): StoredSchemaEffect[] {
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

export function estimateCommitSizeBytes(db: Database, commitId: string): number {
  const engineDb = db;
  const opBytes = engineDb
    .select({ n: sql<number>`coalesce(sum(length(${OpTable.opJson})), 0)` })
    .from(OpTable)
    .where(eq(OpTable.commitId, commitId))
    .get()?.n ?? 0;
  const rowEffectBytes = engineDb
    .select({
      n: sql<number>`coalesce(sum(length(${RowEffectTable.pkJson}) + coalesce(length(${RowEffectTable.beforeJson}), 0) + coalesce(length(${RowEffectTable.afterJson}), 0)), 0)`,
    })
    .from(RowEffectTable)
    .where(eq(RowEffectTable.commitId, commitId))
    .get()?.n ?? 0;
  const schemaEffectBytes = engineDb
    .select({
      n: sql<number>`coalesce(sum(coalesce(length(${SchemaEffectTable.beforeJson}), 0) + coalesce(length(${SchemaEffectTable.afterJson}), 0)), 0)`,
    })
    .from(SchemaEffectTable)
    .where(eq(SchemaEffectTable.commitId, commitId))
    .get()?.n ?? 0;
  const commitMessageBytes = engineDb
    .select({ n: sql<number>`coalesce(length(${CommitTable.message}), 0)` })
    .from(CommitTable)
    .where(eq(CommitTable.commitId, commitId))
    .limit(1)
    .get()?.n ?? 0;
  return opBytes + rowEffectBytes + schemaEffectBytes + commitMessageBytes;
}

export function estimateHistorySizeBytes(db: Database): number {
  const engineDb = db;
  const opBytes = engineDb
    .select({ n: sql<number>`coalesce(sum(length(${OpTable.opJson})), 0)` })
    .from(OpTable)
    .get()?.n ?? 0;
  const rowEffectBytes = engineDb
    .select({
      n: sql<number>`coalesce(sum(length(${RowEffectTable.pkJson}) + coalesce(length(${RowEffectTable.beforeJson}), 0) + coalesce(length(${RowEffectTable.afterJson}), 0)), 0)`,
    })
    .from(RowEffectTable)
    .get()?.n ?? 0;
  const schemaEffectBytes = engineDb
    .select({
      n: sql<number>`coalesce(sum(coalesce(length(${SchemaEffectTable.beforeJson}), 0) + coalesce(length(${SchemaEffectTable.afterJson}), 0)), 0)`,
    })
    .from(SchemaEffectTable)
    .get()?.n ?? 0;
  const commitMessageBytes = engineDb
    .select({ n: sql<number>`coalesce(sum(length(${CommitTable.message})), 0)` })
    .from(CommitTable)
    .get()?.n ?? 0;
  return opBytes + rowEffectBytes + schemaEffectBytes + commitMessageBytes;
}
