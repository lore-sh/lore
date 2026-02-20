import type { Database } from "bun:sqlite";
import { asc, desc, eq, sql } from "drizzle-orm";
import { canonicalJson, sha256Hex } from "./checksum";
import { MAIN_REF_NAME } from "./db";
import { createEngineDb } from "./engine/client";
import {
  CommitParentTable,
  CommitTable,
  EffectRowTable,
  EffectSchemaTable,
  OpTable,
  ReflogTable,
  RefTable,
} from "./engine/schema.sql";
import { TossError } from "./errors";
import type { RowEffect, SchemaEffect } from "./observed";
import type { CommitEntry, CommitKind, Operation } from "./types";

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
  inverseReady: boolean;
  revertedTargetId: string | null;
  operations: Operation[];
  rowEffects: RowEffect[];
  schemaEffects: SchemaEffect[];
}

export interface CommitReplayInput extends CommitWriteInput {
  commitId: string;
}

type CommitRow = typeof CommitTable.$inferSelect;

function decodeCommit(db: Database, row: CommitRow): CommitEntry {
  const engineDb = createEngineDb(db);
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
    inverseReady: row.inverseReady === 1,
    revertedTargetId: row.revertedTargetId,
    operations: operations.map((operation) => JSON.parse(operation.opJson) as Operation),
  };
}

export function getHeadCommitId(db: Database): string | null {
  const row = createEngineDb(db)
    .select({ commitId: RefTable.commitId })
    .from(RefTable)
    .where(eq(RefTable.name, MAIN_REF_NAME))
    .limit(1)
    .get();
  return row?.commitId ?? null;
}

export function getHeadCommit(db: Database): CommitEntry | null {
  const head = getHeadCommitId(db);
  if (!head) {
    return null;
  }
  return getCommitById(db, head);
}

export function getNextCommitSeq(db: Database): number {
  const row = createEngineDb(db)
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
    inverseReady: input.inverseReady,
    revertedTargetId: input.revertedTargetId,
    operations: input.operations,
    rowEffects,
    schemaEffects,
  };
}

export function computeCommitId(input: CommitWriteInput): string {
  return sha256Hex(commitHashPayload(input));
}

export function appendCommit(db: Database, input: CommitWriteInput): CommitEntry {
  return appendCommitExact(db, {
    ...input,
    commitId: computeCommitId(input),
  });
}

export function appendCommitExact(db: Database, input: CommitReplayInput): CommitEntry {
  const commitId = input.commitId;
  const expected = computeCommitId(input);
  if (expected !== commitId) {
    throw new TossError("RECOVER_FAILED", `Commit payload mismatch for replayed commit ${commitId}`);
  }

  const engineDb = createEngineDb(db);
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
      inverseReady: input.inverseReady ? 1 : 0,
      revertedTargetId: input.revertedTargetId,
    })
    .run();

  input.parentIds.forEach((parentCommitId, i) => {
    engineDb.insert(CommitParentTable).values({ commitId, parentCommitId, ord: i }).run();
  });

  input.operations.forEach((operation, i) => {
    engineDb
      .insert(OpTable)
      .values({ commitId, opIndex: i, opType: operation.type, opJson: canonicalJson(operation) })
      .run();
  });

  input.rowEffects.forEach((effect, i) => {
    const beforeRowJson = effect.beforeRow ? canonicalJson(effect.beforeRow) : null;
    const afterRowJson = effect.afterRow ? canonicalJson(effect.afterRow) : null;
    engineDb
      .insert(EffectRowTable)
      .values({
        commitId,
        effectIndex: i,
        tableName: effect.tableName,
        pkJson: canonicalJson(effect.pk),
        opKind: effect.opKind,
        beforeRowJson,
        afterRowJson,
        beforeHash: beforeRowJson ? sha256Hex(beforeRowJson) : null,
        afterHash: afterRowJson ? sha256Hex(afterRowJson) : null,
      })
      .run();
  });

  input.schemaEffects.forEach((effect, i) => {
    engineDb
      .insert(EffectSchemaTable)
      .values({
        commitId,
        effectIndex: i,
        tableName: effect.tableName,
        beforeTableJson: effect.beforeTable ? canonicalJson(effect.beforeTable) : null,
        afterTableJson: effect.afterTable ? canonicalJson(effect.afterTable) : null,
      })
      .run();
  });

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
    throw new TossError("INTERNAL", `Inserted commit not found: ${commitId}`);
  }
  return decodeCommit(db, row);
}

export function getCommitById(db: Database, commitId: string): CommitEntry | null {
  const row = createEngineDb(db).select().from(CommitTable).where(eq(CommitTable.commitId, commitId)).limit(1).get();
  if (!row) {
    return null;
  }
  return decodeCommit(db, row);
}

export function listCommits(db: Database, descending: boolean): CommitEntry[] {
  const rows = createEngineDb(db)
    .select()
    .from(CommitTable)
    .orderBy(descending ? desc(CommitTable.seq) : asc(CommitTable.seq))
    .all();
  return rows.map((row) => decodeCommit(db, row));
}

export interface StoredRowEffect extends RowEffect {
  beforeHash: string | null;
  afterHash: string | null;
}

export type StoredSchemaEffect = SchemaEffect;

export function getRowEffectsByCommitId(db: Database, commitId: string): StoredRowEffect[] {
  const rows = createEngineDb(db)
    .select()
    .from(EffectRowTable)
    .where(eq(EffectRowTable.commitId, commitId))
    .orderBy(asc(EffectRowTable.effectIndex))
    .all();

  return rows.map((row) => ({
    tableName: row.tableName,
    pk: JSON.parse(row.pkJson) as Record<string, string>,
    opKind: row.opKind,
    beforeRow: row.beforeRowJson ? (JSON.parse(row.beforeRowJson) as RowEffect["beforeRow"]) : null,
    afterRow: row.afterRowJson ? (JSON.parse(row.afterRowJson) as RowEffect["afterRow"]) : null,
    beforeHash: row.beforeHash,
    afterHash: row.afterHash,
  }));
}

export function getSchemaEffectsByCommitId(db: Database, commitId: string): StoredSchemaEffect[] {
  const rows = createEngineDb(db)
    .select()
    .from(EffectSchemaTable)
    .where(eq(EffectSchemaTable.commitId, commitId))
    .orderBy(asc(EffectSchemaTable.effectIndex))
    .all();

  return rows.map((row) => ({
    tableName: row.tableName,
    beforeTable: row.beforeTableJson ? (JSON.parse(row.beforeTableJson) as SchemaEffect["beforeTable"]) : null,
    afterTable: row.afterTableJson ? (JSON.parse(row.afterTableJson) as SchemaEffect["afterTable"]) : null,
  }));
}
