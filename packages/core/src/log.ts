import type { Database } from "bun:sqlite";
import { canonicalJson, sha256Hex } from "./checksum";
import { TossError } from "./errors";
import type { RowEffect, SchemaEffect } from "./observed";
import {
  COMMIT_PARENT_TABLE,
  COMMIT_TABLE,
  EFFECT_ROW_TABLE,
  EFFECT_SCHEMA_TABLE,
  MAIN_REF_NAME,
  OP_TABLE,
  REFLOG_TABLE,
  REF_TABLE,
} from "./db";
import type { CommitEntry, CommitKind, Operation } from "./types";

export interface CommitWriteInput {
  seq: number;
  kind: CommitKind;
  message: string;
  createdAt: string;
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

interface RawCommitRow {
  commit_id: string;
  seq: number;
  kind: CommitKind;
  message: string;
  created_at: string;
  parent_count: number;
  schema_hash_before: string;
  schema_hash_after: string;
  state_hash_after: string;
  plan_hash: string;
  inverse_ready: number;
  reverted_target_id: string | null;
}

function decodeCommit(db: Database, row: RawCommitRow): CommitEntry {
  const parents = db
    .query(`SELECT parent_commit_id FROM ${COMMIT_PARENT_TABLE} WHERE commit_id=? ORDER BY ord ASC`)
    .all(row.commit_id) as Array<{ parent_commit_id: string }>;
  const operations = db
    .query(`SELECT op_json FROM ${OP_TABLE} WHERE commit_id=? ORDER BY op_index ASC`)
    .all(row.commit_id) as Array<{ op_json: string }>;

  return {
    commitId: row.commit_id,
    seq: row.seq,
    kind: row.kind,
    message: row.message,
    createdAt: row.created_at,
    parentIds: parents.map((parent) => parent.parent_commit_id),
    parentCount: row.parent_count,
    schemaHashBefore: row.schema_hash_before,
    schemaHashAfter: row.schema_hash_after,
    stateHashAfter: row.state_hash_after,
    planHash: row.plan_hash,
    inverseReady: row.inverse_ready === 1,
    revertedTargetId: row.reverted_target_id,
    operations: operations.map((operation) => JSON.parse(operation.op_json) as Operation),
  };
}

export function getHeadCommitId(db: Database): string | null {
  const row = db.query(`SELECT commit_id FROM ${REF_TABLE} WHERE name=? LIMIT 1`).get(MAIN_REF_NAME) as
    | { commit_id: string | null }
    | null;
  return row?.commit_id ?? null;
}

export function getHeadCommit(db: Database): CommitEntry | null {
  const head = getHeadCommitId(db);
  if (!head) {
    return null;
  }
  return getCommitById(db, head);
}

export function getNextCommitSeq(db: Database): number {
  const row = db.query(`SELECT COALESCE(MAX(seq), 0) AS max_seq FROM ${COMMIT_TABLE}`).get() as { max_seq: number };
  return row.max_seq + 1;
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
  const oldHead = getHeadCommitId(db);

  db.query(
    `
    INSERT INTO ${COMMIT_TABLE}(
      commit_id, seq, kind, message, created_at, parent_count,
      schema_hash_before, schema_hash_after, state_hash_after, plan_hash, inverse_ready, reverted_target_id
    )
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `,
  ).run(
    commitId,
    input.seq,
    input.kind,
    input.message,
    input.createdAt,
    input.parentIds.length,
    input.schemaHashBefore,
    input.schemaHashAfter,
    input.stateHashAfter,
    input.planHash,
    input.inverseReady ? 1 : 0,
    input.revertedTargetId,
  );

  const insertParent = db.query(`INSERT INTO ${COMMIT_PARENT_TABLE}(commit_id, parent_commit_id, ord) VALUES(?, ?, ?)`);
  for (let i = 0; i < input.parentIds.length; i++) {
    insertParent.run(commitId, input.parentIds[i]!, i);
  }

  const insertOp = db.query(`INSERT INTO ${OP_TABLE}(commit_id, op_index, op_type, op_json) VALUES(?, ?, ?, ?)`);
  for (let i = 0; i < input.operations.length; i++) {
    const operation = input.operations[i]!;
    insertOp.run(commitId, i, operation.type, canonicalJson(operation));
  }

  const insertRowEffect = db.query(
    `
    INSERT INTO ${EFFECT_ROW_TABLE}(
      commit_id, effect_index, table_name, pk_json, op_kind, before_row_json, after_row_json, before_hash, after_hash
    )
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
    `,
  );
  for (let i = 0; i < input.rowEffects.length; i++) {
    const effect = input.rowEffects[i]!;
    const beforeRowJson = effect.beforeRow ? canonicalJson(effect.beforeRow) : null;
    const afterRowJson = effect.afterRow ? canonicalJson(effect.afterRow) : null;
    const beforeHash = beforeRowJson ? sha256Hex(beforeRowJson) : null;
    const afterHash = afterRowJson ? sha256Hex(afterRowJson) : null;
    insertRowEffect.run(
      commitId,
      i,
      effect.tableName,
      canonicalJson(effect.pk),
      effect.opKind,
      beforeRowJson,
      afterRowJson,
      beforeHash,
      afterHash,
    );
  }

  const insertSchemaEffect = db.query(
    `
    INSERT INTO ${EFFECT_SCHEMA_TABLE}(
      commit_id, effect_index, table_name, before_table_json, after_table_json
    )
    VALUES(?, ?, ?, ?, ?)
    `,
  );
  for (let i = 0; i < input.schemaEffects.length; i++) {
    const effect = input.schemaEffects[i]!;
    insertSchemaEffect.run(
      commitId,
      i,
      effect.tableName,
      effect.beforeTable ? canonicalJson(effect.beforeTable) : null,
      effect.afterTable ? canonicalJson(effect.afterTable) : null,
    );
  }

  db.query(`UPDATE ${REF_TABLE} SET commit_id=?, updated_at=? WHERE name=?`).run(commitId, input.createdAt, MAIN_REF_NAME);
  db.query(
    `
    INSERT INTO ${REFLOG_TABLE}(ref_name, old_commit_id, new_commit_id, reason, created_at)
    VALUES(?, ?, ?, ?, ?)
    `,
  ).run(MAIN_REF_NAME, oldHead, commitId, input.kind === "revert" ? "revert" : "apply", input.createdAt);

  const row = db.query(`SELECT * FROM ${COMMIT_TABLE} WHERE commit_id=? LIMIT 1`).get(commitId) as RawCommitRow;
  return decodeCommit(db, row);
}

export function getCommitById(db: Database, commitId: string): CommitEntry | null {
  const row = db.query(`SELECT * FROM ${COMMIT_TABLE} WHERE commit_id=? LIMIT 1`).get(commitId) as RawCommitRow | null;
  if (!row) {
    return null;
  }
  return decodeCommit(db, row);
}

export function listCommits(db: Database, descending: boolean): CommitEntry[] {
  const rows = db
    .query(`SELECT * FROM ${COMMIT_TABLE} ORDER BY seq ${descending ? "DESC" : "ASC"}`)
    .all() as RawCommitRow[];
  return rows.map((row) => decodeCommit(db, row));
}

export interface StoredRowEffect extends RowEffect {
  beforeHash: string | null;
  afterHash: string | null;
}

export type StoredSchemaEffect = SchemaEffect;

export function getRowEffectsByCommitId(db: Database, commitId: string): StoredRowEffect[] {
  const rows = db
    .query(
      `
      SELECT table_name, pk_json, op_kind, before_row_json, after_row_json, before_hash, after_hash
      FROM ${EFFECT_ROW_TABLE}
      WHERE commit_id=?
      ORDER BY effect_index ASC
      `,
    )
    .all(commitId) as Array<{
    table_name: string;
    pk_json: string;
    op_kind: "insert" | "update" | "delete";
    before_row_json: string | null;
    after_row_json: string | null;
    before_hash: string | null;
    after_hash: string | null;
  }>;

  return rows.map((row) => ({
    tableName: row.table_name,
    pk: JSON.parse(row.pk_json) as Record<string, string>,
    opKind: row.op_kind,
    beforeRow: row.before_row_json ? (JSON.parse(row.before_row_json) as RowEffect["beforeRow"]) : null,
    afterRow: row.after_row_json ? (JSON.parse(row.after_row_json) as RowEffect["afterRow"]) : null,
    beforeHash: row.before_hash,
    afterHash: row.after_hash,
  }));
}

export function getSchemaEffectsByCommitId(db: Database, commitId: string): StoredSchemaEffect[] {
  const rows = db
    .query(
      `
      SELECT table_name, before_table_json, after_table_json
      FROM ${EFFECT_SCHEMA_TABLE}
      WHERE commit_id=?
      ORDER BY effect_index ASC
      `,
    )
    .all(commitId) as Array<{
    table_name: string;
    before_table_json: string | null;
    after_table_json: string | null;
  }>;

  return rows.map((row) => ({
    tableName: row.table_name,
    beforeTable: row.before_table_json ? (JSON.parse(row.before_table_json) as SchemaEffect["beforeTable"]) : null,
    afterTable: row.after_table_json ? (JSON.parse(row.after_table_json) as SchemaEffect["afterTable"]) : null,
  }));
}
