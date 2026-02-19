import type { Database } from "bun:sqlite";
import { canonicalJson, sha256Hex } from "./checksum";
import { MAIN_REF_NAME } from "./db";
import type { CommitEntry, CommitKind, JsonObject, Operation } from "./types";

export interface RowEffect {
  tableName: string;
  pk: Record<string, string | number | boolean | null>;
  opKind: "insert" | "update" | "delete";
  beforeRow: JsonObject | null;
  afterRow: JsonObject | null;
}

export interface SchemaEffect {
  tableName: string;
  columnName: string | null;
  opKind: "create_table" | "add_column" | "drop_table" | "drop_column" | "alter_column_type";
  ddlBeforeSql: string | null;
  ddlAfterSql: string | null;
  tableRowsBefore: JsonObject[] | null;
}

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

function normalizeObject(value: unknown): JsonObject | null {
  if (value === null) {
    return null;
  }
  return value as JsonObject;
}

function decodeCommit(db: Database, row: RawCommitRow): CommitEntry {
  const parents = db
    .query("SELECT parent_commit_id FROM _toss_commit_parent WHERE commit_id=? ORDER BY ord ASC")
    .all(row.commit_id) as Array<{ parent_commit_id: string }>;
  const operations = db
    .query("SELECT op_json FROM _toss_op WHERE commit_id=? ORDER BY op_index ASC")
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
  const row = db.query("SELECT commit_id FROM _toss_ref WHERE name=? LIMIT 1").get(MAIN_REF_NAME) as
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
  const row = db.query("SELECT COALESCE(MAX(seq), 0) AS max_seq FROM _toss_commit").get() as { max_seq: number };
  return row.max_seq + 1;
}

function commitHashPayload(input: CommitWriteInput): Record<string, unknown> {
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
  };
}

export function computeCommitId(input: CommitWriteInput): string {
  return sha256Hex(commitHashPayload(input));
}

export function appendCommit(db: Database, input: CommitWriteInput): CommitEntry {
  const commitId = computeCommitId(input);
  const oldHead = getHeadCommitId(db);

  db.query(
    `
    INSERT INTO _toss_commit(
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

  const insertParent = db.query(
    "INSERT INTO _toss_commit_parent(commit_id, parent_commit_id, ord) VALUES(?, ?, ?)",
  );
  for (let i = 0; i < input.parentIds.length; i++) {
    insertParent.run(commitId, input.parentIds[i]!, i);
  }

  const insertOp = db.query("INSERT INTO _toss_op(commit_id, op_index, op_type, op_json) VALUES(?, ?, ?, ?)");
  for (let i = 0; i < input.operations.length; i++) {
    const operation = input.operations[i]!;
    insertOp.run(commitId, i, operation.type, canonicalJson(operation));
  }

  const insertRowEffect = db.query(
    `
    INSERT INTO _toss_effect_row(
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
    insertRowEffect.run(commitId, i, effect.tableName, canonicalJson(effect.pk), effect.opKind, beforeRowJson, afterRowJson, beforeHash, afterHash);
  }

  const insertSchemaEffect = db.query(
    `
    INSERT INTO _toss_effect_schema(
      commit_id, effect_index, table_name, column_name, op_kind, ddl_before_sql, ddl_after_sql, table_rows_before_json
    )
    VALUES(?, ?, ?, ?, ?, ?, ?, ?)
    `,
  );
  for (let i = 0; i < input.schemaEffects.length; i++) {
    const effect = input.schemaEffects[i]!;
    insertSchemaEffect.run(
      commitId,
      i,
      effect.tableName,
      effect.columnName,
      effect.opKind,
      effect.ddlBeforeSql,
      effect.ddlAfterSql,
      effect.tableRowsBefore ? canonicalJson(effect.tableRowsBefore) : null,
    );
  }

  db.query("UPDATE _toss_ref SET commit_id=?, updated_at=? WHERE name=?").run(commitId, input.createdAt, MAIN_REF_NAME);
  db.query(
    `
    INSERT INTO _toss_reflog(ref_name, old_commit_id, new_commit_id, reason, created_at)
    VALUES(?, ?, ?, ?, ?)
    `,
  ).run(MAIN_REF_NAME, oldHead, commitId, input.kind === "revert" ? "revert" : "apply", input.createdAt);

  const row = db.query("SELECT * FROM _toss_commit WHERE commit_id=? LIMIT 1").get(commitId) as RawCommitRow;
  return decodeCommit(db, row);
}

export function getCommitById(db: Database, commitId: string): CommitEntry | null {
  const row = db.query("SELECT * FROM _toss_commit WHERE commit_id=? LIMIT 1").get(commitId) as RawCommitRow | null;
  if (!row) {
    return null;
  }
  return decodeCommit(db, row);
}

export function listCommits(db: Database, descending: boolean): CommitEntry[] {
  const rows = db
    .query(`SELECT * FROM _toss_commit ORDER BY seq ${descending ? "DESC" : "ASC"}`)
    .all() as RawCommitRow[];
  return rows.map((row) => decodeCommit(db, row));
}

export interface StoredRowEffect {
  tableName: string;
  pk: Record<string, string | number | boolean | null>;
  opKind: "insert" | "update" | "delete";
  beforeRow: JsonObject | null;
  afterRow: JsonObject | null;
  beforeHash: string | null;
  afterHash: string | null;
}

export interface StoredSchemaEffect {
  tableName: string;
  columnName: string | null;
  opKind: "create_table" | "add_column" | "drop_table" | "drop_column" | "alter_column_type";
  ddlBeforeSql: string | null;
  ddlAfterSql: string | null;
  tableRowsBefore: JsonObject[] | null;
}

export function getRowEffectsByCommitId(db: Database, commitId: string): StoredRowEffect[] {
  const rows = db
    .query(
      `
      SELECT table_name, pk_json, op_kind, before_row_json, after_row_json, before_hash, after_hash
      FROM _toss_effect_row
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
    pk: JSON.parse(row.pk_json) as Record<string, string | number | boolean | null>,
    opKind: row.op_kind,
    beforeRow: normalizeObject(row.before_row_json ? JSON.parse(row.before_row_json) : null),
    afterRow: normalizeObject(row.after_row_json ? JSON.parse(row.after_row_json) : null),
    beforeHash: row.before_hash,
    afterHash: row.after_hash,
  }));
}

export function getSchemaEffectsByCommitId(db: Database, commitId: string): StoredSchemaEffect[] {
  const rows = db
    .query(
      `
      SELECT table_name, column_name, op_kind, ddl_before_sql, ddl_after_sql, table_rows_before_json
      FROM _toss_effect_schema
      WHERE commit_id=?
      ORDER BY effect_index ASC
      `,
    )
    .all(commitId) as Array<{
    table_name: string;
    column_name: string | null;
    op_kind: "create_table" | "add_column" | "drop_table" | "drop_column" | "alter_column_type";
    ddl_before_sql: string | null;
    ddl_after_sql: string | null;
    table_rows_before_json: string | null;
  }>;

  return rows.map((row) => ({
    tableName: row.table_name,
    columnName: row.column_name,
    opKind: row.op_kind,
    ddlBeforeSql: row.ddl_before_sql,
    ddlAfterSql: row.ddl_after_sql,
    tableRowsBefore: row.table_rows_before_json
      ? (JSON.parse(row.table_rows_before_json) as JsonObject[])
      : null,
  }));
}

