import type { Database } from "bun:sqlite";
import { COMMIT_PARENT_TABLE, COMMIT_TABLE, OP_TABLE, ROW_EFFECT_TABLE, SCHEMA_EFFECT_TABLE, getRow, getRows } from "./engine/db";
import { getCommitById, getRowEffectsByCommitId, getSchemaEffectsByCommitId, listCommits } from "./engine/log";
import { normalizePage, normalizePageSize } from "./table";
import type { Commit, CommitKind } from "./types";

export interface CommitEffects {
  rows: ReturnType<typeof getRowEffectsByCommitId>;
  schemas: ReturnType<typeof getSchemaEffectsByCommitId>;
}

export interface CommitSummary {
  commitId: string;
  seq: number;
  kind: CommitKind;
  message: string;
  createdAt: number;
  parentIds: string[];
  operationCount: number;
  rowEffectCount: number;
  schemaEffectCount: number;
  affectedTables: string[];
}

export interface HistoryOptions {
  limit?: number | undefined;
  page?: number | undefined;
  kind?: CommitKind | undefined;
  table?: string | undefined;
}

function toCommitSummary(
  db: Database,
  row: {
    commit_id: string;
    seq: number;
    kind: CommitKind;
    message: string;
    created_at: number;
  },
): CommitSummary {
  const parents = getRows<{ parent_commit_id: string }>(
    db,
    `SELECT parent_commit_id
     FROM ${COMMIT_PARENT_TABLE}
     WHERE commit_id=?
     ORDER BY ord ASC`,
    row.commit_id,
  );
  const affectedRows = getRows<{ table_name: string }>(
    db,
    `
      SELECT DISTINCT table_name
      FROM (
        SELECT table_name FROM ${ROW_EFFECT_TABLE} WHERE commit_id = ?
        UNION ALL
        SELECT table_name FROM ${SCHEMA_EFFECT_TABLE} WHERE commit_id = ?
      )
      ORDER BY table_name ASC
    `,
    row.commit_id,
    row.commit_id,
  );
  const counts = getRow<{ operation_count: number; row_effect_count: number; schema_effect_count: number }>(
    db,
    `
      SELECT
        (SELECT COUNT(*) FROM ${OP_TABLE} WHERE commit_id = ?) AS operation_count,
        (SELECT COUNT(*) FROM ${ROW_EFFECT_TABLE} WHERE commit_id = ?) AS row_effect_count,
        (SELECT COUNT(*) FROM ${SCHEMA_EFFECT_TABLE} WHERE commit_id = ?) AS schema_effect_count
    `,
    row.commit_id,
    row.commit_id,
    row.commit_id,
  );

  return {
    commitId: row.commit_id,
    seq: row.seq,
    kind: row.kind,
    message: row.message,
    createdAt: row.created_at,
    parentIds: parents.map(({ parent_commit_id }) => parent_commit_id),
    operationCount: counts?.operation_count ?? 0,
    rowEffectCount: counts?.row_effect_count ?? 0,
    schemaEffectCount: counts?.schema_effect_count ?? 0,
    affectedTables: affectedRows.map(({ table_name }) => table_name),
  };
}

export function history(db: Database): Commit[] {
  return listCommits(db, true);
}

export function commitHistory(db: Database, options: HistoryOptions = {}): CommitSummary[] {
  const pageSize = normalizePageSize(options.limit);
  const page = normalizePage(options.page);
  const offset = (page - 1) * pageSize;
  const kind = options.kind === "apply" || options.kind === "revert" ? options.kind : null;
  const table = options.table?.trim() || null;

  const rows = getRows<{
    commit_id: string;
    seq: number;
    kind: CommitKind;
    message: string;
    created_at: number;
  }>(
    db,
    `
      SELECT c.commit_id, c.seq, c.kind, c.message, c.created_at
      FROM ${COMMIT_TABLE} AS c
      WHERE (? IS NULL OR c.kind = ?)
        AND (
          ? IS NULL
          OR EXISTS (
            SELECT 1
            FROM ${ROW_EFFECT_TABLE} AS r
            WHERE r.commit_id = c.commit_id
              AND r.table_name = ? COLLATE NOCASE
          )
          OR EXISTS (
            SELECT 1
            FROM ${SCHEMA_EFFECT_TABLE} AS s
            WHERE s.commit_id = c.commit_id
              AND s.table_name = ? COLLATE NOCASE
          )
        )
      ORDER BY c.seq DESC
      LIMIT ?
      OFFSET ?
    `,
    kind,
    kind,
    table,
    table,
    table,
    pageSize,
    offset,
  );

  return rows.map((row) => toCommitSummary(db, row));
}

export function commitById(db: Database, commitId: string): Commit | null {
  return getCommitById(db, commitId);
}

export function commitEffects(db: Database, commitId: string): CommitEffects {
  return {
    rows: getRowEffectsByCommitId(db, commitId),
    schemas: getSchemaEffectsByCommitId(db, commitId),
  };
}
