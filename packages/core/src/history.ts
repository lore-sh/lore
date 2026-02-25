import { eq, sql } from "drizzle-orm";
import { LAST_VERIFIED_AT_META_KEY, LAST_VERIFIED_OK_META_KEY, setMetaValue, type Database } from "./db";
import {
  computeCommitId,
  readCommitsAfter,
} from "./commit";
import { normalizePage, normalizePageSize } from "./inspect";
import { type CommitKind, CommitTable, OpTable, RowEffectTable, SchemaEffectTable } from "./schema";

const ARRAY_SEPARATOR = "\u001f";

function splitCsv(value: string): string[] {
  return value.length === 0 ? [] : value.split(ARRAY_SEPARATOR);
}

export function history(
  db: Database,
  options: {
    limit?: number | undefined;
    page?: number | undefined;
    kind?: CommitKind | undefined;
    table?: string | undefined;
  } = {},
) {
  const pageSize = normalizePageSize(options.limit);
  const page = normalizePage(options.page);
  const offset = (page - 1) * pageSize;
  const kind = options.kind === "apply" || options.kind === "revert" ? options.kind : null;
  const table = options.table?.trim() || null;

  const conditions: string[] = [];
  const args: Array<string | number> = [];
  if (kind) {
    conditions.push("c.kind = ?");
    args.push(kind);
  }
  if (table) {
    conditions.push(`(
      EXISTS (
        SELECT 1
        FROM _lore_row_effect re
        WHERE re.commit_id = c.commit_id AND re.table_name = ? COLLATE NOCASE
      )
      OR EXISTS (
        SELECT 1
        FROM _lore_schema_effect se
        WHERE se.commit_id = c.commit_id AND se.table_name = ? COLLATE NOCASE
      )
    )`);
    args.push(table, table);
  }
  const whereSql = conditions.length === 0 ? "" : `WHERE ${conditions.join(" AND ")}`;
  const rows = db.$client
    .query<
      {
        commit_id: string;
        seq: number;
        kind: CommitKind;
        message: string;
        created_at: number;
        parent_ids: string;
        operation_count: number;
        row_effect_count: number;
        schema_effect_count: number;
        affected_tables: string;
      },
      Array<string | number>
    >(
      `
      SELECT
        c.commit_id,
        c.seq,
        c.kind,
        c.message,
        c.created_at,
        COALESCE((
          SELECT group_concat(parent_commit_id, '${ARRAY_SEPARATOR}')
          FROM (
            SELECT cp.parent_commit_id
            FROM _lore_commit_parent cp
            WHERE cp.commit_id = c.commit_id
            ORDER BY cp.ord
          )
        ), '') AS parent_ids,
        (
          SELECT COUNT(*)
          FROM _lore_op o
          WHERE o.commit_id = c.commit_id
        ) AS operation_count,
        (
          SELECT COUNT(*)
          FROM _lore_row_effect re
          WHERE re.commit_id = c.commit_id
        ) AS row_effect_count,
        (
          SELECT COUNT(*)
          FROM _lore_schema_effect se
          WHERE se.commit_id = c.commit_id
        ) AS schema_effect_count,
        COALESCE((
          SELECT group_concat(table_name, '${ARRAY_SEPARATOR}')
          FROM (
            SELECT table_name
            FROM (
              SELECT re.table_name AS table_name
              FROM _lore_row_effect re
              WHERE re.commit_id = c.commit_id
              UNION
              SELECT se.table_name AS table_name
              FROM _lore_schema_effect se
              WHERE se.commit_id = c.commit_id
            )
            ORDER BY table_name COLLATE NOCASE
          )
        ), '') AS affected_tables
      FROM _lore_commit c
      ${whereSql}
      ORDER BY c.seq DESC
      LIMIT ? OFFSET ?
      `,
    )
    .all(...args, pageSize, offset);

  return rows.map((row) => ({
    commitId: row.commit_id,
    seq: row.seq,
    kind: row.kind,
    message: row.message,
    createdAt: row.created_at,
    parentIds: splitCsv(row.parent_ids),
    operationCount: row.operation_count,
    rowEffectCount: row.row_effect_count,
    schemaEffectCount: row.schema_effect_count,
    affectedTables: splitCsv(row.affected_tables),
  }));
}

export function commitSize(db: Database, commitId: string): number {
  const opBytes = db
    .select({ n: sql<number>`coalesce(sum(length(${OpTable.opJson})), 0)` })
    .from(OpTable)
    .where(eq(OpTable.commitId, commitId))
    .get()?.n ?? 0;
  const rowEffectBytes = db
    .select({
      n: sql<number>`coalesce(sum(length(${RowEffectTable.pkJson}) + coalesce(length(${RowEffectTable.beforeJson}), 0) + coalesce(length(${RowEffectTable.afterJson}), 0)), 0)`,
    })
    .from(RowEffectTable)
    .where(eq(RowEffectTable.commitId, commitId))
    .get()?.n ?? 0;
  const schemaEffectBytes = db
    .select({
      n: sql<number>`coalesce(sum(coalesce(length(${SchemaEffectTable.beforeJson}), 0) + coalesce(length(${SchemaEffectTable.afterJson}), 0)), 0)`,
    })
    .from(SchemaEffectTable)
    .where(eq(SchemaEffectTable.commitId, commitId))
    .get()?.n ?? 0;
  const commitMessageBytes = db
    .select({ n: sql<number>`coalesce(length(${CommitTable.message}), 0)` })
    .from(CommitTable)
    .where(eq(CommitTable.commitId, commitId))
    .limit(1)
    .get()?.n ?? 0;
  return opBytes + rowEffectBytes + schemaEffectBytes + commitMessageBytes;
}

export function historySize(db: Database): number {
  const opBytes = db
    .select({ n: sql<number>`coalesce(sum(length(${OpTable.opJson})), 0)` })
    .from(OpTable)
    .get()?.n ?? 0;
  const rowEffectBytes = db
    .select({
      n: sql<number>`coalesce(sum(length(${RowEffectTable.pkJson}) + coalesce(length(${RowEffectTable.beforeJson}), 0) + coalesce(length(${RowEffectTable.afterJson}), 0)), 0)`,
    })
    .from(RowEffectTable)
    .get()?.n ?? 0;
  const schemaEffectBytes = db
    .select({
      n: sql<number>`coalesce(sum(coalesce(length(${SchemaEffectTable.beforeJson}), 0) + coalesce(length(${SchemaEffectTable.afterJson}), 0)), 0)`,
    })
    .from(SchemaEffectTable)
    .get()?.n ?? 0;
  const commitMessageBytes = db
    .select({ n: sql<number>`coalesce(sum(length(${CommitTable.message})), 0)` })
    .from(CommitTable)
    .get()?.n ?? 0;
  return opBytes + rowEffectBytes + schemaEffectBytes + commitMessageBytes;
}

export function verify(db: Database, options: { full?: boolean } = {}) {
  const mode = options.full ? "full" : "quick";
  const issues: string[] = [];

  const replays = readCommitsAfter(db, 0);
  for (const replay of replays) {
    const commit = replay.commit;
    const expected = computeCommitId({
      seq: replay.commit.seq,
      kind: replay.commit.kind,
      message: replay.commit.message,
      createdAt: replay.commit.createdAt,
      schemaHashBefore: replay.commit.schemaHashBefore,
      schemaHashAfter: replay.commit.schemaHashAfter,
      stateHashAfter: replay.commit.stateHashAfter,
      planHash: replay.commit.planHash,
      revertible: replay.commit.revertible,
      revertTargetId: replay.commit.revertTargetId,
      parentIds: replay.parentIds,
      operations: replay.operations,
      rowEffects: replay.rowEffects,
      schemaEffects: replay.schemaEffects,
    });
    if (expected !== commit.commitId) {
      issues.push(`Commit hash mismatch: ${commit.commitId}`);
    }
    if (commit.parentCount !== replay.parentIds.length) {
      issues.push(`Parent count mismatch: ${commit.commitId}`);
    }
  }

  const quickCheckRow = db.$client.query<{ quick_check: string }, []>("PRAGMA quick_check").get();
  const quickCheck = quickCheckRow?.quick_check ?? "unknown";
  if (quickCheck.toLowerCase() !== "ok") {
    issues.push(`quick_check failed: ${quickCheck}`);
  }

  let integrityCheck: string | undefined;
  if (options.full) {
    integrityCheck = db.$client.query<{ integrity_check: string }, []>("PRAGMA integrity_check").get()?.integrity_check ?? "unknown";
    if (integrityCheck.toLowerCase() !== "ok") {
      issues.push(`integrity_check failed: ${integrityCheck}`);
    }
  }

  const checkedAt = new Date().toISOString();
  setMetaValue(db, LAST_VERIFIED_AT_META_KEY, checkedAt);
  const ok = issues.length === 0;
  setMetaValue(db, LAST_VERIFIED_OK_META_KEY, ok ? "1" : "0");

  return {
    ok,
    mode,
    chainValid: !issues.some((issue) => issue.startsWith("Commit hash mismatch") || issue.startsWith("Parent count mismatch")),
    quickCheck,
    integrityCheck,
    issues,
    checkedAt,
  };
}
