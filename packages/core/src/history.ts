import type { Database } from "./engine/db";
import { and, asc, desc, eq, exists, sql, type SQL } from "drizzle-orm";
import { getCommitById, getRowEffectsByCommitId, getSchemaEffectsByCommitId, listCommits } from "./engine/log";
import {
  CommitParentTable,
  CommitTable,
  OpTable,
  RowEffectTable,
  SchemaEffectTable,
} from "./engine/schema.sql";
import { normalizePage, normalizePageSize } from "./table";
import type { Operation } from "./apply";

export type CommitKind = "apply" | "revert";

export interface Commit {
  commitId: string;
  seq: number;
  kind: CommitKind;
  message: string;
  createdAt: number;
  parentIds: string[];
  parentCount: number;
  schemaHashBefore: string;
  schemaHashAfter: string;
  stateHashAfter: string;
  planHash: string;
  revertible: boolean;
  revertTargetId: string | null;
  operations: Operation[];
}

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
    commitId: string;
    seq: number;
    kind: CommitKind;
    message: string;
    createdAt: number;
  },
): CommitSummary {
  const parents = db
    .select({ parentCommitId: CommitParentTable.parentCommitId })
    .from(CommitParentTable)
    .where(eq(CommitParentTable.commitId, row.commitId))
    .orderBy(asc(CommitParentTable.ord))
    .all();
  const rowTables = db
    .select({ tableName: RowEffectTable.tableName })
    .from(RowEffectTable)
    .where(eq(RowEffectTable.commitId, row.commitId))
    .all()
    .map((entry) => entry.tableName);
  const schemaTables = db
    .select({ tableName: SchemaEffectTable.tableName })
    .from(SchemaEffectTable)
    .where(eq(SchemaEffectTable.commitId, row.commitId))
    .all()
    .map((entry) => entry.tableName);
  const operationCount = db
    .select({ c: sql<number>`count(*)` })
    .from(OpTable)
    .where(eq(OpTable.commitId, row.commitId))
    .get()?.c ?? 0;
  const rowEffectCount = db
    .select({ c: sql<number>`count(*)` })
    .from(RowEffectTable)
    .where(eq(RowEffectTable.commitId, row.commitId))
    .get()?.c ?? 0;
  const schemaEffectCount = db
    .select({ c: sql<number>`count(*)` })
    .from(SchemaEffectTable)
    .where(eq(SchemaEffectTable.commitId, row.commitId))
    .get()?.c ?? 0;

  return {
    commitId: row.commitId,
    seq: row.seq,
    kind: row.kind,
    message: row.message,
    createdAt: row.createdAt,
    parentIds: parents.map(({ parentCommitId }) => parentCommitId),
    operationCount,
    rowEffectCount,
    schemaEffectCount,
    affectedTables: Array.from(new Set([...rowTables, ...schemaTables])).sort((a, b) => a.localeCompare(b)),
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

  const conditions: SQL[] = [];
  if (kind) {
    conditions.push(eq(CommitTable.kind, kind));
  }
  if (table) {
    conditions.push(
      sql`(
        ${exists(
          db.select({ n: sql<number>`1` }).from(RowEffectTable).where(
            and(eq(RowEffectTable.commitId, CommitTable.commitId), sql`${RowEffectTable.tableName} = ${table} COLLATE NOCASE`),
          ),
        )}
        OR
        ${exists(
          db.select({ n: sql<number>`1` }).from(SchemaEffectTable).where(
            and(eq(SchemaEffectTable.commitId, CommitTable.commitId), sql`${SchemaEffectTable.tableName} = ${table} COLLATE NOCASE`),
          ),
        )}
      )`,
    );
  }

  const where = conditions.length > 1 ? and(...conditions) : conditions[0];

  const rows = db
    .select({
      commitId: CommitTable.commitId,
      seq: CommitTable.seq,
      kind: CommitTable.kind,
      message: CommitTable.message,
      createdAt: CommitTable.createdAt,
    })
    .from(CommitTable)
    .where(where)
    .orderBy(desc(CommitTable.seq))
    .limit(pageSize)
    .offset(offset)
    .all();

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
