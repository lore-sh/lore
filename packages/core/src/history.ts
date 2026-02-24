import { and, asc, desc, eq, exists, sql, type SQL } from "drizzle-orm";
import { LAST_VERIFIED_AT_META_KEY, LAST_VERIFIED_OK_META_KEY, setMetaValue, type Database } from "./db";
import {
  computeCommitId,
  getCommitOperations,
  getCommitParentIds,
  getRowEffectsByCommitId,
  getSchemaEffectsByCommitId,
  listCommits,
} from "./commit";
import { normalizePage, normalizePageSize } from "./inspect";
import { type CommitKind, CommitParentTable, CommitTable, OpTable, RowEffectTable, SchemaEffectTable } from "./schema";

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

export interface VerifyResult {
  ok: boolean;
  mode: "quick" | "full";
  chainValid: boolean;
  quickCheck: string;
  integrityCheck?: string | undefined;
  issues: string[];
  checkedAt: string;
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
  const parentIds = db
    .select({ parentCommitId: CommitParentTable.parentCommitId })
    .from(CommitParentTable)
    .where(eq(CommitParentTable.commitId, row.commitId))
    .orderBy(asc(CommitParentTable.ord))
    .all()
    .map((entry) => entry.parentCommitId);
  const rowEffects = db
    .select({ tableName: RowEffectTable.tableName })
    .from(RowEffectTable)
    .where(eq(RowEffectTable.commitId, row.commitId))
    .all();
  const schemaEffects = db
    .select({ tableName: SchemaEffectTable.tableName })
    .from(SchemaEffectTable)
    .where(eq(SchemaEffectTable.commitId, row.commitId))
    .all();
  const operationCount = db
    .select({ c: sql<number>`count(*)` })
    .from(OpTable)
    .where(eq(OpTable.commitId, row.commitId))
    .get()?.c ?? 0;

  const tableSet = new Set<string>();
  for (const entry of rowEffects) tableSet.add(entry.tableName);
  for (const entry of schemaEffects) tableSet.add(entry.tableName);

  return {
    ...row,
    parentIds,
    operationCount,
    rowEffectCount: rowEffects.length,
    schemaEffectCount: schemaEffects.length,
    affectedTables: Array.from(tableSet).sort((a, b) => a.localeCompare(b)),
  };
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

export function estimateCommitSizeBytes(db: Database, commitId: string): number {
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

export function estimateHistorySizeBytes(db: Database): number {
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

export function verify(db: Database, options: { full?: boolean } = {}): VerifyResult {
  const mode = options.full ? "full" : "quick";
  const issues: string[] = [];

  const commits = listCommits(db, false);
  for (const commit of commits) {
    const parentIds = getCommitParentIds(db, commit.commitId);
    const expected = computeCommitId({
      seq: commit.seq,
      kind: commit.kind,
      message: commit.message,
      createdAt: commit.createdAt,
      parentIds,
      schemaHashBefore: commit.schemaHashBefore,
      schemaHashAfter: commit.schemaHashAfter,
      stateHashAfter: commit.stateHashAfter,
      planHash: commit.planHash,
      revertible: commit.revertible,
      revertTargetId: commit.revertTargetId,
      operations: getCommitOperations(db, commit.commitId),
      rowEffects: getRowEffectsByCommitId(db, commit.commitId),
      schemaEffects: getSchemaEffectsByCommitId(db, commit.commitId),
    });
    if (expected !== commit.commitId) {
      issues.push(`Commit hash mismatch: ${commit.commitId}`);
    }
    if (commit.parentCount !== parentIds.length) {
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
