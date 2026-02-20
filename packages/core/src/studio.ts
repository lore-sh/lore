import {
  COMMIT_PARENT_TABLE,
  COMMIT_TABLE,
  EFFECT_ROW_TABLE,
  EFFECT_SCHEMA_TABLE,
  getRow,
  getRows,
  listUserTables,
  withInitializedDatabase,
} from "./db";
import { TossError } from "./errors";
import { getCommitById, getRowEffectsByCommitId, getSchemaEffectsByCommitId } from "./log";
import { describeSchema, normalizeRowObject, type SchemaDescriptor, type SchemaTableDescriptor } from "./rows";
import { asciiCaseFold, quoteName } from "./sql";
import type { CommitEntry, CommitKind, JsonObject } from "./types";

export type StudioSortDirection = "asc" | "desc";

export interface StudioTableSummary {
  name: string;
  rowCount: number;
  columnCount: number;
  lastUpdatedAt: number | null;
}

export interface StudioTablesView {
  dbPath: string;
  generatedAt: string;
  tables: StudioTableSummary[];
}

export interface StudioTableColumn {
  name: string;
  type: string;
  notNull: boolean;
  primaryKey: boolean;
  unique: boolean;
  defaultValue: string | null;
}

export interface StudioTableDataView {
  table: string;
  page: number;
  pageSize: number;
  totalRows: number;
  totalPages: number;
  sortBy: string | null;
  sortDir: StudioSortDirection;
  filters: Record<string, string>;
  columns: StudioTableColumn[];
  rows: StudioRow[];
}

export type StudioCellValue = string | number | boolean | null;
export type StudioRow = Record<string, StudioCellValue>;

export interface ReadStudioTableOptions {
  table: string;
  page?: number | undefined;
  pageSize?: number | undefined;
  sortBy?: string | undefined;
  sortDir?: StudioSortDirection | undefined;
  filters?: Record<string, string> | undefined;
}

export interface StudioSchemaColumn {
  name: string;
  type: string;
  notNull: boolean;
  primaryKey: boolean;
  unique: boolean;
  defaultValue: string | null;
  hidden: boolean;
}

export interface StudioSchemaTable {
  name: string;
  rowCount: number;
  columns: StudioSchemaColumn[];
}

export interface StudioSchemaView {
  dbPath: string;
  generatedAt: string;
  tables: StudioSchemaTable[];
}

export interface StudioHistoryEntry {
  commitId: string;
  shortId: string;
  seq: number;
  kind: CommitKind;
  message: string;
  createdAt: number;
  parentIds: string[];
}

export interface StudioCommitDetail {
  commit: CommitEntry;
  rowEffects: ReturnType<typeof getRowEffectsByCommitId>;
  schemaEffects: ReturnType<typeof getSchemaEffectsByCommitId>;
}

interface OrderTerm {
  key: string;
  sql: string;
}

const MAX_PAGE_SIZE = 500;
const DEFAULT_PAGE_SIZE = 50;

function isVisibleStudioColumn(hidden: number): boolean {
  return hidden === 0 || hidden === 2 || hidden === 3;
}

function visibleStudioColumnNames(table: SchemaTableDescriptor): string[] {
  return table.columns.filter((column) => isVisibleStudioColumn(column.hidden)).map((column) => column.name);
}

function resolveTableName(tableNames: string[], requestedTable: string): string {
  const exact = tableNames.find((name) => name === requestedTable);
  if (exact) {
    return exact;
  }
  const folded = asciiCaseFold(requestedTable);
  const matched = tableNames.find((name) => asciiCaseFold(name) === folded);
  if (matched) {
    return matched;
  }
  throw new TossError("NOT_FOUND", `Table not found: ${requestedTable}`);
}

function normalizePageSize(input: number | undefined): number {
  if (typeof input !== "number" || !Number.isFinite(input) || input < 1) {
    return DEFAULT_PAGE_SIZE;
  }
  const truncated = Math.floor(input);
  return Math.min(MAX_PAGE_SIZE, truncated);
}

function normalizePage(input: number | undefined): number {
  if (typeof input !== "number" || !Number.isFinite(input) || input < 1) {
    return 1;
  }
  return Math.floor(input);
}

function escapeLikePattern(value: string): string {
  return value.replaceAll("\\", "\\\\").replaceAll("%", "\\%").replaceAll("_", "\\_");
}

function uniqueColumnNames(table: SchemaTableDescriptor): Set<string> {
  const names = new Set<string>();
  const primaryKeyColumns = table.columns.filter((column) => column.pk > 0);
  if (primaryKeyColumns.length === 1) {
    const column = primaryKeyColumns[0];
    if (column) {
      names.add(column.name);
    }
  }
  for (const index of table.indexes) {
    if (!index.unique || index.partial) {
      continue;
    }
    const keyTerms = index.columns.filter((column) => column.key === 1);
    if (keyTerms.length === 1) {
      const name = keyTerms[0]?.name;
      if (name) {
        names.add(name);
      }
    }
  }
  return names;
}

function mapColumns(table: SchemaTableDescriptor): StudioTableColumn[] {
  const unique = uniqueColumnNames(table);
  return table.columns
    .filter((column) => isVisibleStudioColumn(column.hidden))
    .map((column) => ({
      name: column.name,
      type: column.type,
      notNull: column.notnull === 1,
      primaryKey: column.pk > 0,
      unique: unique.has(column.name),
      defaultValue: column.dfltValue,
    }));
}

function findTableFromSchema(descriptor: SchemaDescriptor, tableName: string): SchemaTableDescriptor {
  const matched = descriptor.tables.find((table) => table.table === tableName);
  if (matched) {
    return matched;
  }
  throw new TossError("NOT_FOUND", `Table not found: ${tableName}`);
}

function primaryKeyColumnNames(table: SchemaTableDescriptor): string[] {
  return table.columns
    .filter((column) => column.pk > 0)
    .sort((left, right) => left.pk - right.pk)
    .map((column) => column.name);
}

function tieBreakerTerms(table: SchemaTableDescriptor): OrderTerm[] {
  const terms: OrderTerm[] = [];
  const primaryKeyColumns = primaryKeyColumnNames(table);
  const visibleColumns = visibleStudioColumnNames(table);

  if (primaryKeyColumns.length > 0) {
    for (const column of primaryKeyColumns) {
      terms.push({
        key: column,
        sql: `${quoteName(column)} ASC`,
      });
    }
  }

  if (!table.options.withoutRowid) {
    const foldedNames = new Set(table.columns.map((column) => asciiCaseFold(column.name)));
    const pseudoRowIdName = ["rowid", "_rowid_", "oid"].find((candidate) => !foldedNames.has(asciiCaseFold(candidate)));
    if (pseudoRowIdName) {
      terms.push({
        key: "__rowid__",
        sql: `${pseudoRowIdName} ASC`,
      });
    }
  }

  if (terms.length === 0) {
    if (visibleColumns.length === 0) {
      throw new TossError("INTERNAL", `Table has no visible columns for ordering: ${table.table}`);
    }
    for (const column of visibleColumns) {
      terms.push({
        key: column,
        sql: `${quoteName(column)} ASC`,
      });
    }
  }

  return terms;
}

function orderClause(table: SchemaTableDescriptor, sortBy: string | undefined, sortDir: StudioSortDirection): string {
  const tieBreakers = tieBreakerTerms(table);
  if (!sortBy) {
    return ` ORDER BY ${tieBreakers.map((term) => term.sql).join(", ")}`;
  }
  const terms = [`${quoteName(sortBy)} ${sortDir === "desc" ? "DESC" : "ASC"}`];
  for (const tieBreaker of tieBreakers) {
    if (tieBreaker.key === sortBy) {
      continue;
    }
    terms.push(tieBreaker.sql);
  }
  return ` ORDER BY ${terms.join(", ")}`;
}

function toStudioRow(row: JsonObject): StudioRow {
  const output: StudioRow = {};
  for (const [key, value] of Object.entries(row)) {
    if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      output[key] = value;
      continue;
    }
    output[key] = JSON.stringify(value);
  }
  return output;
}

export function listStudioTables(): StudioTablesView {
  return withInitializedDatabase(({ db, dbPath }) => {
    const tableNames = listUserTables(db);
    const tables = tableNames.map((name) => {
      const row = getRow<{ c: number }>(db, `SELECT COUNT(*) AS c FROM ${quoteName(name)}`);
      const column = getRow<{ c: number }>(
        db,
        `SELECT COUNT(*) AS c FROM pragma_table_xinfo(${quoteName(name)}) WHERE hidden IN (0, 2, 3)`,
      );
      const updated = getRow<{ created_at: number }>(
        db,
        `
          SELECT c.created_at
          FROM ${COMMIT_TABLE} AS c
          JOIN (
            SELECT commit_id FROM ${EFFECT_ROW_TABLE} WHERE table_name = ?
            UNION
            SELECT commit_id FROM ${EFFECT_SCHEMA_TABLE} WHERE table_name = ?
          ) AS touched
            ON touched.commit_id = c.commit_id
          ORDER BY c.seq DESC
          LIMIT 1
        `,
        name,
        name,
      );
      return {
        name,
        rowCount: row?.c ?? 0,
        columnCount: column?.c ?? 0,
        lastUpdatedAt: updated?.created_at ?? null,
      };
    });
    return {
      dbPath,
      generatedAt: new Date().toISOString(),
      tables,
    };
  });
}

export function readStudioTable(options: ReadStudioTableOptions): StudioTableDataView {
  return withInitializedDatabase(({ db }) => {
    const tableName = resolveTableName(listUserTables(db), options.table);
    const schema = describeSchema(db);
    const table = findTableFromSchema(schema, tableName);
    const columns = mapColumns(table);
    const columnNames = new Set(columns.map((column) => column.name));
    const pageSize = normalizePageSize(options.pageSize);
    const requestedPage = normalizePage(options.page);
    const sortDir = options.sortDir === "desc" ? "desc" : "asc";

    const filters: Record<string, string> = {};
    const whereParts: string[] = [];
    const bindings: string[] = [];
    const rawFilters = options.filters ?? {};
    for (const [column, rawValue] of Object.entries(rawFilters)) {
      if (!columnNames.has(column)) {
        throw new TossError("INVALID_OPERATION", `Filter column not found: ${column}`);
      }
      const value = rawValue.trim();
      if (value.length === 0) {
        continue;
      }
      filters[column] = value;
      whereParts.push(`CAST(${quoteName(column)} AS TEXT) LIKE ? ESCAPE '\\'`);
      bindings.push(`%${escapeLikePattern(value)}%`);
    }

    const sortBy = options.sortBy?.trim();
    if (sortBy && !columnNames.has(sortBy)) {
      throw new TossError("INVALID_OPERATION", `Sort column not found: ${sortBy}`);
    }

    const whereSql = whereParts.length === 0 ? "" : ` WHERE ${whereParts.join(" AND ")}`;
    const totalRow = getRow<{ c: number }>(db, `SELECT COUNT(*) AS c FROM ${quoteName(tableName)}${whereSql}`, ...bindings);
    const totalRows = totalRow?.c ?? 0;
    const totalPages = totalRows === 0 ? 1 : Math.ceil(totalRows / pageSize);
    const page = Math.min(requestedPage, totalPages);
    const offset = (page - 1) * pageSize;

    const orderSql = orderClause(table, sortBy, sortDir);

    const rows = getRows<Record<string, unknown>>(
      db,
      `SELECT * FROM ${quoteName(tableName)}${whereSql}${orderSql} LIMIT ? OFFSET ?`,
      ...bindings,
      pageSize,
      offset,
    );

    return {
      table: tableName,
      page,
      pageSize,
      totalRows,
      totalPages,
      sortBy: sortBy ?? null,
      sortDir,
      filters,
      columns,
      rows: rows.map((row) => toStudioRow(normalizeRowObject(row))),
    };
  });
}

function mapSchemaTable(table: SchemaTableDescriptor): StudioSchemaTable {
  const unique = uniqueColumnNames(table);
  return {
    name: table.table,
    rowCount: 0,
    columns: table.columns.map((column) => ({
      name: column.name,
      type: column.type,
      notNull: column.notnull === 1,
      primaryKey: column.pk > 0,
      unique: unique.has(column.name),
      defaultValue: column.dfltValue,
      hidden: !isVisibleStudioColumn(column.hidden),
    })),
  };
}

export function getStudioSchema(): StudioSchemaView {
  return withInitializedDatabase(({ db, dbPath }) => {
    const descriptor = describeSchema(db);
    const tables = descriptor.tables.map((table) => {
      const row = getRow<{ c: number }>(db, `SELECT COUNT(*) AS c FROM ${quoteName(table.table)}`);
      const mapped = mapSchemaTable(table);
      return { ...mapped, rowCount: row?.c ?? 0 };
    });
    return {
      dbPath,
      generatedAt: new Date().toISOString(),
      tables,
    };
  });
}

export function getStudioTableSchema(table: string): StudioSchemaTable {
  return withInitializedDatabase(({ db }) => {
    const tableName = resolveTableName(listUserTables(db), table);
    const descriptor = describeSchema(db);
    const target = findTableFromSchema(descriptor, tableName);
    const row = getRow<{ c: number }>(db, `SELECT COUNT(*) AS c FROM ${quoteName(tableName)}`);
    return { ...mapSchemaTable(target), rowCount: row?.c ?? 0 };
  });
}

export function listStudioHistory(
  options: {
    limit?: number | undefined;
  } = {},
): StudioHistoryEntry[] {
  return withInitializedDatabase(({ db }) => {
    const max = normalizePageSize(options.limit);
    const rows = getRows<{
      commit_id: string;
      seq: number;
      kind: CommitKind;
      message: string;
      created_at: number;
    }>(
      db,
      `SELECT commit_id, seq, kind, message, created_at
       FROM ${COMMIT_TABLE}
       ORDER BY seq DESC
       LIMIT ?`,
      max,
    );

    return rows.map((row) => {
      const parents = getRows<{ parent_commit_id: string }>(
        db,
        `SELECT parent_commit_id
         FROM ${COMMIT_PARENT_TABLE}
         WHERE commit_id=?
         ORDER BY ord ASC`,
        row.commit_id,
      );

      return {
        commitId: row.commit_id,
        shortId: row.commit_id.slice(0, 12),
        seq: row.seq,
        kind: row.kind,
        message: row.message,
        createdAt: row.created_at,
        parentIds: parents.map((parent) => parent.parent_commit_id),
      };
    });
  });
}

export function getStudioCommitDetail(commitId: string): StudioCommitDetail {
  return withInitializedDatabase(({ db }) => {
    const commit = getCommitById(db, commitId);
    if (!commit) {
      throw new TossError("NOT_FOUND", `Commit not found: ${commitId}`);
    }
    return {
      commit,
      rowEffects: getRowEffectsByCommitId(db, commitId),
      schemaEffects: getSchemaEffectsByCommitId(db, commitId),
    };
  });
}
