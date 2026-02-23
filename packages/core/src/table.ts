import type { Database } from "bun:sqlite";
import { COMMIT_TABLE, ROW_EFFECT_TABLE, SCHEMA_EFFECT_TABLE, getRow, getRows, listUserTables } from "./engine/db";
import { CodedError } from "./error";
import { describeSchema, type SchemaDescriptor, type SchemaTableDescriptor } from "./engine/inspect";
import { normalizeRowObject } from "./engine/rows";
import { asciiCaseFold, quoteIdentifier } from "./engine/sql";
import type { JsonObject } from "./types";

export interface Table {
  name: string;
  rowCount: number;
  columnCount: number;
  lastUpdatedAt: number | null;
}

export interface TableColumn {
  name: string;
  type: string;
  notNull: boolean;
  primaryKey: boolean;
  unique: boolean;
  defaultValue: string | null;
}

export interface TablePage {
  table: string;
  page: number;
  pageSize: number;
  totalRows: number;
  totalPages: number;
  sortBy: string | null;
  sortDir: "asc" | "desc";
  filters: Record<string, string>;
  columns: TableColumn[];
  rows: JsonObject[];
}

export interface TableQueryOptions {
  table: string;
  page?: number | undefined;
  pageSize?: number | undefined;
  sortBy?: string | undefined;
  sortDir?: "asc" | "desc" | undefined;
  filters?: Record<string, string> | undefined;
}

interface OrderTerm {
  key: string;
  sql: string;
}

const MAX_PAGE_SIZE = 500;
const DEFAULT_PAGE_SIZE = 50;

function countTableRows(db: Database, tableName: string): number {
  return getRow<{ c: number }>(db, `SELECT COUNT(*) AS c FROM ${quoteIdentifier(tableName, { unsafe: true })}`)?.c ?? 0;
}

function isVisibleColumn(hidden: number): boolean {
  return hidden === 0 || hidden === 2 || hidden === 3;
}

function visibleColumnNames(table: SchemaTableDescriptor): string[] {
  return table.columns.filter((column) => isVisibleColumn(column.hidden)).map((column) => column.name);
}

export function resolveTableName(db: Database, requestedTable: string): string {
  const table = requestedTable.trim();
  if (table.length === 0) {
    throw new CodedError("INVALID_OPERATION", "Table name is required");
  }

  const tableNames = listUserTables(db);
  const exact = tableNames.find((name) => name === table);
  if (exact) {
    return exact;
  }

  const folded = asciiCaseFold(table);
  const matched = tableNames.find((name) => asciiCaseFold(name) === folded);
  if (matched) {
    return matched;
  }

  throw new CodedError("NOT_FOUND", `Table not found: ${requestedTable}`);
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
    names.add(primaryKeyColumns[0]!.name);
  }
  for (const index of table.indexes) {
    if (!index.unique || index.partial) {
      continue;
    }
    const keyTerms = index.columns.filter((column) => column.key === 1);
    if (keyTerms.length === 1 && keyTerms[0]!.name) {
      names.add(keyTerms[0]!.name);
    }
  }
  return names;
}

function mapColumns(table: SchemaTableDescriptor): TableColumn[] {
  const unique = uniqueColumnNames(table);
  return table.columns
    .filter((column) => isVisibleColumn(column.hidden))
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
  throw new CodedError("NOT_FOUND", `Table not found: ${tableName}`);
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
  const visibleColumns = visibleColumnNames(table);

  if (primaryKeyColumns.length > 0) {
    for (const column of primaryKeyColumns) {
      terms.push({
        key: column,
        sql: `${quoteIdentifier(column, { unsafe: true })} ASC`,
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
      throw new CodedError("INTERNAL", `Table has no visible columns for ordering: ${table.table}`);
    }
    for (const column of visibleColumns) {
      terms.push({
        key: column,
        sql: `${quoteIdentifier(column, { unsafe: true })} ASC`,
      });
    }
  }

  return terms;
}

function orderClause(table: SchemaTableDescriptor, sortBy: string | undefined, sortDir: "asc" | "desc"): string {
  const tieBreakers = tieBreakerTerms(table);
  if (!sortBy) {
    return ` ORDER BY ${tieBreakers.map((term) => term.sql).join(", ")}`;
  }
  const terms = [`${quoteIdentifier(sortBy, { unsafe: true })} ${sortDir === "desc" ? "DESC" : "ASC"}`];
  for (const tieBreaker of tieBreakers) {
    if (tieBreaker.key === sortBy) {
      continue;
    }
    terms.push(tieBreaker.sql);
  }
  return ` ORDER BY ${terms.join(", ")}`;
}

export function tableOverview(db: Database): Table[] {
  const tableNames = listUserTables(db);
  return tableNames.map((name) => {
    const column = getRow<{ c: number }>(
      db,
      `SELECT COUNT(*) AS c FROM pragma_table_xinfo(${quoteIdentifier(name, { unsafe: true })}) WHERE hidden IN (0, 2, 3)`,
    );
    const updated = getRow<{ created_at: number }>(
      db,
      `
        SELECT c.created_at
        FROM ${COMMIT_TABLE} AS c
        JOIN (
          SELECT commit_id FROM ${ROW_EFFECT_TABLE} WHERE table_name = ?
          UNION
          SELECT commit_id FROM ${SCHEMA_EFFECT_TABLE} WHERE table_name = ?
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
      rowCount: countTableRows(db, name),
      columnCount: column?.c ?? 0,
      lastUpdatedAt: updated?.created_at ?? null,
    };
  });
}

export function queryTable(db: Database, options: TableQueryOptions): TablePage {
  const tableName = resolveTableName(db, options.table);
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
      throw new CodedError("INVALID_OPERATION", `Filter column not found: ${column}`);
    }
    const value = rawValue.trim();
    if (value.length === 0) {
      continue;
    }
    filters[column] = value;
    whereParts.push(`CAST(${quoteIdentifier(column, { unsafe: true })} AS TEXT) LIKE ? ESCAPE '\\'`);
    bindings.push(`%${escapeLikePattern(value)}%`);
  }

  const sortBy = options.sortBy?.trim();
  if (sortBy && !columnNames.has(sortBy)) {
    throw new CodedError("INVALID_OPERATION", `Sort column not found: ${sortBy}`);
  }

  const whereSql = whereParts.length === 0 ? "" : ` WHERE ${whereParts.join(" AND ")}`;
  const totalRow = getRow<{ c: number }>(
    db,
    `SELECT COUNT(*) AS c FROM ${quoteIdentifier(tableName, { unsafe: true })}${whereSql}`,
    ...bindings,
  );
  const totalRows = totalRow?.c ?? 0;
  const totalPages = totalRows === 0 ? 1 : Math.ceil(totalRows / pageSize);
  const page = Math.min(requestedPage, totalPages);
  const offset = (page - 1) * pageSize;

  const orderSql = orderClause(table, sortBy, sortDir);

  const rows = getRows<Record<string, unknown>>(
    db,
    `SELECT * FROM ${quoteIdentifier(tableName, { unsafe: true })}${whereSql}${orderSql} LIMIT ? OFFSET ?`,
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
    rows: rows.map((row) => normalizeRowObject(row)),
  };
}
