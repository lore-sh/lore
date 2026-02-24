import { COMMIT_TABLE, ROW_EFFECT_TABLE, SCHEMA_EFFECT_TABLE, listUserTables, type Database } from "./db";
import { CodedError } from "./error";
import {
  countRows,
  describeSchema,
  isVisible,
  normalizePage,
  normalizePageSize,
  normalizeRow,
  hashSchema,
} from "./inspect";
import { asciiCaseFold, quoteIdentifier } from "./sql";

export function resolveTable(db: Database, requestedTable: string): string {
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

export function uniqueColumnNames(table: ReturnType<typeof describeSchema>["tables"][number]): Set<string> {
  const names = new Set<string>();
  const primaryKeyColumns = table.columns.filter((column) => column.primaryKey);
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

function orderClause(
  table: ReturnType<typeof describeSchema>["tables"][number],
  sortBy: string | undefined,
  sortDir: "asc" | "desc",
): string {
  const terms: Array<{ key: string; sql: string }> = [];
  const primaryKeyColumns = table.columns
    .filter((column) => column.primaryKey)
    .sort((left, right) => left.cid - right.cid)
    .map((column) => column.name);
  const visibleColumns = table.columns.filter((c) => isVisible(c.hidden)).map((c) => c.name);

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

  if (!sortBy) {
    return ` ORDER BY ${terms.map((term) => term.sql).join(", ")}`;
  }
  const orderedTerms = [`${quoteIdentifier(sortBy, { unsafe: true })} ${sortDir === "desc" ? "DESC" : "ASC"}`];
  for (const term of terms) {
    if (term.key !== sortBy) {
      orderedTerms.push(term.sql);
    }
  }
  return ` ORDER BY ${orderedTerms.join(", ")}`;
}

export function describeDb(
  db: Database,
  options: {
    table?: string | undefined;
  } = {},
) {
  const descriptor = describeSchema(db);
  const selectedTable = options.table ? resolveTable(db, options.table) : null;
  const tables = descriptor.tables
    .filter((table) => selectedTable === null || table.table === selectedTable)
    .map((table) => {
      const unique = uniqueColumnNames(table);
      return {
        tableSql: table.tableSql,
        name: table.table,
        options: table.options,
        columns: table.columns.map((column) => ({
          definitionSql: column.definitionSql,
          cid: column.cid,
          name: column.name,
          type: column.type,
          notNull: column.notNull,
          defaultValue: column.defaultValue,
          primaryKey: column.primaryKey,
          unique: unique.has(column.name),
          hidden: !isVisible(column.hidden),
        })),
        foreignKeys: table.foreignKeys,
        indexes: table.indexes,
        checks: table.checks,
        triggers: table.triggers,
        rowCount: countRows(db, table.table),
      };
    });

  return {
    dbPath: db.$client.filename,
    generatedAt: new Date().toISOString(),
    schemaHash: hashSchema(descriptor),
    tables,
  };
}

export function tableOverview(db: Database) {
  const tableNames = listUserTables(db);
  return tableNames.map((name) => {
    const column = db.$client
      .query<{ c: number }, []>(
        `SELECT COUNT(*) AS c FROM pragma_table_xinfo(${quoteIdentifier(name, { unsafe: true })}) WHERE hidden IN (0, 2, 3)`,
      )
      .get();
    const updated = db.$client
      .query<{ created_at: number }, [string, string]>(`
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
      `)
      .get(name, name);
    return {
      name,
      rowCount: countRows(db, name),
      columnCount: column?.c ?? 0,
      lastUpdatedAt: updated?.created_at ?? null,
    };
  });
}

export function queryTable(
  db: Database,
  options: {
    table: string;
    page?: number | undefined;
    pageSize?: number | undefined;
    sortBy?: string | undefined;
    sortDir?: "asc" | "desc" | undefined;
    filters?: Record<string, string> | undefined;
  },
) {
  const tableName = resolveTable(db, options.table);
  const descriptor = describeSchema(db);
  const table = descriptor.tables.find((entry) => entry.table === tableName);
  if (!table) {
    throw new CodedError("NOT_FOUND", `Table not found: ${tableName}`);
  }
  const unique = uniqueColumnNames(table);
  const columns = table.columns
    .filter((column) => isVisible(column.hidden))
    .map((column) => ({
      name: column.name,
      type: column.type,
      notNull: column.notNull,
      primaryKey: column.primaryKey,
      unique: unique.has(column.name),
      defaultValue: column.defaultValue,
    }));
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
    bindings.push(`%${value.replaceAll("\\", "\\\\").replaceAll("%", "\\%").replaceAll("_", "\\_")}%`);
  }

  const sortBy = options.sortBy?.trim();
  if (sortBy && !columnNames.has(sortBy)) {
    throw new CodedError("INVALID_OPERATION", `Sort column not found: ${sortBy}`);
  }

  const whereSql = whereParts.length === 0 ? "" : ` WHERE ${whereParts.join(" AND ")}`;
  const totalRow = db.$client
    .query<{ c: number }, string[]>(
      `SELECT COUNT(*) AS c FROM ${quoteIdentifier(tableName, { unsafe: true })}${whereSql}`,
    )
    .get(...bindings);
  const totalRows = totalRow?.c ?? 0;
  const totalPages = totalRows === 0 ? 1 : Math.ceil(totalRows / pageSize);
  const page = Math.min(requestedPage, totalPages);
  const offset = (page - 1) * pageSize;

  const orderSql = orderClause(table, sortBy, sortDir);

  const rows = db.$client
    .query<Record<string, unknown>, Array<string | number>>(
      `SELECT * FROM ${quoteIdentifier(tableName, { unsafe: true })}${whereSql}${orderSql} LIMIT ? OFFSET ?`,
    )
    .all(...bindings, pageSize, offset);

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
    rows: rows.map((row) => normalizeRow(row)),
  };
}
