import { canonicalJson, sha256Hex } from "./checksum";
import { listUserTables, tableExists, type Database } from "./db";
import { CodedError } from "../error";
import { primaryKeyColumns, tableDDL, tableInfo } from "./inspect";
import { quoteIdentifier } from "./sql";
import type { EncodedCell, EncodedRow, SqlStorageClass, TableSecondaryObject } from "./primitives";

export interface RowEffect {
  tableName: string;
  pk: Record<string, string>;
  opKind: "insert" | "update" | "delete";
  beforeRow: EncodedRow | null;
  afterRow: EncodedRow | null;
}

export interface TableSnapshot {
  tableName: string;
  ddlSql: string;
  rows: EncodedRow[];
  secondaryObjects: TableSecondaryObject[];
  references: string[];
}

export interface SchemaEffect {
  tableName: string;
  beforeTable: TableSnapshot | null;
  afterTable: TableSnapshot | null;
}

interface CapturedRowEntry {
  pk: Record<string, string>;
  row: EncodedRow;
  rowHash: string;
}

interface CapturedTableState {
  snapshot: TableSnapshot;
  schemaSignature: string;
  rowsByPk: Map<string, CapturedRowEntry>;
  keyColumns: string[];
}

export interface CapturedObservedState {
  tables: Map<string, CapturedTableState>;
}

function isSqlStorageClass(value: unknown): value is SqlStorageClass {
  return value === "null" || value === "integer" || value === "real" || value === "text" || value === "blob";
}

function pkKey(pk: Record<string, string>): string {
  return canonicalJson(pk);
}

export function toPkWhereClause(pk: Record<string, string>): string {
  const keys = Object.keys(pk).sort((a, b) => a.localeCompare(b));
  if (keys.length === 0) {
    throw new CodedError("REVERT_FAILED", "PK predicate must not be empty");
  }
  const parts: string[] = [];
  for (const key of keys) {
    const literal = pk[key];
    if (!literal) {
      throw new CodedError("REVERT_FAILED", `Missing PK literal for ${key}`);
    }
    const quoted = quoteIdentifier(key, { unsafe: true });
    parts.push(literal.toUpperCase() === "NULL" ? `${quoted} IS NULL` : `${quoted} = ${literal}`);
  }
  return parts.join(" AND ");
}

function encodeRowFromResult(
  row: Record<string, unknown>,
  columns: string[],
  quoteAliases: string[],
  hexAliases: string[],
  typeAliases: string[],
): EncodedRow {
  const encoded: EncodedRow = {};
  for (let i = 0; i < columns.length; i++) {
    const column = columns[i]!;
    const quoteAlias = quoteAliases[i]!;
    const hexAlias = hexAliases[i]!;
    const typeAlias = typeAliases[i]!;
    const quoteValue = row[quoteAlias];
    const hexValue = row[hexAlias];
    const typeValue = row[typeAlias];
    if (!isSqlStorageClass(typeValue)) {
      throw new CodedError("APPLY_FAILED", `Unsupported sqlite storage class for ${column}: ${String(typeValue)}`);
    }
    let sqlLiteral: string;
    if (typeValue === "null") {
      sqlLiteral = "NULL";
    } else if (typeValue === "text") {
      if (typeof hexValue !== "string") {
        throw new CodedError("APPLY_FAILED", `Failed to encode text bytes for ${column}`);
      }
      sqlLiteral = `CAST(X'${hexValue}' AS TEXT)`;
    } else if (typeValue === "blob") {
      if (typeof hexValue !== "string") {
        throw new CodedError("APPLY_FAILED", `Failed to encode blob bytes for ${column}`);
      }
      sqlLiteral = `X'${hexValue}'`;
    } else {
      if (typeof quoteValue !== "string") {
        throw new CodedError("APPLY_FAILED", `Failed to encode numeric literal for ${column}`);
      }
      sqlLiteral = quoteValue;
    }
    const cell: EncodedCell = {
      storageClass: typeValue,
      sqlLiteral,
    };
    encoded[column] = cell;
  }
  return encoded;
}

function tableColumns(db: Database, table: string): string[] {
  const info = tableInfo(db, table);
  if (info.length === 0) {
    throw new CodedError("APPLY_FAILED", `Unable to inspect table columns: ${table}`);
  }
  return info.map((column) => column.name);
}

function buildRowSelectSql(table: string, columns: string[], pkColumns: string[], whereClause: string | null): string {
  const quoteAliases = columns.map((_, i) => `__toss_quote_${i}`);
  const hexAliases = columns.map((_, i) => `__toss_hex_${i}`);
  const typeAliases = columns.map((_, i) => `__toss_type_${i}`);
  const parts: string[] = [];
  for (let i = 0; i < columns.length; i++) {
    const quotedCol = quoteIdentifier(columns[i]!, { unsafe: true });
    parts.push(`quote(${quotedCol}) AS ${quoteIdentifier(quoteAliases[i]!, { unsafe: true })}`);
    parts.push(`hex(CAST(${quotedCol} AS BLOB)) AS ${quoteIdentifier(hexAliases[i]!, { unsafe: true })}`);
    parts.push(`typeof(${quotedCol}) AS ${quoteIdentifier(typeAliases[i]!, { unsafe: true })}`);
  }

  const orderBy = pkColumns.map((column) => `${quoteIdentifier(column, { unsafe: true })} ASC`).join(", ");
  const whereSql = whereClause ? ` WHERE ${whereClause}` : "";
  return `SELECT ${parts.join(", ")} FROM ${quoteIdentifier(table, { unsafe: true })}${whereSql} ORDER BY ${orderBy}`;
}

function isSystemSequenceTable(table: string): boolean {
  return table === "sqlite_sequence";
}

export function isSystemSideEffectTable(table: string): boolean {
  return isSystemSequenceTable(table);
}

function observedTableNames(db: Database): string[] {
  const names = listUserTables(db);
  if (tableExists(db, "sqlite_sequence")) {
    names.push("sqlite_sequence");
  }
  return names.sort((a, b) => a.localeCompare(b));
}

function keyColumnsForObservedTable(db: Database, table: string): string[] {
  const pkColumns = primaryKeyColumns(db, table);
  if (pkColumns.length > 0) {
    return pkColumns;
  }
  if (isSystemSequenceTable(table)) {
    return ["name"];
  }
  throw new CodedError("NO_PRIMARY_KEY", `Table ${table} must define PRIMARY KEY for tracked operations`);
}

function captureTableState(db: Database, table: string): CapturedTableState {
  const keyColumns = keyColumnsForObservedTable(db, table);

  const ddlSql = tableDDL(db, table) ?? (isSystemSequenceTable(table) ? "CREATE TABLE sqlite_sequence(name,seq)" : null);
  if (!ddlSql) {
    throw new CodedError("APPLY_FAILED", `Unable to read CREATE TABLE SQL for ${table}`);
  }

  const secondaryObjects = db.$client
    .query<{ type: "index" | "trigger"; name: string; sql: string }, [string]>(`
      SELECT type, name, sql
      FROM sqlite_master
      WHERE tbl_name = ? AND type IN ('index', 'trigger') AND sql IS NOT NULL
      ORDER BY type ASC, name ASC
      `)
    .all(table);
  const references = Array.from(
    new Set(
      db.$client
        .query<{ table: string }, []>(`PRAGMA foreign_key_list(${quoteIdentifier(table, { unsafe: true })})`)
        .all()
        .map((row) => row.table),
    ),
  ).sort((a, b) => a.localeCompare(b));

  const columns = tableColumns(db, table);
  const quoteAliases = columns.map((_, i) => `__toss_quote_${i}`);
  const hexAliases = columns.map((_, i) => `__toss_hex_${i}`);
  const typeAliases = columns.map((_, i) => `__toss_type_${i}`);
  const rowsRaw = db.$client.query<Record<string, unknown>, []>(buildRowSelectSql(table, columns, keyColumns, null)).all();

  const rows: EncodedRow[] = [];
  const rowsByPk = new Map<string, CapturedRowEntry>();
  for (const raw of rowsRaw) {
    const row = encodeRowFromResult(raw, columns, quoteAliases, hexAliases, typeAliases);
    const pk: Record<string, string> = {};
    for (const pkColumn of keyColumns) {
      const cell = row[pkColumn];
      if (!cell) {
        throw new CodedError("APPLY_FAILED", `PK column missing in encoded row: ${table}.${pkColumn}`);
      }
      if (cell.sqlLiteral.toUpperCase() === "NULL") {
        throw new CodedError(
          "APPLY_FAILED",
          `Tracked table ${table} has NULL primary key value at column ${pkColumn}; nullable PK values are not supported.`,
        );
      }
      pk[pkColumn] = cell.sqlLiteral;
    }
    const key = pkKey(pk);
    if (rowsByPk.has(key)) {
      throw new CodedError(
        "APPLY_FAILED",
        `Tracked table ${table} has duplicate primary-key identity in observed capture: ${canonicalJson(pk)}`,
      );
    }
    const hash = rowHash(row);
    rows.push(row);
    rowsByPk.set(key, { pk, row, rowHash: hash ?? "" });
  }

  const snapshot: TableSnapshot = {
    tableName: table,
    ddlSql,
    rows,
    secondaryObjects,
    references,
  };
  const schemaSignature = sha256Hex({
    ddlSql: snapshot.ddlSql,
    secondaryObjects: snapshot.secondaryObjects,
    references: snapshot.references,
  });
  return { snapshot, schemaSignature, rowsByPk, keyColumns };
}

export function captureObservedState(db: Database): CapturedObservedState {
  const tables = observedTableNames(db);
  const captured = new Map<string, CapturedTableState>();
  for (const table of tables) {
    captured.set(table, captureTableState(db, table));
  }
  return { tables: captured };
}

function schemaChanged(beforeTable: CapturedTableState | undefined, afterTable: CapturedTableState | undefined): boolean {
  if (!beforeTable || !afterTable) {
    return beforeTable !== afterTable;
  }
  return beforeTable.schemaSignature !== afterTable.schemaSignature;
}

export function diffObservedState(
  before: CapturedObservedState,
  after: CapturedObservedState,
): { rowEffects: RowEffect[]; schemaEffects: SchemaEffect[] } {
  const names = Array.from(new Set([...before.tables.keys(), ...after.tables.keys()])).sort((a, b) =>
    a.localeCompare(b),
  );

  const schemaEffects: SchemaEffect[] = [];
  const rowEffectsByTable = new Map<
    string,
    {
      inserts: RowEffect[];
      updates: RowEffect[];
      deletes: RowEffect[];
    }
  >();
  const tableRefs = new Map<string, string[]>();

  for (const tableName of names) {
    const beforeTable = before.tables.get(tableName);
    const afterTable = after.tables.get(tableName);
    const hasSchemaChange = schemaChanged(beforeTable, afterTable);

    if (hasSchemaChange && !isSystemSequenceTable(tableName)) {
      schemaEffects.push({
        tableName,
        beforeTable: beforeTable ? beforeTable.snapshot : null,
        afterTable: afterTable ? afterTable.snapshot : null,
      });
      continue;
    }

    const refs = afterTable?.snapshot.references ?? beforeTable?.snapshot.references ?? [];
    tableRefs.set(tableName, refs);

    const beforeRows = beforeTable?.rowsByPk ?? new Map<string, CapturedRowEntry>();
    const afterRows = afterTable?.rowsByPk ?? new Map<string, CapturedRowEntry>();
    const pkKeys = Array.from(new Set([...beforeRows.keys(), ...afterRows.keys()])).sort((a, b) => a.localeCompare(b));
    const bucket: {
      inserts: RowEffect[];
      updates: RowEffect[];
      deletes: RowEffect[];
    } = {
      inserts: [],
      updates: [],
      deletes: [],
    };

    for (const key of pkKeys) {
      const beforeEntry = beforeRows.get(key);
      const afterEntry = afterRows.get(key);

      if (!beforeEntry && afterEntry) {
        bucket.inserts.push({
          tableName,
          pk: afterEntry.pk,
          opKind: "insert",
          beforeRow: null,
          afterRow: afterEntry.row,
        });
        continue;
      }

      if (beforeEntry && !afterEntry) {
        bucket.deletes.push({
          tableName,
          pk: beforeEntry.pk,
          opKind: "delete",
          beforeRow: beforeEntry.row,
          afterRow: null,
        });
        continue;
      }

      if (beforeEntry && afterEntry && beforeEntry.rowHash !== afterEntry.rowHash) {
        bucket.updates.push({
          tableName,
          pk: beforeEntry.pk,
          opKind: "update",
          beforeRow: beforeEntry.row,
          afterRow: afterEntry.row,
        });
      }
    }
    rowEffectsByTable.set(tableName, bucket);
  }

  const rowEffects: RowEffect[] = [];
  const rowTables = Array.from(rowEffectsByTable.keys());
  const parentFirst = dependencyOrder(rowTables, tableRefs, "parent-first");
  const childFirst = dependencyOrder(rowTables, tableRefs, "child-first");
  for (const table of childFirst) {
    rowEffects.push(...(rowEffectsByTable.get(table)?.deletes ?? []));
  }
  for (const table of parentFirst) {
    rowEffects.push(...(rowEffectsByTable.get(table)?.updates ?? []));
  }
  for (const table of parentFirst) {
    rowEffects.push(...(rowEffectsByTable.get(table)?.inserts ?? []));
  }

  return { rowEffects, schemaEffects };
}

export function dependencyOrder(
  tables: string[],
  tableRefs: Map<string, string[]>,
  mode: "parent-first" | "child-first",
): string[] {
  const compareTableNames = (a: string, b: string): number => {
    const aSystem = isSystemSideEffectTable(a);
    const bSystem = isSystemSideEffectTable(b);
    if (aSystem !== bSystem) {
      return aSystem ? -1 : 1;
    }
    return a.localeCompare(b);
  };
  const tableSet = new Set(tables);
  const outgoing = new Map<string, string[]>();
  for (const table of tables) {
    const refs = (tableRefs.get(table) ?? []).filter((ref) => tableSet.has(ref));
    outgoing.set(table, refs.sort(compareTableNames));
  }

  const temp = new Set<string>();
  const perm = new Set<string>();
  const parentFirst: string[] = [];
  const visit = (node: string): void => {
    if (perm.has(node)) {
      return;
    }
    if (temp.has(node)) {
      return;
    }
    temp.add(node);
    const refs = outgoing.get(node) ?? [];
    for (const ref of refs) {
      visit(ref);
    }
    temp.delete(node);
    perm.add(node);
    parentFirst.push(node);
  };

  for (const table of [...tables].sort(compareTableNames)) {
    visit(table);
  }
  return mode === "parent-first" ? parentFirst : [...parentFirst].reverse();
}

export function rowHash(row: EncodedRow | null): string | null {
  if (!row) {
    return null;
  }
  return sha256Hex(row);
}

export function fetchObservedRowByPk(db: Database, table: string, pk: Record<string, string>): EncodedRow | null {
  if (!tableExists(db, table)) {
    if (isSystemSideEffectTable(table)) {
      return null;
    }
    throw new CodedError("REVERT_FAILED", `Table does not exist while reading observed row: ${table}`);
  }
  const columns = tableColumns(db, table);
  const keyColumns = Object.keys(pk).sort((a, b) => a.localeCompare(b));
  if (keyColumns.length === 0) {
    throw new CodedError("REVERT_FAILED", `Cannot fetch row without key columns: ${table}`);
  }
  const quoteAliases = columns.map((_, i) => `__toss_quote_${i}`);
  const hexAliases = columns.map((_, i) => `__toss_hex_${i}`);
  const typeAliases = columns.map((_, i) => `__toss_type_${i}`);
  const whereClause = toPkWhereClause(pk);
  const sql = `${buildRowSelectSql(table, columns, keyColumns, whereClause)} LIMIT 1`;
  const row = db.$client.query<Record<string, unknown>, []>(sql).get();
  if (!row) {
    return null;
  }
  return encodeRowFromResult(row, columns, quoteAliases, hexAliases, typeAliases);
}
