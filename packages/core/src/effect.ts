import { listUserTables, tableExists, type Database } from "./db";
import { CodedError, type ErrorCode } from "./error";
import {
  canonicalJson,
  extractCheckConstraints,
  normalizeSqlNullable,
  parseColumnDefinitionsFromCreateTable,
  pragmaLiteral,
  quoteIdentifier,
  sha256Hex,
} from "./sql";
import type {
  EncodedCell,
  EncodedRow,
  JsonObject,
  JsonPrimitive,
  SqlStorageClass,
  TableSecondaryObject,
} from "./schema";
import { executeOperation, type RestoreTableOperation } from "./operation";

export interface TableInfoRow {
  cid: number;
  name: string;
  type: string;
  notnull: number;
  dflt_value: string | null;
  pk: number;
}

interface TableListRow {
  schema: string;
  name: string;
  type: string;
  ncol: number;
  wr: number;
  strict: number;
}

interface TableXInfoRow {
  cid: number;
  name: string;
  type: string;
  notnull: number;
  dflt_value: string | null;
  pk: number;
  hidden: number;
}

interface ForeignKeyRow {
  id: number;
  seq: number;
  table: string;
  from: string;
  to: string | null;
  on_update: string;
  on_delete: string;
  match: string;
}

interface IndexListRow {
  seq: number;
  name: string;
  unique: number;
  origin: "c" | "u" | "pk";
  partial: number;
}

interface IndexXInfoRow {
  seqno: number;
  cid: number;
  name: string | null;
  desc: number;
  coll: string | null;
  key: number;
}

export interface SchemaColumnDescriptor {
  definitionSql: string | null;
  cid: number;
  name: string;
  type: string;
  notnull: number;
  dfltValue: string | null;
  pk: number;
  hidden: number;
}

export interface SchemaForeignKeyDescriptor {
  id: number;
  refTable: string;
  onUpdate: string;
  onDelete: string;
  match: string;
  mappings: Array<{ seq: number; from: string; to: string | null }>;
}

export interface SchemaIndexDescriptor {
  name: string;
  unique: boolean;
  origin: "c" | "u" | "pk";
  partial: boolean;
  sql: string | null;
  columns: Array<{
    seqno: number;
    cid: number;
    name: string | null;
    desc: number;
    coll: string | null;
    key: number;
  }>;
}

export interface SchemaTriggerDescriptor {
  name: string;
  sql: string | null;
}

export interface SchemaTableDescriptor {
  tableSql: string | null;
  table: string;
  options: {
    withoutRowid: boolean;
    strict: boolean;
  };
  columns: SchemaColumnDescriptor[];
  foreignKeys: SchemaForeignKeyDescriptor[];
  indexes: SchemaIndexDescriptor[];
  checks: string[];
  triggers: SchemaTriggerDescriptor[];
}

export interface SchemaDescriptor {
  tables: SchemaTableDescriptor[];
}

export function schemaHashFromDescriptor(descriptor: SchemaDescriptor): string {
  return sha256Hex(descriptor.tables);
}

function serializeStateValue(value: JsonPrimitive): JsonPrimitive {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      return null;
    }
    return value;
  }
  return value;
}

function normalizeStateRow(row: Record<string, unknown>): JsonObject {
  const output: JsonObject = {};
  for (const [key, value] of Object.entries(row)) {
    if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      output[key] = serializeStateValue(value);
      continue;
    }
    if (value instanceof Uint8Array) {
      output[key] = Buffer.from(value).toString("base64");
      continue;
    }
    output[key] = JSON.stringify(value);
  }
  return output;
}

export function tableInfo(db: Database, table: string): TableInfoRow[] {
  return db.$client.query<TableInfoRow, []>(`PRAGMA table_info(${quoteIdentifier(table, { unsafe: true })})`).all();
}

export function primaryKeyColumns(db: Database, table: string): string[] {
  return tableInfo(db, table)
    .filter((column) => column.pk > 0)
    .sort((a, b) => a.pk - b.pk)
    .map((column) => column.name);
}

export function assertTableHasPrimaryKey(db: Database, table: string): string[] {
  const pkCols = primaryKeyColumns(db, table);
  if (pkCols.length === 0) {
    throw new CodedError("NO_PRIMARY_KEY", `Table ${table} must define PRIMARY KEY for tracked operations`);
  }
  return pkCols;
}

export function tableDDL(db: Database, table: string): string | null {
  const row = db.$client
    .query<{ sql: string | null }, [string]>(
      "SELECT sql FROM sqlite_master WHERE type='table' AND name = ? COLLATE NOCASE LIMIT 1",
    )
    .get(table);
  return row?.sql ?? null;
}

export function describeSchema(db: Database): SchemaDescriptor {
  const tableNames = listUserTables(db);
  const tableList = db.$client.query<TableListRow, []>("PRAGMA table_list").all();
  const tableOpts = new Map(
    tableList
      .filter((row) => row.schema === "main" && row.type === "table")
      .map((row) => [row.name, { withoutRowid: row.wr === 1, strict: row.strict === 1 }] as const),
  );

  const tables = tableNames.map((table) => {
    const tableDdl = tableDDL(db, table);
    const columnDefs = parseColumnDefinitionsFromCreateTable(tableDdl);
    const checks = extractCheckConstraints(tableDdl);
    return {
      tableSql: normalizeSqlNullable(tableDdl),
      table,
      options: tableOpts.get(table) ?? { withoutRowid: false, strict: false },
      columns: db.$client.query<TableXInfoRow, []>(`PRAGMA table_xinfo(${pragmaLiteral(table)})`).all()
        .map((column) => ({
          definitionSql: columnDefs.get(column.name.toLowerCase()) ?? null,
          cid: column.cid,
          name: column.name,
          type: column.type.trim().toUpperCase(),
          notnull: column.notnull,
          dfltValue: column.dflt_value,
          pk: column.pk,
          hidden: column.hidden,
        }))
        .sort((a, b) => a.cid - b.cid),
      foreignKeys: (() => {
        const rows = db.$client.query<ForeignKeyRow, []>(`PRAGMA foreign_key_list(${pragmaLiteral(table)})`).all();
        const byId = new Map<
          number,
          {
            id: number;
            refTable: string;
            onUpdate: string;
            onDelete: string;
            match: string;
            mappings: Array<{ seq: number; from: string; to: string | null }>;
          }
        >();
        for (const row of rows) {
          const existing = byId.get(row.id);
          if (!existing) {
            byId.set(row.id, {
              id: row.id,
              refTable: row.table,
              onUpdate: row.on_update,
              onDelete: row.on_delete,
              match: row.match,
              mappings: [{ seq: row.seq, from: row.from, to: row.to }],
            });
            continue;
          }
          existing.mappings.push({ seq: row.seq, from: row.from, to: row.to });
        }
        return Array.from(byId.values())
          .map((fk) => ({
            ...fk,
            mappings: fk.mappings.sort((a, b) => a.seq - b.seq),
          }))
          .sort((a, b) => a.id - b.id);
      })(),
      indexes: db.$client.query<IndexListRow, []>(`PRAGMA index_list(${pragmaLiteral(table)})`).all()
        .map((index) => {
          const indexSqlRow = db.$client
            .query<{ sql: string | null }, [string]>("SELECT sql FROM sqlite_master WHERE type='index' AND name=? LIMIT 1")
            .get(index.name);
          const indexColumns = db.$client.query<IndexXInfoRow, []>(`PRAGMA index_xinfo(${pragmaLiteral(index.name)})`).all()
            .map((entry) => ({
              seqno: entry.seqno,
              cid: entry.cid,
              name: entry.name,
              desc: entry.desc,
              coll: entry.coll,
              key: entry.key,
            }))
            .sort((a, b) => a.seqno - b.seqno);
          return {
            name: index.name,
            unique: index.unique === 1,
            origin: index.origin,
            partial: index.partial === 1,
            sql: normalizeSqlNullable(indexSqlRow?.sql ?? null),
            columns: indexColumns,
          };
        })
        .sort((a, b) => a.name.localeCompare(b.name)),
      checks,
      triggers: (
        db.$client
          .query<{ name: string; sql: string | null }, [string]>(
            "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=? ORDER BY name ASC",
          )
          .all(table)
      ).map((trigger) => ({
        name: trigger.name,
        sql: normalizeSqlNullable(trigger.sql),
      })),
    };
  });
  return { tables };
}

export function schemaHash(db: Database): string {
  return schemaHashFromDescriptor(describeSchema(db));
}

export function stateHash(db: Database): string {
  const tables = listUserTables(db);
  const state: Record<string, JsonObject[]> = {};
  for (const table of tables) {
    const pkColumns = assertTableHasPrimaryKey(db, table);
    const orderBy = pkColumns.map((column) => `${quoteIdentifier(column, { unsafe: true })} ASC`).join(", ");
    const rows = db.$client.query<Record<string, unknown>, []>(
      `SELECT * FROM ${quoteIdentifier(table, { unsafe: true })} ORDER BY ${orderBy}`,
    ).all();
    state[table] = rows.map((row) => normalizeStateRow(row));
  }
  return sha256Hex(state);
}


export function serializeValue(value: JsonPrimitive): JsonPrimitive {
  if (typeof value === "number" && !Number.isFinite(value)) {
    return null;
  }
  return value;
}

export function normalizeRowObject(row: Record<string, unknown>): JsonObject {
  const output: JsonObject = {};
  for (const [key, value] of Object.entries(row)) {
    if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      output[key] = serializeValue(value);
      continue;
    }
    if (value instanceof Uint8Array) {
      output[key] = Buffer.from(value).toString("base64");
      continue;
    }
    output[key] = JSON.stringify(value);
  }
  return output;
}

export function whereClauseFromRecord(
  values: Record<string, JsonPrimitive>,
): { clause: string; bindings: JsonPrimitive[] } {
  const keys = Object.keys(values);
  if (keys.length === 0) {
    throw new CodedError("INVALID_OPERATION", "where must not be empty");
  }

  const terms: string[] = [];
  const bindings: JsonPrimitive[] = [];
  for (const key of keys) {
    const value = values[key];
    if (value === undefined) {
      throw new CodedError("INVALID_OPERATION", `where value missing for key: ${key}`);
    }
    const quoted = quoteIdentifier(key);
    if (value === null) {
      terms.push(`${quoted} IS NULL`);
      continue;
    }
    terms.push(`${quoted} = ?`);
    bindings.push(serializeValue(value));
  }
  return { clause: terms.join(" AND "), bindings };
}

export function fetchRowsByWhere(
  db: Database,
  table: string,
  where: Record<string, JsonPrimitive>,
): Array<Record<string, unknown>> {
  const { clause, bindings } = whereClauseFromRecord(where);
  const sql = `SELECT * FROM ${quoteIdentifier(table)} WHERE ${clause}`;
  return db.$client.query<Record<string, unknown>, JsonPrimitive[]>(sql).all(...bindings);
}

export function fetchAllRows(db: Database, table: string): JsonObject[] {
  const rows = db.$client.query<Record<string, unknown>, []>(`SELECT * FROM ${quoteIdentifier(table)} ORDER BY rowid ASC`).all();
  return rows.map(normalizeRowObject);
}

export function pkFromRow(db: Database, table: string, row: Record<string, unknown>): Record<string, JsonPrimitive> {
  const pkCols = assertTableHasPrimaryKey(db, table);

  const pk: Record<string, JsonPrimitive> = {};
  for (const column of pkCols) {
    const value = row[column];
    if (value === undefined) {
      throw new CodedError("INVALID_OPERATION", `PK column missing in row: ${table}.${column}`);
    }
    if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      pk[column] = value;
    } else {
      throw new CodedError("INVALID_OPERATION", `Unsupported PK value type in ${table}.${column}`);
    }
  }
  return pk;
}

export function fetchRowByPk(
  db: Database,
  table: string,
  pk: Record<string, JsonPrimitive>,
): Record<string, unknown> | null {
  const { clause, bindings } = whereClauseFromRecord(pk);
  return db.$client
    .query<Record<string, unknown>, JsonPrimitive[]>(`SELECT * FROM ${quoteIdentifier(table)} WHERE ${clause} LIMIT 1`)
    .get(...bindings);
}


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


function insertEncodedRow(db: Database, table: string, row: EncodedRow): void {
  const columns = Object.keys(row).sort((a, b) => a.localeCompare(b));
  if (columns.length === 0) {
    throw new CodedError("REVERT_FAILED", `Cannot insert empty encoded row for ${table}`);
  }
  const colSql = columns.map((column) => quoteIdentifier(column, { unsafe: true })).join(", ");
  const valuesSql = columns
    .map((column) => {
      const cell = row[column];
      if (!cell) {
        throw new CodedError("REVERT_FAILED", `Missing encoded cell for ${table}.${column}`);
      }
      return cell.sqlLiteral;
    })
    .join(", ");
  db.$client.run(`INSERT INTO ${quoteIdentifier(table, { unsafe: true })} (${colSql}) VALUES (${valuesSql})`);
}

function updateEncodedRow(db: Database, table: string, pk: Record<string, string>, row: EncodedRow): void {
  const columns = Object.keys(row).sort((a, b) => a.localeCompare(b));
  if (columns.length === 0) {
    throw new CodedError("REVERT_FAILED", `Cannot update empty encoded row for ${table}`);
  }
  const setSql = columns
    .map((column) => {
      const cell = row[column];
      if (!cell) {
        throw new CodedError("REVERT_FAILED", `Missing encoded cell for ${table}.${column}`);
      }
      return `${quoteIdentifier(column, { unsafe: true })} = ${cell.sqlLiteral}`;
    })
    .join(", ");
  db.$client.run(`UPDATE ${quoteIdentifier(table, { unsafe: true })} SET ${setSql} WHERE ${toPkWhereClause(pk)}`);
}

function deleteByPk(db: Database, table: string, pk: Record<string, string>): void {
  db.$client.run(`DELETE FROM ${quoteIdentifier(table, { unsafe: true })} WHERE ${toPkWhereClause(pk)}`);
}

function referencedTables(db: Database, table: string): string[] {
  if (!tableExists(db, table)) {
    return [];
  }
  const rows = db.$client.query<{ table: string }, []>(`PRAGMA foreign_key_list(${quoteIdentifier(table, { unsafe: true })})`).all();
  return Array.from(new Set(rows.map((row) => row.table))).sort((a, b) => a.localeCompare(b));
}

function missingReferencedTables(db: Database, table: string): string[] {
  return referencedTables(db, table).filter((refTable) => !tableExists(db, refTable));
}

function effectRowMode(
  effect: RowEffect,
  direction: "forward" | "inverse",
): { expectedCurrent: EncodedRow | null; target: EncodedRow | null; opLabel: string } {
  if (direction === "forward") {
    if (effect.opKind === "insert") {
      return { expectedCurrent: null, target: effect.afterRow, opLabel: "insert" };
    }
    if (effect.opKind === "update") {
      return { expectedCurrent: effect.beforeRow, target: effect.afterRow, opLabel: "update" };
    }
    return { expectedCurrent: effect.beforeRow, target: null, opLabel: "delete" };
  }

  if (effect.opKind === "insert") {
    return { expectedCurrent: effect.afterRow, target: null, opLabel: "inverse-delete" };
  }
  if (effect.opKind === "update") {
    return { expectedCurrent: effect.afterRow, target: effect.beforeRow, opLabel: "inverse-update" };
  }
  return { expectedCurrent: null, target: effect.beforeRow, opLabel: "inverse-insert" };
}

export function applyRowEffectsWithOptions(
  db: Database,
  effects: RowEffect[],
  direction: "forward" | "inverse",
  options: {
    disableTableTriggers: boolean;
    includeSystemEffects?: boolean;
    includeUserEffects?: boolean;
    systemPolicy?: "strict" | "reconcile";
  },
): void {
  const includeSystemEffects = options.includeSystemEffects ?? true;
  const includeUserEffects = options.includeUserEffects ?? true;
  const systemPolicy = options.systemPolicy ?? "strict";
  const filtered = effects.filter((effect) =>
    isSystemSideEffectTable(effect.tableName) ? includeSystemEffects : includeUserEffects,
  );
  const droppedTriggers = options.disableTableTriggers ? dropTriggersForTables(db, filtered) : null;
  const ordered = direction === "forward" ? filtered : filtered.toReversed();
  for (const effect of ordered) {
    const { expectedCurrent, target, opLabel } = effectRowMode(effect, direction);
    const isSystem = isSystemSideEffectTable(effect.tableName);
    if (isSystem && systemPolicy === "reconcile") {
      applySystemRowEffectReconciled(db, effect.tableName, effect.pk, target);
      continue;
    }
    const current = fetchObservedRowByPk(db, effect.tableName, effect.pk);
    const currentHash = rowHash(current);
    const expectedHash = rowHash(expectedCurrent);
    if (currentHash !== expectedHash) {
      throw new CodedError(
        "REVERT_FAILED",
        `Observed row mismatch during ${opLabel} on ${effect.tableName} (pk=${canonicalJson(effect.pk)})`,
      );
    }
    if (!target) {
      deleteByPk(db, effect.tableName, effect.pk);
      continue;
    }
    if (!current) {
      insertEncodedRow(db, effect.tableName, target);
      continue;
    }
    updateEncodedRow(db, effect.tableName, effect.pk, target);
  }
  if (droppedTriggers) {
    restoreDroppedTriggers(db, droppedTriggers);
  }
}

function applySystemRowEffectReconciled(
  db: Database,
  table: string,
  pk: Record<string, string>,
  target: EncodedRow | null,
): void {
  const exists = tableExists(db, table);
  if (!target) {
    if (!exists) {
      return;
    }
    deleteByPk(db, table, pk);
    return;
  }
  if (!exists) {
    throw new CodedError("REVERT_FAILED", `System table does not exist for reconciled effect: ${table}`);
  }
  const current = fetchObservedRowByPk(db, table, pk);
  if (!current) {
    insertEncodedRow(db, table, target);
    return;
  }
  updateEncodedRow(db, table, pk, target);
}

interface DroppedTrigger {
  name: string;
  sql: string;
}

function dropTriggersForTables(db: Database, effects: RowEffect[]): DroppedTrigger[] {
  const touched = Array.from(new Set(effects.map((effect) => effect.tableName))).sort((a, b) => a.localeCompare(b));
  const dropped: DroppedTrigger[] = [];
  for (const table of touched) {
    const rows = db.$client
      .query<DroppedTrigger, [string]>(
        "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=? AND sql IS NOT NULL ORDER BY name ASC",
      )
      .all(table);
    for (const row of rows) {
      db.$client.run(`DROP TRIGGER IF EXISTS ${quoteIdentifier(row.name, { unsafe: true })}`);
      dropped.push(row);
    }
  }
  return dropped;
}

function restoreDroppedTriggers(db: Database, dropped: DroppedTrigger[]): void {
  for (const trigger of dropped) {
    db.$client.run(trigger.sql);
  }
}

function applySingleSchemaEffect(db: Database, effect: SchemaEffect, direction: "forward" | "inverse"): void {
  const snapshot = direction === "forward" ? effect.afterTable : effect.beforeTable;
  if (!snapshot) {
    executeOperation(db, { type: "drop_table", table: effect.tableName });
    return;
  }
  const restore: RestoreTableOperation = {
    type: "restore_table",
    table: effect.tableName,
    ddlSql: snapshot.ddlSql,
    rows: snapshot.rows,
    secondaryObjects: snapshot.secondaryObjects,
  };
  executeOperation(db, restore);
}

function orderSchemaEffectsForReplay(effects: SchemaEffect[], direction: "forward" | "inverse"): SchemaEffect[] {
  if (effects.length <= 1) {
    return effects;
  }

  const byTable = new Map<string, SchemaEffect>();
  for (const effect of effects) {
    byTable.set(effect.tableName, effect);
  }

  const restoreRefs = new Map<string, string[]>();
  const restoreTables: string[] = [];
  const dropRefs = new Map<string, string[]>();
  const dropTables: string[] = [];

  for (const effect of effects) {
    const target = direction === "forward" ? effect.afterTable : effect.beforeTable;
    if (target) {
      restoreTables.push(effect.tableName);
      restoreRefs.set(effect.tableName, target.references);
      continue;
    }
    const current = direction === "forward" ? effect.beforeTable : effect.afterTable;
    dropTables.push(effect.tableName);
    dropRefs.set(effect.tableName, current?.references ?? []);
  }

  const restoreOrdered = dependencyOrder(restoreTables, restoreRefs, "parent-first");
  const dropOrdered = dependencyOrder(dropTables, dropRefs, "child-first");
  const orderedTables = [...restoreOrdered, ...dropOrdered];
  return orderedTables.map((table) => byTable.get(table)).filter((effect): effect is SchemaEffect => effect !== undefined);
}

function canApplyUserRowEffectNow(db: Database, effect: RowEffect): boolean {
  if (isSystemSideEffectTable(effect.tableName)) {
    return false;
  }
  if (!tableExists(db, effect.tableName)) {
    return false;
  }
  return missingReferencedTables(db, effect.tableName).length === 0;
}

export function applyUserRowAndSchemaEffects(
  db: Database,
  rowEffects: RowEffect[],
  schemaEffects: SchemaEffect[],
  direction: "forward" | "inverse",
  options: {
    disableTableTriggers: boolean;
  },
): void {
  const pendingRows = (direction === "forward" ? rowEffects : rowEffects.toReversed()).filter(
    (effect) => !isSystemSideEffectTable(effect.tableName),
  );
  const orderedSchemas = orderSchemaEffectsForReplay(schemaEffects, direction);
  let schemaIndex = 0;

  while (pendingRows.length > 0 || schemaIndex < orderedSchemas.length) {
    while (pendingRows.length > 0 && canApplyUserRowEffectNow(db, pendingRows[0]!)) {
      applyRowEffectsWithOptions(db, [pendingRows.shift()!], direction, {
        disableTableTriggers: options.disableTableTriggers,
        includeUserEffects: true,
        includeSystemEffects: false,
      });
    }

    if (pendingRows.length === 0) {
      if (schemaIndex < orderedSchemas.length) {
        applySingleSchemaEffect(db, orderedSchemas[schemaIndex]!, direction);
        schemaIndex += 1;
        continue;
      }
      break;
    }

    if (schemaIndex < orderedSchemas.length) {
      applySingleSchemaEffect(db, orderedSchemas[schemaIndex]!, direction);
      schemaIndex += 1;
      continue;
    }

    const blocked = pendingRows[0]!;
    if (!tableExists(db, blocked.tableName)) {
      throw new CodedError(
        "REVERT_FAILED",
        `Observed row effect blocked because target table does not exist: ${blocked.tableName}`,
      );
    }
    const missingRefs = missingReferencedTables(db, blocked.tableName);
    if (missingRefs.length > 0) {
      throw new CodedError(
        "REVERT_FAILED",
        `Observed row effect blocked by missing referenced table(s): ${blocked.tableName} -> ${missingRefs.join(", ")}`,
      );
    }
    applyRowEffectsWithOptions(db, [blocked], direction, {
      disableTableTriggers: options.disableTableTriggers,
      includeUserEffects: true,
      includeSystemEffects: false,
    });
    pendingRows.shift();
  }
}

export function applySchemaEffects(db: Database, effects: SchemaEffect[], direction: "forward" | "inverse"): void {
  const ordered = orderSchemaEffectsForReplay(effects, direction);
  for (const effect of ordered) {
    applySingleSchemaEffect(db, effect, direction);
  }
}

export function assertNoForeignKeyViolations(db: Database, errorCode: ErrorCode, context: string): void {
  const rows = db.$client.query<{
    table: string;
    rowid: number;
    parent: string;
    fkid: number;
  }, []>("PRAGMA foreign_key_check").all();
  if (rows.length === 0) {
    return;
  }

  const first = rows[0]!;
  throw new CodedError(
    errorCode,
    `${context}: foreign_key_check failed at ${first.table} rowid=${first.rowid} parent=${first.parent} fk=${first.fkid}`,
  );
}
