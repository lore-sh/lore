import type { Database } from "bun:sqlite";
import { canonicalJson, sha256Hex } from "./checksum";
import { listUserTables } from "./db";
import { TossError } from "./errors";
import { quoteIdentifier } from "./sql";
import type { JsonObject, JsonPrimitive } from "./types";

export interface TableInfoRow {
  cid: number;
  name: string;
  type: string;
  notnull: number;
  dflt_value: string | null;
  pk: number;
}

export function serializeValue(value: JsonPrimitive): JsonPrimitive {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      return null;
    }
    return value;
  }
  return value;
}

export function normalizeRowObject(row: Record<string, unknown>): JsonObject {
  const output: JsonObject = {};
  for (const [key, value] of Object.entries(row)) {
    if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      output[key] = value;
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
  return db.query(`PRAGMA table_info(${quoteIdentifier(table)})`).all() as TableInfoRow[];
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
    throw new TossError("TABLE_WITHOUT_PRIMARY_KEY", `Table ${table} must define PRIMARY KEY for tracked operations`);
  }
  return pkCols;
}

export function tableDDL(db: Database, table: string): string | null {
  const row = db
    .query("SELECT sql FROM sqlite_master WHERE type='table' AND name=? LIMIT 1")
    .get(table) as { sql: string | null } | null;
  return row?.sql ?? null;
}

export function whereClauseFromRecord(
  values: Record<string, JsonPrimitive>,
): { clause: string; bindings: JsonPrimitive[] } {
  const keys = Object.keys(values);
  if (keys.length === 0) {
    throw new TossError("INVALID_OPERATION", "where must not be empty");
  }

  const terms: string[] = [];
  const bindings: JsonPrimitive[] = [];
  for (const key of keys) {
    const value = values[key];
    if (value === undefined) {
      throw new TossError("INVALID_OPERATION", `where value missing for key: ${key}`);
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
  return db.query(sql).all(...bindings) as Array<Record<string, unknown>>;
}

export function fetchAllRows(db: Database, table: string): JsonObject[] {
  const rows = db.query(`SELECT * FROM ${quoteIdentifier(table)} ORDER BY rowid ASC`).all() as Array<
    Record<string, unknown>
  >;
  return rows.map((row) => normalizeRowObject(row));
}

export function pkFromRow(db: Database, table: string, row: Record<string, unknown>): Record<string, JsonPrimitive> {
  const pkCols = assertTableHasPrimaryKey(db, table);

  const pk: Record<string, JsonPrimitive> = {};
  for (const column of pkCols) {
    const value = row[column];
    if (value === undefined) {
      throw new TossError("INVALID_OPERATION", `PK column missing in row: ${table}.${column}`);
    }
    if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      pk[column] = value;
    } else {
      throw new TossError("INVALID_OPERATION", `Unsupported PK value type in ${table}.${column}`);
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
  return db
    .query(`SELECT * FROM ${quoteIdentifier(table)} WHERE ${clause} LIMIT 1`)
    .get(...bindings) as Record<string, unknown> | null;
}

export function rowHash(row: JsonObject | null): string | null {
  if (!row) {
    return null;
  }
  return sha256Hex(canonicalJson(row));
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

function pragmaLiteral(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

function normalizeSql(sql: string | null): string | null {
  if (sql === null) {
    return null;
  }

  let i = 0;
  let pendingSpace = false;
  let out = "";
  let inSingle = false;
  let inDouble = false;
  let inBacktick = false;
  let inBracket = false;
  let inLineComment = false;
  let inBlockComment = false;

  const flushSpace = (nextChar: string | undefined): void => {
    if (!pendingSpace || out.length === 0) {
      pendingSpace = false;
      return;
    }
    const prev = out[out.length - 1];
    if (prev === " " || prev === "(" || nextChar === ")" || nextChar === "," || nextChar === ";") {
      pendingSpace = false;
      return;
    }
    out += " ";
    pendingSpace = false;
  };

  while (i < sql.length) {
    const ch = sql[i]!;
    const next = sql[i + 1];

    if (inLineComment) {
      if (ch === "\n") {
        inLineComment = false;
        pendingSpace = true;
      }
      i += 1;
      continue;
    }

    if (inBlockComment) {
      if (ch === "*" && next === "/") {
        inBlockComment = false;
        pendingSpace = true;
        i += 2;
        continue;
      }
      i += 1;
      continue;
    }

    if (inSingle) {
      out += ch;
      if (ch === "'" && next === "'") {
        out += next;
        i += 2;
        continue;
      }
      if (ch === "'") {
        inSingle = false;
      }
      i += 1;
      continue;
    }

    if (inDouble) {
      out += ch;
      if (ch === '"' && next === '"') {
        out += next;
        i += 2;
        continue;
      }
      if (ch === '"') {
        inDouble = false;
      }
      i += 1;
      continue;
    }

    if (inBacktick) {
      out += ch;
      if (ch === "`") {
        inBacktick = false;
      }
      i += 1;
      continue;
    }

    if (inBracket) {
      out += ch;
      if (ch === "]") {
        inBracket = false;
      }
      i += 1;
      continue;
    }

    if (ch === "-" && next === "-") {
      inLineComment = true;
      pendingSpace = true;
      i += 2;
      continue;
    }

    if (ch === "/" && next === "*") {
      inBlockComment = true;
      pendingSpace = true;
      i += 2;
      continue;
    }

    if (/\s/.test(ch)) {
      pendingSpace = true;
      i += 1;
      continue;
    }

    flushSpace(ch);
    out += ch;
    if (ch === "'") {
      inSingle = true;
    } else if (ch === '"') {
      inDouble = true;
    } else if (ch === "`") {
      inBacktick = true;
    } else if (ch === "[") {
      inBracket = true;
    }
    i += 1;
  }

  return out.trim();
}

function isWordBoundary(char: string | undefined): boolean {
  if (!char) {
    return true;
  }
  return !/[A-Za-z0-9_]/.test(char);
}

function readQuotedIdentifier(segment: string, start: number): { raw: string; next: number } | null {
  const quote = segment[start];
  if (!quote || (quote !== '"' && quote !== "`" && quote !== "[")) {
    return null;
  }
  const close = quote === "[" ? "]" : quote;
  let i = start + 1;
  let out = "";
  while (i < segment.length) {
    const ch = segment[i]!;
    const next = segment[i + 1];
    if (quote !== "[" && ch === close && next === close) {
      out += close;
      i += 2;
      continue;
    }
    if (ch === close) {
      return { raw: out, next: i + 1 };
    }
    out += ch;
    i += 1;
  }
  throw new TossError("SCHEMA_HASH_FAILED", `Malformed quoted identifier in CREATE TABLE: ${segment}`);
}

function readBareIdentifier(segment: string, start: number): { raw: string; next: number } | null {
  let i = start;
  let out = "";
  while (i < segment.length) {
    const ch = segment[i]!;
    if (!/[A-Za-z0-9_$]/.test(ch)) {
      break;
    }
    out += ch;
    i += 1;
  }
  if (out.length === 0) {
    return null;
  }
  return { raw: out, next: i };
}

function leadingIdentifier(segment: string): { name: string; next: number } | null {
  let i = 0;
  while (i < segment.length && /\s/.test(segment[i]!)) {
    i += 1;
  }
  const quoted = readQuotedIdentifier(segment, i);
  if (quoted) {
    return { name: quoted.raw, next: quoted.next };
  }
  const bare = readBareIdentifier(segment, i);
  if (!bare) {
    return null;
  }
  return { name: bare.raw, next: bare.next };
}

function splitTopLevelCommaList(payload: string): string[] {
  const parts: string[] = [];
  let start = 0;
  let i = 0;
  let depth = 0;
  let inSingle = false;
  let inDouble = false;
  let inBacktick = false;
  let inBracket = false;
  let inLineComment = false;
  let inBlockComment = false;

  while (i < payload.length) {
    const ch = payload[i]!;
    const next = payload[i + 1];

    if (inLineComment) {
      if (ch === "\n") {
        inLineComment = false;
      }
      i += 1;
      continue;
    }

    if (inBlockComment) {
      if (ch === "*" && next === "/") {
        inBlockComment = false;
        i += 2;
        continue;
      }
      i += 1;
      continue;
    }

    if (inSingle) {
      if (ch === "'" && next === "'") {
        i += 2;
        continue;
      }
      if (ch === "'") {
        inSingle = false;
      }
      i += 1;
      continue;
    }

    if (inDouble) {
      if (ch === '"' && next === '"') {
        i += 2;
        continue;
      }
      if (ch === '"') {
        inDouble = false;
      }
      i += 1;
      continue;
    }

    if (inBacktick) {
      if (ch === "`") {
        inBacktick = false;
      }
      i += 1;
      continue;
    }

    if (inBracket) {
      if (ch === "]") {
        inBracket = false;
      }
      i += 1;
      continue;
    }

    if (ch === "-" && next === "-") {
      inLineComment = true;
      i += 2;
      continue;
    }

    if (ch === "/" && next === "*") {
      inBlockComment = true;
      i += 2;
      continue;
    }

    if (ch === "'") {
      inSingle = true;
      i += 1;
      continue;
    }
    if (ch === '"') {
      inDouble = true;
      i += 1;
      continue;
    }
    if (ch === "`") {
      inBacktick = true;
      i += 1;
      continue;
    }
    if (ch === "[") {
      inBracket = true;
      i += 1;
      continue;
    }

    if (ch === "(") {
      depth += 1;
      i += 1;
      continue;
    }
    if (ch === ")") {
      if (depth > 0) {
        depth -= 1;
      }
      i += 1;
      continue;
    }
    if (ch === "," && depth === 0) {
      parts.push(payload.slice(start, i).trim());
      start = i + 1;
      i += 1;
      continue;
    }

    i += 1;
  }
  const tail = payload.slice(start).trim();
  if (tail.length > 0) {
    parts.push(tail);
  }
  return parts;
}

function parseColumnDefinitionsFromCreateTable(tableSql: string | null): Map<string, string> {
  const defs = new Map<string, string>();
  if (!tableSql) {
    return defs;
  }

  const open = tableSql.indexOf("(");
  if (open < 0) {
    return defs;
  }
  let i = open;
  let depth = 0;
  let end = -1;
  let inSingle = false;
  let inDouble = false;
  let inBacktick = false;
  let inBracket = false;

  while (i < tableSql.length) {
    const ch = tableSql[i]!;
    const next = tableSql[i + 1];
    if (inSingle) {
      if (ch === "'" && next === "'") {
        i += 2;
        continue;
      }
      if (ch === "'") {
        inSingle = false;
      }
      i += 1;
      continue;
    }
    if (inDouble) {
      if (ch === '"' && next === '"') {
        i += 2;
        continue;
      }
      if (ch === '"') {
        inDouble = false;
      }
      i += 1;
      continue;
    }
    if (inBacktick) {
      if (ch === "`") {
        inBacktick = false;
      }
      i += 1;
      continue;
    }
    if (inBracket) {
      if (ch === "]") {
        inBracket = false;
      }
      i += 1;
      continue;
    }

    if (ch === "'") {
      inSingle = true;
      i += 1;
      continue;
    }
    if (ch === '"') {
      inDouble = true;
      i += 1;
      continue;
    }
    if (ch === "`") {
      inBacktick = true;
      i += 1;
      continue;
    }
    if (ch === "[") {
      inBracket = true;
      i += 1;
      continue;
    }
    if (ch === "(") {
      depth += 1;
      i += 1;
      continue;
    }
    if (ch === ")") {
      depth -= 1;
      if (depth === 0) {
        end = i;
        break;
      }
      i += 1;
      continue;
    }
    i += 1;
  }
  if (end < 0) {
    return defs;
  }

  const payload = tableSql.slice(open + 1, end);
  const segments = splitTopLevelCommaList(payload);
  const tableConstraint = /^(CONSTRAINT|PRIMARY|UNIQUE|CHECK|FOREIGN)\b/i;
  for (const segment of segments) {
    if (tableConstraint.test(segment)) {
      continue;
    }
    const lead = leadingIdentifier(segment);
    if (!lead) {
      continue;
    }
    defs.set(lead.name.toLowerCase(), normalizeSql(segment) ?? segment.trim());
  }

  return defs;
}

function extractCheckConstraints(tableSql: string | null): string[] {
  if (!tableSql) {
    return [];
  }
  const checks: string[] = [];
  const sql = tableSql;
  let i = 0;
  let inSingle = false;
  let inDouble = false;
  let inBacktick = false;
  let inBracket = false;

  while (i < sql.length) {
    const ch = sql[i]!;
    const next = sql[i + 1];

    if (inSingle) {
      if (ch === "'" && next === "'") {
        i += 2;
        continue;
      }
      if (ch === "'") {
        inSingle = false;
      }
      i += 1;
      continue;
    }
    if (inDouble) {
      if (ch === '"' && next === '"') {
        i += 2;
        continue;
      }
      if (ch === '"') {
        inDouble = false;
      }
      i += 1;
      continue;
    }
    if (inBacktick) {
      if (ch === "`") {
        inBacktick = false;
      }
      i += 1;
      continue;
    }
    if (inBracket) {
      if (ch === "]") {
        inBracket = false;
      }
      i += 1;
      continue;
    }

    if (ch === "'") {
      inSingle = true;
      i += 1;
      continue;
    }
    if (ch === '"') {
      inDouble = true;
      i += 1;
      continue;
    }
    if (ch === "`") {
      inBacktick = true;
      i += 1;
      continue;
    }
    if (ch === "[") {
      inBracket = true;
      i += 1;
      continue;
    }

    if (
      i + 5 <= sql.length &&
      sql.slice(i, i + 5).toUpperCase() === "CHECK" &&
      isWordBoundary(sql[i - 1]) &&
      isWordBoundary(sql[i + 5])
    ) {
      let j = i + 5;
      while (j < sql.length && /\s/.test(sql[j]!)) {
        j += 1;
      }
      if (sql[j] !== "(") {
        i += 1;
        continue;
      }
      let depth = 0;
      let k = j;
      let litSingle = false;
      let litDouble = false;
      let litBacktick = false;
      let litBracket = false;
      while (k < sql.length) {
        const c = sql[k]!;
        const n = sql[k + 1];
        if (litSingle) {
          if (c === "'" && n === "'") {
            k += 2;
            continue;
          }
          if (c === "'") {
            litSingle = false;
          }
          k += 1;
          continue;
        }
        if (litDouble) {
          if (c === '"' && n === '"') {
            k += 2;
            continue;
          }
          if (c === '"') {
            litDouble = false;
          }
          k += 1;
          continue;
        }
        if (litBacktick) {
          if (c === "`") {
            litBacktick = false;
          }
          k += 1;
          continue;
        }
        if (litBracket) {
          if (c === "]") {
            litBracket = false;
          }
          k += 1;
          continue;
        }
        if (c === "'") {
          litSingle = true;
          k += 1;
          continue;
        }
        if (c === '"') {
          litDouble = true;
          k += 1;
          continue;
        }
        if (c === "`") {
          litBacktick = true;
          k += 1;
          continue;
        }
        if (c === "[") {
          litBracket = true;
          k += 1;
          continue;
        }
        if (c === "(") {
          depth += 1;
        } else if (c === ")") {
          depth -= 1;
          if (depth === 0) {
            const expr = normalizeSql(sql.slice(j + 1, k));
            if (expr) {
              checks.push(expr);
            }
            i = k + 1;
            break;
          }
        }
        k += 1;
      }
      if (k >= sql.length) {
        i += 1;
      }
      continue;
    }
    i += 1;
  }

  return checks.sort((a, b) => a.localeCompare(b));
}

export function schemaHash(db: Database): string {
  const tables = listUserTables(db);
  const tableList = db.query("PRAGMA table_list").all() as TableListRow[];
  const tableOpts = new Map(
    tableList
      .filter((row) => row.schema === "main" && row.type === "table")
      .map((row) => [row.name, { withoutRowid: row.wr === 1, strict: row.strict === 1 }] as const),
  );

  const descriptor = tables.map((table) => {
    const tableDdl = tableDDL(db, table);
    const columnDefs = parseColumnDefinitionsFromCreateTable(tableDdl);
    const checks = extractCheckConstraints(tableDdl);
    return {
      tableSql: normalizeSql(tableDdl),
      table,
      options: tableOpts.get(table) ?? { withoutRowid: false, strict: false },
      columns: (db.query(`PRAGMA table_xinfo(${pragmaLiteral(table)})`).all() as TableXInfoRow[])
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
        const rows = db.query(`PRAGMA foreign_key_list(${pragmaLiteral(table)})`).all() as ForeignKeyRow[];
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
      indexes: (db.query(`PRAGMA index_list(${pragmaLiteral(table)})`).all() as IndexListRow[])
        .map((index) => {
          const indexSqlRow = db
            .query("SELECT sql FROM sqlite_master WHERE type='index' AND name=? LIMIT 1")
            .get(index.name) as { sql: string | null } | null;
          const indexColumns = (db.query(`PRAGMA index_xinfo(${pragmaLiteral(index.name)})`).all() as IndexXInfoRow[])
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
            sql: normalizeSql(indexSqlRow?.sql ?? null),
            columns: indexColumns,
          };
        })
        .sort((a, b) => a.name.localeCompare(b.name)),
      checks,
      triggers: (
        db
          .query("SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=? ORDER BY name ASC")
          .all(table) as Array<{ name: string; sql: string | null }>
      ).map((trigger) => ({
        name: trigger.name,
        sql: normalizeSql(trigger.sql),
      })),
    };
  });
  return sha256Hex(descriptor);
}

export function stateHash(db: Database): string {
  const tables = listUserTables(db);
  const state: Record<string, JsonObject[]> = {};
  for (const table of tables) {
    state[table] = fetchAllRows(db, table);
  }
  return sha256Hex(state);
}
