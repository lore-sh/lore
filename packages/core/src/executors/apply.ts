import type { Database } from "bun:sqlite";
import { getRow, getRows, runInSavepoint, tableExists } from "../db";
import { TossError } from "../errors";
import { type TableInfoRow, whereClauseFromRecord } from "../rows";
import { COLUMN_TYPE_PATTERN, createScanner, findMatchingParen, isWordBoundary, quoteIdentifier, splitTopLevelCommaList } from "../sql";
import type {
  AddCheckOperation,
  AddColumnOperation,
  AlterColumnTypeOperation,
  EncodedCell,
  ColumnDefinition,
  CreateTableOperation,
  DeleteOperation,
  DropColumnOperation,
  DropCheckOperation,
  DropTableOperation,
  InsertOperation,
  Operation,
  RestoreTableOperation,
  UpdateOperation,
} from "../types";

function serializeLiteral(value: string | number | boolean | null): string {
  if (value === null) {
    return "NULL";
  }
  if (typeof value === "string") {
    return `'${value.replaceAll("'", "''")}'`;
  }
  if (typeof value === "boolean") {
    return value ? "1" : "0";
  }
  return String(value);
}

function normalizeColumnType(value: string): string {
  const normalized = value.trim().toUpperCase();
  if (!COLUMN_TYPE_PATTERN.test(normalized)) {
    throw new TossError("INVALID_OPERATION", `Invalid column type: ${value}`);
  }
  return normalized;
}

function buildColumnSql(column: ColumnDefinition, forAddColumn = false): string {
  const tokens = [quoteIdentifier(column.name), normalizeColumnType(column.type)];

  if (column.primaryKey) {
    if (forAddColumn) {
      throw new TossError("UNSUPPORTED_OPERATION", "add_column does not support primaryKey");
    }
    tokens.push("PRIMARY KEY");
  }
  if (column.unique) {
    if (forAddColumn) {
      throw new TossError("UNSUPPORTED_OPERATION", "add_column does not support unique");
    }
    tokens.push("UNIQUE");
  }
  if (column.notNull) {
    tokens.push("NOT NULL");
  }
  if (Object.hasOwn(column, "default")) {
    tokens.push("DEFAULT", serializeLiteral(column.default ?? null));
  }

  return tokens.join(" ");
}

function executeCreateTable(db: Database, operation: CreateTableOperation): void {
  const columns = operation.columns.map((column) => buildColumnSql(column)).join(", ");
  db.run(`CREATE TABLE ${quoteIdentifier(operation.table)} (${columns})`);
}

function executeAddColumn(db: Database, operation: AddColumnOperation): void {
  const column = buildColumnSql(operation.column, true);
  db.run(`ALTER TABLE ${quoteIdentifier(operation.table)} ADD COLUMN ${column}`);
}

function executeInsert(db: Database, operation: InsertOperation): void {
  const keys = Object.keys(operation.values);
  if (keys.length === 0) {
    throw new TossError("INVALID_OPERATION", "insert values must not be empty");
  }

  const columns = keys.map((key) => quoteIdentifier(key)).join(", ");
  const placeholders = keys.map(() => "?").join(", ");
  const values = keys.map((key) => {
    const value = operation.values[key];
    if (value === undefined) {
      throw new TossError("INVALID_OPERATION", `insert value is missing for key: ${key}`);
    }
    return value;
  });

  db.query(`INSERT INTO ${quoteIdentifier(operation.table)} (${columns}) VALUES (${placeholders})`).run(...values);
}

function executeUpdate(db: Database, operation: UpdateOperation): void {
  const valueKeys = Object.keys(operation.values);
  if (valueKeys.length === 0) {
    throw new TossError("INVALID_OPERATION", "update values must not be empty");
  }

  const setParts: string[] = [];
  const setBindings: Array<string | number | boolean | null> = [];
  for (const key of valueKeys) {
    const value = operation.values[key];
    if (value === undefined) {
      throw new TossError("INVALID_OPERATION", `update value is missing for key: ${key}`);
    }
    setParts.push(`${quoteIdentifier(key)} = ?`);
    setBindings.push(value);
  }

  const where = whereClauseFromRecord(operation.where);
  db.query(`UPDATE ${quoteIdentifier(operation.table)} SET ${setParts.join(", ")} WHERE ${where.clause}`).run(
    ...setBindings,
    ...where.bindings,
  );
}

function executeDelete(db: Database, operation: DeleteOperation): void {
  const where = whereClauseFromRecord(operation.where);
  db.query(`DELETE FROM ${quoteIdentifier(operation.table)} WHERE ${where.clause}`).run(...where.bindings);
}

function executeDropTable(db: Database, operation: DropTableOperation): void {
  db.run(`DROP TABLE ${quoteIdentifier(operation.table)}`);
}

function executeDropColumn(db: Database, operation: DropColumnOperation): void {
  db.run(`ALTER TABLE ${quoteIdentifier(operation.table)} DROP COLUMN ${quoteIdentifier(operation.column)}`);
}

function skipWhitespace(sql: string, start: number): number {
  let i = start;
  while (i < sql.length && /\s/.test(sql[i]!)) {
    i += 1;
  }
  return i;
}

function readKeyword(sql: string, start: number): { value: string; end: number } | null {
  let i = start;
  let value = "";
  while (i < sql.length) {
    const ch = sql[i]!;
    const isAlpha = (ch >= "A" && ch <= "Z") || (ch >= "a" && ch <= "z") || ch === "_";
    if (!isAlpha) {
      break;
    }
    value += ch;
    i += 1;
  }
  if (value.length === 0) {
    return null;
  }
  return { value: value.toUpperCase(), end: i };
}

function expectKeyword(sql: string, start: number, expected: string): number {
  const kw = readKeyword(sql, start);
  if (!kw || kw.value !== expected) {
    throw new TossError("INVALID_OPERATION", `Expected ${expected} in CREATE TABLE`);
  }
  return kw.end;
}

function rewriteCreateTableName(ddlSql: string, newTable: string): string {
  let i = skipWhitespace(ddlSql, 0);
  i = expectKeyword(ddlSql, i, "CREATE");
  i = skipWhitespace(ddlSql, i);

  const maybeTemp = readKeyword(ddlSql, i);
  if (maybeTemp && (maybeTemp.value === "TEMP" || maybeTemp.value === "TEMPORARY")) {
    i = maybeTemp.end;
    i = skipWhitespace(ddlSql, i);
  }

  i = expectKeyword(ddlSql, i, "TABLE");
  i = skipWhitespace(ddlSql, i);

  const maybeIf = readKeyword(ddlSql, i);
  if (maybeIf?.value === "IF") {
    i = maybeIf.end;
    i = skipWhitespace(ddlSql, i);
    i = expectKeyword(ddlSql, i, "NOT");
    i = skipWhitespace(ddlSql, i);
    i = expectKeyword(ddlSql, i, "EXISTS");
    i = skipWhitespace(ddlSql, i);
  }

  const nameStart = i;
  const firstIdent = readIdentifierToken(ddlSql, i);
  i = firstIdent.end;
  i = skipWhitespace(ddlSql, i);
  let sourceTableName = firstIdent.name;
  if (ddlSql[i] === ".") {
    i += 1;
    i = skipWhitespace(ddlSql, i);
    const secondIdent = readIdentifierToken(ddlSql, i);
    sourceTableName = secondIdent.name;
    i = secondIdent.end;
  }
  const nameEnd = i;
  const rewritten = `${ddlSql.slice(0, nameStart)}${quoteIdentifier(newTable)}${ddlSql.slice(nameEnd)}`;
  return rewriteSelfReferentialForeignKeyTargets(rewritten, sourceTableName, newTable);
}

function readIdentifierToken(sql: string, start: number): { name: string; end: number } {
  const parsed = parseIdentifierToken(sql, start);
  if (!parsed) {
    throw new TossError("INVALID_OPERATION", "Malformed identifier in CREATE TABLE");
  }
  return { name: parsed.name, end: parsed.end };
}

function readEscapedQuotedIdentifier(
  sql: string,
  start: number,
  quote: string,
  label: string,
): { name: string; end: number; quoted: true } {
  let i = start + 1;
  let name = "";
  while (i < sql.length) {
    const cur = sql[i]!;
    if (cur === quote && sql[i + 1] === quote) {
      name += quote;
      i += 2;
      continue;
    }
    if (cur === quote) {
      return { name, end: i + 1, quoted: true };
    }
    name += cur;
    i += 1;
  }
  throw new TossError("INVALID_OPERATION", `Malformed ${label} identifier in CREATE TABLE`);
}

const ID_START = /^(?:[_$]|\p{ID_Start})$/u;
const ID_CONTINUE = /^(?:[_$]|\p{ID_Continue})$/u;

function parseIdentifierToken(sql: string, start: number): { name: string; end: number; quoted: boolean } | null {
  const ch = sql[start];
  if (!ch) {
    return null;
  }

  if (ch === '"') {
    return readEscapedQuotedIdentifier(sql, start, '"', "quoted");
  }
  if (ch === "'") {
    return readEscapedQuotedIdentifier(sql, start, "'", "single-quoted");
  }
  if (ch === "`") {
    return readEscapedQuotedIdentifier(sql, start, "`", "backtick");
  }
  if (ch === "[") {
    const end = sql.indexOf("]", start + 1);
    if (end < 0) {
      throw new TossError("INVALID_OPERATION", "Malformed bracket identifier in CREATE TABLE");
    }
    return { name: sql.slice(start + 1, end), end: end + 1, quoted: true };
  }

  const readCodePoint = (index: number): { value: string; next: number } | null => {
    if (index >= sql.length) {
      return null;
    }
    const codePoint = sql.codePointAt(index);
    if (codePoint === undefined) {
      return null;
    }
    const value = String.fromCodePoint(codePoint);
    return { value, next: index + value.length };
  };

  const first = readCodePoint(start);
  if (!first || !ID_START.test(first.value)) {
    return null;
  }

  let i = first.next;
  while (i < sql.length) {
    const next = readCodePoint(i);
    if (!next || !ID_CONTINUE.test(next.value)) {
      break;
    }
    i = next.next;
  }
  return { name: sql.slice(start, i), end: i, quoted: false };
}

function skipLeadingTrivia(sql: string, start: number): number {
  let i = start;
  while (i < sql.length) {
    i = skipWhitespace(sql, i);
    const ch = sql[i];
    const next = sql[i + 1];
    if (ch === "-" && next === "-") {
      i += 2;
      while (i < sql.length && sql[i] !== "\n") {
        i += 1;
      }
      continue;
    }
    if (ch === "/" && next === "*") {
      const end = sql.indexOf("*/", i + 2);
      if (end < 0) {
        throw new TossError("INVALID_OPERATION", "Malformed block comment in CREATE TABLE");
      }
      i = end + 2;
      continue;
    }
    break;
  }
  return i;
}

function equalsSqliteIdentifier(left: string, right: string): boolean {
  if (left === right) {
    return true;
  }
  const asciiIdentifier = /^[A-Za-z0-9_$]+$/;
  if (!asciiIdentifier.test(left) || !asciiIdentifier.test(right)) {
    return false;
  }
  return left.toUpperCase() === right.toUpperCase();
}

function rewriteSelfReferentialForeignKeyTargets(ddlSql: string, sourceTable: string, newTable: string): string {
  const replacements: Array<{ start: number; end: number; value: string }> = [];
  const scanner = createScanner(ddlSql);

  while (scanner.pos < ddlSql.length) {
    if (scanner.skipInterior()) {
      continue;
    }

    if (
      scanner.pos + 10 <= ddlSql.length &&
      ddlSql.slice(scanner.pos, scanner.pos + 10).toUpperCase() === "REFERENCES" &&
      isWordBoundary(ddlSql[scanner.pos - 1]) &&
      isWordBoundary(ddlSql[scanner.pos + 10])
    ) {
      let j = skipLeadingTrivia(ddlSql, scanner.pos + 10);
      const firstStart = j;
      const first = parseIdentifierToken(ddlSql, firstStart);
      if (!first) {
        scanner.advance(10);
        continue;
      }

      let schemaName: string | null = null;
      let targetName = first.name;
      let targetStart = firstStart;
      let targetEnd = first.end;
      j = skipLeadingTrivia(ddlSql, first.end);
      if (ddlSql[j] === ".") {
        j += 1;
        j = skipLeadingTrivia(ddlSql, j);
        const secondStart = j;
        const second = parseIdentifierToken(ddlSql, secondStart);
        if (second) {
          schemaName = first.name;
          targetName = second.name;
          targetStart = secondStart;
          targetEnd = second.end;
          j = second.end;
        }
      }

      const matchesSelf =
        equalsSqliteIdentifier(targetName, sourceTable) &&
        (schemaName === null || equalsSqliteIdentifier(schemaName, "main"));
      if (matchesSelf) {
        replacements.push({
          start: targetStart,
          end: targetEnd,
          value: quoteIdentifier(newTable),
        });
      }
      scanner.pos = j;
      continue;
    }

    scanner.advance();
  }

  if (replacements.length === 0) {
    return ddlSql;
  }

  let rewritten = ddlSql;
  for (const replacement of replacements.toReversed()) {
    rewritten = `${rewritten.slice(0, replacement.start)}${replacement.value}${rewritten.slice(replacement.end)}`;
  }
  return rewritten;
}

function findCreateTablePayloadRange(ddlSql: string): { start: number; end: number } {
  const scanner = createScanner(ddlSql);
  let open = -1;
  let depth = 0;

  while (scanner.pos < ddlSql.length) {
    if (scanner.skipInterior()) {
      continue;
    }
    const ch = ddlSql[scanner.pos]!;
    if (ch === "(") {
      if (open < 0) {
        open = scanner.pos;
        depth = 1;
      } else {
        depth += 1;
      }
    } else if (ch === ")" && open >= 0) {
      depth -= 1;
      if (depth === 0) {
        return { start: open + 1, end: scanner.pos };
      }
    }
    scanner.advance();
  }

  throw new TossError("INVALID_OPERATION", "Malformed CREATE TABLE statement: column list not found");
}

function normalizeSqlFragment(sql: string): string {
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
    if (
      prev === " " ||
      prev === "(" ||
      prev === "," ||
      nextChar === "(" ||
      nextChar === ")" ||
      nextChar === "," ||
      nextChar === ";"
    ) {
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
    out += ch >= "a" && ch <= "z" ? ch.toUpperCase() : ch;
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

function findConstraintStart(segment: string, from: number): number {
  const constraintKeywords = new Set([
    "CONSTRAINT",
    "PRIMARY",
    "NOT",
    "UNIQUE",
    "CHECK",
    "DEFAULT",
    "COLLATE",
    "REFERENCES",
    "GENERATED",
    "AS",
  ]);
  const scanner = createScanner(segment);
  scanner.pos = from;
  let depth = 0;

  while (scanner.pos < segment.length) {
    if (scanner.skipInterior()) {
      continue;
    }
    const ch = segment[scanner.pos]!;

    if (ch === "(") {
      depth += 1;
      scanner.advance();
      continue;
    }
    if (ch === ")") {
      if (depth > 0) {
        depth -= 1;
      }
      scanner.advance();
      continue;
    }

    if (depth === 0 && /[A-Za-z_]/.test(ch) && isWordBoundary(segment[scanner.pos - 1])) {
      let j = scanner.pos + 1;
      while (j < segment.length && /[A-Za-z_]/.test(segment[j]!)) {
        j += 1;
      }
      const word = segment.slice(scanner.pos, j).toUpperCase();
      if (constraintKeywords.has(word) && isWordBoundary(segment[j])) {
        return scanner.pos;
      }
      scanner.pos = j;
      continue;
    }

    scanner.advance();
  }

  return segment.length;
}

function rewriteColumnSegmentType(segment: string, identifierEnd: number, newType: string): string {
  let typeStart = identifierEnd;
  while (typeStart < segment.length && /\s/.test(segment[typeStart]!)) {
    typeStart += 1;
  }

  if (typeStart >= segment.length) {
    return `${segment.trimEnd()} ${newType}`;
  }

  const constraintStart = findConstraintStart(segment, typeStart);
  const prefix = segment.slice(0, typeStart).trimEnd();
  const suffix = segment.slice(constraintStart).trimStart();
  if (suffix.length === 0) {
    return `${prefix} ${newType}`;
  }
  return `${prefix} ${newType} ${suffix}`;
}

function rewriteColumnTypeInCreateTable(ddlSql: string, column: string, newType: string): string {
  const payloadRange = findCreateTablePayloadRange(ddlSql);
  const payload = ddlSql.slice(payloadRange.start, payloadRange.end);
  const segments = splitTopLevelCommaList(payload);
  const tableConstraintLead = new Set(["CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN"]);
  let rewritten = false;

  const rewrittenSegments = segments.map((segment) => {
    const lead = parseIdentifierToken(segment, skipLeadingTrivia(segment, 0));
    if (!lead) {
      return segment;
    }
    if (!lead.quoted && tableConstraintLead.has(lead.name.toUpperCase())) {
      return segment;
    }
    if (lead.name !== column) {
      return segment;
    }
    rewritten = true;
    return rewriteColumnSegmentType(segment, lead.end, newType);
  });

  if (!rewritten) {
    throw new TossError("INVALID_OPERATION", `Column does not exist in CREATE TABLE SQL: ${column}`);
  }

  return `${ddlSql.slice(0, payloadRange.start)}${rewrittenSegments.join(",")}${ddlSql.slice(payloadRange.end)}`;
}

interface SecondaryObjectRow {
  type: "index" | "trigger";
  name: string;
  sql: string;
}

interface MutableTableState {
  tableInfo: TableInfoRow[];
  resolvedTableName: string;
  quotedTableName: string;
  tableDdlSql: string;
  secondaryObjects: SecondaryObjectRow[];
}

interface SqliteSequenceSnapshot {
  seqLiteral: string;
}

function resolveMutableTableState(db: Database, table: string): MutableTableState {
  const requestedTableName = quoteIdentifier(table);
  const tableInfo = getRows<TableInfoRow>(db, `PRAGMA table_info(${requestedTableName})`);
  if (tableInfo.length === 0) {
    throw new TossError("INVALID_OPERATION", `Table does not exist: ${table}`);
  }

  const tableDdlRow = getRow<{ name: string; sql: string | null }>(
    db,
    "SELECT name, sql FROM sqlite_master WHERE type='table' AND name = ? COLLATE NOCASE LIMIT 1",
    table,
  );
  if (!tableDdlRow?.sql) {
    throw new TossError("INVALID_OPERATION", `Table DDL is not available: ${table}`);
  }

  const secondaryObjects = getRows<SecondaryObjectRow>(
    db,
    `
      SELECT type, name, sql
      FROM sqlite_master
      WHERE tbl_name = ? AND type IN ('index', 'trigger') AND sql IS NOT NULL
      `,
    tableDdlRow.name,
  );

  return {
    tableInfo,
    resolvedTableName: tableDdlRow.name,
    quotedTableName: quoteIdentifier(tableDdlRow.name),
    tableDdlSql: tableDdlRow.sql,
    secondaryObjects,
  };
}

function captureSqliteSequenceSnapshot(db: Database, tableName: string): SqliteSequenceSnapshot | null {
  if (!tableExists(db, "sqlite_sequence")) {
    return null;
  }
  const row = getRow<{ seqLiteral: string | null }>(
    db,
    "SELECT quote(seq) AS seqLiteral FROM sqlite_sequence WHERE name = ? LIMIT 1",
    tableName,
  );
  if (!row || typeof row.seqLiteral !== "string") {
    return null;
  }
  return { seqLiteral: row.seqLiteral };
}

function restoreSqliteSequenceSnapshot(db: Database, tableName: string, snapshot: SqliteSequenceSnapshot | null): void {
  if (!snapshot || !tableExists(db, "sqlite_sequence")) {
    return;
  }
  db.query("DELETE FROM sqlite_sequence WHERE name = ?").run(tableName);
  db.run(`INSERT INTO sqlite_sequence(name, seq) VALUES (${serializeLiteral(tableName)}, ${snapshot.seqLiteral})`);
}

function rebuildTableWithRewrittenDdl(
  db: Database,
  state: MutableTableState,
  rewrittenDdl: string,
  options: { savepointName: string; selectList?: string | undefined },
): void {
  const tempTable = `__toss_tmp_${state.resolvedTableName}_${crypto.randomUUID().replaceAll("-", "")}`;
  const quotedTempTable = quoteIdentifier(tempTable);
  const columnList = state.tableInfo.map((column) => quoteIdentifier(column.name)).join(", ");
  const selectList = options.selectList ?? columnList;

  runInSavepoint(db, options.savepointName, () => {
    const sequenceSnapshot = captureSqliteSequenceSnapshot(db, state.resolvedTableName);
    db.run(rewriteCreateTableName(rewrittenDdl, tempTable));
    db.run(`INSERT INTO ${quotedTempTable} (${columnList}) SELECT ${selectList} FROM ${state.quotedTableName}`);
    db.run(`DROP TABLE ${state.quotedTableName}`);
    db.run(`ALTER TABLE ${quotedTempTable} RENAME TO ${state.quotedTableName}`);
    restoreSqliteSequenceSnapshot(db, state.resolvedTableName, sequenceSnapshot);

    for (const object of state.secondaryObjects) {
      db.run(object.sql);
    }
  });
}

function extractTableCheckExpression(segment: string): string | null {
  let i = skipLeadingTrivia(segment, 0);
  let keyword = readKeyword(segment, i);
  if (!keyword) {
    return null;
  }

  if (keyword.value === "CONSTRAINT") {
    i = skipLeadingTrivia(segment, keyword.end);
    const constraintName = parseIdentifierToken(segment, i);
    if (!constraintName) {
      return null;
    }
    i = skipLeadingTrivia(segment, constraintName.end);
    keyword = readKeyword(segment, i);
    if (!keyword) {
      return null;
    }
  }

  if (keyword.value !== "CHECK") {
    return null;
  }

  i = skipLeadingTrivia(segment, keyword.end);
  if (segment[i] !== "(") {
    return null;
  }

  const close = findMatchingParen(segment, i);
  if (close < 0) {
    return null;
  }

  const expression = segment.slice(i + 1, close);
  return normalizeSqlFragment(expression);
}

function rewriteAddCheckInCreateTable(ddlSql: string, expression: string): string {
  const payloadRange = findCreateTablePayloadRange(ddlSql);
  const payload = ddlSql.slice(payloadRange.start, payloadRange.end);
  const segments = splitTopLevelCommaList(payload);
  const normalizedExpression = normalizeSqlFragment(expression);

  for (const segment of segments) {
    const existing = extractTableCheckExpression(segment);
    if (existing && existing === normalizedExpression) {
      throw new TossError("INVALID_OPERATION", "Equivalent CHECK constraint already exists");
    }
  }

  const rewrittenSegments = [...segments, ` CHECK (${expression.trim()})`];
  return `${ddlSql.slice(0, payloadRange.start)}${rewrittenSegments.join(",")}${ddlSql.slice(payloadRange.end)}`;
}

function rewriteDropCheckInCreateTable(ddlSql: string, expression: string): string {
  const payloadRange = findCreateTablePayloadRange(ddlSql);
  const payload = ddlSql.slice(payloadRange.start, payloadRange.end);
  const segments = splitTopLevelCommaList(payload);
  const normalizedExpression = normalizeSqlFragment(expression);

  let removed = 0;
  const rewrittenSegments = segments.filter((segment) => {
    const existing = extractTableCheckExpression(segment);
    if (!existing || existing !== normalizedExpression) {
      return true;
    }
    removed += 1;
    return false;
  });

  if (removed === 0) {
    throw new TossError("INVALID_OPERATION", "CHECK constraint not found");
  }

  return `${ddlSql.slice(0, payloadRange.start)}${rewrittenSegments.join(",")}${ddlSql.slice(payloadRange.end)}`;
}

function executeAddCheck(db: Database, operation: AddCheckOperation): void {
  const state = resolveMutableTableState(db, operation.table);
  const rewrittenDdl = rewriteAddCheckInCreateTable(state.tableDdlSql, operation.expression);
  rebuildTableWithRewrittenDdl(db, state, rewrittenDdl, { savepointName: "toss_add_check" });
}

function executeDropCheck(db: Database, operation: DropCheckOperation): void {
  const state = resolveMutableTableState(db, operation.table);
  const rewrittenDdl = rewriteDropCheckInCreateTable(state.tableDdlSql, operation.expression);
  rebuildTableWithRewrittenDdl(db, state, rewrittenDdl, { savepointName: "toss_drop_check" });
}

function executeRestoreTable(db: Database, operation: RestoreTableOperation): void {
  const tmpTable = `__toss_restore_${operation.table}_${crypto.randomUUID().replaceAll("-", "")}`;
  const quotedTmp = quoteIdentifier(tmpTable);
  const quotedTable = quoteIdentifier(operation.table);
  const isSqlStorageClass = (value: unknown): value is EncodedCell["storageClass"] =>
    value === "null" || value === "integer" || value === "real" || value === "text" || value === "blob";
  const rowForRestore = (value: unknown): Record<string, unknown> => {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw new TossError("INVALID_OPERATION", "restore_table row must be an object");
    }
    return value as Record<string, unknown>;
  };

  const literalForRestoreCell = (value: unknown): string => {
    if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      return serializeLiteral(value);
    }
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw new TossError("INVALID_OPERATION", "restore_table row contains unsupported encoded value");
    }
    const storageClass = "storageClass" in value ? value.storageClass : undefined;
    const sqlLiteral = "sqlLiteral" in value ? value.sqlLiteral : undefined;
    if (isSqlStorageClass(storageClass) && typeof sqlLiteral === "string") {
      return sqlLiteral;
    }
    throw new TossError("INVALID_OPERATION", "restore_table row contains unsupported encoded value");
  };

  runInSavepoint(db, "toss_restore_table", () => {
    const sequenceSnapshot = captureSqliteSequenceSnapshot(db, operation.table);
    db.run(rewriteCreateTableName(operation.ddlSql, tmpTable));
    const first = operation.rows?.[0];
    if (first) {
      const firstRow = rowForRestore(first);
      const columns = Object.keys(firstRow).sort((a, b) => a.localeCompare(b));
      if (columns.length === 0) {
        throw new TossError("INVALID_OPERATION", "restore_table row must include at least one column");
      }
      const expected = new Set(columns);
      const columnSql = columns.map((column) => quoteIdentifier(column)).join(", ");
      for (const rawRow of operation.rows!) {
        const row = rowForRestore(rawRow);
        const rowColumns = Object.keys(row);
        if (rowColumns.length !== columns.length || rowColumns.some((column) => !expected.has(column))) {
          throw new TossError("INVALID_OPERATION", "restore_table row column set does not match snapshot");
        }
        const valuesSql = columns.map((column) => literalForRestoreCell(row[column])).join(", ");
        db.run(`INSERT INTO ${quotedTmp} (${columnSql}) VALUES (${valuesSql})`);
      }
    }

    db.run(`DROP TABLE IF EXISTS ${quotedTable}`);
    db.run(`ALTER TABLE ${quotedTmp} RENAME TO ${quotedTable}`);
    restoreSqliteSequenceSnapshot(db, operation.table, sequenceSnapshot);
    for (const object of operation.secondaryObjects ?? []) {
      db.run(object.sql);
    }
  });
}

function executeAlterColumnType(db: Database, operation: AlterColumnTypeOperation): void {
  const newType = normalizeColumnType(operation.newType);
  const state = resolveMutableTableState(db, operation.table);

  const target = state.tableInfo.find((column) => column.name === operation.column);
  if (!target) {
    throw new TossError("INVALID_OPERATION", `Column does not exist: ${operation.table}.${operation.column}`);
  }

  const rewrittenDdl = rewriteColumnTypeInCreateTable(state.tableDdlSql, operation.column, newType);
  const selectList = state.tableInfo
    .map((column) => {
      const quoted = quoteIdentifier(column.name);
      if (column.name === operation.column) {
        return `CAST(${quoted} AS ${newType}) AS ${quoted}`;
      }
      return quoted;
    })
    .join(", ");

  rebuildTableWithRewrittenDdl(db, state, rewrittenDdl, {
    savepointName: "toss_alter_column_type",
    selectList,
  });
}

export function executeOperation(db: Database, operation: Operation): void {
  switch (operation.type) {
    case "create_table":
      executeCreateTable(db, operation);
      return;
    case "add_column":
      executeAddColumn(db, operation);
      return;
    case "insert":
      executeInsert(db, operation);
      return;
    case "update":
      executeUpdate(db, operation);
      return;
    case "delete":
      executeDelete(db, operation);
      return;
    case "drop_table":
      executeDropTable(db, operation);
      return;
    case "drop_column":
      executeDropColumn(db, operation);
      return;
    case "alter_column_type":
      executeAlterColumnType(db, operation);
      return;
    case "add_check":
      executeAddCheck(db, operation);
      return;
    case "drop_check":
      executeDropCheck(db, operation);
      return;
    case "restore_table":
      executeRestoreTable(db, operation);
      return;
    default:
      throw new TossError("UNSUPPORTED_OPERATION", `Unsupported operation type: ${(operation as Operation).type}`);
  }
}
