import type { Database } from "bun:sqlite";
import { TossError } from "../errors";
import { type TableInfoRow, whereClauseFromRecord } from "../rows";
import { COLUMN_TYPE_PATTERN, quoteIdentifier } from "../sql";
import type {
  AddColumnOperation,
  AlterColumnTypeOperation,
  EncodedCell,
  ColumnDefinition,
  CreateTableOperation,
  DeleteOperation,
  DropColumnOperation,
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

function parseIdentifierToken(sql: string, start: number): { name: string; end: number; quoted: boolean } | null {
  const ch = sql[start];
  if (!ch) {
    return null;
  }

  if (ch === '"') {
    let i = start + 1;
    let name = "";
    while (i < sql.length) {
      const cur = sql[i]!;
      const next = sql[i + 1];
      if (cur === '"' && next === '"') {
        name += '"';
        i += 2;
        continue;
      }
      if (cur === '"') {
        return { name, end: i + 1, quoted: true };
      }
      name += cur;
      i += 1;
    }
    throw new TossError("INVALID_OPERATION", "Malformed quoted identifier in CREATE TABLE");
  }

  if (ch === "'") {
    let i = start + 1;
    let name = "";
    while (i < sql.length) {
      const cur = sql[i]!;
      const next = sql[i + 1];
      if (cur === "'" && next === "'") {
        name += "'";
        i += 2;
        continue;
      }
      if (cur === "'") {
        return { name, end: i + 1, quoted: true };
      }
      name += cur;
      i += 1;
    }
    throw new TossError("INVALID_OPERATION", "Malformed single-quoted identifier in CREATE TABLE");
  }

  if (ch === "`") {
    let i = start + 1;
    let name = "";
    while (i < sql.length) {
      const cur = sql[i]!;
      const next = sql[i + 1];
      if (cur === "`" && next === "`") {
        name += "`";
        i += 2;
        continue;
      }
      if (cur === "`") {
        return { name, end: i + 1, quoted: true };
      }
      name += cur;
      i += 1;
    }
    throw new TossError("INVALID_OPERATION", "Malformed backtick identifier in CREATE TABLE");
  }

  if (ch === "[") {
    const end = sql.indexOf("]", start + 1);
    if (end < 0) {
      throw new TossError("INVALID_OPERATION", "Malformed bracket identifier in CREATE TABLE");
    }
    return { name: sql.slice(start + 1, end), end: end + 1, quoted: true };
  }

  const isIdentifierStartChar = (ch: string): boolean => /^(?:[_$]|\p{ID_Start})$/u.test(ch);
  const isIdentifierContinueChar = (ch: string): boolean => /^(?:[_$]|\p{ID_Continue})$/u.test(ch);
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
  if (!first || !isIdentifierStartChar(first.value)) {
    return null;
  }

  let i = first.next;
  while (i < sql.length) {
    const next = readCodePoint(i);
    if (!next || !isIdentifierContinueChar(next.value)) {
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

function isWordBoundary(ch: string | undefined): boolean {
  if (!ch) {
    return true;
  }
  return !/[A-Za-z0-9_]/.test(ch);
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
  let i = 0;
  let inSingle = false;
  let inDouble = false;
  let inBacktick = false;
  let inBracket = false;
  let inLineComment = false;
  let inBlockComment = false;

  while (i < ddlSql.length) {
    const ch = ddlSql[i]!;
    const next = ddlSql[i + 1];

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

    if (
      i + 10 <= ddlSql.length &&
      ddlSql.slice(i, i + 10).toUpperCase() === "REFERENCES" &&
      isWordBoundary(ddlSql[i - 1]) &&
      isWordBoundary(ddlSql[i + 10])
    ) {
      let j = skipLeadingTrivia(ddlSql, i + 10);
      const firstStart = j;
      const first = parseIdentifierToken(ddlSql, firstStart);
      if (!first) {
        i += 10;
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
      i = j;
      continue;
    }

    i += 1;
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
  let i = 0;
  let open = -1;
  let depth = 0;
  let inSingle = false;
  let inDouble = false;
  let inBacktick = false;
  let inBracket = false;
  let inLineComment = false;
  let inBlockComment = false;

  while (i < ddlSql.length) {
    const ch = ddlSql[i]!;
    const next = ddlSql[i + 1];

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
      if (open < 0) {
        open = i;
        depth = 1;
      } else {
        depth += 1;
      }
      i += 1;
      continue;
    }
    if (ch === ")" && open >= 0) {
      depth -= 1;
      if (depth === 0) {
        return { start: open + 1, end: i };
      }
      i += 1;
      continue;
    }

    i += 1;
  }

  throw new TossError("INVALID_OPERATION", "Malformed CREATE TABLE statement: column list not found");
}

function splitTopLevelCommaList(sql: string): string[] {
  const segments: string[] = [];
  let start = 0;
  let i = 0;
  let depth = 0;
  let inSingle = false;
  let inDouble = false;
  let inBacktick = false;
  let inBracket = false;
  let inLineComment = false;
  let inBlockComment = false;

  while (i < sql.length) {
    const ch = sql[i]!;
    const next = sql[i + 1];

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
      segments.push(sql.slice(start, i));
      start = i + 1;
      i += 1;
      continue;
    }

    i += 1;
  }

  const tail = sql.slice(start);
  if (tail.trim().length > 0) {
    segments.push(tail);
  }
  return segments;
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
  let i = from;
  let depth = 0;
  let inSingle = false;
  let inDouble = false;
  let inBacktick = false;
  let inBracket = false;
  let inLineComment = false;
  let inBlockComment = false;

  while (i < segment.length) {
    const ch = segment[i]!;
    const next = segment[i + 1];

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

    if (depth === 0 && /[A-Za-z_]/.test(ch) && isWordBoundary(segment[i - 1])) {
      let j = i + 1;
      while (j < segment.length && /[A-Za-z_]/.test(segment[j]!)) {
        j += 1;
      }
      const word = segment.slice(i, j).toUpperCase();
      if (constraintKeywords.has(word) && isWordBoundary(segment[j])) {
        return i;
      }
      i = j;
      continue;
    }

    i += 1;
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

function executeRestoreTable(db: Database, operation: RestoreTableOperation): void {
  const tmpTable = `__toss_restore_${operation.table}_${crypto.randomUUID().replaceAll("-", "")}`;
  const quotedTmp = quoteIdentifier(tmpTable);
  const quotedTable = quoteIdentifier(operation.table);

  const literalForRestoreCell = (value: unknown): string => {
    if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      return serializeLiteral(value);
    }
    const maybeCell = value as EncodedCell;
    if (
      maybeCell &&
      typeof maybeCell === "object" &&
      typeof maybeCell.storageClass === "string" &&
      typeof maybeCell.sqlLiteral === "string"
    ) {
      return maybeCell.sqlLiteral;
    }
    throw new TossError("INVALID_OPERATION", "restore_table row contains unsupported encoded value");
  };

  db.run("SAVEPOINT toss_restore_table");
  try {
    db.run(rewriteCreateTableName(operation.ddlSql, tmpTable));
    if (operation.rows && operation.rows.length > 0) {
      const first = operation.rows[0];
      if (first) {
        const columns = Object.keys(first).sort((a, b) => a.localeCompare(b));
        if (columns.length > 0) {
          const columnSql = columns.map((column) => quoteIdentifier(column)).join(", ");
          for (const row of operation.rows) {
            const valuesSql = columns
              .map((column) => {
                const value = row[column];
                return literalForRestoreCell(value);
              })
              .join(", ");
            db.run(`INSERT INTO ${quotedTmp} (${columnSql}) VALUES (${valuesSql})`);
          }
        }
      }
    }

    db.run(`DROP TABLE IF EXISTS ${quotedTable}`);
    db.run(`ALTER TABLE ${quotedTmp} RENAME TO ${quotedTable}`);
    if (operation.secondaryObjects && operation.secondaryObjects.length > 0) {
      for (const object of operation.secondaryObjects) {
        db.run(object.sql);
      }
    }
    db.run("RELEASE toss_restore_table");
  } catch (error) {
    try {
      db.run(`DROP TABLE IF EXISTS ${quotedTmp}`);
    } catch {
      // no-op
    }
    db.run("ROLLBACK TO toss_restore_table");
    db.run("RELEASE toss_restore_table");
    throw error;
  }
}

function executeAlterColumnType(db: Database, operation: AlterColumnTypeOperation): void {
  const newType = normalizeColumnType(operation.newType);
  const requestedTableName = quoteIdentifier(operation.table);
  const tableInfo = db.query(`PRAGMA table_info(${requestedTableName})`).all() as TableInfoRow[];
  if (tableInfo.length === 0) {
    throw new TossError("INVALID_OPERATION", `Table does not exist: ${operation.table}`);
  }

  const target = tableInfo.find((column) => column.name === operation.column);
  if (!target) {
    throw new TossError("INVALID_OPERATION", `Column does not exist: ${operation.table}.${operation.column}`);
  }

  const tableDdlRow = db
    .query("SELECT name, sql FROM sqlite_master WHERE type='table' AND name = ? COLLATE NOCASE LIMIT 1")
    .get(operation.table) as { name: string; sql: string | null } | null;
  if (!tableDdlRow?.sql) {
    throw new TossError("INVALID_OPERATION", `Table DDL is not available: ${operation.table}`);
  }
  const resolvedTableName = tableDdlRow.name;
  const tableName = quoteIdentifier(resolvedTableName);
  const rewrittenDdl = rewriteColumnTypeInCreateTable(tableDdlRow.sql, operation.column, newType);

  const secondaryObjects = db
    .query(
      `
      SELECT type, name, sql
      FROM sqlite_master
      WHERE tbl_name = ? AND type IN ('index', 'trigger') AND sql IS NOT NULL
      `,
    )
    .all(resolvedTableName) as Array<{ type: "index" | "trigger"; name: string; sql: string }>;

  const tempTable = `__toss_tmp_${operation.table}_${crypto.randomUUID().replaceAll("-", "")}`;
  const quotedTempTable = quoteIdentifier(tempTable);

  const columnList = tableInfo.map((column) => quoteIdentifier(column.name)).join(", ");
  const selectList = tableInfo
    .map((column) => {
      const quoted = quoteIdentifier(column.name);
      if (column.name === operation.column) {
        return `CAST(${quoted} AS ${newType}) AS ${quoted}`;
      }
      return quoted;
    })
    .join(", ");

  db.run("SAVEPOINT toss_alter_column_type");
  try {
    db.run(rewriteCreateTableName(rewrittenDdl, tempTable));
    db.run(`INSERT INTO ${quotedTempTable} (${columnList}) SELECT ${selectList} FROM ${tableName}`);
    db.run(`DROP TABLE ${tableName}`);
    db.run(`ALTER TABLE ${quotedTempTable} RENAME TO ${tableName}`);

    for (const object of secondaryObjects) {
      db.run(object.sql);
    }
    db.run("RELEASE toss_alter_column_type");
  } catch (error) {
    try {
      db.run(`DROP TABLE IF EXISTS ${quotedTempTable}`);
    } catch {
      // no-op
    }
    db.run("ROLLBACK TO toss_alter_column_type");
    db.run("RELEASE toss_alter_column_type");
    throw error;
  }
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
    case "restore_table":
      executeRestoreTable(db, operation);
      return;
    default:
      throw new TossError("UNSUPPORTED_OPERATION", `Unsupported operation type: ${(operation as Operation).type}`);
  }
}

export function executeOperations(db: Database, operations: Operation[]): void {
  for (const operation of operations) {
    executeOperation(db, operation);
  }
}
