import { z } from "zod";
import { CodedError } from "./error";

export const IDENTIFIER_PATTERN = /^(?:[_$]|\p{ID_Start})(?:[_$]|\p{ID_Continue})*$/u;
export const COLUMN_TYPE_PATTERN = /^[A-Za-z][A-Za-z0-9_]*(\s*\(\s*\d+\s*(,\s*\d+\s*)?\))?$/;

export function quoteIdentifier(value: string, options: { unsafe?: boolean } = {}): string {
  if (!options.unsafe && !IDENTIFIER_PATTERN.test(value)) {
    throw new CodedError("INVALID_IDENTIFIER", `Invalid identifier: ${value}`);
  }
  return `"${value.replaceAll('"', '""')}"`;
}

export function isWordBoundary(char: string | undefined): boolean {
  if (!char) {
    return true;
  }
  return !/[A-Za-z0-9_]/.test(char);
}

export function asciiCaseFold(value: string): string {
  return value.replace(/[A-Z]/g, (ch) => ch.toLowerCase());
}

const ID_START = /^(?:[_$]|\p{ID_Start})$/u;
const ID_CONTINUE = /^(?:[_$]|\p{ID_Continue})$/u;

function readCodePoint(sql: string, index: number): { value: string; next: number } | null {
  if (index >= sql.length) {
    return null;
  }
  const codePoint = sql.codePointAt(index);
  if (codePoint === undefined) {
    return null;
  }
  const value = String.fromCodePoint(codePoint);
  return { value, next: index + value.length };
}

function readBareIdentifierToken(sql: string, start: number): { name: string; end: number } | null {
  const first = readCodePoint(sql, start);
  if (!first || !ID_START.test(first.value)) {
    return null;
  }
  let i = first.next;
  while (i < sql.length) {
    const next = readCodePoint(sql, i);
    if (!next || !ID_CONTINUE.test(next.value)) {
      break;
    }
    i = next.next;
  }
  return { name: sql.slice(start, i), end: i };
}

function readEscapedIdentifierToken(sql: string, start: number, quote: string): { name: string; end: number } | null {
  if (sql[start] !== quote) {
    return null;
  }
  let i = start + 1;
  let name = "";
  while (i < sql.length) {
    const ch = sql[i]!;
    if (ch === quote && sql[i + 1] === quote) {
      name += quote;
      i += 2;
      continue;
    }
    if (ch === quote) {
      return { name, end: i + 1 };
    }
    name += ch;
    i += 1;
  }
  return null;
}

function readBracketIdentifierToken(sql: string, start: number): { name: string; end: number } | null {
  if (sql[start] !== "[") {
    return null;
  }
  const end = sql.indexOf("]", start + 1);
  if (end < 0) {
    return null;
  }
  return { name: sql.slice(start + 1, end), end: end + 1 };
}

export function sqlMentionsIdentifier(sql: string, identifier: string): boolean {
  const target = asciiCaseFold(identifier);
  let i = 0;
  let inSingle = false;
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

    const doubleQuoted = readEscapedIdentifierToken(sql, i, '"');
    if (doubleQuoted) {
      if (asciiCaseFold(doubleQuoted.name) === target) {
        return true;
      }
      i = doubleQuoted.end;
      continue;
    }

    const backtickQuoted = readEscapedIdentifierToken(sql, i, "`");
    if (backtickQuoted) {
      if (asciiCaseFold(backtickQuoted.name) === target) {
        return true;
      }
      i = backtickQuoted.end;
      continue;
    }

    const bracketQuoted = readBracketIdentifierToken(sql, i);
    if (bracketQuoted) {
      if (asciiCaseFold(bracketQuoted.name) === target) {
        return true;
      }
      i = bracketQuoted.end;
      continue;
    }

    const bare = readBareIdentifierToken(sql, i);
    if (bare) {
      if (asciiCaseFold(bare.name) === target) {
        return true;
      }
      i = bare.end;
      continue;
    }

    i += 1;
  }

  return false;
}

const SINGLE = 1;
const DOUBLE = 2;
const BACKTICK = 4;
const BRACKET = 8;
const LINE = 16;
const BLOCK = 32;

export function createScanner(sql: string) {
  let pos = 0;
  let state = 0;

  function skipInterior(): boolean {
    const ch = sql[pos];
    if (ch === undefined) return false;
    const next = sql[pos + 1];

    if (state & LINE) {
      if (ch === "\n") state &= ~LINE;
      pos += 1;
      return true;
    }
    if (state & BLOCK) {
      if (ch === "*" && next === "/") {
        state &= ~BLOCK;
        pos += 2;
      } else {
        pos += 1;
      }
      return true;
    }
    if (state & SINGLE) {
      if (ch === "'" && next === "'") pos += 2;
      else {
        if (ch === "'") state &= ~SINGLE;
        pos += 1;
      }
      return true;
    }
    if (state & DOUBLE) {
      if (ch === '"' && next === '"') pos += 2;
      else {
        if (ch === '"') state &= ~DOUBLE;
        pos += 1;
      }
      return true;
    }
    if (state & BACKTICK) {
      if (ch === "`") state &= ~BACKTICK;
      pos += 1;
      return true;
    }
    if (state & BRACKET) {
      if (ch === "]") state &= ~BRACKET;
      pos += 1;
      return true;
    }

    if (ch === "-" && next === "-") { state |= LINE; pos += 2; return true; }
    if (ch === "/" && next === "*") { state |= BLOCK; pos += 2; return true; }
    if (ch === "'") { state |= SINGLE; pos += 1; return true; }
    if (ch === '"') { state |= DOUBLE; pos += 1; return true; }
    if (ch === "`") { state |= BACKTICK; pos += 1; return true; }
    if (ch === "[") { state |= BRACKET; pos += 1; return true; }

    return false;
  }

  return {
    get pos() { return pos; },
    set pos(v: number) { pos = v; },
    get insideLiteral() { return state !== 0; },
    skipInterior,
    advance(n = 1) { pos += n; },
  };
}

export function findMatchingParen(sql: string, openIndex: number): number {
  const s = createScanner(sql);
  s.pos = openIndex;
  let depth = 0;

  while (s.pos < sql.length) {
    if (s.skipInterior()) continue;
    const ch = sql[s.pos];
    if (ch === "(") depth += 1;
    else if (ch === ")") {
      depth -= 1;
      if (depth === 0) return s.pos;
    }
    s.advance();
  }
  return -1;
}

export function splitTopLevelCommaList(sql: string): string[] {
  const parts: string[] = [];
  const s = createScanner(sql);
  let start = 0;
  let depth = 0;

  while (s.pos < sql.length) {
    if (s.skipInterior()) continue;
    const ch = sql[s.pos];
    if (ch === "(") depth += 1;
    else if (ch === ")") { if (depth > 0) depth -= 1; }
    else if (ch === "," && depth === 0) {
      parts.push(sql.slice(start, s.pos));
      start = s.pos + 1;
    }
    s.advance();
  }
  const tail = sql.slice(start);
  if (tail.trim().length > 0) parts.push(tail);
  return parts;
}

export function normalizeSql(sql: string, options: { tight?: boolean } = {}): string {
  const tight = options.tight ?? false;
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
    pendingSpace = false;
    const prev = out[out.length - 1];
    if (
      prev === " " ||
      prev === "(" ||
      (tight && prev === ",") ||
      (tight && nextChar === "(") ||
      nextChar === ")" ||
      nextChar === "," ||
      nextChar === ";"
    ) {
      return;
    }
    out += " ";
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

export function normalizeSqlNullable(sql: string | null): string | null {
  if (sql === null) {
    return null;
  }
  return normalizeSql(sql, { tight: true });
}

export function pragmaLiteral(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

const sqlInputSchema = z.string().trim().min(1, "SQL must not be empty");

const FORBIDDEN_KEYWORDS = [
  "INSERT",
  "UPDATE",
  "DELETE",
  "DROP",
  "ALTER",
  "CREATE",
  "REPLACE",
  "TRUNCATE",
  "ATTACH",
  "DETACH",
  "PRAGMA",
  "VACUUM",
  "BEGIN",
  "COMMIT",
  "ROLLBACK",
] as const;

function stripStringLiterals(sql: string): string {
  return sql
    .replace(/'([^']|'')*'/g, "''")
    .replace(/\"([^\"\\]|\\.)*\"/g, '""')
    .replace(/`([^`]|``)*`/g, "``")
    .replace(/--.*$/gm, "")
    .replace(/\/\*[\s\S]*?\*\//g, "");
}

export function validateReadSql(inputSql: string): string {
  const parsed = sqlInputSchema.safeParse(inputSql);
  if (!parsed.success) {
    throw new CodedError("INVALID_SQL", parsed.error.issues.map((issue) => issue.message).join("; "));
  }

  let sql = parsed.data.trim();
  if (sql.endsWith(";")) {
    sql = sql.slice(0, -1).trim();
  }

  const stripped = stripStringLiterals(sql);
  if (stripped.includes(";")) {
    throw new CodedError("INVALID_SQL", "Multiple SQL statements are not allowed");
  }

  const upper = stripped.trim().toUpperCase();
  if (!(upper.startsWith("SELECT") || upper.startsWith("WITH"))) {
    throw new CodedError("INVALID_SQL", "Only SELECT / WITH ... SELECT queries are allowed");
  }

  for (const keyword of FORBIDDEN_KEYWORDS) {
    const regex = new RegExp(`\\b${keyword}\\b`, "i");
    if (regex.test(stripped)) {
      throw new CodedError("INVALID_SQL", `Forbidden keyword in read-only query: ${keyword}`);
    }
  }

  return sql;
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
    throw new CodedError("INVALID_OPERATION", `Expected ${expected} in CREATE TABLE`);
  }
  return kw.end;
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
  throw new CodedError("INVALID_OPERATION", `Malformed ${label} identifier in CREATE TABLE`);
}

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
      throw new CodedError("INVALID_OPERATION", "Malformed bracket identifier in CREATE TABLE");
    }
    return { name: sql.slice(start + 1, end), end: end + 1, quoted: true };
  }

  const first = readCodePoint(sql, start);
  if (!first || !ID_START.test(first.value)) {
    return null;
  }

  let i = first.next;
  while (i < sql.length) {
    const next = readCodePoint(sql, i);
    if (!next || !ID_CONTINUE.test(next.value)) {
      break;
    }
    i = next.next;
  }
  return { name: sql.slice(start, i), end: i, quoted: false };
}

function readIdentifierToken(sql: string, start: number): { name: string; end: number } {
  const parsed = parseIdentifierToken(sql, start);
  if (!parsed) {
    throw new CodedError("INVALID_OPERATION", "Malformed identifier in CREATE TABLE");
  }
  return { name: parsed.name, end: parsed.end };
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
        throw new CodedError("INVALID_OPERATION", "Malformed block comment in CREATE TABLE");
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

export function rewriteCreateTableName(ddlSql: string, newTable: string): string {
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

  throw new CodedError("INVALID_OPERATION", "Malformed CREATE TABLE statement: column list not found");
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

export function rewriteColumnTypeInCreateTable(ddlSql: string, column: string, newType: string): string {
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
    throw new CodedError("INVALID_OPERATION", `Column does not exist in CREATE TABLE SQL: ${column}`);
  }

  return `${ddlSql.slice(0, payloadRange.start)}${rewrittenSegments.join(",")}${ddlSql.slice(payloadRange.end)}`;
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
  return normalizeSql(expression, { tight: true });
}

export function rewriteAddCheckInCreateTable(ddlSql: string, expression: string): string {
  const payloadRange = findCreateTablePayloadRange(ddlSql);
  const payload = ddlSql.slice(payloadRange.start, payloadRange.end);
  const segments = splitTopLevelCommaList(payload);
  const normalizedExpression = normalizeSql(expression, { tight: true });

  for (const segment of segments) {
    const existing = extractTableCheckExpression(segment);
    if (existing && existing === normalizedExpression) {
      throw new CodedError("INVALID_OPERATION", "Equivalent CHECK constraint already exists");
    }
  }

  const rewrittenSegments = [...segments, ` CHECK (${expression.trim()})`];
  return `${ddlSql.slice(0, payloadRange.start)}${rewrittenSegments.join(",")}${ddlSql.slice(payloadRange.end)}`;
}

export function rewriteDropCheckInCreateTable(ddlSql: string, expression: string): string {
  const payloadRange = findCreateTablePayloadRange(ddlSql);
  const payload = ddlSql.slice(payloadRange.start, payloadRange.end);
  const segments = splitTopLevelCommaList(payload);
  const normalizedExpression = normalizeSql(expression, { tight: true });

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
    throw new CodedError("INVALID_OPERATION", "CHECK constraint not found");
  }

  return `${ddlSql.slice(0, payloadRange.start)}${rewrittenSegments.join(",")}${ddlSql.slice(payloadRange.end)}`;
}

export function rewriteDropColumnInCreateTable(ddlSql: string, column: string): string {
  const payloadRange = findCreateTablePayloadRange(ddlSql);
  const payload = ddlSql.slice(payloadRange.start, payloadRange.end);
  const segments = splitTopLevelCommaList(payload);
  const tableConstraintLead = new Set(["CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN"]);
  let removed = false;

  const rewrittenSegments = segments.filter((segment) => {
    const lead = parseIdentifierToken(segment, skipLeadingTrivia(segment, 0));
    if (!lead) {
      return true;
    }
    if (!lead.quoted && tableConstraintLead.has(lead.name.toUpperCase())) {
      return true;
    }
    if (lead.name !== column) {
      return true;
    }
    removed = true;
    return false;
  });

  if (!removed) {
    throw new CodedError("INVALID_OPERATION", `Column does not exist in CREATE TABLE SQL: ${column}`);
  }

  const remainingColumns = rewrittenSegments.filter((segment) => {
    const lead = parseIdentifierToken(segment, skipLeadingTrivia(segment, 0));
    if (!lead) {
      return false;
    }
    return lead.quoted || !tableConstraintLead.has(lead.name.toUpperCase());
  });
  if (remainingColumns.length === 0) {
    throw new CodedError("INVALID_OPERATION", "drop_column cannot remove the last column");
  }

  return `${ddlSql.slice(0, payloadRange.start)}${rewrittenSegments.join(",")}${ddlSql.slice(payloadRange.end)}`;
}

export function parseColumnDefinitionsFromCreateTable(tableSql: string | null): Map<string, string> {
  const defs = new Map<string, string>();
  if (!tableSql) {
    return defs;
  }

  const open = tableSql.indexOf("(");
  if (open < 0) {
    return defs;
  }
  const end = findMatchingParen(tableSql, open);
  if (end < 0) {
    return defs;
  }

  const payload = tableSql.slice(open + 1, end);
  const segments = splitTopLevelCommaList(payload);
  const tableConstraintLead = new Set(["CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN"]);

  for (const segment of segments) {
    const lead = parseIdentifierToken(segment, skipLeadingTrivia(segment, 0));
    if (!lead) {
      continue;
    }
    if (!lead.quoted && tableConstraintLead.has(lead.name.toUpperCase())) {
      continue;
    }
    defs.set(lead.name.toLowerCase(), normalizeSql(segment) ?? segment.trim());
  }

  return defs;
}

export function extractCheckConstraints(tableSql: string | null): string[] {
  if (!tableSql) {
    return [];
  }
  const checks: string[] = [];
  const scanner = createScanner(tableSql);

  while (scanner.pos < tableSql.length) {
    if (scanner.skipInterior()) {
      continue;
    }

    if (
      scanner.pos + 5 <= tableSql.length &&
      tableSql.slice(scanner.pos, scanner.pos + 5).toUpperCase() === "CHECK" &&
      isWordBoundary(tableSql[scanner.pos - 1]) &&
      isWordBoundary(tableSql[scanner.pos + 5])
    ) {
      let j = scanner.pos + 5;
      while (j < tableSql.length && /\s/.test(tableSql[j]!)) {
        j += 1;
      }
      if (tableSql[j] !== "(") {
        scanner.advance();
        continue;
      }
      const closeIndex = findMatchingParen(tableSql, j);
      if (closeIndex < 0) {
        scanner.advance();
        continue;
      }
      const expr = normalizeSql(tableSql.slice(j + 1, closeIndex));
      if (expr) {
        checks.push(expr);
      }
      scanner.pos = closeIndex + 1;
      continue;
    }
    scanner.advance();
  }

  return checks.sort((a, b) => a.localeCompare(b));
}
