import { CodedError } from "../error";
import { createScanner, findMatchingParen, isWordBoundary, normalizeSql, quoteIdentifier, splitTopLevelCommaList } from "./sql";

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
      throw new CodedError("INVALID_OPERATION", "Malformed bracket identifier in CREATE TABLE");
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
