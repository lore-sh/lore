import { TossError } from "./errors";

export const IDENTIFIER_PATTERN = /^(?:[_$]|\p{ID_Start})(?:[_$]|\p{ID_Continue})*$/u;
export const COLUMN_TYPE_PATTERN = /^[A-Za-z][A-Za-z0-9_]*(\s*\(\s*\d+\s*(,\s*\d+\s*)?\))?$/;

export function quoteIdentifier(value: string): string {
  if (!IDENTIFIER_PATTERN.test(value)) {
    throw new TossError("INVALID_IDENTIFIER", `Invalid identifier: ${value}`);
  }
  return `"${value.replaceAll('"', '""')}"`;
}

export function quoteName(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
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
