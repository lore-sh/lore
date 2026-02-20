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

export function splitTopLevelCommaList(sql: string): string[] {
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
      parts.push(sql.slice(start, i));
      start = i + 1;
      i += 1;
      continue;
    }

    i += 1;
  }
  const tail = sql.slice(start);
  if (tail.trim().length > 0) {
    parts.push(tail);
  }
  return parts;
}
