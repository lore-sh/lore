import type { EncodedCell, EncodedRow, Operation, StudioCommitDetail, StudioHistoryEntry } from "@toss/core";

export interface RenderedLine {
  kind: "add" | "remove" | "neutral";
  text: string;
}

function decodeHex(hex: string): string {
  const bytes: number[] = [];
  for (let i = 0; i < hex.length; i += 2) {
    const byte = Number.parseInt(hex.slice(i, i + 2), 16);
    if (Number.isNaN(byte)) {
      return hex;
    }
    bytes.push(byte);
  }
  return new TextDecoder().decode(new Uint8Array(bytes));
}

function formatEncodedCell(cell: EncodedCell): string {
  if (cell.storageClass === "null") {
    return "NULL";
  }
  if (cell.storageClass === "integer" || cell.storageClass === "real") {
    return cell.sqlLiteral;
  }
  if (cell.storageClass === "blob") {
    const blob = /^X'([0-9a-fA-F]*)'$/.exec(cell.sqlLiteral);
    if (!blob) {
      return `BLOB(${cell.sqlLiteral})`;
    }
    const hex = blob[1] ?? "";
    return `BLOB(${hex.length / 2} bytes)`;
  }
  const text = /^CAST\(X'([0-9a-fA-F]*)' AS TEXT\)$/.exec(cell.sqlLiteral);
  if (!text) {
    return JSON.stringify(cell.sqlLiteral);
  }
  return JSON.stringify(decodeHex(text[1] ?? ""));
}

function sortedRowEntries(row: EncodedRow): Array<[string, EncodedCell]> {
  return Object.entries(row).sort(([left], [right]) => left.localeCompare(right));
}

function rowValue(row: EncodedRow | null, key: string): string | null {
  if (!row) {
    return null;
  }
  const cell = row[key];
  if (!cell) {
    return null;
  }
  return formatEncodedCell(cell);
}

function formatPrimitive(value: unknown): string {
  if (value === null) {
    return "NULL";
  }
  if (typeof value === "string") {
    return JSON.stringify(value);
  }
  return String(value);
}

function formatObjectFields(values: Record<string, unknown>): string {
  const entries = Object.entries(values).sort(([left], [right]) => left.localeCompare(right));
  if (entries.length === 0) {
    return "{}";
  }
  return entries.map(([key, value]) => `${key}: ${formatPrimitive(value)}`).join(", ");
}

export function renderOperationLine(operation: Operation): string {
  switch (operation.type) {
    case "create_table":
      return `CREATE ${operation.table} - ${operation.columns.length} columns (${operation.columns.map((column) => column.name).join(", ")})`;
    case "add_column":
      return `ADD COLUMN ${operation.column.name} ${operation.column.type} to ${operation.table}`;
    case "insert":
      return `INSERT into ${operation.table} - ${formatObjectFields(operation.values)}`;
    case "update":
      return `UPDATE ${operation.table} WHERE ${formatObjectFields(operation.where)} - ${formatObjectFields(operation.values)}`;
    case "delete":
      return `DELETE from ${operation.table} WHERE ${formatObjectFields(operation.where)}`;
    case "drop_table":
      return `DROP ${operation.table}`;
    case "drop_column":
      return `DROP COLUMN ${operation.column} from ${operation.table}`;
    case "alter_column_type":
      return `ALTER ${operation.table}.${operation.column} - ${operation.newType}`;
    case "add_check":
      return `ADD CHECK on ${operation.table} - ${operation.expression}`;
    case "drop_check":
      return `DROP CHECK on ${operation.table} - ${operation.expression}`;
    case "restore_table":
      return `RESTORE ${operation.table}`;
  }
}

export function renderRowEffectLines(effect: StudioCommitDetail["rowEffects"][number]): RenderedLine[] {
  if (effect.opKind === "insert" && effect.afterRow) {
    return sortedRowEntries(effect.afterRow).map(([column, cell]) => ({
      kind: "add",
      text: `+ ${column}: ${formatEncodedCell(cell)}`,
    }));
  }

  if (effect.opKind === "delete" && effect.beforeRow) {
    return sortedRowEntries(effect.beforeRow).map(([column, cell]) => ({
      kind: "remove",
      text: `- ${column}: ${formatEncodedCell(cell)}`,
    }));
  }

  const before = effect.beforeRow;
  const after = effect.afterRow;
  if (!before && !after) {
    return [];
  }
  const keys = new Set<string>([...Object.keys(before ?? {}), ...Object.keys(after ?? {})]);
  const sortedKeys = Array.from(keys).sort((left, right) => left.localeCompare(right));
  const lines: RenderedLine[] = [];
  for (const key of sortedKeys) {
    const beforeValue = rowValue(before, key);
    const afterValue = rowValue(after, key);
    if (beforeValue === afterValue) {
      continue;
    }
    if (beforeValue !== null) {
      lines.push({ kind: "remove", text: `- ${key}: ${beforeValue}` });
    }
    if (afterValue !== null) {
      lines.push({ kind: "add", text: `+ ${key}: ${afterValue}` });
    }
  }
  return lines;
}

export function renderSchemaEffectLine(effect: StudioCommitDetail["schemaEffects"][number]): RenderedLine {
  if (!effect.beforeTable && effect.afterTable) {
    return {
      kind: "add",
      text: `+ Table created: ${effect.tableName}`,
    };
  }
  if (effect.beforeTable && !effect.afterTable) {
    return {
      kind: "remove",
      text: `- Table dropped: ${effect.tableName}`,
    };
  }
  return {
    kind: "neutral",
    text: `~ Table changed: ${effect.tableName}`,
  };
}

export function renderPkLabel(pk: Record<string, string>): string {
  const entries = Object.entries(pk).sort(([left], [right]) => left.localeCompare(right));
  if (entries.length === 0) {
    return "";
  }
  return entries.map(([key, value]) => `${key}=${value}`).join(", ");
}

export function summarizeHistoryEntry(entry: StudioHistoryEntry): string {
  const parts: string[] = [];
  if (entry.rowEffectCount > 0) {
    parts.push(`${entry.rowEffectCount} row`);
  }
  if (entry.schemaEffectCount > 0) {
    parts.push(`${entry.schemaEffectCount} schema`);
  }
  if (parts.length === 0) {
    parts.push(`${entry.operationCount} op`);
  }
  return parts.join(" · ");
}
